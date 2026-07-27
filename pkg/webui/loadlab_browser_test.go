package webui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/chromedp/chromedp"
)

type loadLabBrowserAPI struct {
	mu                sync.Mutex
	tenants           map[string]map[string]any
	submitted         int
	idempotencyKey    map[string]bool
	workerCapacity    int
	capacityWrites    int
	allCapacityWrites int
	clearTenantWrites int
	allocationReads   int
}

func newLoadLabBrowserAPI() *loadLabBrowserAPI {
	return &loadLabBrowserAPI{
		tenants:        make(map[string]map[string]any),
		idempotencyKey: make(map[string]bool),
		workerCapacity: 100,
	}
}

func (a *loadLabBrowserAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.URL.Path == "/api/v1/health":
		writeBrowserJSON(w, map[string]any{
			"status": "ok", "node_id": "control-0", "leader": "127.0.0.1:7000",
		})
	case r.URL.Path == "/api/v1/admin/nodes":
		a.mu.Lock()
		workerCapacity := a.workerCapacity
		a.mu.Unlock()
		writeBrowserJSON(w, map[string]any{"nodes": []map[string]any{
			{
				"node_id": "worker-0", "role": "worker", "status": "up",
				"total_workers": workerCapacity,
				"capacity_override": func() int {
					if workerCapacity == 100 {
						return 0
					}
					return workerCapacity
				}(),
			},
			{
				"node_id": "worker-retained", "role": "worker", "status": "down",
				"total_workers": 100,
			},
			{
				"node_id": "control-0", "role": "control", "status": "up",
				"total_workers": 0,
			},
		}})
	case r.URL.Path == "/api/v1/admin/allocations":
		a.mu.Lock()
		a.allocationReads++
		a.mu.Unlock()
		writeBrowserJSON(w, map[string]any{"nodes": []map[string]any{
			{"node_id": "worker-0", "tenants": map[string]int{}},
			{"node_id": "worker-retained", "tenants": map[string]int{"ghost": 100}},
		}})
	case r.URL.Path == "/api/v1/metrics":
		writeBrowserJSON(w, []any{})
	case r.URL.Path == "/api/v1/admin/autoscaling":
		writeBrowserJSON(w, map[string]any{
			"observed_at": time.Now().UTC(), "unfinished_tasks": 12,
			"pending_tasks": 7, "running_tasks": 5,
			"oldest_pending_age_ms": 2500, "task_breakdown_valid": true,
			"worker_capacity": 100, "worker_instances": 1,
			"execution_signals_valid": true, "reporting_workers": 1,
			"executing_tasks": 5, "average_worker_cpu_millis": 420,
			"max_worker_cpu_millis": 720, "rate_counters_valid": true,
			"telemetry_source":      "control-0",
			"submitted_tasks_total": 120, "completed_tasks_total": 108,
		})
	case r.URL.Path == "/api/v1/admin/performance":
		writeBrowserJSON(w, map[string]any{
			"node_id": "control-0", "collected_at": time.Now().UTC(),
			"current": map[string]any{
				"raft": map[string]any{},
				"scheduler": map[string]any{
					"load_aware_requests":     12,
					"load_throttled_requests": 3,
					"max_worker_cpu_millis":   720,
					"worker_loads": map[string]any{
						"worker-0": map[string]any{
							"cpu_utilization_millis": 720,
							"running_tasks":          5,
							"capacity":               100,
							"observed_at":            time.Now().UTC(),
						},
					},
				},
			},
			"history": map[string]any{},
		})
	case r.Method == http.MethodPut &&
		r.URL.Path == "/api/v1/admin/nodes/worker-0/capacity":
		var request struct {
			TotalWorkers int `json:"total_workers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
			request.TotalWorkers < 1 || request.TotalWorkers > 1000 {
			http.Error(w, `{"error":"bad worker capacity"}`, http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.workerCapacity = request.TotalWorkers
		a.capacityWrites++
		a.mu.Unlock()
		writeBrowserJSON(w, map[string]any{
			"node_id": "worker-0", "total_workers": request.TotalWorkers,
			"capacity_override": request.TotalWorkers,
		})
	case r.Method == http.MethodPut &&
		r.URL.Path == "/api/v1/admin/nodes/capacity":
		var request struct {
			TotalWorkers int `json:"total_workers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil ||
			request.TotalWorkers < 1 || request.TotalWorkers > 1000 {
			http.Error(w, `{"error":"bad all-worker capacity"}`, http.StatusBadRequest)
			return
		}
		a.mu.Lock()
		a.workerCapacity = request.TotalWorkers
		a.allCapacityWrites++
		a.mu.Unlock()
		writeBrowserJSON(w, map[string]any{
			"total_workers": request.TotalWorkers,
			"updated":       1,
			"nodes": []map[string]any{{
				"node_id": "worker-0", "total_workers": request.TotalWorkers,
				"capacity_override": request.TotalWorkers,
			}},
		})
	case r.Method == http.MethodPut &&
		strings.HasPrefix(r.URL.Path, "/api/v1/admin/tenants/"):
		var request struct {
			Name       string `json:"name"`
			MaxWorkers int    `json:"max_workers"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, `{"error":"bad tenant"}`, http.StatusBadRequest)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/api/v1/admin/tenants/")
		a.mu.Lock()
		a.tenants[id] = map[string]any{
			"id": id, "name": request.Name, "max_workers": request.MaxWorkers,
			"inflight": 0,
		}
		tenant := a.tenants[id]
		a.mu.Unlock()
		writeBrowserJSON(w, tenant)
	case r.Method == http.MethodDelete &&
		r.URL.Path == "/api/v1/admin/tenants":
		a.mu.Lock()
		deleted := len(a.tenants)
		a.tenants = make(map[string]map[string]any)
		a.clearTenantWrites++
		a.mu.Unlock()
		writeBrowserJSON(w, map[string]any{"deleted": deleted})
	case r.URL.Path == "/api/v1/admin/tenants":
		a.mu.Lock()
		snapshot := make(map[string]map[string]any, len(a.tenants))
		for id, tenant := range a.tenants {
			snapshot[id] = tenant
		}
		a.mu.Unlock()
		writeBrowserJSON(w, snapshot)
	case r.Method == http.MethodPost && r.URL.Path == "/api/v1/tasks/batch":
		var request struct {
			Tasks []struct {
				TenantID       string `json:"tenant_id"`
				IdempotencyKey string `json:"idempotency_key"`
			} `json:"tasks"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, `{"error":"bad batch"}`, http.StatusBadRequest)
			return
		}
		tasks := make([]map[string]any, 0, len(request.Tasks))
		a.mu.Lock()
		for index, task := range request.Tasks {
			if task.IdempotencyKey != "" && a.idempotencyKey[task.IdempotencyKey] {
				a.mu.Unlock()
				http.Error(w, `{"error":"duplicate idempotency key"}`, http.StatusConflict)
				return
			}
			if task.IdempotencyKey != "" {
				a.idempotencyKey[task.IdempotencyKey] = true
			}
			a.submitted++
			tasks = append(tasks, map[string]any{
				"task_id":   fmt.Sprintf("task-%d-%d", a.submitted, index),
				"tenant_id": task.TenantID, "status": "pending",
			})
		}
		a.mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		writeBrowserJSON(w, map[string]any{"tasks": tasks})
	default:
		http.NotFound(w, r)
	}
}

func writeBrowserJSON(w http.ResponseWriter, value any) {
	_ = json.NewEncoder(w).Encode(value)
}

type workerLoadBrowserAPI struct{}

func (workerLoadBrowserAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/v1/health":
		writeBrowserJSON(w, map[string]any{
			"status": "ok", "node_id": "control-0", "leader": "127.0.0.1:7000",
		})
	case "/api/v1/admin/nodes":
		nodes := []map[string]any{{
			"node_id": "worker-1", "role": "worker", "status": "down",
			"total_workers": 100,
		}}
		for index := 13; index >= 2; index-- {
			capacity := 50
			if index == 2 {
				capacity = 100
			} else if index == 10 {
				capacity = 20
			}
			nodes = append(nodes, map[string]any{
				"node_id": fmt.Sprintf("worker-%d", index),
				"role":    "worker", "status": "up",
				"total_workers": capacity,
			})
		}
		nodes = append(nodes, map[string]any{
			"node_id": "control-0", "role": "control", "status": "up",
			"total_workers": 0,
		})
		writeBrowserJSON(w, map[string]any{"nodes": nodes})
	case "/api/v1/admin/allocations":
		writeBrowserJSON(w, map[string]any{"nodes": []any{}})
	case "/api/v1/admin/tenants":
		writeBrowserJSON(w, map[string]any{})
	case "/api/v1/metrics":
		writeBrowserJSON(w, []any{})
	case "/api/v1/admin/performance":
		loads := make(map[string]any)
		for index := 2; index <= 12; index++ {
			cpu, running, capacity := index*10, 1, 50
			if index == 2 {
				cpu, running, capacity = 720, 5, 100
			} else if index == 10 {
				cpu, running, capacity = 120, 1, 20
			}
			loads[fmt.Sprintf("worker-%d", index)] = map[string]any{
				"cpu_utilization_millis": cpu,
				"running_tasks":          running,
				"capacity":               capacity,
				"observed_at":            time.Now().UTC(),
			}
		}
		loads["worker-1"] = map[string]any{
			"cpu_utilization_millis": 990,
			"running_tasks":          100,
			"capacity":               100,
			"observed_at":            time.Now().UTC(),
		}
		writeBrowserJSON(w, map[string]any{
			"node_id": "control-0", "collected_at": time.Now().UTC(),
			"current": map[string]any{
				"raft": map[string]any{},
				"scheduler": map[string]any{
					"max_worker_cpu_millis": 720,
					"worker_telemetry":      loads,
				},
			},
			"history": map[string]any{},
		})
	default:
		http.NotFound(w, r)
	}
}

func TestWorkerPodLoadBrowserBuildsBoundedSessionChart(t *testing.T) {
	chromePath := findChrome()
	if chromePath == "" {
		t.Skip("Chrome/Chromium is not installed")
	}
	server := httptest.NewServer(Handler(workerLoadBrowserAPI{}))
	defer server.Close()

	allocator, cancelAllocator := chromedp.NewExecAllocator(
		context.Background(),
		append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.ExecPath(chromePath),
			chromedp.Flag("headless", true),
			chromedp.Flag("disable-gpu", true),
			chromedp.Flag("no-sandbox", true),
			chromedp.WindowSize(1440, 1000),
		)...,
	)
	defer cancelAllocator()
	browserContext, cancelBrowser := chromedp.NewContext(allocator)
	defer cancelBrowser()
	ctx, cancel := context.WithTimeout(browserContext, 15*time.Second)
	defer cancel()

	var state struct {
		Summary          string `json:"summary"`
		Legend           string `json:"legend"`
		LegendItems      int    `json:"legendItems"`
		Samples          int    `json:"samples"`
		DownVisible      bool   `json:"downVisible"`
		PanelInMonitor   bool   `json:"panelInMonitor"`
		HasCanvas        bool   `json:"hasCanvas"`
		HasTable         bool   `json:"hasTable"`
		AverageCPU       string `json:"averageCPU"`
		MaximumCPU       string `json:"maximumCPU"`
		RunningTasks     string `json:"runningTasks"`
		JSONLabel        string `json:"jsonLabel"`
		JSONTarget       string `json:"jsonTarget"`
		JSONRelationship string `json:"jsonRelationship"`
	}
	if err := chromedp.Run(ctx,
		chromedp.Navigate(server.URL),
		chromedp.WaitVisible("#worker-cpu-session-chart", chromedp.ByQuery),
		chromedp.Sleep(1200*time.Millisecond),
		chromedp.Evaluate(`(() => {
			const panel = document.querySelector("#worker-session-panel");
			const link = panel.querySelector(".json-link");
			return {
				summary: panel.querySelector("#worker-load-summary").textContent.trim(),
				legend: panel.querySelector("#worker-cpu-session-legend").textContent,
				legendItems: panel.querySelectorAll("#worker-cpu-session-legend .legend-item").length,
				samples: S.sessionHistory.workers["worker-2"].cpu.length,
				downVisible: S.workerCPUSessionHistories.some(item => item.id === "worker-1"),
				panelInMonitor: document.querySelector("#monitoring-main").contains(panel),
				hasCanvas: Boolean(panel.querySelector("#worker-cpu-session-chart")),
				hasTable: Boolean(panel.querySelector("table")),
				averageCPU: panel.querySelector("#session-worker-average").textContent.trim(),
				maximumCPU: panel.querySelector("#session-worker-max").textContent.trim(),
				runningTasks: panel.querySelector("#session-worker-running").textContent.trim(),
				jsonLabel: link.textContent.trim(),
				jsonTarget: link.target,
				jsonRelationship: link.rel,
			};
		})()`, &state),
	); err != nil {
		t.Fatal(err)
	}
	if state.Summary != "11 / 12 reporting" ||
		state.LegendItems != 9 ||
		!strings.Contains(state.Legend, "worker-2") ||
		!strings.Contains(state.Legend, "Other 3 Pods (avg)") ||
		state.Samples < 2 || state.DownVisible || !state.PanelInMonitor ||
		!state.HasCanvas || state.HasTable || state.AverageCPU == "—" ||
		state.MaximumCPU != "72.0%" || state.RunningTasks != "15" ||
		state.JSONLabel != "JSON ↗" ||
		state.JSONTarget != "_blank" ||
		!strings.Contains(state.JSONRelationship, "noopener") {
		t.Fatalf("Worker Pod session chart state = %+v", state)
	}
}

type dashboardTrendBrowserAPI struct {
	mu   sync.Mutex
	tick int
}

func (a *dashboardTrendBrowserAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/api/v1/health":
		writeBrowserJSON(w, map[string]any{
			"status": "ok", "node_id": "control-0", "leader": "127.0.0.1:7000",
		})
	case "/api/v1/admin/nodes":
		nodes := []map[string]any{{
			"node_id": "control-0", "role": "control", "status": "up",
			"total_workers": 0,
		}}
		for index := 0; index < 12; index++ {
			nodes = append(nodes, map[string]any{
				"node_id": fmt.Sprintf("worker-%02d", index),
				"role":    "worker", "status": "up", "total_workers": 100,
			})
		}
		writeBrowserJSON(w, map[string]any{"nodes": nodes})
	case "/api/v1/admin/tenants":
		tenants := make(map[string]any)
		for index := 0; index < 12; index++ {
			id := fmt.Sprintf("tenant-%02d", index)
			tenants[id] = map[string]any{
				"id": id, "name": fmt.Sprintf("Tenant %02d", index),
				"max_workers": 100 - index, "inflight": 12 - index,
			}
		}
		writeBrowserJSON(w, tenants)
	case "/api/v1/metrics":
		prefix := r.URL.Query().Get("prefix")
		metrics := make([]map[string]any, 0, 12)
		for index := 0; index < 12; index++ {
			value := 12 - index
			name := ""
			switch prefix {
			case "unfinished:":
				name = "unfinished:" + fmt.Sprintf("tenant-%02d", index)
			case "allocated-workers:node:":
				name = "allocated-workers:node:" + fmt.Sprintf("worker-%02d", index)
			case "allocated-workers:tenant:":
				name = "allocated-workers:tenant:" + fmt.Sprintf("tenant-%02d", index)
			}
			if name == "" {
				continue
			}
			metrics = append(metrics, map[string]any{
				"name": name, "days": []int{value}, "hours": []int{value},
				"mins": []int{value}, "secs": []int{value, value + 1},
			})
		}
		writeBrowserJSON(w, metrics)
	case "/api/v1/admin/autoscaling":
		writeBrowserJSON(w, map[string]any{
			"observed_at": time.Now().UTC(), "pending_tasks": 50,
			"running_tasks": 28, "oldest_pending_age_ms": 1700,
			"task_breakdown_valid": true, "average_worker_cpu_millis": 410,
			"max_worker_cpu_millis": 690, "reporting_workers": 12,
			"worker_instances": 12, "telemetry_source": "control-0",
		})
	case "/api/v1/admin/performance":
		a.mu.Lock()
		a.tick++
		tick := a.tick
		a.mu.Unlock()
		loads := make(map[string]any)
		for index := 0; index < 12; index++ {
			loads[fmt.Sprintf("worker-%02d", index)] = map[string]any{
				"cpu_utilization_millis": 200 + index*20 + tick,
				"running_tasks":          index + tick,
				"capacity":               100,
				"observed_at":            time.Now().UTC(),
			}
		}
		writeBrowserJSON(w, map[string]any{
			"node_id": "control-0", "collected_at": time.Now().UTC(),
			"current": map[string]any{
				"raft": map[string]any{
					"create_task_batch": map[string]any{
						"applies": 10, "average_us": 1200, "max_us": 2000,
						"average_batch": 50,
					},
				},
				"scheduler": map[string]any{
					"pending_scanned": 100, "tasks_selected": 50,
					"submission_batches": 2, "submission_requests": 5,
					"submission_tasks": 1000, "average_submission_batch": 500,
					"average_submission_requests": 2,
					"average_submission_queue_us": 2300,
					"submission_queue_depth":      1,
					"assignment_queue_depth":      3, "completion_queue_depth": 2,
					"load_aware_requests": 10, "load_throttled_requests": 1,
					"max_worker_cpu_millis": 690, "worker_telemetry": loads,
					"worker_loads": loads,
				},
			},
			"history": map[string]any{},
		})
	default:
		http.NotFound(w, r)
	}
}

func TestDashboardBrowserGroupsChartsBoundsLabelsAndSamplesSessionTrends(t *testing.T) {
	chromePath := findChrome()
	if chromePath == "" {
		t.Skip("Chrome/Chromium is not installed")
	}
	api := &dashboardTrendBrowserAPI{}
	server := httptest.NewServer(Handler(api))
	defer server.Close()

	allocator, cancelAllocator := chromedp.NewExecAllocator(
		context.Background(),
		append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.ExecPath(chromePath),
			chromedp.Flag("headless", true),
			chromedp.Flag("disable-gpu", true),
			chromedp.Flag("no-sandbox", true),
			chromedp.WindowSize(1440, 1000),
		)...,
	)
	defer cancelAllocator()
	browserContext, cancelBrowser := chromedp.NewContext(allocator)
	defer cancelBrowser()
	ctx, cancel := context.WithTimeout(browserContext, 15*time.Second)
	defer cancel()

	var state struct {
		WorkerLegendItems   int    `json:"workerLegendItems"`
		TenantLegendItems   int    `json:"tenantLegendItems"`
		SessionWorkerItems  int    `json:"sessionWorkerItems"`
		SessionTenantItems  int    `json:"sessionTenantItems"`
		WorkerLegend        string `json:"workerLegend"`
		TenantLegend        string `json:"tenantLegend"`
		SessionWorkerLegend string `json:"sessionWorkerLegend"`
		SessionTenantLegend string `json:"sessionTenantLegend"`
		WorkerSamples       int    `json:"workerSamples"`
		TenantSamples       int    `json:"tenantSamples"`
		SessionAfterCharts  bool   `json:"sessionAfterCharts"`
		RaftAffinity        bool   `json:"raftAffinity"`
		SchedulerAffinity   bool   `json:"schedulerAffinity"`
		StandalonePressure  bool   `json:"standalonePressure"`
		HasTable            bool   `json:"hasTable"`
		SessionBadge        string `json:"sessionBadge"`
		SubmissionQueues    string `json:"submissionQueues"`
		SubmissionQueueNote string `json:"submissionQueueNote"`
	}
	if err := chromedp.Run(ctx,
		chromedp.Navigate(server.URL),
		chromedp.WaitVisible("#tenant-allocation-session-chart", chromedp.ByQuery),
		chromedp.Sleep(2200*time.Millisecond),
		chromedp.Evaluate(`(() => {
			const charts = document.querySelector("#primary-charts");
			const session = document.querySelector("#session-charts");
			const performanceCards = document.querySelectorAll(".performance-chart");
			return {
				workerLegendItems: document.querySelectorAll("#worker-chart-legend .legend-item").length,
				tenantLegendItems: document.querySelectorAll("#tenant-chart-legend .legend-item").length,
				sessionWorkerItems: document.querySelectorAll("#worker-cpu-session-legend .legend-item").length,
				sessionTenantItems: document.querySelectorAll("#tenant-allocation-session-legend .legend-item").length,
				workerLegend: document.querySelector("#worker-chart-legend").textContent,
				tenantLegend: document.querySelector("#tenant-chart-legend").textContent,
				sessionWorkerLegend: document.querySelector("#worker-cpu-session-legend").textContent,
				sessionTenantLegend: document.querySelector("#tenant-allocation-session-legend").textContent,
				workerSamples: S.sessionHistory.workers["worker-00"].cpu.length,
				tenantSamples: S.sessionHistory.tenants["tenant-00"].allocated.length,
				sessionAfterCharts: Boolean(
					charts.compareDocumentPosition(session) & Node.DOCUMENT_POSITION_FOLLOWING
				),
				raftAffinity: performanceCards[0].contains(document.querySelector("#performance-create-apply")) &&
					performanceCards[0].contains(document.querySelector("#performance-raft-chart")),
				schedulerAffinity: performanceCards[1].contains(document.querySelector("#performance-scan-ratio")) &&
					performanceCards[1].contains(document.querySelector("#performance-scheduler-chart")),
				standalonePressure: Boolean(document.querySelector(".autoscaling-panel")),
				hasTable: Boolean(document.querySelector("#monitoring-main table")),
				sessionBadge: document.querySelector(".session-badge").textContent.trim(),
				submissionQueues: document.querySelector("#performance-queues").textContent.trim(),
				submissionQueueNote: document.querySelector("#performance-queues-note").textContent.trim(),
			};
		})()`, &state),
	); err != nil {
		t.Fatal(err)
	}
	if state.WorkerLegendItems != 9 || state.TenantLegendItems != 9 ||
		state.SessionWorkerItems != 9 || state.SessionTenantItems != 9 ||
		!strings.Contains(state.WorkerLegend, "Other 4 Pods (avg)") ||
		!strings.Contains(state.TenantLegend, "Other 4 tenants (avg)") ||
		!strings.Contains(state.SessionWorkerLegend, "Other 4 Pods (avg)") ||
		!strings.Contains(state.SessionTenantLegend, "Other 4 tenants (avg)") ||
		state.WorkerSamples < 2 || state.TenantSamples < 2 ||
		!state.SessionAfterCharts || !state.RaftAffinity || !state.SchedulerAffinity ||
		state.StandalonePressure || state.HasTable ||
		state.SessionBadge != "Up to 60 samples · reset on refresh" ||
		state.SubmissionQueues != "1 · 3 · 2" ||
		!strings.Contains(state.SubmissionQueueNote, "submit avg 500 tasks / 2 requests") {
		t.Fatalf("dashboard trend browser state = %+v", state)
	}
}

func TestLoadLabBrowserCreatesTenantsSubmitsAndShowsCompletedJSON(t *testing.T) {
	chromePath := findChrome()
	if chromePath == "" {
		t.Skip("Chrome/Chromium is not installed")
	}
	api := newLoadLabBrowserAPI()
	server := httptest.NewServer(Handler(api))
	defer server.Close()

	allocator, cancelAllocator := chromedp.NewExecAllocator(
		context.Background(),
		append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.ExecPath(chromePath),
			chromedp.Flag("headless", true),
			chromedp.Flag("disable-gpu", true),
			chromedp.Flag("no-sandbox", true),
			chromedp.WindowSize(1440, 1000),
		)...,
	)
	defer cancelAllocator()
	browserContext, cancelBrowser := chromedp.NewContext(allocator)
	defer cancelBrowser()
	ctx, cancel := context.WithTimeout(browserContext, 25*time.Second)
	defer cancel()

	var layout struct {
		ConfigInSidebar    bool    `json:"configInSidebar"`
		LoadInSidebar      bool    `json:"loadInSidebar"`
		ChartsInMonitoring bool    `json:"chartsInMonitoring"`
		LoadInMonitoring   bool    `json:"loadInMonitoring"`
		WriteInTable       bool    `json:"writeInTable"`
		ConfigLeft         float64 `json:"configLeft"`
		ConfigRight        float64 `json:"configRight"`
		MonitoringLeft     float64 `json:"monitoringLeft"`
		MonitoringRight    float64 `json:"monitoringRight"`
		WorkloadLeft       float64 `json:"workloadLeft"`
		WorkloadRight      float64 `json:"workloadRight"`
		ConfigBackground   string  `json:"configBackground"`
		ConfigShadow       string  `json:"configShadow"`
		ConfigRadius       string  `json:"configRadius"`
		GroupBackground    string  `json:"groupBackground"`
		ToggleWidth        float64 `json:"toggleWidth"`
	}
	if err := chromedp.Run(ctx,
		chromedp.Navigate(server.URL),
		chromedp.WaitVisible("#load-lab", chromedp.ByQuery),
		chromedp.Evaluate(`(() => {
			const config = document.querySelector("#config-sidebar");
			const workload = document.querySelector("#workload-sidebar");
			const monitoring = document.querySelector("#monitoring-main");
			const configRect = config.getBoundingClientRect();
			const workloadRect = workload.getBoundingClientRect();
			const monitoringRect = monitoring.getBoundingClientRect();
			const configPanelStyle = getComputedStyle(config.querySelector(".sidebar-panel"));
			const quickStyle = getComputedStyle(document.querySelector("#quick-load"));
			const toggleRect = document.querySelector("#config-sidebar-toggle").getBoundingClientRect();
			return {
				configInSidebar: config.contains(document.querySelector("#config-tenant")) &&
					config.contains(document.querySelector("#edit-tenant")) &&
					config.contains(document.querySelector("#worker-capacity-apply")),
				loadInSidebar: workload.contains(document.querySelector("#load-lab")) &&
					workload.contains(document.querySelector("#add-one")) &&
					workload.contains(document.querySelector("#add-all")),
				chartsInMonitoring: monitoring.contains(document.querySelector("#workers-chart")) &&
					monitoring.contains(document.querySelector("#tenant-unfinished-chart")),
				loadInMonitoring: monitoring.contains(document.querySelector("#load-lab")),
				writeInTable: Boolean(document.querySelector("#tenant-list [data-action=load]")),
				configLeft: configRect.left,
				configRight: configRect.right,
				monitoringLeft: monitoringRect.left,
				monitoringRight: monitoringRect.right,
				workloadLeft: workloadRect.left,
				workloadRight: workloadRect.right,
				configBackground: configPanelStyle.backgroundColor,
				configShadow: configPanelStyle.boxShadow,
				configRadius: configPanelStyle.borderRadius,
				groupBackground: quickStyle.backgroundColor,
				toggleWidth: toggleRect.width,
			};
		})()`, &layout),
	); err != nil {
		t.Fatal(err)
	}
	if !layout.ConfigInSidebar || !layout.LoadInSidebar || !layout.ChartsInMonitoring ||
		layout.LoadInMonitoring || layout.WriteInTable ||
		layout.ConfigLeft >= layout.MonitoringLeft ||
		layout.ConfigRight > layout.MonitoringLeft ||
		layout.MonitoringRight > layout.WorkloadLeft ||
		layout.WorkloadLeft >= layout.WorkloadRight ||
		layout.ConfigBackground != "rgb(248, 250, 252)" ||
		layout.ConfigShadow != "none" ||
		layout.ConfigRadius != "12px" ||
		layout.GroupBackground != "rgb(241, 245, 249)" ||
		layout.ToggleWidth != 28 {
		t.Fatalf("dashboard configuration/monitor/workload layout = %+v", layout)
	}

	var collapsed struct {
		ConfigCollapsed     bool    `json:"configCollapsed"`
		WorkloadCollapsed   bool    `json:"workloadCollapsed"`
		ConfigExpanded      string  `json:"configExpanded"`
		WorkloadExpanded    string  `json:"workloadExpanded"`
		ConfigWidth         float64 `json:"configWidth"`
		WorkloadWidth       float64 `json:"workloadWidth"`
		ConfigDisplay       string  `json:"configDisplay"`
		WorkloadDisplay     string  `json:"workloadDisplay"`
		ConfigReopenDisplay string  `json:"configReopenDisplay"`
		LoadReopenDisplay   string  `json:"loadReopenDisplay"`
		MonitoringWidth     float64 `json:"monitoringWidth"`
		StateConfig         bool    `json:"stateConfig"`
		StateWorkload       bool    `json:"stateWorkload"`
	}
	readCollapsedState := chromedp.Evaluate(`(() => {
		const state = JSON.parse(localStorage.getItem("sluice.dashboard.sidebars.v1") || "{}");
		const config = document.querySelector("#config-sidebar");
		const workload = document.querySelector("#workload-sidebar");
		return {
			configCollapsed: config.classList.contains("is-collapsed"),
			workloadCollapsed: workload.classList.contains("is-collapsed"),
			configExpanded: document.querySelector("#config-sidebar-toggle").getAttribute("aria-expanded"),
			workloadExpanded: document.querySelector("#workload-sidebar-toggle").getAttribute("aria-expanded"),
			configWidth: config.getBoundingClientRect().width,
			workloadWidth: workload.getBoundingClientRect().width,
			configDisplay: getComputedStyle(config).display,
			workloadDisplay: getComputedStyle(workload).display,
			configReopenDisplay: getComputedStyle(document.querySelector("#config-sidebar-reopen")).display,
			loadReopenDisplay: getComputedStyle(document.querySelector("#workload-sidebar-reopen")).display,
			monitoringWidth: document.querySelector("#monitoring-main").getBoundingClientRect().width,
			stateConfig: Boolean(state.config),
			stateWorkload: Boolean(state.workload),
		};
	})()`, &collapsed)
	if err := chromedp.Run(ctx,
		chromedp.Click("#config-sidebar-toggle", chromedp.ByQuery),
		chromedp.Click("#workload-sidebar-toggle", chromedp.ByQuery),
		readCollapsedState,
	); err != nil {
		t.Fatal(err)
	}
	initialMonitoringWidth := layout.MonitoringRight - layout.MonitoringLeft
	if !collapsed.ConfigCollapsed || !collapsed.WorkloadCollapsed ||
		collapsed.ConfigExpanded != "false" || collapsed.WorkloadExpanded != "false" ||
		collapsed.ConfigWidth != 0 || collapsed.WorkloadWidth != 0 ||
		collapsed.ConfigDisplay != "none" || collapsed.WorkloadDisplay != "none" ||
		collapsed.ConfigReopenDisplay != "grid" || collapsed.LoadReopenDisplay != "grid" ||
		!collapsed.StateConfig || !collapsed.StateWorkload ||
		collapsed.MonitoringWidth <= initialMonitoringWidth {
		t.Fatalf("collapsed sidebars were not fully removed: initial=%f collapsed=%+v", initialMonitoringWidth, collapsed)
	}
	if err := chromedp.Run(ctx,
		chromedp.Reload(),
		chromedp.WaitVisible("#monitoring-main", chromedp.ByQuery),
		readCollapsedState,
	); err != nil {
		t.Fatal(err)
	}
	if !collapsed.ConfigCollapsed || !collapsed.WorkloadCollapsed ||
		collapsed.ConfigExpanded != "false" || collapsed.WorkloadExpanded != "false" ||
		collapsed.ConfigDisplay != "none" || collapsed.WorkloadDisplay != "none" ||
		collapsed.ConfigReopenDisplay != "grid" || collapsed.LoadReopenDisplay != "grid" {
		t.Fatalf("sidebar collapse state was not restored after reload: %+v", collapsed)
	}
	if err := chromedp.Run(ctx,
		chromedp.Click("#config-sidebar-reopen", chromedp.ByQuery),
		chromedp.Click("#workload-sidebar-reopen", chromedp.ByQuery),
		chromedp.SetValue("#load-tenant-count", "3", chromedp.ByQuery),
		chromedp.SetValue("#load-tasks-per-tenant", "2", chromedp.ByQuery),
		chromedp.SetValue("#load-quota", "4", chromedp.ByQuery),
		chromedp.Click("#load-create-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	var capacity, allocated, nodeSummary, workerLegend, cpuAdmission, cpuNote string
	var queuePressure, cpuPressure, telemetryCoverage string
	if err := chromedp.Run(ctx,
		chromedp.Text("#metric-capacity", &capacity, chromedp.ByQuery),
		chromedp.Text("#metric-allocated", &allocated, chromedp.ByQuery),
		chromedp.Text("#metric-nodes", &nodeSummary, chromedp.ByQuery),
		chromedp.Text("#worker-chart-legend", &workerLegend, chromedp.ByQuery),
		chromedp.Text("#performance-cpu-admission", &cpuAdmission, chromedp.ByQuery),
		chromedp.Text("#performance-cpu-note", &cpuNote, chromedp.ByQuery),
		chromedp.Text("#autoscaling-queue", &queuePressure, chromedp.ByQuery),
		chromedp.Text("#autoscaling-cpu", &cpuPressure, chromedp.ByQuery),
		chromedp.Text("#autoscaling-telemetry-note", &telemetryCoverage, chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	if capacity != "100" || allocated != "0" || nodeSummary != "2" ||
		strings.Contains(workerLegend, "worker-retained") ||
		cpuAdmission != "3 / 12" || !strings.Contains(cpuNote, "max 72% CPU") ||
		queuePressure != "7 / 5" ||
		cpuPressure != "42% / 72%" || telemetryCoverage != "100% reporting coverage" {
		t.Fatalf(
			"live Worker UI capacity=%q allocated=%q nodes=%q legend=%q CPU=%q note=%q autoscaling=%q %q %q",
			capacity, allocated, nodeSummary, workerLegend, cpuAdmission, cpuNote,
			queuePressure, cpuPressure, telemetryCoverage,
		)
	}
	if err := chromedp.Run(ctx,
		chromedp.SetValue("#worker-capacity-value", "7", chromedp.ByQuery),
		chromedp.Click("#worker-capacity-apply", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForWorkerCapacity(t, ctx, api, 7, 8*time.Second)
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(
			`const select=document.querySelector("#worker-capacity-node");
			 select.value="__all__";
			 select.dispatchEvent(new Event("change",{bubbles:true}))`,
			nil,
		),
		chromedp.SetValue("#worker-capacity-value", "9", chromedp.ByQuery),
		chromedp.Click("#worker-capacity-apply", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForAllWorkerCapacity(t, ctx, api, 9, 8*time.Second)
	waitForLoadLabStatus(t, ctx, "completed", 8*time.Second)

	if err := chromedp.Run(ctx,
		chromedp.SetValue("#quick-tenant", "load-lab-001", chromedp.ByQuery),
		chromedp.SetValue("#quick-count", "1", chromedp.ByQuery),
		chromedp.Click("#add-one", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForSubmitted(t, api, 1, 5*time.Second)

	if err := chromedp.Run(ctx,
		chromedp.WaitEnabled("#load-run-custom", chromedp.ByQuery),
		chromedp.Click("#load-run-custom", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForLoadLabStatus(t, ctx, "completed", 10*time.Second)

	var currentText string
	if err := chromedp.Run(ctx, chromedp.Text("#load-run-current", &currentText, chromedp.ByQuery)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(currentText, "6") || !strings.Contains(currentText, "All 6 tasks drained") {
		t.Fatalf("current execution did not show completed load: %q", currentText)
	}
	var jsonControls struct {
		Count       int     `json:"count"`
		AllCompact  bool    `json:"allCompact"`
		AllLabels   bool    `json:"allLabels"`
		LegacyCount int     `json:"legacyCount"`
		MaxHeight   float64 `json:"maxHeight"`
	}
	if err := chromedp.Run(ctx, chromedp.Evaluate(`(() => {
		const controls = [...document.querySelectorAll(".json-link")];
		const heights = controls.map(control => control.getBoundingClientRect().height);
		return {
			count: controls.length,
			allCompact: controls.every(control => {
				const style = getComputedStyle(control);
				return style.fontSize === "10px" &&
					style.paddingTop === "0px" && style.paddingRight === "0px" &&
					style.paddingBottom === "0px" && style.paddingLeft === "0px" &&
					style.borderTopWidth === "0px" &&
					!control.classList.contains("btn");
			}),
			allLabels: controls.every(control => control.textContent.trim() === "JSON ↗"),
			legacyCount: document.querySelectorAll(
				".btn-link,.chart-json-link,.load-run-json"
			).length,
			maxHeight: Math.max(0, ...heights),
		};
	})()`, &jsonControls)); err != nil {
		t.Fatal(err)
	}
	if jsonControls.Count != 9 || !jsonControls.AllCompact ||
		!jsonControls.AllLabels || jsonControls.LegacyCount != 0 ||
		jsonControls.MaxHeight > 14 {
		t.Fatalf("JSON controls are not uniformly compact: %+v", jsonControls)
	}
	if err := chromedp.Run(ctx,
		chromedp.Evaluate(`window.confirm=()=>true`, nil),
		chromedp.Click("#clear-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForBrowserTenantCount(t, ctx, api, 0, 8*time.Second)
	var clearToast string
	if err := chromedp.Run(ctx, chromedp.Text("#toast", &clearToast, chromedp.ByQuery)); err != nil {
		t.Fatal(err)
	}
	if clearToast != "3 tenants cleared" {
		t.Fatalf("bulk clear toast = %q", clearToast)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.tenants) != 0 || api.submitted != 7 ||
		len(api.idempotencyKey) != 6 || api.capacityWrites != 1 ||
		api.allCapacityWrites != 1 || api.clearTenantWrites != 1 ||
		api.allocationReads != 0 {
		t.Fatalf(
			"browser operations = %d tenants, %d tasks, %d keys, %d single/%d all capacity writes, %d tenant clears, %d allocation reads",
			len(api.tenants), api.submitted, len(api.idempotencyKey),
			api.capacityWrites, api.allCapacityWrites, api.clearTenantWrites,
			api.allocationReads,
		)
	}
}

func TestLoadSamplesBrowserAddsFreshRandomTenantConfigs(t *testing.T) {
	chromePath := findChrome()
	if chromePath == "" {
		t.Skip("Chrome/Chromium is not installed")
	}
	api := newLoadLabBrowserAPI()
	api.tenants["existing-tenant"] = map[string]any{
		"id": "existing-tenant", "name": "Existing tenant",
		"max_workers": 77, "inflight": 0,
	}
	server := httptest.NewServer(Handler(api))
	defer server.Close()

	allocator, cancelAllocator := chromedp.NewExecAllocator(
		context.Background(),
		append(chromedp.DefaultExecAllocatorOptions[:],
			chromedp.ExecPath(chromePath),
			chromedp.Flag("headless", true),
			chromedp.Flag("disable-gpu", true),
			chromedp.Flag("no-sandbox", true),
			chromedp.WindowSize(1280, 900),
		)...,
	)
	defer cancelAllocator()
	browserContext, cancelBrowser := chromedp.NewContext(allocator)
	defer cancelBrowser()
	ctx, cancel := context.WithTimeout(browserContext, 20*time.Second)
	defer cancel()

	var buttonText string
	if err := chromedp.Run(
		ctx,
		chromedp.Navigate(server.URL),
		chromedp.WaitVisible("#seed-tenants", chromedp.ByQuery),
		chromedp.Text("#seed-tenants", &buttonText, chromedp.ByQuery),
		chromedp.Click("#seed-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	if buttonText != "Add random tenants" {
		t.Fatalf("sample button text = %q", buttonText)
	}
	waitForTenantCountAtLeast(t, api, 4, 8*time.Second)
	if err := chromedp.Run(
		ctx,
		chromedp.WaitEnabled("#seed-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	first := browserTenantSnapshot(api)
	firstAdded := len(first) - 1
	if firstAdded < 3 || firstAdded > 7 {
		t.Fatalf("first random click added %d tenants, want 3..7", firstAdded)
	}

	if err := chromedp.Run(
		ctx,
		chromedp.Click("#seed-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	waitForTenantCountAtLeast(t, api, len(first)+3, 8*time.Second)
	if err := chromedp.Run(
		ctx,
		chromedp.WaitEnabled("#seed-tenants", chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	final := browserTenantSnapshot(api)
	secondAdded := len(final) - len(first)
	if secondAdded < 3 || secondAdded > 7 {
		t.Fatalf("second random click added %d tenants, want 3..7", secondAdded)
	}
	existing := final["existing-tenant"]
	if existing["name"] != "Existing tenant" || existing["max_workers"] != 77 {
		t.Fatalf("existing tenant was modified: %+v", existing)
	}
	allowedLimits := map[int]bool{
		5: true, 10: true, 20: true, 30: true, 50: true,
		60: true, 100: true, 200: true, 500: true,
	}
	for id, tenant := range final {
		if id == "existing-tenant" {
			continue
		}
		if !strings.HasPrefix(id, "sample-") {
			t.Fatalf("random tenant ID %q does not use sample prefix", id)
		}
		limit, ok := tenant["max_workers"].(int)
		if !ok || !allowedLimits[limit] {
			t.Fatalf("random tenant %q has invalid limit: %+v", id, tenant)
		}
		if before, existed := first[id]; existed &&
			(before["name"] != tenant["name"] ||
				before["max_workers"] != tenant["max_workers"]) {
			t.Fatalf("second click replaced first-click tenant %q: before=%+v after=%+v", id, before, tenant)
		}
	}
}

func browserTenantSnapshot(api *loadLabBrowserAPI) map[string]map[string]any {
	api.mu.Lock()
	defer api.mu.Unlock()
	snapshot := make(map[string]map[string]any, len(api.tenants))
	for id, tenant := range api.tenants {
		copy := make(map[string]any, len(tenant))
		for key, value := range tenant {
			copy[key] = value
		}
		snapshot[id] = copy
	}
	return snapshot
}

func waitForBrowserTenantCount(
	t *testing.T,
	ctx context.Context,
	api *loadLabBrowserAPI,
	want int,
	deadline time.Duration,
) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		api.mu.Lock()
		count := len(api.tenants)
		api.mu.Unlock()
		var options int
		err := chromedp.Run(
			ctx,
			chromedp.Evaluate(
				`document.querySelectorAll("#config-tenant option:not([value=''])").length`,
				&options,
			),
		)
		if err == nil && count == want && options == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("browser tenant count did not reach %d", want)
}

func waitForTenantCountAtLeast(
	t *testing.T,
	api *loadLabBrowserAPI,
	minimum int,
	deadline time.Duration,
) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		api.mu.Lock()
		got := len(api.tenants)
		api.mu.Unlock()
		if got >= minimum {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	api.mu.Lock()
	got := len(api.tenants)
	api.mu.Unlock()
	t.Fatalf("tenant count = %d, want at least %d", got, minimum)
}

func waitForSubmitted(t *testing.T, api *loadLabBrowserAPI, want int, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		api.mu.Lock()
		got := api.submitted
		api.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	api.mu.Lock()
	got := api.submitted
	api.mu.Unlock()
	t.Fatalf("submitted tasks = %d, want %d", got, want)
}

func waitForWorkerCapacity(
	t *testing.T,
	ctx context.Context,
	api *loadLabBrowserAPI,
	want int,
	deadline time.Duration,
) {
	t.Helper()
	end := time.Now().Add(deadline)
	var metric, status string
	for time.Now().Before(end) {
		api.mu.Lock()
		got := api.workerCapacity
		api.mu.Unlock()
		err := chromedp.Run(
			ctx,
			chromedp.Text("#metric-capacity", &metric, chromedp.ByQuery),
			chromedp.Text("#worker-capacity-status", &status, chromedp.ByQuery),
		)
		if err == nil && got == want && metric == fmt.Sprint(want) &&
			strings.Contains(status, fmt.Sprintf("Effective %d slots", want)) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf(
		"Worker capacity UI metric=%q status=%q, want %d",
		metric, status, want,
	)
}

func waitForAllWorkerCapacity(
	t *testing.T,
	ctx context.Context,
	api *loadLabBrowserAPI,
	want int,
	deadline time.Duration,
) {
	t.Helper()
	end := time.Now().Add(deadline)
	var metric, status, button string
	for time.Now().Before(end) {
		api.mu.Lock()
		got := api.workerCapacity
		writes := api.allCapacityWrites
		api.mu.Unlock()
		err := chromedp.Run(
			ctx,
			chromedp.Text("#metric-capacity", &metric, chromedp.ByQuery),
			chromedp.Text("#worker-capacity-status", &status, chromedp.ByQuery),
			chromedp.Text("#worker-capacity-apply", &button, chromedp.ByQuery),
		)
		if err == nil && got == want && writes == 1 && metric == fmt.Sprint(want) &&
			strings.Contains(status, fmt.Sprintf("all currently %d slots", want)) &&
			button == "Apply to all" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf(
		"all-Worker capacity UI metric=%q status=%q button=%q, want %d",
		metric, status, button, want,
	)
}

func waitForLoadLabStatus(t *testing.T, ctx context.Context, want string, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var got string
	for time.Now().Before(end) {
		err := chromedp.Run(
			ctx,
			chromedp.AttributeValue(
				"#load-run-current [data-status]", "data-status", &got, nil,
				chromedp.ByQuery,
			),
		)
		if err == nil && got == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Load Lab status = %q, want %q", got, want)
}

func findChrome() string {
	for _, name := range []string{
		"google-chrome", "google-chrome-stable", "chromium", "chromium-browser",
	} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	for _, path := range []string{
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"/Applications/Chromium.app/Contents/MacOS/Chromium",
	} {
		if info, err := os.Stat(filepath.Clean(path)); err == nil && !info.IsDir() {
			return path
		}
	}
	return ""
}
