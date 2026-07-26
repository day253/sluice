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
	var queuePressure, executionPressure, cpuPressure, telemetryCoverage string
	if err := chromedp.Run(ctx,
		chromedp.Text("#metric-capacity", &capacity, chromedp.ByQuery),
		chromedp.Text("#metric-allocated", &allocated, chromedp.ByQuery),
		chromedp.Text("#metric-nodes", &nodeSummary, chromedp.ByQuery),
		chromedp.Text("#worker-chart-legend", &workerLegend, chromedp.ByQuery),
		chromedp.Text("#performance-cpu-admission", &cpuAdmission, chromedp.ByQuery),
		chromedp.Text("#performance-cpu-note", &cpuNote, chromedp.ByQuery),
		chromedp.Text("#autoscaling-queue", &queuePressure, chromedp.ByQuery),
		chromedp.Text("#autoscaling-execution", &executionPressure, chromedp.ByQuery),
		chromedp.Text("#autoscaling-cpu", &cpuPressure, chromedp.ByQuery),
		chromedp.Text("#autoscaling-telemetry-note", &telemetryCoverage, chromedp.ByQuery),
	); err != nil {
		t.Fatal(err)
	}
	if capacity != "100" || allocated != "0" || nodeSummary != "2" ||
		strings.Contains(workerLegend, "worker-retained") ||
		cpuAdmission != "3 / 12" || !strings.Contains(cpuNote, "max 72% CPU") ||
		queuePressure != "7 / 5" || executionPressure != "5 / 100" ||
		cpuPressure != "42% / 72%" || telemetryCoverage != "100% reporting coverage" {
		t.Fatalf(
			"live Worker UI capacity=%q allocated=%q nodes=%q legend=%q CPU=%q note=%q autoscaling=%q %q %q %q",
			capacity, allocated, nodeSummary, workerLegend, cpuAdmission, cpuNote,
			queuePressure, executionPressure, cpuPressure, telemetryCoverage,
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
	if jsonControls.Count < 8 || !jsonControls.AllCompact ||
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
		api.allCapacityWrites != 1 || api.clearTenantWrites != 1 {
		t.Fatalf(
			"browser operations = %d tenants, %d tasks, %d keys, %d single/%d all capacity writes, %d tenant clears",
			len(api.tenants), api.submitted, len(api.idempotencyKey),
			api.capacityWrites, api.allCapacityWrites, api.clearTenantWrites,
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
