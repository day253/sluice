package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDashboardIncludesPerformanceVisualizationAndJSONLink(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`id="performance-title"`,
		`id="performance-create-apply"`,
		`id="performance-scan-ratio"`,
		`id="performance-raft-chart"`,
		`id="performance-scheduler-chart"`,
		`href="/api/v1/admin/performance"`,
		`getJSON('/api/v1/metrics?prefix=allocated-workers%3Atenant%3A&performance=0&current=1')`,
		`getJSON('/api/v1/admin/performance')`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing performance fragment %q", fragment)
		}
	}
}

func TestDashboardChartsExposeNearestPointTooltip(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`.chart-tooltip{`,
		`const chartTimeLabel=index=>`,
		`tooltip.setAttribute('role','tooltip')`,
		`canvas.addEventListener('pointermove',event=>moveChartHover(canvas,event))`,
		`if(id!==canvas.id)hideChartHover($(id))`,
		`Number.isFinite(selected.item.limit)`,
		`' workers'`,
		`' tasks'`,
		`' ms'`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing chart tooltip fragment %q", fragment)
		}
	}
}

func TestDashboardChartsExposeRawJSONLinks(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`href="/api/v1/admin/autoscaling"`,
		`aria-label="View autoscaling diagnostics as JSON"`,
		`href="/api/v1/metrics?prefix=allocated-workers%3Anode%3A&amp;performance=0"`,
		`aria-label="View worker allocation history as JSON"`,
		`href="/api/v1/metrics?prefix=unfinished%3A&amp;performance=0"`,
		`aria-label="View unfinished task history as JSON"`,
		`href="/api/v1/metrics?prefix=allocated-workers%3Atenant%3A&amp;performance=0&amp;current=1"`,
		`aria-label="View current tenant allocation totals as JSON"`,
		`aria-label="View performance diagnostics as JSON"`,
		`aria-label="View Raft Apply history as JSON"`,
		`aria-label="View scheduler history as JSON"`,
		`target="_blank" rel="noopener"`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing raw JSON link fragment %q", fragment)
		}
	}
}

func TestDashboardUsesOneCompactJSONLinkStyle(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	if count := strings.Count(body, `class="json-link"`); count != 9 {
		t.Fatalf("compact JSON control count = %d, want 9", count)
	}
	for _, fragment := range []string{
		`.json-link{display:inline-flex;align-items:center;border:0;background:transparent;`,
		`padding:0;font-size:10px;`,
		`aria-label="View current workload run as JSON">JSON ↗</button>`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing compact JSON control fragment %q", fragment)
		}
	}
	for _, legacy := range []string{
		`btn-link`,
		`chart-json-link`,
		`load-run-json`,
		`View saved workload run as JSON`,
		`View JSON ↗`,
		`View raw JSON ↗`,
	} {
		if strings.Contains(body, legacy) {
			t.Errorf("dashboard still contains legacy large/mixed JSON control %q", legacy)
		}
	}
}

func TestDashboardTurnsCurrentWorkerPodLoadIntoSessionChart(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`id="worker-load-title"`,
		`id="worker-load-summary"`,
		`id="worker-cpu-session-chart"`,
		`id="worker-cpu-session-legend"`,
		`aria-label="Worker Pod CPU trend recorded in this browser session"`,
		`aria-label="View current Worker Pod load as JSON"`,
		`loads=scheduler.worker_telemetry||scheduler.worker_loads||{}`,
		`cpu_utilization_millis`,
		`running_tasks`,
		`S.sessionHistory.workers`,
		`S.sessionHistory.workers`,
		`TREND.recordSample`,
		`renderSessionTrends()`,
		`mode='server'`,
		`'session'`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing Worker Pod load fragment %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`worker-load-history`,
		`/api/v1/admin/worker-load`,
		`id="worker-load-list"`,
		`<table`,
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("current Worker Pod load mirror unexpectedly adds %q", forbidden)
		}
	}
}

func TestWorkerChartUsesOnlyCurrentLiveWorkerNodes(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if fragment := `executionNodes=S.nodes.filter(node=>node.role==='worker'&&node.status==='up'&&Number(node.total_workers||0)>0)`; !strings.Contains(recorder.Body.String(), fragment) {
		t.Fatalf("dashboard is missing live execution-role chart filter %q", fragment)
	}
	for _, fragment := range []string{
		`S.nodeAllocationTotals[node.node_id]||0`,
		`getJSON('/api/v1/metrics?prefix=allocated-workers%3Anode%3A&performance=0')`,
		`const capacity=liveWorkers.reduce((sum,node)=>sum+Number(node.total_workers||0),0)`,
		`S.workerSeriesMeta.limitTotal`,
	} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("dashboard current Worker mirror is missing %q", fragment)
		}
	}
}

func TestDashboardChartsAggregateTenantSlotsWithoutPlacementMatrix(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`Tenant allocated slots`,
		`id="tenant-allocation-session-chart"`,
		`id="tenant-allocation-session-legend"`,
		`aria-label="Tenant allocated slots trend recorded in this browser session"`,
		`S.tenantAllocationTotals`,
		`totals[tenant.id]||0`,
		`prefix=allocated-workers%3Atenant%3A&performance=0&current=1`,
		`S.sessionHistory.tenants`,
		`TREND.collapseSeries(tenantSeries,CHART_SERIES_LIMIT,'tenants',false,'average')`,
		`limit:Number(tenant.max_workers||0)`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("aggregate tenant-slot mirror is missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`/api/v1/admin/allocations`,
		`S.allocations`,
		`<th>Placement</th>`,
		`placement-chip`,
		`id="tenant-list"`,
		`<table`,
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("dashboard still exposes per-Pod allocation detail %q", forbidden)
		}
	}
}

func TestDashboardPrioritizesChartsAndGroupsRelatedValues(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	primaryCharts := strings.Index(body, `id="primary-charts"`)
	performance := strings.Index(body, `id="performance-title"`)
	details := strings.Index(body, `class="details-heading"`)
	workerSession := strings.Index(body, `id="worker-session-panel"`)
	tenantSession := strings.Index(body, `id="tenant-session-panel"`)
	if primaryCharts < 0 || performance <= primaryCharts || details <= performance ||
		workerSession <= details || tenantSession <= workerSession {
		t.Fatalf(
			"dashboard order charts=%d performance=%d details=%d worker=%d tenant=%d",
			primaryCharts, performance, details, workerSession, tenantSession,
		)
	}
	for _, fragment := range []string{
		`id="workload-trend-panel"`,
		`aria-label="Queue signals related to unfinished tasks"`,
		`id="autoscaling-queue"`,
		`id="autoscaling-oldest"`,
		`id="capacity-trend-panel"`,
		`aria-label="Worker signals related to allocation capacity"`,
		`id="autoscaling-cpu"`,
		`id="autoscaling-telemetry"`,
		`aria-label="Current Raft Apply values"`,
		`aria-label="Current scheduler values"`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard affinity layout is missing %q", fragment)
		}
	}
}

func TestDashboardUsesMaximumSubmissionBatchesAndShowsIngressDiagnostics(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`const SUBMIT_BATCH_SIZE=1000`,
		`id="submit-concurrency"`,
		`Auto · 8 → 16`,
		`LOAD.runRolling`,
		`createSubmissionController`,
		`Queues S / A / C`,
		`submission_queue_depth`,
		`submission_apply_inflight`,
		`submission_apply_limit`,
		`submission_backpressure_waits`,
		`average_submission_batch`,
		`average_submission_requests`,
		`average_submission_queue_us`,
		`performance:scheduler:submission-tasks`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("submission batching diagnostics are missing %q", fragment)
		}
	}
	if strings.Contains(body, `const SUBMIT_BATCH_SIZE=500`) {
		t.Fatal("dashboard still emits half-filled submission batches")
	}
	if strings.Contains(body, `SUBMIT_BATCH_CONCURRENCY=4`) ||
		strings.Contains(body, `Promise.allSettled(batches.map`) {
		t.Fatal("dashboard still imposes fixed four-request submission waves")
	}
}

func TestDashboardBoundsHighCardinalityChartsAndUsesEphemeralSessionHistory(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	body := recorder.Body.String()
	for _, fragment := range []string{
		`<script src="/assets/dashboardtrend.js"></script>`,
		`SESSION_HISTORY_LIMIT=60`,
		`CHART_SERIES_LIMIT=8`,
		`TREND.collapseSeries(workerSeries,CHART_SERIES_LIMIT,'Pods',false,'average')`,
		`TREND.collapseSeries(tenantSeries,CHART_SERIES_LIMIT,'tenants',true,'average')`,
		`sampleSessionState()`,
		`Up to 60 samples · reset on refresh`,
		`id="session-charts"`,
		`id="worker-cpu-session-chart"`,
		`id="tenant-allocation-session-chart"`,
		`S.workerSeriesMeta.hidden?`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("bounded dashboard history contract is missing %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`localStorage.setItem('sluice.dashboard.trends`,
		`sessionStorage`,
		`autoscaling-counters`,
		`submitted_tasks_total`,
		`completed_tasks_total`,
		`<table`,
	} {
		if strings.Contains(body, forbidden) {
			t.Errorf("ephemeral chart trend unexpectedly contains %q", forbidden)
		}
	}
}

func TestPerformanceJSONRouteStillDelegatesToAPI(t *testing.T) {
	const diagnostics = `{"node_id":"leader-1","current":{"raft":{}},"history":{}}`
	apiCalled := false
	apiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiCalled = true
		if r.URL.Path != "/api/v1/admin/performance" {
			t.Errorf("API path = %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(diagnostics))
	})

	recorder := httptest.NewRecorder()
	Handler(apiHandler).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/api/v1/admin/performance", nil),
	)

	if !apiCalled {
		t.Fatal("performance JSON request did not reach the API handler")
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", got)
	}
	if got := strings.TrimSpace(recorder.Body.String()); got != diagnostics {
		t.Fatalf("performance JSON body = %q, want %q", got, diagnostics)
	}
}
