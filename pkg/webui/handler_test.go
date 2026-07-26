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
		`getJSON('/api/v1/metrics?performance=0')`,
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
		`aria-label="View saved workload run as JSON">JSON ↗</button>`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing compact JSON control fragment %q", fragment)
		}
	}
	for _, legacy := range []string{
		`btn-link`,
		`chart-json-link`,
		`load-run-json`,
		`View JSON ↗`,
		`View raw JSON ↗`,
	} {
		if strings.Contains(body, legacy) {
			t.Errorf("dashboard still contains legacy large/mixed JSON control %q", legacy)
		}
	}
}

func TestDashboardShowsCurrentWorkerPodLoadMirror(t *testing.T) {
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
		`id="worker-load-list"`,
		`aria-label="Current load for every live Worker Pod"`,
		`aria-label="View current Worker Pod load as JSON"`,
		`loads=scheduler.worker_telemetry||scheduler.worker_loads||{}`,
		`cpu_utilization_millis`,
		`running_tasks`,
		`reportedCapacity`,
		`age>5000`,
		`No fresh sample`,
		`sorted by Pod name · no history`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing Worker Pod load fragment %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`worker-load-history`,
		`/api/v1/admin/worker-load`,
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
		`function currentAllocations(){const live=new Set(S.nodes.filter(node=>node.role==='worker'&&node.status==='up')`,
		`const capacity=liveWorkers.reduce((sum,node)=>sum+Number(node.total_workers||0),0)`,
		`S.workerHistories.reduce((sum,item)=>sum+item.limit,0)`,
		`S.nodes.filter(node=>node.role==='worker'&&node.status==='up').map`,
	} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("dashboard current Worker mirror is missing %q", fragment)
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
