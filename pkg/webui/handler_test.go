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
		`href="/api/v1/metrics?prefix=allocated-workers%3Anode%3A&amp;performance=0"`,
		`aria-label="View worker allocation history as JSON"`,
		`href="/api/v1/metrics?prefix=unfinished%3A&amp;performance=0"`,
		`aria-label="View unfinished task history as JSON"`,
		`aria-label="View Raft Apply history as JSON"`,
		`aria-label="View scheduler history as JSON"`,
		`target="_blank" rel="noopener"`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing raw JSON link fragment %q", fragment)
		}
	}
}

func TestWorkerChartExcludesControlOnlyNodes(t *testing.T) {
	handler := Handler(http.NotFoundHandler())
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d, want %d", recorder.Code, http.StatusOK)
	}
	if fragment := `executionNodes=S.nodes.filter(node=>Number(node.total_workers||0)>0)`;
		!strings.Contains(recorder.Body.String(), fragment) {
		t.Fatalf("dashboard is missing execution-role chart filter %q", fragment)
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
