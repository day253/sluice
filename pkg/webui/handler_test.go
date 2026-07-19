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
		`getJSON('/api/v1/admin/performance?history=0')`,
	} {
		if !strings.Contains(body, fragment) {
			t.Errorf("dashboard is missing performance fragment %q", fragment)
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
