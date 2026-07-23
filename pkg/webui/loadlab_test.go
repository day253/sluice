package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dop251/goja"
)

func loadLabRuntime(t *testing.T) *goja.Runtime {
	t.Helper()
	source, err := content.ReadFile("loadlab.js")
	if err != nil {
		t.Fatal(err)
	}
	runtime := goja.New()
	if _, err := runtime.RunString(string(source)); err != nil {
		t.Fatalf("evaluate load lab module: %v", err)
	}
	return runtime
}

func evaluateJSON[T any](t *testing.T, runtime *goja.Runtime, expression string) T {
	t.Helper()
	value, err := runtime.RunString("JSON.stringify(" + expression + ")")
	if err != nil {
		t.Fatalf("evaluate %q: %v", expression, err)
	}
	var result T
	if err := json.Unmarshal([]byte(value.String()), &result); err != nil {
		t.Fatalf("decode %q: %v", value.String(), err)
	}
	return result
}

func TestLoadLabBuildsBoundedStableRoundRobinWorkload(t *testing.T) {
	runtime := loadLabRuntime(t)
	summary := evaluateJSON[struct {
		TenantCount int `json:"tenantCount"`
		TotalTasks  int `json:"totalTasks"`
		Specs       []struct {
			ID         string `json:"id"`
			MaxWorkers int    `json:"maxWorkers"`
			TaskCount  int    `json:"taskCount"`
		} `json:"specs"`
	}](t, runtime, `SluiceLoadLab.summarize(
		SluiceLoadLab.recipe("hundred-tenant-burst").options, "Regression")`)
	if summary.TenantCount != 100 || summary.TotalTasks != 20_000 {
		t.Fatalf("summary = %+v", summary)
	}
	if summary.Specs[0].ID != "load-lab-001" ||
		summary.Specs[99].ID != "load-lab-100" ||
		summary.Specs[0].MaxWorkers != 50 ||
		summary.Specs[0].TaskCount != 200 {
		t.Fatalf("stable pool specs = first %+v last %+v", summary.Specs[0], summary.Specs[99])
	}
	firstRound := evaluateJSON[[]struct {
		Tenant string `json:"tenant"`
		Index  int    `json:"index"`
	}](t, runtime, `SluiceLoadLab.buildRoundRobinJobs(
		SluiceLoadLab.buildTenantSpecs({tenantCount:100,tasksPerTenant:2}, "Round Robin")
	).slice(0,100)`)
	seen := make(map[string]bool, 100)
	for _, job := range firstRound {
		if job.Index != 0 || seen[job.Tenant] {
			t.Fatalf("first round is not one task per tenant: %+v", firstRound)
		}
		seen[job.Tenant] = true
	}
	if len(seen) != 100 {
		t.Fatalf("first round covered %d tenants, want 100", len(seen))
	}
}

func TestLoadLabComposesHotspotAndWaveAtomicOperations(t *testing.T) {
	runtime := loadLabRuntime(t)
	total := evaluateJSON[int](t, runtime, `SluiceLoadLab.summarize(
		{tenantCount:100,tasksPerTenant:50,loadShape:"hotspot"}, "Hot"
	).totalTasks`)
	if total != 9_950 {
		t.Fatalf("hotspot tasks = %d, want 9950", total)
	}
	waveSizes := evaluateJSON[[]int](t, runtime, `SluiceLoadLab.splitWaves(
		SluiceLoadLab.buildRoundRobinJobs(
			SluiceLoadLab.buildTenantSpecs({tenantCount:5,tasksPerTenant:4}, "Waves")
		), 3
	).map(wave => wave.length)`)
	if len(waveSizes) != 3 || waveSizes[0]+waveSizes[1]+waveSizes[2] != 20 {
		t.Fatalf("wave sizes = %v", waveSizes)
	}
	if _, err := runtime.RunString(
		`SluiceLoadLab.buildTenantSpecs({tenantCount:100,tasksPerTenant:5000}, "Too large")`,
	); err == nil || !strings.Contains(err.Error(), "browser safety limit") {
		t.Fatalf("oversized workload error = %v", err)
	}
}

func TestDashboardExposesAtomicLoadLabAndExecutionHistory(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}
	for _, fragment := range []string{
		`id="load-lab"`, `Atomic workload builder`,
		`id="load-create-tenants"`, `id="load-run-custom"`,
		`id="load-run-current"`, `id="load-run-history"`,
		`id="load-stop"`, `data-load-json=`,
		`id="worker-capacity-node"`, `id="worker-capacity-value"`,
		`id="worker-capacity-apply"`, `Processor slots`,
		`/capacity`, `body:JSON.stringify({total_workers:totalWorkers})`,
		`idempotency_key:`, `buildRoundRobinJobs`,
		`<script src="/assets/loadlab.js"></script>`,
	} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Errorf("dashboard is missing Load Lab fragment %q", fragment)
		}
	}
}

func TestLoadLabAssetIsServedAsJavaScript(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/assets/loadlab.js", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET loadlab.js status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "javascript") {
		t.Fatalf("Content-Type = %q", got)
	}
	if !strings.Contains(recorder.Body.String(), "var SluiceLoadLab") {
		t.Fatal("Load Lab module is missing")
	}
}
