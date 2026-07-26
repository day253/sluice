package webui

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
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

func TestLoadLabBuildsBoundedUniqueRandomTenantConfigs(t *testing.T) {
	runtime := loadLabRuntime(t)
	type tenantConfig struct {
		ID         string `json:"id"`
		Name       string `json:"name"`
		MaxWorkers int    `json:"maxWorkers"`
	}
	first := evaluateJSON[[]tenantConfig](
		t, runtime, `SluiceLoadLab.buildRandomTenantConfigs([], 12345)`,
	)
	repeated := evaluateJSON[[]tenantConfig](
		t, runtime, `SluiceLoadLab.buildRandomTenantConfigs([], 12345)`,
	)
	different := evaluateJSON[[]tenantConfig](
		t, runtime, `SluiceLoadLab.buildRandomTenantConfigs([], 54321)`,
	)
	if len(first) < 3 || len(first) > 7 {
		t.Fatalf("random tenant count = %d, want 3..7", len(first))
	}
	if !reflect.DeepEqual(first, repeated) {
		t.Fatalf("same seed is not deterministic: first=%+v repeated=%+v", first, repeated)
	}
	if reflect.DeepEqual(first, different) {
		t.Fatalf("different seeds produced identical configs: %+v", first)
	}
	allowedLimits := map[int]bool{
		5: true, 10: true, 20: true, 30: true, 50: true,
		60: true, 100: true, 200: true, 500: true,
	}
	seen := make(map[string]bool, len(first))
	for _, tenant := range first {
		if !strings.HasPrefix(tenant.ID, "sample-") || seen[tenant.ID] {
			t.Fatalf("random tenant ID is not unique: %+v", first)
		}
		if tenant.Name == "" || !allowedLimits[tenant.MaxWorkers] {
			t.Fatalf("invalid random tenant config: %+v", tenant)
		}
		seen[tenant.ID] = true
	}
	collisionSafe := evaluateJSON[[]tenantConfig](
		t,
		runtime,
		`SluiceLoadLab.buildRandomTenantConfigs(["`+first[0].ID+`"], 12345)`,
	)
	if collisionSafe[0].ID == first[0].ID {
		t.Fatalf("existing tenant was not protected from replacement: %+v", collisionSafe[0])
	}
}

func TestDashboardExposesAtomicLoadLabAndOnlyTheActiveOperation(t *testing.T) {
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
		`id="load-run-status"`, `id="load-run-current"`,
		`id="load-stop"`, `data-load-json=`,
		`id="worker-capacity-node"`, `id="worker-capacity-value"`,
		`id="worker-capacity-apply"`, `Processor slots`,
		`allValue='__all__'`, `All live Worker Pods`,
		`'/api/v1/admin/nodes/capacity'`,
		`body:JSON.stringify({total_workers:totalWorkers})`,
		`id="performance-cpu-admission"`, `CPU admission`,
		`load_throttled_requests`, `worker_loads`,
		`id="autoscaling-title"`, `Autoscaling pressure`,
		`id="autoscaling-queue"`, `id="autoscaling-cpu"`,
		`id="autoscaling-telemetry"`, `/api/v1/admin/autoscaling`,
		`idempotency_key:`, `buildRoundRobinJobs`,
		`Add random tenants`, `buildRandomTenantConfigs`,
		`<script src="/assets/loadlab.js"></script>`,
	} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Errorf("dashboard is missing Load Lab fragment %q", fragment)
		}
	}
	for _, forbidden := range []string{
		`id="load-run-history"`,
		`id="load-clear-history"`,
		`Clear history`,
		`id="autoscaling-execution"`,
		`View saved workload run as JSON`,
	} {
		if strings.Contains(recorder.Body.String(), forbidden) {
			t.Errorf("dashboard still exposes removed execution history %q", forbidden)
		}
	}
}

func TestDashboardKeepsWorkloadWritesInSidebarAndMonitoringMainReadOnly(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}

	page := recorder.Body.String()
	configStart := strings.Index(page, `<aside id="config-sidebar"`)
	sidebarStart := strings.Index(page, `<aside id="workload-sidebar"`)
	if configStart < 0 || sidebarStart < 0 {
		t.Fatalf("dashboard sidebars are missing: config=%d workload=%d", configStart, sidebarStart)
	}
	sidebarEndOffset := strings.Index(page[sidebarStart:], `</aside>`)
	sidebarEnd := sidebarStart + sidebarEndOffset
	monitoringStart := strings.Index(page, `<div id="monitoring-main"`)
	if sidebarEndOffset < 0 || sidebarEnd <= sidebarStart || monitoringStart < 0 {
		t.Fatalf(
			"dashboard regions are missing: config=%d workload=%d..%d monitoring=%d",
			configStart,
			sidebarStart,
			sidebarEnd,
			monitoringStart,
		)
	}

	sidebar := page[sidebarStart:sidebarEnd]
	for _, fragment := range []string{
		`id="load-lab"`,
		`id="quick-tenant"`,
		`id="quick-count"`,
		`id="add-one"`,
		`id="add-all"`,
		`id="load-run-custom"`,
	} {
		if !strings.Contains(sidebar, fragment) {
			t.Errorf("workload sidebar is missing write control %q", fragment)
		}
	}

	monitoring := page[monitoringStart:strings.Index(page, `</main>`)]
	for _, fragment := range []string{
		`Worker allocation by instance`,
		`Unfinished tasks by tenant`,
		`Tenant allocation`,
	} {
		if !strings.Contains(monitoring, fragment) {
			t.Errorf("monitoring main is missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`id="load-lab"`,
		`data-action="load"`,
		`id="add-all"`,
	} {
		if strings.Contains(monitoring, fragment) {
			t.Errorf("monitoring main still contains workload write control %q", fragment)
		}
	}
}

func TestDashboardSeparatesConfigurationMonitoringAndLoadSidebars(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}

	page := recorder.Body.String()
	region := func(id string) string {
		t.Helper()
		start := strings.Index(page, `<aside id="`+id+`"`)
		if start < 0 {
			t.Fatalf("dashboard is missing %s", id)
		}
		endOffset := strings.Index(page[start:], `</aside>`)
		if endOffset < 0 {
			t.Fatalf("dashboard %s is not closed", id)
		}
		return page[start : start+endOffset]
	}
	config := region("config-sidebar")
	workload := region("workload-sidebar")
	monitoringStart := strings.Index(page, `<div id="monitoring-main"`)
	monitoringEnd := strings.Index(page, `</main>`)
	if monitoringStart < 0 || monitoringEnd <= monitoringStart {
		t.Fatal("dashboard monitoring main is missing")
	}
	monitoring := page[monitoringStart:monitoringEnd]

	for _, fragment := range []string{
		`id="config-tenant"`,
		`id="open-tenant"`,
		`id="edit-tenant"`,
		`id="seed-tenants"`,
		`id="worker-capacity-node"`,
		`id="worker-capacity-value"`,
		`id="worker-capacity-apply"`,
		`all live Worker Pods`,
		`id="config-sidebar-toggle"`,
		`aria-controls="config-sidebar-body"`,
	} {
		if !strings.Contains(config, fragment) {
			t.Errorf("configuration sidebar is missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`id="quick-tenant"`,
		`id="quick-count"`,
		`id="add-one"`,
		`id="add-all"`,
		`id="load-lab"`,
		`id="load-run-custom"`,
		`id="workload-sidebar-toggle"`,
		`aria-controls="workload-sidebar-body"`,
	} {
		if !strings.Contains(workload, fragment) {
			t.Errorf("workload sidebar is missing %q", fragment)
		}
	}
	for _, fragment := range []string{
		`id="quick-tenant"`,
		`id="add-one"`,
		`id="worker-capacity-apply"`,
		`id="open-tenant"`,
	} {
		if strings.Contains(monitoring, fragment) {
			t.Errorf("monitoring main contains write control %q", fragment)
		}
	}
	for _, crossRegion := range []struct {
		name     string
		region   string
		fragment string
	}{
		{name: "configuration", region: config, fragment: `id="add-one"`},
		{name: "configuration", region: config, fragment: `id="load-run-custom"`},
		{name: "workload", region: workload, fragment: `id="worker-capacity-apply"`},
		{name: "workload", region: workload, fragment: `id="edit-tenant"`},
	} {
		if strings.Contains(crossRegion.region, crossRegion.fragment) {
			t.Errorf("%s sidebar contains misplaced control %q", crossRegion.name, crossRegion.fragment)
		}
	}
	for _, fragment := range []string{
		`SIDEBAR_STATE_KEY='sluice.dashboard.sidebars.v1'`,
		`setSidebarCollapsed('config'`,
		`setSidebarCollapsed('workload'`,
		`localStorage.setItem(SIDEBAR_STATE_KEY`,
		`grid-template-areas:"config main workload"`,
	} {
		if !strings.Contains(page, fragment) {
			t.Errorf("collapsible three-column contract is missing %q", fragment)
		}
	}
}

func TestDashboardUsesShadcnStyleSidebarShell(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}

	page := recorder.Body.String()
	for _, fragment := range []string{
		`--sidebar:#f8fafc`,
		`--sidebar-border:#e2e8f0`,
		`.dashboard-sidebar .sidebar-panel{overflow:hidden;border-color:var(--sidebar-border);border-radius:12px;background:var(--sidebar);box-shadow:none}`,
		`.sidebar-reopen{position:fixed`,
		`class="sidebar-brand-icon"`,
		`class="sidebar-config-section sidebar-group"`,
		`class="sidebar-group sidebar-recipe-group"`,
		`class="sidebar-group-label">Load presets`,
		`class="sidebar-group sidebar-runtime-group hidden"`,
	} {
		if !strings.Contains(page, fragment) {
			t.Errorf("shadcn-style sidebar shell is missing %q", fragment)
		}
	}
}

func TestDashboardFullyHidesCollapsedSidebars(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}

	page := recorder.Body.String()
	for _, fragment := range []string{
		`.dashboard-sidebar.is-collapsed{display:none}`,
		`.app-shell.config-collapsed{grid-template-columns:minmax(0,1fr) var(--workload-width);grid-template-areas:"main workload"}`,
		`.app-shell.workload-collapsed{grid-template-columns:var(--config-width) minmax(0,1fr);grid-template-areas:"config main"}`,
		`.app-shell.config-collapsed.workload-collapsed{grid-template-columns:minmax(0,1fr);grid-template-areas:"main"}`,
		`id="config-sidebar-reopen"`,
		`id="workload-sidebar-reopen"`,
		`reopen.hidden=!collapsed`,
		`$('config-sidebar-reopen').addEventListener('click'`,
		`$('workload-sidebar-reopen').addEventListener('click'`,
	} {
		if !strings.Contains(page, fragment) {
			t.Errorf("fully hidden sidebar contract is missing %q", fragment)
		}
	}
	for _, obsolete := range []string{
		`--config-width:48px`,
		`--workload-width:48px`,
		`sidebar-collapsed-label`,
	} {
		if strings.Contains(page, obsolete) {
			t.Errorf("collapsed sidebar still retains obsolete rail %q", obsolete)
		}
	}
}

func TestDashboardOffersAtomicClearAllTenantsInConfigurationSidebar(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET / status = %d", recorder.Code)
	}

	page := recorder.Body.String()
	configStart := strings.Index(page, `id="config-sidebar"`)
	configEnd := strings.Index(page, `id="workload-sidebar"`)
	if configStart < 0 || configEnd <= configStart {
		t.Fatal("configuration sidebar boundaries are missing")
	}
	config := page[configStart:configEnd]
	for _, fragment := range []string{
		`id="clear-tenants"`,
		`Clear all tenants`,
		`getJSON('/api/v1/admin/tenants',{method:'DELETE'})`,
		`Clearing ${fmt(count)} tenants in one Raft commit`,
		`unfinished tasks are still visible`,
		`Completed task history, nodes and Worker capacity are preserved`,
	} {
		if !strings.Contains(page, fragment) {
			t.Errorf("bulk tenant clear contract is missing %q", fragment)
		}
	}
	if !strings.Contains(config, `id="clear-tenants"`) {
		t.Fatal("bulk tenant clear is outside the configuration sidebar")
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
