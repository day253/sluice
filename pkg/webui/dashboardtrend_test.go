package webui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dop251/goja"
)

func dashboardTrendRuntime(t *testing.T) *goja.Runtime {
	t.Helper()
	source, err := content.ReadFile("dashboardtrend.js")
	if err != nil {
		t.Fatal(err)
	}
	runtime := goja.New()
	if _, err := runtime.RunString(string(source)); err != nil {
		t.Fatalf("evaluate dashboard trend module: %v", err)
	}
	return runtime
}

func TestDashboardTrendKeepsBoundedSessionSamples(t *testing.T) {
	runtime := dashboardTrendRuntime(t)
	result := evaluateJSON[struct {
		Length int      `json:"length"`
		First  float64  `json:"first"`
		Last   float64  `json:"last"`
		Keys   []string `json:"keys"`
	}](t, runtime, `(() => {
		const store = {};
		for (let index = 0; index < 80; index++) {
			SluiceDashboardTrend.recordSample(
				store, "worker-1", {cpu:index, running:index*2}, 60
			);
		}
		store["removed"] = {cpu:[1]};
		SluiceDashboardTrend.prune(store, ["worker-1"]);
		return {
			length: store["worker-1"].cpu.length,
			first: store["worker-1"].cpu[0],
			last: store["worker-1"].cpu[59],
			keys: Object.keys(store)
		};
	})()`)
	if result.Length != 60 || result.First != 20 || result.Last != 79 ||
		len(result.Keys) != 1 || result.Keys[0] != "worker-1" {
		t.Fatalf("bounded session trend = %+v", result)
	}
}

func TestDashboardTrendCollapsesHighCardinalitySeries(t *testing.T) {
	runtime := dashboardTrendRuntime(t)
	result := evaluateJSON[struct {
		Length       int       `json:"length"`
		Hidden       int       `json:"hidden"`
		Total        int       `json:"total"`
		OtherName    string    `json:"otherName"`
		OtherCurrent float64   `json:"otherCurrent"`
		OtherLimit   float64   `json:"otherLimit"`
		OtherSecs    []float64 `json:"otherSecs"`
	}](t, runtime, `(() => {
		const history = value => ({
			days:[value], hours:[value], mins:[value], secs:[value,value+1]
		});
		const input = Array.from({length:12}, (_, index) => ({
			id:"worker-"+index,
			name:"worker-"+index,
			current:12-index,
			limit:100,
			history:history(12-index)
		}));
		const result = SluiceDashboardTrend.collapseSeries(input, 8, "Pods", false, "average");
		const other = result.series[result.series.length-1];
		return {
			length:result.series.length,
			hidden:result.hidden,
			total:result.total,
			otherName:other.name,
			otherCurrent:other.current,
			otherLimit:other.limit,
			otherSecs:other.history.secs
		};
	})()`)
	if result.Length != 9 || result.Hidden != 4 || result.Total != 12 ||
		result.OtherName != "Other 4 Pods (avg)" || result.OtherCurrent != 2.5 ||
		result.OtherLimit != 100 ||
		len(result.OtherSecs) != 2 || result.OtherSecs[0] != 2.5 ||
		result.OtherSecs[1] != 3.5 {
		t.Fatalf("collapsed chart series = %+v", result)
	}
}

func TestDashboardTrendDropsEmptySeries(t *testing.T) {
	runtime := dashboardTrendRuntime(t)
	result := evaluateJSON[struct {
		Series int `json:"series"`
	}](t, runtime, `(() => {
		const empty = {
			id:"empty", name:"empty", current:0,
			history:{days:[0],hours:[0],mins:[0],secs:[0,0]}
		};
		return {
			series:SluiceDashboardTrend.collapseSeries([empty], 8, "tenants", true).series.length
		};
	})()`)
	if result.Series != 0 {
		t.Fatalf("empty-series result = %+v", result)
	}
}

func TestDashboardTrendAssetIsServedAsJavaScript(t *testing.T) {
	recorder := httptest.NewRecorder()
	Handler(http.NotFoundHandler()).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/assets/dashboardtrend.js", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET dashboardtrend.js status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); !strings.Contains(got, "javascript") {
		t.Fatalf("Content-Type = %q", got)
	}
	if !strings.Contains(recorder.Body.String(), "var SluiceDashboardTrend") {
		t.Fatal("dashboard trend module is missing")
	}
}
