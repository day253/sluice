package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
	metricspkg "github.com/day253/sluice/pkg/metrics"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

type mockRaft struct {
	leader bool
	fsm    *raftpkg.FSM
}

func TestResourceExhaustedMapsToHTTPTooManyRequests(t *testing.T) {
	recorder := httptest.NewRecorder()
	(&Handler{}).writeGRPCError(
		recorder,
		status.Error(codes.ResourceExhausted, "submission apply backlog is full"),
	)
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("HTTP status = %d, want 429", recorder.Code)
	}
	var body types.ErrorResponse
	if err := json.NewDecoder(recorder.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Code != http.StatusTooManyRequests ||
		body.Error != "submission apply backlog is full" {
		t.Fatalf("HTTP error body = %+v", body)
	}
}

func (m *mockRaft) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	// Route to FSM so state is consistent.
	response := m.fsm.Apply(&hashicorpraft.Log{Data: cmd, Type: hashicorpraft.LogCommand})
	return &mockResult{response: response}
}

func (m *mockRaft) IsLeader() bool     { return m.leader }
func (m *mockRaft) LeaderAddr() string { return "mock:7000" }

type mockResult struct{ response interface{} }

func (r *mockResult) Error() error          { return nil }
func (r *mockResult) Response() interface{} { return r.response }

func setupHandler(t *testing.T) (*Handler, *raftpkg.FSM, *queue.MemoryQueue) {
	t.Helper()

	fsm := raftpkg.NewFSM(zap.NewNop())
	q := queue.NewMemoryQueue()
	raft := &mockRaft{leader: true, fsm: fsm}
	pool := worker.NewPool("n1", q, fsm, raft, &mockProcessor{}, zap.NewNop())

	// Seed a tenant so task submission works.
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "company-a", MaxWorkers: 100})

	grpcSvc := grpcpkg.NewService("n1", q, fsm, raft, pool, zap.NewNop())
	handler := NewHandler("n1", grpcSvc, zap.NewNop())
	return handler, fsm, q
}

func applyOp(fsm *raftpkg.FSM, op string, data interface{}) {
	cmd := raftpkg.MustMarshalCommand(op, data)
	_ = fsm.Apply(&hashicorpraft.Log{Data: cmd, Type: hashicorpraft.LogCommand})
}

type mockProcessor struct{}

func (p *mockProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	return "ok", nil
}

type recordingMetricsCollector struct {
	name          string
	includePrefix string
	excludePrefix string
	current       bool
}

func (c *recordingMetricsCollector) Query(name, includePrefix, excludePrefix string) ([]MetricsData, int) {
	c.name = name
	c.includePrefix = includePrefix
	c.excludePrefix = excludePrefix
	return []MetricsData{{Name: "unfinished:company-a"}}, 1
}

func (c *recordingMetricsCollector) QueryCurrent(
	name, includePrefix, excludePrefix string,
) ([]MetricsData, int) {
	c.name = name
	c.includePrefix = includePrefix
	c.excludePrefix = excludePrefix
	c.current = true
	return []MetricsData{{
		Name: "allocated-workers:tenant:company-a", Secs: []int64{7},
	}}, 1
}

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func newRouter(h *Handler) *mux.Router {
	r := mux.NewRouter()
	h.RegisterRoutes(r)
	return r
}

func TestLoadGeneratorProxyPreservesCompactRunRequest(t *testing.T) {
	var upstreamMethod, upstreamPath, upstreamBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamMethod = r.Method
		upstreamPath = r.URL.Path
		data := new(bytes.Buffer)
		if _, err := data.ReadFrom(r.Body); err != nil {
			t.Fatal(err)
		}
		upstreamBody = data.String()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"run":{"id":"load-1","status":"preparing"}}`))
	}))
	defer upstream.Close()

	handler := NewHandler("control-0", nil, zap.NewNop())
	if err := handler.SetLoadGeneratorAddress(upstream.URL); err != nil {
		t.Fatal(err)
	}
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	requestBody := `{"operation":"load","tenantIds":["acme"],"options":{"tasksPerTenant":5}}`
	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(
			http.MethodPost,
			"/api/v1/load-runs",
			strings.NewReader(requestBody),
		),
	)
	if recorder.Code != http.StatusAccepted ||
		upstreamMethod != http.MethodPost ||
		upstreamPath != "/api/v1/load-runs" ||
		upstreamBody != requestBody {
		t.Fatalf(
			"proxy status=%d method=%q path=%q body=%q",
			recorder.Code, upstreamMethod, upstreamPath, upstreamBody,
		)
	}
}

func TestLoadGeneratorProxyReturnsUnavailableWhenNotConfigured(t *testing.T) {
	handler := NewHandler("control-0", nil, zap.NewNop())
	router := mux.NewRouter()
	handler.RegisterRoutes(router)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/api/v1/load-runs/current", nil),
	)
	if recorder.Code != http.StatusServiceUnavailable ||
		!strings.Contains(recorder.Body.String(), "not configured") {
		t.Fatalf("unconfigured status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

// ---------------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------------

func TestHealthEndpoint(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	req := httptest.NewRequest("GET", "/api/v1/health", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("health: status = %d, want 200", rec.Code)
	}

	var body map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &body)
	if body["node_id"] != "n1" {
		t.Errorf("health: node_id = %v, want n1", body["node_id"])
	}
}

func TestStatelessWorkerRegistrationHasDedicatedNonRaftBoundary(t *testing.T) {
	h, _, _ := setupHandler(t)
	var registered types.NodeInfo
	calls := 0
	h.SetWorkerRegisterFunc(func(info types.NodeInfo) error {
		calls++
		registered = info
		return nil
	})
	router := newRouter(h)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/workers/register", bytes.NewReader(mustMarshal(map[string]interface{}{
		"node_id": "worker-1", "session_id": "session-1",
		"http_address": "127.0.0.1:9001", "total_workers": 100,
	})))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("worker registration status = %d body=%s", rec.Code, rec.Body.String())
	}
	if calls != 1 || registered.ID != "worker-1" || registered.Role != types.NodeRoleWorker ||
		registered.SessionID != "session-1" || registered.TotalWorkers != 100 || registered.RaftAddress != "" {
		t.Fatalf("worker registration = calls:%d info:%+v", calls, registered)
	}

	bad := httptest.NewRequest(http.MethodPost, "/api/v1/cluster/workers/register",
		bytes.NewReader([]byte(`{"node_id":"worker-2","total_workers":0}`)))
	badRec := httptest.NewRecorder()
	router.ServeHTTP(badRec, bad)
	if badRec.Code != http.StatusBadRequest || calls != 1 {
		t.Fatalf("invalid registration status=%d calls=%d", badRec.Code, calls)
	}
}

func TestRaftStatusEndpointReportsBoundedMembership(t *testing.T) {
	h, _, _ := setupHandler(t)
	h.SetRaftStatusFunc(func() (raftpkg.MembershipStatus, error) {
		return raftpkg.MembershipStatus{
			LeaderID: "node-0", Voters: []string{"node-0", "node-1", "node-2"},
			Nonvoters: []string{"node-3", "node-4"},
		}, nil
	})
	rec := httptest.NewRecorder()
	newRouter(h).ServeHTTP(rec, httptest.NewRequest("GET", "/api/v1/admin/raft", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("raft status code = %d, want 200", rec.Code)
	}
	var status raftpkg.MembershipStatus
	if err := json.Unmarshal(rec.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if status.LeaderID != "node-0" || len(status.Voters) != 3 || len(status.Nonvoters) != 2 {
		t.Fatalf("raft status = %+v", status)
	}
}

func TestCapabilitiesAdvertiseAtomicWorkerCapacityOperation(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/capabilities",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("capabilities status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var capabilities types.AdminCapabilities
	if err := json.Unmarshal(recorder.Body.Bytes(), &capabilities); err != nil {
		t.Fatal(err)
	}
	if len(capabilities.RaftOperations) != 1 ||
		capabilities.RaftOperations[0] != raftpkg.OpSetWorkerCapacities {
		t.Fatalf("capabilities = %+v", capabilities)
	}
}

func TestPerformanceEndpointReturnsConfiguredLeaderDiagnostics(t *testing.T) {
	h, _, _ := setupHandler(t)
	localOnly := false
	includeHistory := false
	h.SetPerformanceFunc(func(_ context.Context, local, history bool) (metricspkg.PerformanceDiagnostics, error) {
		localOnly = local
		includeHistory = history
		return metricspkg.PerformanceDiagnostics{
			NodeID: "node-0",
			Current: metricspkg.PerformanceSnapshot{Raft: map[string]metricspkg.RaftOperationSnapshot{
				raftpkg.OpClaimBatch: {Applies: 3, Items: 384, AverageBatch: 128},
			}},
		}, nil
	})
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/admin/performance?local=1", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("performance status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if !localOnly {
		t.Fatal("performance endpoint did not preserve local-only proxy guard")
	}
	if !includeHistory {
		t.Fatal("performance endpoint did not include history by default")
	}
	var diagnostics metricspkg.PerformanceDiagnostics
	if err := json.Unmarshal(recorder.Body.Bytes(), &diagnostics); err != nil {
		t.Fatal(err)
	}
	if diagnostics.NodeID != "node-0" || diagnostics.Current.Raft[raftpkg.OpClaimBatch].Items != 384 {
		t.Fatalf("performance diagnostics = %+v", diagnostics)
	}
}

func TestPerformanceEndpointCanReturnCurrentSnapshotWithoutHistory(t *testing.T) {
	h, _, _ := setupHandler(t)
	includeHistory := true
	h.SetPerformanceFunc(func(_ context.Context, _, history bool) (metricspkg.PerformanceDiagnostics, error) {
		includeHistory = history
		return metricspkg.PerformanceDiagnostics{NodeID: "node-0"}, nil
	})
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/admin/performance?history=0", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("performance status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if includeHistory {
		t.Fatal("performance endpoint ignored history=0")
	}
}

func TestAutoscalingEndpointSeparatesReplicatedQueueAndLeaderSoftSignals(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "worker-live", Role: types.NodeRoleWorker,
		Status: types.NodeStatusUp, TotalWorkers: 100,
	})
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "worker-down", Role: types.NodeRoleWorker,
		Status: types.NodeStatusDown, TotalWorkers: 100,
	})
	applyOp(fsm, raftpkg.OpNodeDown, raftpkg.NodeDownData{ID: "worker-down"})
	applyOp(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"worker-live": {
			NodeID: "worker-live", Tenants: map[string]int{"company-a": 30},
		},
		"worker-down": {
			NodeID: "worker-down", Tenants: map[string]int{"company-a": 99},
		},
	})
	applyOp(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "pending", TenantID: "company-a", Payload: `{}`,
	})
	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "running", TenantID: "company-a",
		NodeID: "worker-live", Payload: `{}`,
	})
	startedAt := time.Unix(1000, 0).UTC()
	h.SetPerformanceFunc(func(
		_ context.Context, _, history bool,
	) (metricspkg.PerformanceDiagnostics, error) {
		if history {
			t.Fatal("autoscaling endpoint requested bounded performance history")
		}
		return metricspkg.PerformanceDiagnostics{
			NodeID: "leader-0",
			Current: metricspkg.PerformanceSnapshot{
				StartedAt: startedAt,
				Raft: map[string]metricspkg.RaftOperationSnapshot{
					raftpkg.OpCreateTask:      {Items: 2},
					raftpkg.OpCreateTaskBatch: {Items: 12},
					raftpkg.OpFailTask:        {Items: 1},
					raftpkg.OpCompleteBatch:   {Items: 7},
				},
				Scheduler: metricspkg.SchedulerSnapshot{
					WorkerLoads: map[string]metricspkg.WorkerLoadSnapshot{
						"worker-live": {
							CPUUtilizationMillis: 640,
							RunningTasks:         9, Capacity: 30,
							ObservedAt: time.Now().UTC(),
						},
						"worker-down": {
							CPUUtilizationMillis: 1000,
							RunningTasks:         99, Capacity: 100,
							ObservedAt: time.Now().UTC(),
						},
					},
				},
			},
		}, nil
	})

	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/api/v1/admin/autoscaling", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("autoscaling status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot types.AutoscalingSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.TaskBreakdownValid || snapshot.UnfinishedTasks != 2 || snapshot.PendingTasks != 1 ||
		snapshot.RunningTasks != 1 || snapshot.OldestPendingAgeMillis < 0 {
		t.Fatalf("task pressure = %+v", snapshot)
	}
	if snapshot.WorkerInstances != 1 || snapshot.WorkerCapacity != 100 ||
		snapshot.AllocatedWorkers != 30 {
		t.Fatalf("live capacity snapshot included stale Worker: %+v", snapshot)
	}
	if !snapshot.ExecutionSignalsValid || snapshot.ReportingWorkers != 1 ||
		snapshot.ExecutingTasks != 9 || snapshot.AverageWorkerCPUMillis != 640 ||
		snapshot.MaxWorkerCPUMillis != 640 {
		t.Fatalf("execution pressure = %+v", snapshot)
	}
	if !snapshot.RateCountersValid || snapshot.TelemetrySource != "leader-0" ||
		!snapshot.TelemetryStartedAt.Equal(startedAt) ||
		snapshot.SubmittedTasksTotal != 14 || snapshot.CompletedTasksTotal != 8 {
		t.Fatalf("rate counters = %+v", snapshot)
	}
}

func TestAutoscalingEndpointKeepsQueueScaleUpSignalsWhenPerformanceUnavailable(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	applyOp(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "pending", TenantID: "company-a", Payload: `{}`,
	})
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(
		recorder,
		httptest.NewRequest(http.MethodGet, "/api/v1/admin/autoscaling", nil),
	)
	if recorder.Code != http.StatusOK {
		t.Fatalf("autoscaling status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var snapshot types.AutoscalingSnapshot
	if err := json.Unmarshal(recorder.Body.Bytes(), &snapshot); err != nil {
		t.Fatal(err)
	}
	if !snapshot.TaskBreakdownValid || snapshot.PendingTasks != 1 || snapshot.ExecutionSignalsValid ||
		snapshot.RateCountersValid || snapshot.ExecutionSignalsError == "" {
		t.Fatalf("degraded autoscaling snapshot = %+v", snapshot)
	}
}

func TestMetricsEndpointCanExcludePerformanceHistories(t *testing.T) {
	h, _, _ := setupHandler(t)
	collector := &recordingMetricsCollector{}
	h.SetCollector(collector)
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/metrics?performance=0", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if collector.name != "" || collector.includePrefix != "" || collector.excludePrefix != "performance:" {
		t.Fatalf("metrics query name=%q include=%q exclude=%q", collector.name, collector.includePrefix, collector.excludePrefix)
	}
}

func TestMetricsEndpointCanFilterHistoriesByPrefix(t *testing.T) {
	h, _, _ := setupHandler(t)
	collector := &recordingMetricsCollector{}
	h.SetCollector(collector)
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(recorder, httptest.NewRequest("GET", "/api/v1/metrics?prefix=unfinished%3A&performance=0", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if collector.name != "" || collector.includePrefix != "unfinished:" || collector.excludePrefix != "performance:" {
		t.Fatalf("metrics query name=%q include=%q exclude=%q", collector.name, collector.includePrefix, collector.excludePrefix)
	}
}

func TestMetricsEndpointCanReturnCurrentValuesWithoutCopyingHistory(t *testing.T) {
	h, _, _ := setupHandler(t)
	collector := &recordingMetricsCollector{}
	h.SetCollector(collector)
	recorder := httptest.NewRecorder()
	newRouter(h).ServeHTTP(recorder, httptest.NewRequest(
		http.MethodGet,
		"/api/v1/metrics?prefix=allocated-workers%3Atenant%3A&performance=0&current=1",
		nil,
	))
	if recorder.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if !collector.current || collector.name != "" ||
		collector.includePrefix != "allocated-workers:tenant:" ||
		collector.excludePrefix != "performance:" {
		t.Fatalf(
			"current metrics query current=%t name=%q include=%q exclude=%q",
			collector.current, collector.name,
			collector.includePrefix, collector.excludePrefix,
		)
	}
	var data []MetricsData
	if err := json.Unmarshal(recorder.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data) != 1 || len(data[0].Secs) != 1 || data[0].Secs[0] != 7 ||
		len(data[0].Mins)+len(data[0].Hours)+len(data[0].Days) != 0 {
		t.Fatalf("current metrics response = %+v", data)
	}
}

// ---------------------------------------------------------------------------
// Task submission
// ---------------------------------------------------------------------------

func TestSubmitTask_Success(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	body := mustMarshal(types.TaskSubmitRequest{
		TenantID: "company-a",
		Payload:  json.RawMessage(`{"url":"https://example.com"}`),
	})
	req := httptest.NewRequest("POST", "/api/v1/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Errorf("submit: status = %d, want 202\nbody: %s", rec.Code, rec.Body.String())
	}

	var resp types.TaskResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.TaskID == "" {
		t.Error("submit: task_id is empty")
	}
	if resp.Status != types.TaskStatusPending {
		t.Errorf("submit: status = %s, want pending", resp.Status)
	}
}

func TestSubmitBatch_Success(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	body := mustMarshal(types.BatchTaskSubmitRequest{Tasks: []types.TaskSubmitRequest{
		{TenantID: "company-a", Payload: json.RawMessage(`{"n":1}`)},
		{TenantID: "company-a", Payload: json.RawMessage(`{"n":2}`)},
	}})
	req := httptest.NewRequest("POST", "/api/v1/tasks/batch", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("batch submit: status = %d, want 202\nbody: %s", rec.Code, rec.Body.String())
	}
	var resp types.BatchTaskResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("batch response length = %d, want 2", len(resp.Tasks))
	}
	for _, task := range resp.Tasks {
		if task.TaskID == "" || fsm.GetTask(task.TaskID) == nil {
			t.Fatalf("batch task not persisted: %+v", task)
		}
	}
}

func TestSubmitTask_MissingTenant(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	body := mustMarshal(types.TaskSubmitRequest{
		TenantID: "nonexistent",
		Payload:  json.RawMessage(`{}`),
	})
	req := httptest.NewRequest("POST", "/api/v1/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 for nonexistent tenant, got %d", rec.Code)
	}
}

func TestSubmitTask_NoTenantID(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	body := mustMarshal(map[string]string{"payload": "x"})
	req := httptest.NewRequest("POST", "/api/v1/tasks", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty tenant_id, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Task query
// ---------------------------------------------------------------------------

func TestGetTask_Inflight(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	// Manually create an inflight task in the FSM.
	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "test-inflight", TenantID: "company-a", NodeID: "n1", Payload: `{}`,
	})

	req := httptest.NewRequest("GET", "/api/v1/tasks/test-inflight", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("get task: status = %d, want 200", rec.Code)
	}

	var resp types.TaskResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Status != types.TaskStatusInflight {
		t.Errorf("get task: status = %s, want inflight", resp.Status)
	}
}

func TestGetTask_Completed(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "test-done", TenantID: "company-a", NodeID: "n1", Payload: `{}`,
	})
	applyOp(fsm, raftpkg.OpCompleteTask, raftpkg.CompleteTaskData{
		TaskID: "test-done", TenantID: "company-a", Result: "hello",
	})

	req := httptest.NewRequest("GET", "/api/v1/tasks/test-done", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	var resp types.TaskResponse
	json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Status != types.TaskStatusDone {
		t.Errorf("get task: status = %s, want done", resp.Status)
	}
	if resp.Result != "hello" {
		t.Errorf("get task: result = %s, want hello", resp.Result)
	}
}

func TestGetTask_NotFound(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	req := httptest.NewRequest("GET", "/api/v1/tasks/nonexistent", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("get task: status = %d, want 404", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Admin — tenants
// ---------------------------------------------------------------------------

func TestUpsertTenant(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	body := mustMarshal(map[string]interface{}{
		"name":        "NewCo",
		"max_workers": 50,
	})
	req := httptest.NewRequest("PUT", "/api/v1/admin/tenants/newco", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("upsert tenant: status = %d, want 200", rec.Code)
	}

	// Verify in FSM.
	tc, ok := fsm.GetTenant("newco")
	if !ok {
		t.Fatal("tenant not found in FSM")
	}
	if tc.MaxWorkers != 50 {
		t.Errorf("max_workers = %d, want 50", tc.MaxWorkers)
	}
}

func TestListTenants(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	req := httptest.NewRequest("GET", "/api/v1/admin/tenants", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("list tenants: status = %d, want 200", rec.Code)
	}

	var tenants map[string]*types.TenantConfig
	json.Unmarshal(rec.Body.Bytes(), &tenants)
	if _, ok := tenants["company-a"]; !ok {
		t.Error("company-a not found in tenant list")
	}
}

func TestDeleteTenant(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	// First create a tenant to delete.
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "to-delete", MaxWorkers: 10})

	req := httptest.NewRequest("DELETE", "/api/v1/admin/tenants/to-delete", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("delete tenant: status = %d, want 200", rec.Code)
	}
	if _, ok := fsm.GetTenant("to-delete"); ok {
		t.Error("tenant should have been deleted")
	}
}

func TestDeleteAllTenantsRejectsUnfinishedThenDeletesInOneRequest(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{
		ID: "company-b", MaxWorkers: 20,
	})
	applyOp(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "pending-clear", TenantID: "company-a", Payload: `{}`,
	})

	blocked := httptest.NewRecorder()
	router.ServeHTTP(
		blocked,
		httptest.NewRequest(http.MethodDelete, "/api/v1/admin/tenants", nil),
	)
	if blocked.Code != http.StatusConflict ||
		!bytes.Contains(blocked.Body.Bytes(), []byte("1 unfinished tasks")) {
		t.Fatalf("blocked delete all status=%d body=%s", blocked.Code, blocked.Body.String())
	}
	if got := len(fsm.GetAllTenants()); got != 2 {
		t.Fatalf("blocked delete all retained %d tenants, want 2", got)
	}

	applyOp(fsm, raftpkg.OpCompleteTask, raftpkg.CompleteTaskData{
		TaskID: "pending-clear", TenantID: "company-a", Result: "done",
	})
	deleted := httptest.NewRecorder()
	router.ServeHTTP(
		deleted,
		httptest.NewRequest(http.MethodDelete, "/api/v1/admin/tenants", nil),
	)
	if deleted.Code != http.StatusOK ||
		!bytes.Contains(deleted.Body.Bytes(), []byte(`"deleted":2`)) {
		t.Fatalf("delete all status=%d body=%s", deleted.Code, deleted.Body.String())
	}
	if got := len(fsm.GetAllTenants()); got != 0 {
		t.Fatalf("delete all retained %d tenants", got)
	}
}

// ---------------------------------------------------------------------------
// Admin — cluster
// ---------------------------------------------------------------------------

func TestListNodes(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "n1", Role: types.NodeRoleWorker, TotalWorkers: 250,
		CapacityOverride: 250,
	})

	req := httptest.NewRequest("GET", "/api/v1/admin/nodes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("list nodes: status = %d, want 200", rec.Code)
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(`"capacity_override":250`)) {
		t.Fatalf("list nodes omitted capacity override: %s", rec.Body.String())
	}
}

func TestSetWorkerCapacityEndpointValidatesAndDelegates(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "worker-1", Role: types.NodeRoleWorker, TotalWorkers: 100,
	})
	var gotNodeID string
	var gotWorkers int
	calls := 0
	h.SetWorkerCapacityFunc(func(
		_ context.Context,
		nodeID string,
		totalWorkers int,
	) (types.WorkerCapacityResponse, error) {
		calls++
		gotNodeID, gotWorkers = nodeID, totalWorkers
		return types.WorkerCapacityResponse{
			NodeID: nodeID, TotalWorkers: totalWorkers, CapacityOverride: totalWorkers,
		}, nil
	})
	router := newRouter(h)

	request := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/admin/nodes/worker-1/capacity",
		bytes.NewReader([]byte(`{"total_workers":250}`)),
	)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || calls != 1 ||
		gotNodeID != "worker-1" || gotWorkers != 250 {
		t.Fatalf(
			"capacity update status=%d calls=%d node=%q workers=%d body=%s",
			recorder.Code, calls, gotNodeID, gotWorkers, recorder.Body.String(),
		)
	}
	var response types.WorkerCapacityResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.TotalWorkers != 250 || response.CapacityOverride != 250 {
		t.Fatalf("capacity response = %+v", response)
	}

	for name, test := range map[string]struct {
		path       string
		body       string
		wantStatus int
	}{
		"zero": {
			path: "/api/v1/admin/nodes/worker-1/capacity",
			body: `{"total_workers":0}`, wantStatus: http.StatusBadRequest,
		},
		"over maximum": {
			path: "/api/v1/admin/nodes/worker-1/capacity",
			body: `{"total_workers":1001}`, wantStatus: http.StatusBadRequest,
		},
		"missing": {
			path: "/api/v1/admin/nodes/missing/capacity",
			body: `{"total_workers":10}`, wantStatus: http.StatusNotFound,
		},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPut, test.path, bytes.NewReader([]byte(test.body)),
			)
			out := httptest.NewRecorder()
			router.ServeHTTP(out, req)
			if out.Code != test.wantStatus {
				t.Fatalf(
					"status=%d want=%d body=%s",
					out.Code, test.wantStatus, out.Body.String(),
				)
			}
		})
	}
	if calls != 1 {
		t.Fatalf("invalid requests reached mutation callback %d times", calls)
	}
}

func TestSetAllWorkerCapacitiesEndpointValidatesAndDelegatesOnce(t *testing.T) {
	h, _, _ := setupHandler(t)
	calls := 0
	gotWorkers := 0
	h.SetAllWorkerCapacitiesFunc(func(
		_ context.Context,
		totalWorkers int,
	) (types.WorkerCapacityBatchResponse, error) {
		calls++
		gotWorkers = totalWorkers
		return types.WorkerCapacityBatchResponse{
			TotalWorkers: totalWorkers,
			Updated:      2,
			Nodes: []types.WorkerCapacityResponse{
				{
					NodeID: "worker-0", TotalWorkers: totalWorkers,
					CapacityOverride: totalWorkers,
				},
				{
					NodeID: "worker-1", TotalWorkers: totalWorkers,
					CapacityOverride: totalWorkers,
				},
			},
		}, nil
	})
	router := newRouter(h)

	request := httptest.NewRequest(
		http.MethodPut,
		"/api/v1/admin/nodes/capacity",
		bytes.NewReader([]byte(`{"total_workers":300}`)),
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || calls != 1 || gotWorkers != 300 {
		t.Fatalf(
			"all-worker capacity status=%d calls=%d workers=%d body=%s",
			recorder.Code, calls, gotWorkers, recorder.Body.String(),
		)
	}
	var response types.WorkerCapacityBatchResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Updated != 2 || len(response.Nodes) != 2 ||
		response.TotalWorkers != 300 {
		t.Fatalf("all-worker capacity response = %+v", response)
	}

	for name, body := range map[string]string{
		"zero":         `{"total_workers":0}`,
		"over maximum": `{"total_workers":1001}`,
		"invalid JSON": `{`,
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(
				http.MethodPut,
				"/api/v1/admin/nodes/capacity",
				bytes.NewReader([]byte(body)),
			)
			out := httptest.NewRecorder()
			router.ServeHTTP(out, req)
			if out.Code != http.StatusBadRequest {
				t.Fatalf("status=%d want=400 body=%s", out.Code, out.Body.String())
			}
		})
	}
	if calls != 1 {
		t.Fatalf("invalid all-worker requests reached mutation callback %d times", calls)
	}
}

func TestGetAllocations(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	applyOp(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"n1": {
			NodeID:   "n1",
			Tenants:  map[string]int{"company-a": 53},
			Borrowed: map[string]int{"company-a": 3},
		},
	})

	req := httptest.NewRequest("GET", "/api/v1/admin/allocations", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("allocations: status = %d, want 200", rec.Code)
	}
	var response types.AllocationResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode allocations: %v", err)
	}
	if len(response.Nodes) != 1 || response.Nodes[0].Borrowed["company-a"] != 3 {
		t.Fatalf("allocation borrowed mirror = %+v, want company-a=3", response.Nodes)
	}
}

// ---------------------------------------------------------------------------
// Wait (long-poll) endpoint
// ---------------------------------------------------------------------------

func TestWaitTask_CompletedImmediately(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	// Task already done.
	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "instant", TenantID: "company-a", NodeID: "n1", Payload: `{}`,
	})
	applyOp(fsm, raftpkg.OpCompleteTask, raftpkg.CompleteTaskData{
		TaskID: "instant", TenantID: "company-a", Result: "fast",
	})

	req := httptest.NewRequest("GET", "/api/v1/tasks/instant/wait?timeout=1s", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("wait: status = %d, want 200", rec.Code)
	}
}

func TestWaitTask_Timeout(t *testing.T) {
	h, _, _ := setupHandler(t)
	router := newRouter(h)

	req := httptest.NewRequest("GET", "/api/v1/tasks/nonexistent/wait?timeout=100ms", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestTimeout {
		t.Errorf("wait timeout: status = %d, want 408", rec.Code)
	}
}
