package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
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

func mustMarshal(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func newRouter(h *Handler) *mux.Router {
	r := mux.NewRouter()
	h.RegisterRoutes(r)
	return r
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

// ---------------------------------------------------------------------------
// Admin — cluster
// ---------------------------------------------------------------------------

func TestListNodes(t *testing.T) {
	h, fsm, _ := setupHandler(t)
	router := newRouter(h)

	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{ID: "n1"})

	req := httptest.NewRequest("GET", "/api/v1/admin/nodes", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("list nodes: status = %d, want 200", rec.Code)
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
