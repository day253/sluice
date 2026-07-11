package raft

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/distributed-rate-limiting/pkg/types"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newTestFSM(t *testing.T) *FSM {
	t.Helper()
	return NewFSM(zap.NewNop())
}

func applyCmd(t *testing.T, fsm *FSM, op string, data interface{}) interface{} {
	t.Helper()
	cmd := MustMarshalCommand(op, data)
	resp := fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	if err, ok := resp.(error); ok {
		t.Fatalf("apply %s: %v", op, err)
	}
	return resp
}

// ---------------------------------------------------------------------------
// Tenant tests
// ---------------------------------------------------------------------------

func TestApplyUpsertTenant(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{
		ID: "tenant-a", Name: "A", MaxWorkers: 100,
	})

	tc, ok := fsm.GetTenant("tenant-a")
	if !ok {
		t.Fatal("tenant not found")
	}
	if tc.MaxWorkers != 100 {
		t.Errorf("MaxWorkers = %d, want 100", tc.MaxWorkers)
	}
	if tc.Name != "A" {
		t.Errorf("Name = %s, want A", tc.Name)
	}
}

func TestApplyUpsertTenant_Update(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 10})
	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 20})

	tc, _ := fsm.GetTenant("t1")
	if tc.MaxWorkers != 20 {
		t.Errorf("MaxWorkers = %d, want 20 after update", tc.MaxWorkers)
	}
	if tc.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestApplyUpsertTenant_RejectsZeroWorkers(t *testing.T) {
	fsm := newTestFSM(t)
	cmd := MustMarshalCommand(OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 0})
	resp := fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	// Should return an error.
	if _, ok := resp.(error); !ok {
		t.Error("expected error for max_workers=0, got nil")
	}
	if _, ok := fsm.GetTenant("t1"); ok {
		t.Error("tenant with 0 max_workers should not have been created")
	}
}

func TestApplyDeleteTenant(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 10})
	applyCmd(t, fsm, OpDeleteTenant, DeleteTenantData{ID: "t1"})

	if _, ok := fsm.GetTenant("t1"); ok {
		t.Error("tenant should have been deleted")
	}
}

// ---------------------------------------------------------------------------
// Node tests
// ---------------------------------------------------------------------------

func TestApplyNodeUpAndDown(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpNodeUp, types.NodeInfo{ID: "n1", TotalWorkers: 50})
	nodes := fsm.GetActiveNodes()
	if len(nodes) != 1 {
		t.Fatalf("expected 1 active node, got %d", len(nodes))
	}
	if nodes[0].Status != types.NodeStatusUp {
		t.Errorf("status = %s, want up", nodes[0].Status)
	}

	applyCmd(t, fsm, OpNodeDown, NodeDownData{ID: "n1"})
	nodes = fsm.GetActiveNodes()
	if len(nodes) != 0 {
		t.Errorf("expected 0 active nodes after down, got %d", len(nodes))
	}
}

// ---------------------------------------------------------------------------
// Task lifecycle tests
// ---------------------------------------------------------------------------

func TestTaskClaimCompleteDone(t *testing.T) {
	fsm := newTestFSM(t)

	// Claim a task.
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{"x":1}`,
	})

	task := fsm.GetTask("task-1")
	if task == nil {
		t.Fatal("task not found after claim")
	}
	if task.Status != types.TaskStatusInflight {
		t.Errorf("status = %s, want inflight", task.Status)
	}
	if task.NodeID != "n1" {
		t.Errorf("node_id = %s, want n1", task.NodeID)
	}

	// Complete.
	applyCmd(t, fsm, OpCompleteTask, CompleteTaskData{
		TaskID: "task-1", TenantID: "t1", Result: "OK",
	})

	if fsm.GetTask("task-1") != nil {
		t.Error("task should be gone from inflight")
	}
	result := fsm.GetResult("task-1")
	if result == nil {
		t.Fatal("result not found")
	}
	if result.Status != types.TaskStatusDone {
		t.Errorf("status = %s, want done", result.Status)
	}
	if result.Result != "OK" {
		t.Errorf("result = %s, want OK", result.Result)
	}
}

func TestTaskClaimCompleteFailed(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})
	applyCmd(t, fsm, OpFailTask, CompleteTaskData{
		TaskID: "task-1", TenantID: "t1", Error: "timeout",
	})

	result := fsm.GetResult("task-1")
	if result.Status != types.TaskStatusFailed {
		t.Errorf("status = %s, want failed", result.Status)
	}
	if result.Error != "timeout" {
		t.Errorf("error = %s, want timeout", result.Error)
	}
}

func TestTaskClaimDuplicateRejected(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})

	// Second claim should fail — call Apply directly since it returns error.
	cmd := MustMarshalCommand(OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n2", Payload: `{}`,
	})
	resp := fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	if _, ok := resp.(error); !ok {
		t.Error("expected error for duplicate claim, got nil")
	}
}

// ---------------------------------------------------------------------------
// Node-down re-queue tests
// ---------------------------------------------------------------------------

func TestNodeDownRequeuesInflightTasks(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpNodeUp, types.NodeInfo{ID: "n1"})
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-2", TenantID: "t2", NodeID: "n2", Payload: `{}`,
	})
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-3", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})

	// n1 goes down → task-1 and task-3 should become pending.
	applyCmd(t, fsm, OpNodeDown, NodeDownData{ID: "n1"})

	// task-2 (on n2) should still be inflight.
	t2 := fsm.GetTask("task-2")
	if t2 == nil || t2.Status != types.TaskStatusInflight {
		t.Errorf("task-2 should still be inflight, got %v", t2)
	}

	// task-1 and task-3 should be pending.
	pending := fsm.FindPendingTasks("t1")
	if len(pending) != 2 {
		t.Errorf("expected 2 pending tasks for t1, got %d", len(pending))
	}
}

// ---------------------------------------------------------------------------
// Allocation tests
// ---------------------------------------------------------------------------

func TestApplyUpdateAllocation(t *testing.T) {
	fsm := newTestFSM(t)

	allocs := map[string]*types.NodeAllocation{
		"n1": {NodeID: "n1", Tenants: map[string]int{"a": 10, "b": 40}},
	}
	applyCmd(t, fsm, OpUpdateAllocation, allocs)

	result, ok := fsm.GetAllocation("n1")
	if !ok {
		t.Fatal("allocation not found")
	}
	if result.Tenants["a"] != 10 || result.Tenants["b"] != 40 {
		t.Errorf("allocation mismatch: %v", result.Tenants)
	}
}

// ---------------------------------------------------------------------------
// Snapshot / Restore
// ---------------------------------------------------------------------------

func TestSnapshotRestore_RoundTrip(t *testing.T) {
	fsm := newTestFSM(t)

	// Seed some state.
	applyCmd(t, fsm, OpNodeUp, types.NodeInfo{ID: "n1", TotalWorkers: 50})
	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 10})
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})

	// Snapshot.
	snap, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Persist to buffer.
	sink := &testSnapshotSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("persist: %v", err)
	}

	// Restore into a fresh FSM.
	fsm2 := newTestFSM(t)
	if err := fsm2.Restore(io.NopCloser(&sink.buf)); err != nil {
		t.Fatalf("restore: %v", err)
	}

	// Verify.
	nodes := fsm2.GetActiveNodes()
	if len(nodes) != 1 || nodes[0].ID != "n1" {
		t.Error("nodes not restored correctly")
	}
	tc, ok := fsm2.GetTenant("t1")
	if !ok || tc.MaxWorkers != 10 {
		t.Error("tenant not restored correctly")
	}
	task := fsm2.GetTask("task-1")
	if task == nil || task.Status != types.TaskStatusInflight {
		t.Error("task not restored correctly")
	}

	// Version should be preserved.
	state := fsm2.GetState()
	origState := fsm.GetState()
	if state.Version != origState.Version {
		t.Errorf("version mismatch: %d vs %d", state.Version, origState.Version)
	}
}

// ---------------------------------------------------------------------------
// Read-accessor concurrency smoke test
// ---------------------------------------------------------------------------

func TestConcurrentReads(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 10})

	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 100; j++ {
				fsm.GetTenant("t1")
				fsm.GetAllTenants()
				fsm.GetActiveNodes()
				fsm.CountInflightPerTenant()
			}
			done <- struct{}{}
		}()
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}

// ---------------------------------------------------------------------------
// testSnapshotSink
// ---------------------------------------------------------------------------

type testSnapshotSink struct {
	buf bytes.Buffer
}

func (s *testSnapshotSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *testSnapshotSink) Close() error                { return nil }
func (s *testSnapshotSink) ID() string                  { return "test" }
func (s *testSnapshotSink) Cancel() error               { return nil }
