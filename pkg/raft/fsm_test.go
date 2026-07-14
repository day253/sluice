package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/types"
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

func TestAllocationBorrowedMirrorPersistsAsCurrentSnapshot(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpUpdateAllocation, map[string]*types.NodeAllocation{
		"node-1": {
			NodeID:   "node-1",
			Tenants:  map[string]int{"tenant-a": 8},
			Borrowed: map[string]int{"tenant-a": 3},
		},
	})

	allocation, ok := fsm.GetAllocation("node-1")
	if !ok || allocation.Tenants["tenant-a"] != 8 || allocation.Borrowed["tenant-a"] != 3 {
		t.Fatalf("allocation mirror = %+v, want effective=8 borrowed=3", allocation)
	}
	// Accessors must return a copy so a UI/API caller cannot mutate replicated
	// state while inspecting the current mirror.
	allocation.Borrowed["tenant-a"] = 99
	again, _ := fsm.GetAllocation("node-1")
	if again.Borrowed["tenant-a"] != 3 {
		t.Fatalf("borrowed mirror was mutated through accessor: %+v", again)
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

func TestCompletedTaskCannotBeResurrected(t *testing.T) {
	fsm := newTestFSM(t)

	applyCmd(t, fsm, OpCreateTask, CreateTaskData{
		TaskID: "task-1", TenantID: "t1", Payload: `{}`,
	})
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})
	applyCmd(t, fsm, OpCompleteTask, CompleteTaskData{
		TaskID: "task-1", TenantID: "t1", Result: "OK",
	})

	// A worker on another node may already hold a stale local queue copy. Its
	// late claim must be rejected instead of recreating the finished task.
	claim := MustMarshalCommand(OpClaimTask, ClaimTaskData{
		TaskID: "task-1", TenantID: "t1", NodeID: "n2", Payload: `{}`,
	})
	if resp := fsm.Apply(&raft.Log{Data: claim, Type: raft.LogCommand}); resp == nil {
		t.Fatal("late claim unexpectedly succeeded")
	}

	batch := applyCmd(t, fsm, OpClaimBatch, ClaimBatchData{Tasks: []ClaimTaskData{{
		TaskID: "task-1", TenantID: "t1", NodeID: "n2", Payload: `{}`,
	}}}).(*ClaimBatchResult)
	if len(batch.Claimed) != 0 || len(batch.Failed) != 1 || batch.Failed[0] != "task-1" {
		t.Fatalf("late batch claim = %+v, want task-1 rejected", batch)
	}

	// Duplicate API delivery is idempotent too.
	applyCmd(t, fsm, OpCreateTask, CreateTaskData{
		TaskID: "task-1", TenantID: "t1", Payload: `{}`,
	})
	if task := fsm.GetTask("task-1"); task != nil {
		t.Fatalf("completed task was resurrected: %+v", task)
	}
	if result := fsm.GetResult("task-1"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("completed result was lost: %+v", result)
	}
}

func TestCreateTaskBatchPersistsAllTasksAndIsIdempotent(t *testing.T) {
	fsm := newTestFSM(t)
	batch := CreateTaskBatchData{Tasks: []CreateTaskData{
		{TaskID: "batch-1", TenantID: "tenant-a", Payload: `{"n":1}`, EstimatedDurationMs: 100},
		{TaskID: "batch-2", TenantID: "tenant-b", Payload: `{"n":2}`, EstimatedDurationMs: 10},
	}}
	applyCmd(t, fsm, OpCreateTaskBatch, batch)
	applyCmd(t, fsm, OpCreateTaskBatch, batch)

	for _, want := range batch.Tasks {
		task := fsm.GetTask(want.TaskID)
		if task == nil {
			t.Fatalf("batch task %s missing", want.TaskID)
		}
		if task.TenantID != want.TenantID || task.Payload != want.Payload || task.Status != types.TaskStatusPending {
			t.Fatalf("batch task %s = %+v", want.TaskID, task)
		}
		if task.EstimatedDurationMs != want.EstimatedDurationMs {
			t.Fatalf("batch task %s estimate = %d, want %d", want.TaskID, task.EstimatedDurationMs, want.EstimatedDurationMs)
		}
	}
	if got := fsm.CountUnfinishedPerTenant(); got["tenant-a"] != 1 || got["tenant-b"] != 1 {
		t.Fatalf("unfinished counts after duplicate batch = %+v", got)
	}
}

func TestFindPendingTasksUsesShortestEstimatedDurationFirst(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpCreateTaskBatch, CreateTaskBatchData{Tasks: []CreateTaskData{
		{TaskID: "long", TenantID: "tenant-a", EstimatedDurationMs: 500},
		{TaskID: "unknown", TenantID: "tenant-a"},
		{TaskID: "short", TenantID: "tenant-a", EstimatedDurationMs: 10},
	}})
	pending := fsm.FindPendingTasks("tenant-a")
	if len(pending) != 3 {
		t.Fatalf("pending tasks = %d, want 3", len(pending))
	}
	if pending[0].TaskID != "short" || pending[1].TaskID != "long" || pending[2].TaskID != "unknown" {
		t.Fatalf("pending order = [%s %s %s], want [short long unknown]", pending[0].TaskID, pending[1].TaskID, pending[2].TaskID)
	}
}

func TestRestoreRepairsHistoricalCompletedAndUnfinishedOverlap(t *testing.T) {
	state := types.NewFSMState()
	state.Tasks["historical-duplicate"] = &types.TaskRecord{
		TaskID: "historical-duplicate", TenantID: "globex", Status: types.TaskStatusPending,
	}
	state.Results["historical-duplicate"] = &types.TaskResult{
		TaskID: "historical-duplicate", TenantID: "globex", Status: types.TaskStatusDone,
		CompletedAt: time.Now().UTC().Add(-time.Hour),
	}
	state.ResultOrder = []string{"historical-duplicate"}
	persisted, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}

	fsm := newTestFSM(t)
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(persisted))); err != nil {
		t.Fatal(err)
	}
	if task := fsm.GetTask("historical-duplicate"); task != nil {
		t.Fatalf("historical unfinished copy survived restore: %+v", task)
	}
	if result := fsm.GetResult("historical-duplicate"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("authoritative completed result was lost: %+v", result)
	}
	if unfinished := fsm.CountUnfinishedPerTenant()["globex"]; unfinished != 0 {
		t.Fatalf("unfinished count after repair = %d, want 0", unfinished)
	}
}

func TestClaimRepairsLiveHistoricalCompletedAndUnfinishedOverlap(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "historical-duplicate", TenantID: "globex", NodeID: "old-node", Payload: `{}`,
	})
	applyCmd(t, fsm, OpCompleteTask, CompleteTaskData{
		TaskID: "historical-duplicate", TenantID: "globex", Result: "done",
	})

	// Reproduce the state emitted by the old stale-queue claim behavior.
	fsm.state.Tasks["historical-duplicate"] = &types.TaskRecord{
		TaskID: "historical-duplicate", TenantID: "globex", Status: types.TaskStatusPending,
	}
	batch := applyCmd(t, fsm, OpClaimBatch, ClaimBatchData{Tasks: []ClaimTaskData{{
		TaskID: "historical-duplicate", TenantID: "globex", NodeID: "new-node", Payload: `{}`,
	}}}).(*ClaimBatchResult)
	if len(batch.Claimed) != 0 || len(batch.Failed) != 1 {
		t.Fatalf("historical duplicate claim = %+v, want rejection", batch)
	}
	if task := fsm.GetTask("historical-duplicate"); task != nil {
		t.Fatalf("live historical unfinished copy was not repaired: %+v", task)
	}
	if result := fsm.GetResult("historical-duplicate"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("authoritative completed result was lost: %+v", result)
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

func TestCompleteBatchPreservesFailedStatus(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "failed-batch-task", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})
	applyCmd(t, fsm, OpCompleteBatch, CompleteBatchData{Tasks: []CompleteTaskData{{
		TaskID: "failed-batch-task", TenantID: "t1", Status: types.TaskStatusFailed, Error: "boom",
	}}})

	result := fsm.GetResult("failed-batch-task")
	if result == nil || result.Status != types.TaskStatusFailed || result.Error != "boom" {
		t.Fatalf("failed batch result = %+v", result)
	}
}

func TestRecentResultsStayBounded(t *testing.T) {
	fsm := newTestFSM(t)
	now := time.Now().UTC()

	for i := 0; i <= maxRetainedTaskResults; i++ {
		taskID := fmt.Sprintf("task-%05d", i)
		fsm.state.Tasks[taskID] = &types.TaskRecord{TaskID: taskID, TenantID: "t1"}
		fsm.finishTask(CompleteTaskData{TaskID: taskID, TenantID: "t1", Result: "ok"}, types.TaskStatusDone, now)
	}

	if got := len(fsm.state.Results); got != maxRetainedTaskResults {
		t.Fatalf("retained results = %d, want %d", got, maxRetainedTaskResults)
	}
	if fsm.GetResult("task-00000") != nil {
		t.Fatal("oldest result should have been evicted")
	}
	if fsm.GetResult(fmt.Sprintf("task-%05d", maxRetainedTaskResults)) == nil {
		t.Fatal("newest result should still be queryable")
	}

	// A retried completion no longer has a live task and must not add a result.
	fsm.finishTask(CompleteTaskData{TaskID: "task-00001", TenantID: "t1"}, types.TaskStatusDone, now)
	if got := len(fsm.state.Results); got != maxRetainedTaskResults {
		t.Fatalf("duplicate completion changed retained results to %d", got)
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

func TestExpiredClaimCanBeRequeued(t *testing.T) {
	fsm := newTestFSM(t)
	applyCmd(t, fsm, OpClaimTask, ClaimTaskData{
		TaskID: "stale-task", TenantID: "t1", NodeID: "n1", Payload: `{}`,
	})
	fsm.state.Tasks["stale-task"].ClaimedAt = time.Now().UTC().Add(-10 * time.Minute)

	taskIDs := fsm.FindStaleInflightTaskIDs(time.Now().UTC().Add(-5 * time.Minute))
	if len(taskIDs) != 1 || taskIDs[0] != "stale-task" {
		t.Fatalf("stale task IDs = %v, want [stale-task]", taskIDs)
	}
	applyCmd(t, fsm, OpRequeueTasks, RequeueTasksData{TaskIDs: taskIDs})

	task := fsm.GetTask("stale-task")
	if task == nil || task.Status != types.TaskStatusPending || task.NodeID != "" || !task.ClaimedAt.IsZero() {
		t.Fatalf("requeued task = %+v, want clean pending task", task)
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
				fsm.CountUnfinishedPerTenant()
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
