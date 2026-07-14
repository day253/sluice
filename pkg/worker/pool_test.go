package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockRaftApplier struct {
	mu          sync.Mutex
	appliedCmds [][]byte
}

func (m *mockRaftApplier) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	m.mu.Lock()
	m.appliedCmds = append(m.appliedCmds, cmd)
	m.mu.Unlock()
	return &mockApplyResult{}
}

func (m *mockRaftApplier) IsLeader() bool     { return true }
func (m *mockRaftApplier) LeaderAddr() string { return "mock:7000" }
func (m *mockRaftApplier) appliedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.appliedCmds)
}

type mockApplyResult struct{}

func (r *mockApplyResult) Error() error          { return nil }
func (r *mockApplyResult) Response() interface{} { return nil }

type mockProcessor struct {
	mu        sync.Mutex
	processed []string // task IDs
}

type fsmTaskClient struct {
	fsm *raftpkg.FSM

	mu            sync.Mutex
	activeClaims  int
	maxConcurrent int
}

type stealTrackingClient struct {
	*fsmTaskClient
	mu     sync.Mutex
	stolen bool
}

func (c *stealTrackingClient) ClaimSteal(taskID, tenantID, payload string) (bool, error) {
	c.mu.Lock()
	c.stolen = true
	c.mu.Unlock()
	return c.Claim(taskID, tenantID, payload)
}

func (c *stealTrackingClient) didSteal() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stolen
}

func (c *fsmTaskClient) Claim(taskID, tenantID, payload string) (bool, error) {
	c.mu.Lock()
	c.activeClaims++
	if c.activeClaims > c.maxConcurrent {
		c.maxConcurrent = c.activeClaims
	}
	c.mu.Unlock()
	time.Sleep(40 * time.Millisecond)
	resp := c.fsm.Apply(&hashicorpraft.Log{Data: raftpkg.MustMarshalCommand(raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: taskID, TenantID: tenantID, NodeID: "n1", Payload: payload,
	}), Type: hashicorpraft.LogCommand})
	c.mu.Lock()
	c.activeClaims--
	c.mu.Unlock()
	if err, ok := resp.(error); ok {
		return false, err
	}
	return true, nil
}

func (c *fsmTaskClient) Complete(taskID, tenantID, result, errStr string, failed bool) error {
	op := raftpkg.OpCompleteTask
	if failed {
		op = raftpkg.OpFailTask
	}
	resp := c.fsm.Apply(&hashicorpraft.Log{Data: raftpkg.MustMarshalCommand(op, raftpkg.CompleteTaskData{
		TaskID: taskID, TenantID: tenantID, Result: result, Error: errStr,
	}), Type: hashicorpraft.LogCommand})
	if err, ok := resp.(error); ok {
		return err
	}
	return nil
}

func (c *fsmTaskClient) maxClaims() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxConcurrent
}

func (p *mockProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	p.mu.Lock()
	p.processed = append(p.processed, taskID)
	p.mu.Unlock()
	return "ok-" + taskID, nil
}

func (p *mockProcessor) processedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.processed)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPoolReconcile_SpawnsWorkers(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &mockProcessor{}

	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	// Enqueue some tasks for tenant a.
	q.Enqueue("a", &queue.TaskEnvelope{
		TaskID: "task-1", TenantID: "a", Payload: json.RawMessage(`{}`),
	})
	q.Enqueue("a", &queue.TaskEnvelope{
		TaskID: "task-2", TenantID: "a", Payload: json.RawMessage(`{}`),
	})

	// Reconcile to spawn 2 workers.
	pool.Reconcile(map[string]int{"a": 2})

	// Wait a bit for workers to process.
	time.Sleep(300 * time.Millisecond)

	if proc.processedCount() < 2 {
		t.Errorf("expected at least 2 processed tasks, got %d", proc.processedCount())
	}

	// Shutdown.
	if err := pool.Shutdown(2 * time.Second); err != nil {
		t.Errorf("shutdown: %v", err)
	}
}

func TestPoolReconcile_KillsWorkers(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &mockProcessor{}

	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	// Spawn 3 workers.
	pool.Reconcile(map[string]int{"a": 3})
	time.Sleep(100 * time.Millisecond)

	status := pool.GetStatus()
	if status["a"] != 3 {
		t.Fatalf("expected 3 workers, got %d", status["a"])
	}

	// Kill 2.
	pool.Reconcile(map[string]int{"a": 1})
	time.Sleep(100 * time.Millisecond)

	status = pool.GetStatus()
	if status["a"] != 1 {
		t.Errorf("expected 1 worker after kill, got %d", status["a"])
	}

	pool.Shutdown(2 * time.Second)
}

func TestPoolReconcile_NewTenant(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	pool := NewPool("n1", q, fsm, &mockRaftApplier{}, &mockProcessor{}, zap.NewNop())

	// Tenant doesn't exist yet, reconcile adds it.
	pool.Reconcile(map[string]int{"new-tenant": 5})
	status := pool.GetStatus()

	if status["new-tenant"] != 5 {
		t.Errorf("expected 5 workers for new tenant, got %d", status["new-tenant"])
	}

	// Remove tenant.
	pool.Reconcile(map[string]int{})
	time.Sleep(100 * time.Millisecond)
	status = pool.GetStatus()
	if status["new-tenant"] != 0 {
		t.Errorf("expected 0 workers after removal, got %d", status["new-tenant"])
	}

	pool.Shutdown(2 * time.Second)
}

func TestPoolShutdown_DrainsGracefully(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}

	// Slow processor that does NOT respond to cancellation (simulates
	// a task that must complete).
	proc := &slowProcessor{delay: 500 * time.Millisecond, ignoreCancel: true}
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	q.Enqueue("a", &queue.TaskEnvelope{TaskID: "slow", TenantID: "a", Payload: json.RawMessage(`{}`)})
	pool.Reconcile(map[string]int{"a": 1})
	time.Sleep(100 * time.Millisecond)

	// Shutdown waits for in-flight tasks to finish.
	start := time.Now()
	if err := pool.Shutdown(5 * time.Second); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 350*time.Millisecond {
		t.Errorf("shutdown too fast (%v), should have waited for task", elapsed)
	}
}

type slowProcessor struct {
	delay        time.Duration
	ignoreCancel bool
}

func (p *slowProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	if p.ignoreCancel {
		time.Sleep(p.delay)
		return "done", nil
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(p.delay):
		return "done", nil
	}
}

// ---------------------------------------------------------------------------
// Task lifecycles through worker pool
// ---------------------------------------------------------------------------

func TestPoolWorker_ClaimsAndCompletesTask(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &mockProcessor{}

	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	// Enqueue a task.
	q.Enqueue("a", &queue.TaskEnvelope{
		TaskID: "task-claim", TenantID: "a", Payload: json.RawMessage(`"hello"`),
	})

	pool.Reconcile(map[string]int{"a": 1})
	time.Sleep(300 * time.Millisecond)

	// Verify the task was claimed (2 raft applies: claim + complete).
	count := raft.appliedCount()
	if count < 2 {
		t.Errorf("expected at least 2 raft applies (claim + complete), got %d", count)
	}

	pool.Shutdown(2 * time.Second)
}

func TestPoolWorker_RecoveryTasks(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &mockProcessor{}

	// Register node and seed FSM with a recovery-pending task.
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{ID: "dead-node"})
	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "rec-task", TenantID: "a", NodeID: "dead-node", Payload: `"recovery"`,
	})
	applyOp(fsm, raftpkg.OpNodeDown, raftpkg.NodeDownData{ID: "dead-node"})

	// Verify the task is pending in FSM.
	pending := fsm.FindPendingTasks("a")
	if len(pending) == 0 {
		t.Fatal("expected pending recovery task in FSM, got none")
	}

	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())
	pool.Reconcile(map[string]int{"a": 1})

	// Poll until processed.
	for i := 0; i < 20 && proc.processedCount() == 0; i++ {
		time.Sleep(100 * time.Millisecond)
	}

	if proc.processedCount() == 0 {
		t.Error("recovery task was not processed")
	}

	pool.Shutdown(2 * time.Second)
}

func TestPoolWorker_StealsAgedTaskFromAnotherTenant(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyOp(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "steal-task", TenantID: "other", Payload: `"payload"`,
	})
	state := fsm.GetState()
	state.Tasks["steal-task"].CreatedAt = time.Now().UTC().Add(-time.Minute)
	persisted, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(persisted))); err != nil {
		t.Fatal(err)
	}

	client := &stealTrackingClient{fsmTaskClient: &fsmTaskClient{fsm: fsm}}
	proc := &mockProcessor{}
	pool := NewPool("worker-node", q, fsm, &mockRaftApplier{}, proc, zap.NewNop())
	pool.SetClaimer(client)
	pool.SetCompleter(client)
	pool.Reconcile(map[string]int{"worker": 1})

	for i := 0; i < 40 && fsm.GetResult("steal-task") == nil; i++ {
		time.Sleep(25 * time.Millisecond)
	}
	if result := fsm.GetResult("steal-task"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("stolen task result = %+v", result)
	}
	if !client.didSteal() {
		t.Fatal("worker completed another tenant's task without the steal claim path")
	}
	if err := pool.Shutdown(2 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

func TestPoolWorker_ReservesPendingTaskBeforeClaim(t *testing.T) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyOp(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "one-pending-task", TenantID: "a", Payload: `"payload"`,
	})
	client := &fsmTaskClient{fsm: fsm}
	pool := NewPool("n1", q, fsm, &mockRaftApplier{}, &mockProcessor{}, zap.NewNop())
	pool.SetClaimer(client)
	pool.SetCompleter(client)
	pool.Reconcile(map[string]int{"a": 20})

	for i := 0; i < 30 && fsm.GetResult("one-pending-task") == nil; i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if result := fsm.GetResult("one-pending-task"); result == nil {
		t.Fatal("pending task was not completed")
	}
	if got := client.maxClaims(); got != 1 {
		t.Fatalf("maximum concurrent claims for one task = %d, want 1", got)
	}

	if err := pool.Shutdown(2 * time.Second); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
}

// Helper to apply FSM operations directly.
func applyOp(fsm *raftpkg.FSM, op string, data interface{}) {
	cmd := raftpkg.MustMarshalCommand(op, data)
	_ = fsm.Apply(&hashicorpraft.Log{Data: cmd, Type: hashicorpraft.LogCommand})
}
