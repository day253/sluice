package allocator

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func newTestEngine(fsm *raftpkg.FSM) *Engine {
	return &Engine{
		nodeID:              "test-node",
		fsm:                 fsm,
		logger:              zap.NewNop(),
		idleCycles:          make(map[string]int),
		minWorkersPerTenant: 1,
	}
}

func newTestFSM() *raftpkg.FSM {
	fsm := raftpkg.NewFSM(zap.NewNop())
	// Seed tenants by applying through the FSM.
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "a", MaxWorkers: 100})
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "b", MaxWorkers: 50})
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "c", MaxWorkers: 30})
	// Register nodes.
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "n1", Status: types.NodeStatusUp, TotalWorkers: 50,
	})
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
		ID: "n2", Status: types.NodeStatusUp, TotalWorkers: 50,
	})
	return fsm
}

func applyOp(fsm *raftpkg.FSM, op string, data interface{}) {
	cmd := raftpkg.MustMarshalCommand(op, data)
	_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
}

func addInflight(fsm *raftpkg.FSM, tenantID string, count int) {
	for i := 0; i < count; i++ {
		applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
			TaskID:   tenantID + "-task-" + string(rune(i+'0')),
			TenantID: tenantID,
			NodeID:   "n1",
			Payload:  `{}`,
		})
	}
}

// ---------------------------------------------------------------------------
// Max-min fairness tests
// ---------------------------------------------------------------------------

func TestMaxMinFairness_Oversubscribed(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 100},
		{ID: "b", MaxWorkers: 50},
		{ID: "c", MaxWorkers: 30},
	}
	// sum(limits)=180 > total=100 → oversubscribed
	alloc := e.maxMinFairness(tenants, 100)

	// Every tenant gets at least 1.
	for _, tc := range tenants {
		if alloc[tc.ID] < 1 {
			t.Errorf("tenant %s got %d, want at least 1", tc.ID, alloc[tc.ID])
		}
		if alloc[tc.ID] > tc.MaxWorkers {
			t.Errorf("tenant %s got %d, exceeds limit %d", tc.ID, alloc[tc.ID], tc.MaxWorkers)
		}
	}

	total := alloc["a"] + alloc["b"] + alloc["c"]
	if total != 100 {
		t.Errorf("total allocation = %d, want 100", total)
	}

	t.Logf("alloc: a=%d b=%d c=%d", alloc["a"], alloc["b"], alloc["c"])
}

func TestMaxMinFairness_Undersubscribed(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 10},
		{ID: "b", MaxWorkers: 10},
	}
	// sum(limits)=20 < total=100 → every tenant gets its full limit
	alloc := e.maxMinFairness(tenants, 100)

	for _, tc := range tenants {
		if alloc[tc.ID] != tc.MaxWorkers {
			t.Errorf("tenant %s: got %d, want %d (undersubscribed)", tc.ID, alloc[tc.ID], tc.MaxWorkers)
		}
	}
}

func TestMaxMinFairness_SingleTenant(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 50},
	}
	alloc := e.maxMinFairness(tenants, 100)

	if alloc["a"] != 50 {
		t.Errorf("single tenant: got %d, want 50 (capped at limit)", alloc["a"])
	}
}

func TestMaxMinFairness_MinimumGuarantee(t *testing.T) {
	e := newTestEngine(nil)
	e.minWorkersPerTenant = 1

	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 1},
	}

	alloc := e.maxMinFairness(tenants, 3)
	if alloc["a"] != 1 {
		t.Errorf("tenant a: got %d, want 1", alloc["a"])
	}
}

// ---------------------------------------------------------------------------
// Idle detection tests
// ---------------------------------------------------------------------------

func TestUpdateIdleState_MarksIdleAfterThreshold(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 100},
	}

	// Zero inflight for idleThreshold cycles.
	for i := 0; i < idleThreshold; i++ {
		idle := e.updateIdleState(tenants, map[string]int{})
		if i < idleThreshold-1 {
			if idle["a"] {
				t.Fatalf("cycle %d: should not be idle yet, idleCycles=%d", i, e.idleCycles["a"])
			}
		} else {
			if !idle["a"] {
				t.Fatalf("cycle %d: should be idle now, idleCycles=%d", i, e.idleCycles["a"])
			}
		}
	}
}

func TestUpdateIdleState_WakesOnInflight(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 100},
	}

	// Make it idle first.
	for i := 0; i < idleThreshold; i++ {
		e.updateIdleState(tenants, map[string]int{})
	}
	if !e.updateIdleState(tenants, map[string]int{})["a"] {
		t.Fatal("should be idle")
	}

	// Now add inflight — should wake up immediately.
	idle := e.updateIdleState(tenants, map[string]int{"a": 5})
	if idle["a"] {
		t.Fatalf("should have woken up, idleCycles=%d", e.idleCycles["a"])
	}
	if e.idleCycles["a"] != 0 {
		t.Errorf("idleCycles should be reset to 0, got %d", e.idleCycles["a"])
	}
}

func TestUpdateIdleState_CleansUpDeletedTenants(t *testing.T) {
	e := newTestEngine(nil)
	e.idleCycles = map[string]int{"ghost": 10}

	e.updateIdleState(nil, nil)
	if _, ok := e.idleCycles["ghost"]; ok {
		t.Error("should have cleaned up ghost tenant counter")
	}
}

// ---------------------------------------------------------------------------
// Idle adjustment tests
// ---------------------------------------------------------------------------

func TestApplyIdleAdjustment_RedistributesToActive(t *testing.T) {
	e := newTestEngine(nil)
	e.minWorkersPerTenant = 1

	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 100},
		{ID: "b", MaxWorkers: 50},
		{ID: "c", MaxWorkers: 30},
	}

	// Base alloc from max-min with 100 total.
	baseAlloc := map[string]int{"a": 55, "b": 27, "c": 18}
	idleSet := map[string]bool{"c": true}

	final := e.applyIdleAdjustment(tenants, baseAlloc, idleSet)

	// Idle tenant gets exactly 1.
	if final["c"] != 1 {
		t.Errorf("idle tenant c: got %d, want 1", final["c"])
	}

	// Total should still equal cluster capacity (minus waste from idle cap).
	total := final["a"] + final["b"] + final["c"]
	if total != 100 {
		t.Errorf("total allocation = %d, want 100", total)
	}

	// Active tenants should have gotten c's released workers.
	if final["a"] < baseAlloc["a"] {
		t.Errorf("active tenant a should not lose workers: was %d, now %d", baseAlloc["a"], final["a"])
	}
	if final["b"] < baseAlloc["b"] {
		t.Errorf("active tenant b should not lose workers: was %d, now %d", baseAlloc["b"], final["b"])
	}

	t.Logf("adjusted: a=%d b=%d c=%d (released from c: %d)", final["a"], final["b"], final["c"], baseAlloc["c"]-1)
}

func TestApplyIdleAdjustment_AllIdle(t *testing.T) {
	e := newTestEngine(nil)
	e.minWorkersPerTenant = 1

	tenants := []*types.TenantConfig{
		{ID: "a", MaxWorkers: 100},
		{ID: "b", MaxWorkers: 50},
	}

	baseAlloc := map[string]int{"a": 66, "b": 34}
	idleSet := map[string]bool{"a": true, "b": true}

	final := e.applyIdleAdjustment(tenants, baseAlloc, idleSet)

	if final["a"] != 1 || final["b"] != 1 {
		t.Errorf("all-idle: expected {a:1, b:1}, got {a:%d, b:%d}", final["a"], final["b"])
	}
}

// ---------------------------------------------------------------------------
// Integration: full reconcile pipeline
// ---------------------------------------------------------------------------

type fakeRaftApplier struct {
	lastCmd []byte
	fsm     *raftpkg.FSM
}

func (f *fakeRaftApplier) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	f.lastCmd = cmd
	// Route the command to the FSM so it reflects in state reads.
	_ = f.fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	return &fakeApplyResult{}
}

func (f *fakeRaftApplier) IsLeader() bool     { return true }
func (f *fakeRaftApplier) LeaderAddr() string { return "test:7000" }

type fakeApplyResult struct{}

func (r *fakeApplyResult) Error() error          { return nil }
func (r *fakeApplyResult) Response() interface{} { return nil }

func TestReconcile_ProducesValidPlan(t *testing.T) {
	fsm := newTestFSM()
	raft := &fakeRaftApplier{fsm: fsm}
	e := &Engine{
		nodeID:              "test-node",
		fsm:                 fsm,
		raft:                raft,
		logger:              zap.NewNop(),
		idleCycles:          make(map[string]int),
		minWorkersPerTenant: 1,
	}

	if err := e.Reconcile(); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify the Raft command was built.
	if raft.lastCmd == nil {
		t.Fatal("no raft command applied")
	}

	// Unmarshal and check structure.
	var cmd raftpkg.Command
	if err := json.Unmarshal(raft.lastCmd, &cmd); err != nil {
		t.Fatalf("unmarshal command: %v", err)
	}
	if cmd.Op != raftpkg.OpUpdateAllocation {
		t.Errorf("expected %s, got %s", raftpkg.OpUpdateAllocation, cmd.Op)
	}

	var allocMap map[string]*types.NodeAllocation
	if err := json.Unmarshal(cmd.Data, &allocMap); err != nil {
		t.Fatalf("unmarshal allocation: %v", err)
	}

	// 2 nodes, each should have allocations for all 3 tenants.
	if len(allocMap) != 2 {
		t.Errorf("expected 2 node allocations, got %d", len(allocMap))
	}
	for _, na := range allocMap {
		if len(na.Tenants) != 3 {
			t.Errorf("node %s: expected 3 tenants, got %d", na.NodeID, len(na.Tenants))
		}
	}

	t.Logf("reconcile produced valid plan with %d nodes", len(allocMap))
}

func TestReconcile_IdleTenantTriggersRedistribution(t *testing.T) {
	fsm := newTestFSM()

	// Add inflight tasks for tenants a and b, leave c idle.
	addInflight(fsm, "a", 10)
	addInflight(fsm, "b", 5)

	e := &Engine{
		nodeID:              "test-node",
		fsm:                 fsm,
		raft:                &fakeRaftApplier{fsm: fsm},
		logger:              zap.NewNop(),
		idleCycles:          map[string]int{"c": idleThreshold}, // c already idle
		minWorkersPerTenant: 1,
	}

	if err := e.Reconcile(); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Check FSM allocation after reconcile.
	alloc, ok := fsm.GetAllocation("n1")
	if !ok {
		t.Fatal("no allocation for n1")
	}

	// Tenant c should have been reduced to ~1 per node.
	if alloc.Tenants["c"] > 1 {
		t.Errorf("idle tenant c should have ≤1 worker per node, got %d", alloc.Tenants["c"])
	}
	if alloc.Tenants["a"] <= 0 {
		t.Error("active tenant a should have workers")
	}

	t.Logf("post-reconcile n1: a=%d b=%d c=%d", alloc.Tenants["a"], alloc.Tenants["b"], alloc.Tenants["c"])
}

func TestRequeueStaleTasks_ExpiredClaimReturnsToPending(t *testing.T) {
	fsm := newTestFSM()
	applyOp(fsm, raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: "expired", TenantID: "a", NodeID: "n1", Payload: `{}`,
	})

	// Restore a snapshot with an expired ClaimedAt value. This exercises the
	// same persisted state a leader sees after a full-cluster restart.
	state := fsm.GetState()
	state.Tasks["expired"].ClaimedAt = time.Now().UTC().Add(-taskClaimLease - time.Second)
	snapshot, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(snapshot))); err != nil {
		t.Fatal(err)
	}

	e := &Engine{
		nodeID:              "test-node",
		fsm:                 fsm,
		raft:                &fakeRaftApplier{fsm: fsm},
		logger:              zap.NewNop(),
		idleCycles:          make(map[string]int),
		minWorkersPerTenant: 1,
	}
	if err := e.requeueStaleTasks(); err != nil {
		t.Fatalf("requeue stale tasks: %v", err)
	}

	task := fsm.GetTask("expired")
	if task == nil || task.Status != types.TaskStatusPending {
		t.Fatalf("expired task = %+v, want pending", task)
	}
	if task.NodeID != "" || !task.ClaimedAt.IsZero() {
		t.Fatalf("expired claim metadata was not cleared: %+v", task)
	}
}

// ---------------------------------------------------------------------------
// Distribution tests
// ---------------------------------------------------------------------------

func TestDistributeAcrossNodes_EvenSplit(t *testing.T) {
	e := newTestEngine(nil)
	tenantAlloc := map[string]int{"a": 60, "b": 40}
	nodes := []*types.NodeInfo{
		{ID: "n1"},
		{ID: "n2"},
	}

	result := e.distributeAcrossNodes(tenantAlloc, nodes)
	if len(result) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(result))
	}

	// Each node should get half.
	for _, na := range result {
		total := 0
		for _, c := range na.Tenants {
			total += c
		}
		if total != 50 {
			t.Errorf("node %s: expected total ~50, got %d", na.NodeID, total)
		}
	}
}
