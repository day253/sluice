package allocator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
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
		borrowedTargets:     make(map[string]int),
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
// Adaptive idle-capacity borrowing
// ---------------------------------------------------------------------------

func TestApplyBorrowing_ProbesSpareCapacityExponentially(t *testing.T) {
	e := newTestEngine(nil)
	tenants := []*types.TenantConfig{
		{ID: "small", MaxWorkers: 1},
		{ID: "idle", MaxWorkers: 1},
	}
	base := map[string]int{"small": 1, "idle": 1}
	idleSet := map[string]bool{"idle": true}

	oldest := map[string]time.Time{"small": time.Now().Add(-pendingBorrowThreshold - time.Second)}
	first, borrowed := e.applyBorrowing(tenants, base, 20, map[string]int{"small": 100}, map[string]int{"small": 100}, oldest, idleSet)
	if first["small"] != 2 || borrowed["small"] != 1 {
		t.Fatalf("first probe = effective %d borrowed %d, want 2/1", first["small"], borrowed["small"])
	}
	second, borrowed := e.applyBorrowing(tenants, base, 20, map[string]int{"small": 100}, map[string]int{"small": 100}, oldest, idleSet)
	if second["small"] != 4 || borrowed["small"] != 3 {
		t.Fatalf("second probe = effective %d borrowed %d, want 4/3", second["small"], borrowed["small"])
	}
	third, borrowed := e.applyBorrowing(tenants, base, 20, map[string]int{"small": 100}, map[string]int{"small": 100}, oldest, idleSet)
	if third["small"] != 8 || borrowed["small"] != 7 {
		t.Fatalf("third probe = effective %d borrowed %d, want 8/7", third["small"], borrowed["small"])
	}
	if sumWorkers(third) > 20 {
		t.Fatalf("probe exceeded cluster capacity: %d > 20", sumWorkers(third))
	}
}

func TestApplyBorrowing_SharesCapacityAcrossBackloggedTenants(t *testing.T) {
	e := newTestEngine(nil)
	e.borrowedTargets["small"] = 7
	tenants := []*types.TenantConfig{
		{ID: "small", MaxWorkers: 1},
		{ID: "other", MaxWorkers: 1},
	}
	base := map[string]int{"small": 1, "other": 1}
	effective, borrowed := e.applyBorrowing(
		tenants, base, 20,
		map[string]int{"small": 100, "other": 1},
		map[string]int{"small": 100, "other": 1},
		map[string]time.Time{
			"small": time.Now().Add(-pendingBorrowThreshold - time.Second),
			"other": time.Now().Add(-pendingBorrowThreshold - time.Second),
		},
		map[string]bool{},
	)
	if borrowed["small"] != 15 || borrowed["other"] != 1 {
		t.Fatalf("borrowed allocation = %v, want small=15 other=1", borrowed)
	}
	if effective["small"] != 16 || effective["other"] != 2 {
		t.Fatalf("effective allocation = %v, want small=16 other=2", effective)
	}
	if _, ok := e.borrowedTargets["small"]; !ok {
		t.Fatal("small borrow target was unexpectedly released")
	}
}

func TestApplyBorrowing_DoesNotProbeWithoutPendingBacklog(t *testing.T) {
	e := newTestEngine(nil)
	e.borrowedTargets["small"] = 7
	tenants := []*types.TenantConfig{{ID: "small", MaxWorkers: 1}}
	effective, borrowed := e.applyBorrowing(
		tenants,
		map[string]int{"small": 1},
		20,
		map[string]int{"small": 1}, // one task is already inflight
		map[string]int{},           // no queued work remains
		map[string]time.Time{},
		map[string]bool{},
	)
	if len(borrowed) != 0 || effective["small"] != 1 {
		t.Fatalf("inflight-only allocation = effective %v borrowed %v, want 1/no borrow", effective, borrowed)
	}
	if _, ok := e.borrowedTargets["small"]; ok {
		t.Fatal("stale borrow target was retained without pending backlog")
	}
}

func TestDistributeAcrossNodesWithBorrowed_PreservesCurrentMirror(t *testing.T) {
	e := newTestEngine(nil)
	result := e.distributeAcrossNodesWithBorrowed(
		map[string]int{"small": 9},
		map[string]int{"small": 7},
		[]*types.NodeInfo{{ID: "node-1"}, {ID: "node-2"}},
	)
	if len(result) != 2 {
		t.Fatalf("nodes = %d, want 2", len(result))
	}
	effective, borrowed := 0, 0
	for _, allocation := range result {
		effective += allocation.Tenants["small"]
		borrowed += allocation.Borrowed["small"]
	}
	if effective != 9 || borrowed != 7 {
		t.Fatalf("distributed effective/borrowed = %d/%d, want 9/7", effective, borrowed)
	}
}

func TestActiveNodes_SortedByID(t *testing.T) {
	e := newTestEngine(nil)
	active := e.activeNodes(map[string]*types.NodeInfo{
		"node-10": {ID: "node-10", Status: types.NodeStatusUp},
		"node-2":  {ID: "node-2", Status: types.NodeStatusUp},
		"node-1":  {ID: "node-1", Status: types.NodeStatusUp},
	})
	got := []string{active[0].ID, active[1].ID, active[2].ID}
	want := []string{"node-1", "node-2", "node-10"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("active node order = %v, want %v", got, want)
		}
	}
}

func TestExecutionNodes_ExcludesLeaderAndPreservesStableOrder(t *testing.T) {
	e := newTestEngine(nil)
	e.nodeID = "node-2"
	execution := e.executionNodes(map[string]*types.NodeInfo{
		"node-10": {ID: "node-10", Status: types.NodeStatusUp},
		"node-2":  {ID: "node-2", Status: types.NodeStatusUp},
		"node-1":  {ID: "node-1", Status: types.NodeStatusUp},
		"node-3":  {ID: "node-3", Status: types.NodeStatusDown},
	})
	got := make([]string, len(execution))
	for i, node := range execution {
		got[i] = node.ID
	}
	want := []string{"node-1", "node-10"}
	if len(got) != len(want) {
		t.Fatalf("execution nodes = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("execution nodes = %v, want %v", got, want)
		}
	}
}

func TestExecutionNodesExcludeEveryExplicitControlReplica(t *testing.T) {
	e := newTestEngine(nil)
	e.nodeID = "control-0"
	execution := e.executionNodes(map[string]*types.NodeInfo{
		"control-0": {ID: "control-0", Role: types.NodeRoleControl, Status: types.NodeStatusUp},
		"control-1": {ID: "control-1", Role: types.NodeRoleControl, Status: types.NodeStatusUp},
		"worker-0": {
			ID: "worker-0", Role: types.NodeRoleWorker, Status: types.NodeStatusUp, TotalWorkers: 100,
		},
		"worker-zero": {
			ID: "worker-zero", Role: types.NodeRoleWorker, Status: types.NodeStatusUp, TotalWorkers: 0,
		},
	})
	if len(execution) != 1 || execution[0].ID != "worker-0" {
		t.Fatalf("execution nodes = %+v, want only worker-0", execution)
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

type blockingRaftApplier struct {
	fsm     *raftpkg.FSM
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingRaftApplier) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	b.entered <- struct{}{}
	<-b.release
	b.once.Do(func() {
		_ = b.fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	})
	return &fakeApplyResult{}
}

func (b *blockingRaftApplier) IsLeader() bool     { return true }
func (b *blockingRaftApplier) LeaderAddr() string { return "test:7000" }

func TestConcurrentReconcileRequestsAreSerialized(t *testing.T) {
	fsm := newTestFSM()
	raft := &blockingRaftApplier{
		fsm: fsm, entered: make(chan struct{}, 2), release: make(chan struct{}, 2),
	}
	e := NewEngine("test-node", fsm, raft, zap.NewNop())

	done := make(chan error, 2)
	go func() { done <- e.Reconcile() }()
	select {
	case <-raft.entered:
	case <-time.After(time.Second):
		t.Fatal("first reconciliation did not reach Raft Apply")
	}

	go func() { done <- e.Reconcile() }()
	select {
	case <-raft.entered:
		raft.release <- struct{}{}
		raft.release <- struct{}{}
		<-done
		<-done
		t.Fatal("second reconciliation entered Raft Apply before the first completed")
	case <-time.After(100 * time.Millisecond):
		// The second caller must remain behind reconcileMu.
	}

	raft.release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first reconciliation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("first reconciliation did not finish")
	}
	select {
	case <-raft.entered:
	case <-time.After(time.Second):
		t.Fatal("second reconciliation did not start after the first completed")
	}
	raft.release <- struct{}{}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second reconciliation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reconciliation did not finish")
	}
}

type countingRaftApplier struct {
	fsm     *raftpkg.FSM
	applies atomic.Int64
}

func (c *countingRaftApplier) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	c.applies.Add(1)
	_ = c.fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	return &fakeApplyResult{}
}

func (c *countingRaftApplier) IsLeader() bool     { return true }
func (c *countingRaftApplier) LeaderAddr() string { return "test:7000" }

func TestWorkNotificationsCoalesceBeforeAllocatorRunLoop(t *testing.T) {
	fsm := newTestFSM()
	raft := &countingRaftApplier{fsm: fsm}
	e := NewEngine("test-node", fsm, raft, zap.NewNop())
	e.SetLeader(true)
	for i := 0; i < 100; i++ {
		e.NotifyWorkAvailable([]string{"a"})
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	e.Start(ctx, time.Hour)
	deadline := time.After(time.Second)
	for raft.applies.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("work notification did not trigger reconciliation")
		case <-time.After(time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := raft.applies.Load(); got != 1 {
		t.Fatalf("100 concurrent notifications caused %d reconciliations, want one coalesced run", got)
	}
	for i := 0; i < 100; i++ {
		e.NotifyWorkAvailable([]string{"a"})
	}
	time.Sleep(50 * time.Millisecond)
	if got := raft.applies.Load(); got != 1 {
		t.Fatalf("active tenant notifications caused %d reconciliations, want no redundant allocation log", got)
	}
}

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

func TestReconcile_LeaderHasNoAllocation(t *testing.T) {
	fsm := newTestFSM()
	raft := &fakeRaftApplier{fsm: fsm}
	e := &Engine{
		nodeID: "n1", fsm: fsm, raft: raft, logger: zap.NewNop(),
		idleCycles: make(map[string]int), borrowedTargets: make(map[string]int),
		minWorkersPerTenant: 1,
	}
	if err := e.Reconcile(); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	allocations := fsm.GetAllAllocations()
	if _, ok := allocations["n1"]; ok {
		t.Fatal("leader n1 received a worker allocation")
	}
	if allocation, ok := allocations["n2"]; !ok || len(allocation.Tenants) == 0 {
		t.Fatalf("follower n2 allocation = %+v, want non-empty", allocation)
	}
	total := 0
	for _, workers := range allocations["n2"].Tenants {
		total += workers
	}
	if total > 50 {
		t.Fatalf("allocated workers = %d, exceeds follower capacity 50", total)
	}
}

func TestReconcile_OnlyLeaderClearsStaleAllocation(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{ID: "leader", Status: types.NodeStatusUp, TotalWorkers: 50})
	applyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "a", MaxWorkers: 10})
	applyOp(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"leader": {NodeID: "leader", Tenants: map[string]int{"a": 10}},
	})
	raft := &fakeRaftApplier{fsm: fsm}
	e := &Engine{
		nodeID: "leader", fsm: fsm, raft: raft, logger: zap.NewNop(),
		idleCycles: make(map[string]int), borrowedTargets: make(map[string]int),
		minWorkersPerTenant: 1,
	}
	if err := e.Reconcile(); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if allocations := fsm.GetAllAllocations(); len(allocations) != 0 {
		t.Fatalf("single-node control plane retained stale allocation: %+v", allocations)
	}
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

func TestDistributeManySingleWorkerTenantsRespectsPerNodeCapacity(t *testing.T) {
	e := newTestEngine(nil)
	const (
		tenantCount  = 108
		nodeCount    = 50
		nodeCapacity = 100
	)
	tenantAlloc := make(map[string]int, tenantCount)
	for index := 0; index < tenantCount; index++ {
		tenantAlloc[fmt.Sprintf("tenant-%03d", index)] = 1
	}
	nodes := make([]*types.NodeInfo, nodeCount)
	for index := range nodes {
		nodes[index] = &types.NodeInfo{
			ID: fmt.Sprintf("worker-%02d", index), TotalWorkers: nodeCapacity,
		}
	}

	result := e.distributeAcrossNodes(tenantAlloc, nodes)
	if err := validateNodeAllocationCapacity(result, nodes, tenantAlloc, nil); err != nil {
		t.Fatal(err)
	}
	minWorkers, maxWorkers, total := tenantCount, 0, 0
	for _, allocation := range result {
		workers := sumWorkers(allocation.Tenants)
		minWorkers = min(minWorkers, workers)
		maxWorkers = max(maxWorkers, workers)
		total += workers
	}
	if total != tenantCount || minWorkers != 2 || maxWorkers != 3 {
		t.Fatalf(
			"single-worker tenant placement total/min/max = %d/%d/%d, want 108/2/3",
			total, minWorkers, maxWorkers,
		)
	}
}

func TestValidateNodeAllocationCapacityRejectsOverflow(t *testing.T) {
	nodes := []*types.NodeInfo{{ID: "worker-0", TotalWorkers: 1}}
	allocations := []*types.NodeAllocation{{
		NodeID: "worker-0", Tenants: map[string]int{"a": 1, "b": 1},
	}}
	err := validateNodeAllocationCapacity(
		allocations, nodes, map[string]int{"a": 1, "b": 1}, nil,
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds capacity") {
		t.Fatalf("capacity validation error = %v, want overflow rejection", err)
	}
}
