// Package allocator implements the max-min fairness worker allocation
// algorithm with starvation-aware idle detection.  It runs on the Raft
// leader and writes the computed allocation plan to the Raft log so every
// node sees the same view.
//
// Idle detection: when a tenant has zero inflight tasks for
// idleThreshold consecutive reconciliation cycles, it is marked "idle"
// and its worker allocation is reduced to the catch-all minimum (1).
// The released workers are redistributed to active tenants using
// max-min fairness.  The idle tenant's single keep-alive worker ensures
// new tasks are picked up immediately; the tenant's full allocation is
// restored on the next reconciliation cycle.
package allocator

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

const (
	// idleThreshold is how many consecutive zero-load cycles a tenant must
	// experience before being classified as idle (with the default 3 s
	// interval this ≈ 9 s of inactivity).
	idleThreshold = 3
	// taskClaimLease bounds how long an inflight task may remain without a
	// completion. Expired claims are returned to pending by the leader within
	// one reconciliation cycle. Demo tasks finish in milliseconds, so 30 s
	// leaves ample headroom while keeping rollout recovery observable.
	taskClaimLease = 30 * time.Second
	// borrowProbeStep is the initial additive probe for a single backlogged
	// tenant using otherwise spare cluster capacity. Later probes double the
	// previous target (1, 3, 7, ...), bounded by the actual spare capacity.
	borrowProbeStep = 1
	// pendingBorrowThreshold avoids scaling out on a transient queue blip.
	// The allocator runs every 3 seconds, so a second cycle is enough to
	// identify work that is genuinely waiting for capacity.
	pendingBorrowThreshold = 5 * time.Second
)

// ---------------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------------

// Engine periodically recomputes the global worker allocation and pushes it
// into the Raft log.  It only runs when the local node is the Raft leader.
type Engine struct {
	nodeID string
	fsm    *raftpkg.FSM
	raft   raftpkg.RaftApplier
	logger *zap.Logger

	// leader tracking
	mu       sync.Mutex
	isLeader bool

	// per-tenant idle tracking (leader-local, not replicated)
	idleCycles map[string]int // tenantID → consecutive cycles with 0 inflight

	// Borrowed targets are controller state, not historical data. The current
	// effective allocation and borrowed portion are replicated in the FSM, but
	// a new leader starts probing from the configured limits again.
	borrowedTargets map[string]int // tenantID → borrowed workers

	// configuration
	minWorkersPerTenant int
}

// NewEngine creates an allocator engine.
func NewEngine(
	nodeID string,
	fsm *raftpkg.FSM,
	raft raftpkg.RaftApplier,
	logger *zap.Logger,
) *Engine {
	return &Engine{
		nodeID:              nodeID,
		fsm:                 fsm,
		raft:                raft,
		logger:              logger,
		idleCycles:          make(map[string]int),
		borrowedTargets:     make(map[string]int),
		minWorkersPerTenant: 1,
	}
}

// ---------------------------------------------------------------------------
// Leadership
// ---------------------------------------------------------------------------

// SetLeader must be called with true when this node becomes the Raft leader,
// and false when it steps down.
func (e *Engine) SetLeader(leader bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.isLeader = leader
	if leader {
		e.logger.Info("allocator: became leader — will run reconciliation")
		// Reset idle tracking on leadership change.
		e.idleCycles = make(map[string]int)
		e.borrowedTargets = make(map[string]int)
	}
}

// IsLeader returns whether this node thinks it is the leader.
func (e *Engine) IsLeader() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.isLeader
}

// ---------------------------------------------------------------------------
// Run loop
// ---------------------------------------------------------------------------

// Start launches a background goroutine that periodically reconciles the
// allocation when this node is leader.
func (e *Engine) Start(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if !e.IsLeader() {
					continue
				}
				if err := e.Reconcile(); err != nil {
					e.logger.Error("allocator: reconcile failed", zap.Error(err))
				}
			}
		}
	}()
}

// ReconcileNow triggers an immediate reconciliation (if leader).
func (e *Engine) ReconcileNow() error {
	if !e.IsLeader() {
		return nil
	}
	return e.Reconcile()
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

// Reconcile computes the fair allocation, applies idle detection, and writes
// the final plan to Raft.
func (e *Engine) Reconcile() error {
	if err := e.requeueStaleTasks(); err != nil {
		return err
	}
	state := e.fsm.GetState()

	// 1. Determine active nodes and total cluster capacity.
	activeNodes := e.activeNodes(state.Nodes)
	if len(activeNodes) == 0 {
		e.logger.Warn("allocator: no active nodes, skipping")
		return nil
	}

	totalClusterWorkers := 0
	for _, n := range activeNodes {
		totalClusterWorkers += n.TotalWorkers
	}

	// 2. Get tenants.
	tenantList := e.tenantList(state.Tenants)
	if len(tenantList) == 0 {
		e.logger.Debug("allocator: no tenants configured")
		// Reset idle tracking when there are no tenants.
		e.idleCycles = make(map[string]int)
		return nil
	}

	// 3. Build load snapshot: inflight tasks per tenant (FSM-level).
	inflightCount := e.fsm.CountUnfinishedPerTenant()

	// 4. Update idle-cycle counters and classify tenants.
	idleSet := e.updateIdleState(tenantList, inflightCount)

	// 5. Compute standard max-min fairness (base allocation).
	baseAlloc := e.maxMinFairness(tenantList, totalClusterWorkers)

	// 6. Apply idle penalty and redistribute released workers.
	finalAlloc := e.applyIdleAdjustment(tenantList, baseAlloc, idleSet)

	// 7. Let every tenant with an aged pending backlog probe otherwise spare
	// capacity above its configured limit. Probes are shared fairly and shrink
	// as soon as a tenant's backlog disappears.
	pendingCount := e.fsm.CountPendingPerTenant()
	finalAlloc, borrowed := e.applyBorrowing(
		tenantList,
		finalAlloc,
		totalClusterWorkers,
		inflightCount,
		pendingCount,
		e.fsm.OldestPendingCreatedAtByTenant(),
		idleSet,
	)

	// 8. Distribute each tenant's effective and borrowed workers across nodes.
	nodeAllocs := e.distributeAcrossNodesWithBorrowed(finalAlloc, borrowed, activeNodes)

	// 9. Write to Raft.
	allocMap := make(map[string]*types.NodeAllocation, len(nodeAllocs))
	for _, na := range nodeAllocs {
		allocMap[na.NodeID] = na
	}

	data, err := json.Marshal(allocMap)
	if err != nil {
		return err
	}

	cmd := raftpkg.MustMarshalCommand(raftpkg.OpUpdateAllocation, json.RawMessage(data))
	result := e.raft.Apply(cmd, 5000)
	if err := result.Error(); err != nil {
		return err
	}

	// Log idle tenants and any status changes.
	idleCount := 0
	idleNames := make([]string, 0)
	for _, t := range tenantList {
		if idleSet[t.ID] {
			idleCount++
			if len(idleNames) < 5 { // cap log noise
				idleNames = append(idleNames, t.ID)
			}
		}
	}

	e.logger.Info("allocator: plan committed",
		zap.Int("nodes", len(activeNodes)),
		zap.Int("tenants", len(tenantList)),
		zap.Int("idle_tenants", idleCount),
		zap.Strings("idle_examples", idleNames),
		zap.Int("total_workers", totalClusterWorkers),
		zap.Int("borrowed_workers", sumWorkers(borrowed)),
	)
	return nil
}

func (e *Engine) requeueStaleTasks() error {
	taskIDs := e.fsm.FindStaleInflightTaskIDs(time.Now().UTC().Add(-taskClaimLease))
	if len(taskIDs) == 0 {
		return nil
	}
	result := e.raft.Apply(raftpkg.MustMarshalCommand(raftpkg.OpRequeueTasks, raftpkg.RequeueTasksData{TaskIDs: taskIDs}), 5000)
	if err := result.Error(); err != nil {
		return err
	}
	e.logger.Warn("allocator: expired task claims returned to pending", zap.Int("tasks", len(taskIDs)))
	return nil
}

// ---------------------------------------------------------------------------
// Idle detection
// ---------------------------------------------------------------------------

// updateIdleState increments or resets the idle-cycle counters and returns
// the set of tenants currently classified as idle.
func (e *Engine) updateIdleState(tenants []*types.TenantConfig, inflightCount map[string]int) map[string]bool {
	idleSet := make(map[string]bool, len(tenants))

	for _, t := range tenants {
		if inflightCount[t.ID] > 0 {
			// Has work — reset the counter.
			if e.idleCycles[t.ID] >= idleThreshold {
				e.logger.Info("allocator: tenant woke up from idle",
					zap.String("tenant", t.ID),
				)
			}
			e.idleCycles[t.ID] = 0
		} else {
			e.idleCycles[t.ID]++
			if e.idleCycles[t.ID] == idleThreshold {
				e.logger.Info("allocator: tenant marked idle",
					zap.String("tenant", t.ID),
					zap.Int("cycles", e.idleCycles[t.ID]),
				)
			}
		}

		if e.idleCycles[t.ID] >= idleThreshold {
			idleSet[t.ID] = true
		}
	}

	// Clean up counters for tenants that no longer exist.
	for tid := range e.idleCycles {
		found := false
		for _, t := range tenants {
			if t.ID == tid {
				found = true
				break
			}
		}
		if !found {
			delete(e.idleCycles, tid)
		}
	}

	return idleSet
}

// ---------------------------------------------------------------------------
// Idle penalty + redistribution
// ---------------------------------------------------------------------------

// applyIdleAdjustment takes the base max-min allocation and reduces idle
// tenants to a single catch-all worker.  The released capacity is
// redistributed among the active tenants using a second max-min pass.
func (e *Engine) applyIdleAdjustment(
	tenants []*types.TenantConfig,
	baseAlloc map[string]int,
	idleSet map[string]bool,
) map[string]int {
	final := make(map[string]int, len(tenants))
	released := 0

	// Split tenants.
	var active []*types.TenantConfig
	for _, t := range tenants {
		if idleSet[t.ID] {
			// Idle: keep only the catch-all minimum.
			final[t.ID] = 1
			if baseAlloc[t.ID] > 1 {
				released += baseAlloc[t.ID] - 1
			}
		} else {
			active = append(active, t)
			final[t.ID] = baseAlloc[t.ID]
		}
	}

	if released == 0 || len(active) == 0 {
		return final
	}

	// Redistribute released workers among active tenants.
	// We treat each active tenant's remaining headroom (MaxWorkers - current)
	// as the cap for a secondary max-min pass.
	extraAlloc := e.maxMinFairness(active, released)

	for _, t := range active {
		final[t.ID] += extraAlloc[t.ID]
	}

	return final
}

// applyBorrowing applies the adaptive idle-capacity policy after normal
// fairness and idle redistribution. Any tenant with an aged pending backlog
// may borrow otherwise unused cluster capacity; multiple backlogged tenants
// share the spare capacity deterministically. Each tenant's target ramps as
// 1,3,7,... (or a larger initial probe on a very large cluster), bounded by
// its pending count and the remaining capacity. Borrowing is a current
// allocation mirror, not history.
//
// The release decision includes inflight work: even if a newly submitted
// task has already been claimed, its tenant is considered active and causes a
// borrowed tenant to give capacity back. A tenant with only inflight work and
// no pending backlog does not receive new borrowed workers.
func (e *Engine) applyBorrowing(
	tenants []*types.TenantConfig,
	allocation map[string]int,
	totalWorkers int,
	unfinished map[string]int,
	pending map[string]int,
	oldestPending map[string]time.Time,
	idleSet map[string]bool,
) (map[string]int, map[string]int) {
	effective := make(map[string]int, len(allocation))
	for tenantID, count := range allocation {
		effective[tenantID] = count
	}
	borrowed := make(map[string]int)
	if e.borrowedTargets == nil {
		e.borrowedTargets = make(map[string]int)
	}

	backlogged := make([]string, 0, len(tenants))
	known := make(map[string]struct{}, len(tenants))
	for _, tenant := range tenants {
		known[tenant.ID] = struct{}{}
		oldest := oldestPending[tenant.ID]
		aged := !oldest.IsZero() && time.Since(oldest) >= pendingBorrowThreshold
		if unfinished[tenant.ID] > 0 && pending[tenant.ID] > 0 && !idleSet[tenant.ID] && aged {
			backlogged = append(backlogged, tenant.ID)
		} else {
			delete(e.borrowedTargets, tenant.ID)
		}
	}
	for tenantID := range e.borrowedTargets {
		if _, ok := known[tenantID]; !ok {
			delete(e.borrowedTargets, tenantID)
		}
	}

	sort.Strings(backlogged)
	if len(backlogged) == 0 {
		return effective, borrowed
	}

	spare := totalWorkers - sumWorkers(effective)
	if spare <= 0 {
		for _, tenantID := range backlogged {
			delete(e.borrowedTargets, tenantID)
		}
		return effective, borrowed
	}

	// Give every aged tenant a bounded probe in this cycle. A large cluster
	// should not spend dozens of 3-second cycles discovering that thousands of
	// workers are available, while small-cluster behavior stays conservative.
	remainingTenants := len(backlogged)
	for _, tenantID := range backlogged {
		if spare <= 0 {
			delete(e.borrowedTargets, tenantID)
			continue
		}
		previous := e.borrowedTargets[tenantID]
		step := borrowProbeStep
		if previous == 0 && totalWorkers >= 1000 {
			step = 64
		}
		target := step
		if previous > 0 {
			target = previous*2 + step
		}
		if target > pending[tenantID] {
			target = pending[tenantID]
		}
		// Keep the probe fair when several tenants are backlogged.
		share := (spare + remainingTenants - 1) / remainingTenants
		if target > share {
			target = share
		}
		if target <= 0 {
			delete(e.borrowedTargets, tenantID)
			remainingTenants--
			continue
		}
		e.borrowedTargets[tenantID] = target
		effective[tenantID] += target
		borrowed[tenantID] = target
		spare -= target
		remainingTenants--
	}
	return effective, borrowed
}

func sumWorkers(workers map[string]int) int {
	total := 0
	for _, count := range workers {
		total += count
	}
	return total
}

// ---------------------------------------------------------------------------
// Max-min fairness algorithm
// ---------------------------------------------------------------------------

// maxMinFairness distributes totalWorkers across tenants using a progressive-
// filling max-min fairness algorithm.  Each tenant receives at least
// minWorkersPerTenant workers (capped at its own MaxWorkers limit).
func (e *Engine) maxMinFairness(tenants []*types.TenantConfig, total int) map[string]int {
	alloc := make(map[string]int, len(tenants))
	remaining := total

	// Phase 1: guarantee every tenant at least minWorkersPerTenant.
	for _, t := range tenants {
		give := e.minWorkersPerTenant
		if give > t.MaxWorkers {
			give = t.MaxWorkers
		}
		alloc[t.ID] = give
		remaining -= give
	}

	// Phase 2: progressive filling for unsatisfied tenants.
	for remaining > 0 {
		var unsatisfied []*types.TenantConfig
		for _, t := range tenants {
			if alloc[t.ID] < t.MaxWorkers {
				unsatisfied = append(unsatisfied, t)
			}
		}
		if len(unsatisfied) == 0 {
			break
		}

		fair := remaining / len(unsatisfied)
		if fair == 0 {
			for _, t := range unsatisfied {
				if remaining == 0 {
					break
				}
				if alloc[t.ID] < t.MaxWorkers {
					alloc[t.ID]++
					remaining--
				}
			}
			break
		}

		roundAllocated := 0
		for _, t := range unsatisfied {
			need := t.MaxWorkers - alloc[t.ID]
			give := fair
			if give > need {
				give = need
			}
			alloc[t.ID] += give
			roundAllocated += give
		}
		remaining -= roundAllocated

		if roundAllocated == 0 {
			break
		}
	}

	return alloc
}

// ---------------------------------------------------------------------------
// Distribution across nodes
// ---------------------------------------------------------------------------

// distributeAcrossNodes takes a per-tenant worker count and spreads it
// evenly across all active nodes. It is retained as a small compatibility
// wrapper for callers and tests that do not need borrowed metadata.
func (e *Engine) distributeAcrossNodes(
	tenantAlloc map[string]int,
	nodes []*types.NodeInfo,
) []*types.NodeAllocation {
	return e.distributeAcrossNodesWithBorrowed(tenantAlloc, nil, nodes)
}

// distributeAcrossNodesWithBorrowed spreads effective and borrowed worker
// counts using the same deterministic split. Borrowed is a current snapshot
// field for observability; workers enforce only the effective Tenants count.
func (e *Engine) distributeAcrossNodesWithBorrowed(
	tenantAlloc map[string]int,
	borrowedAlloc map[string]int,
	nodes []*types.NodeInfo,
) []*types.NodeAllocation {
	nodeCount := len(nodes)
	result := make([]*types.NodeAllocation, nodeCount)
	for i, n := range nodes {
		result[i] = &types.NodeAllocation{
			NodeID:   n.ID,
			Tenants:  make(map[string]int, len(tenantAlloc)),
			Borrowed: make(map[string]int, len(borrowedAlloc)),
		}
	}

	distribute := func(values map[string]int, field func(*types.NodeAllocation) map[string]int) {
		for tenantID, total := range values {
			if total <= 0 {
				continue
			}
			perNode := total / nodeCount
			remainder := total % nodeCount

			for i, na := range result {
				count := perNode
				if i < remainder {
					count++
				}
				if count > 0 {
					field(na)[tenantID] = count
				}
			}
		}
	}
	distribute(tenantAlloc, func(na *types.NodeAllocation) map[string]int { return na.Tenants })
	distribute(borrowedAlloc, func(na *types.NodeAllocation) map[string]int { return na.Borrowed })

	return result
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (e *Engine) activeNodes(nodes map[string]*types.NodeInfo) []*types.NodeInfo {
	var active []*types.NodeInfo
	for _, n := range nodes {
		if n.Status == types.NodeStatusUp {
			active = append(active, n)
		}
	}
	sort.Slice(active, func(i, j int) bool { return nodeIDLess(active[i].ID, active[j].ID) })
	return active
}

// nodeIDLess keeps allocation placement stable while treating the usual
// node-2/node-10 suffixes numerically. IDs without a numeric suffix fall back
// to a lexical comparison.
func nodeIDLess(left, right string) bool {
	leftPrefix, leftNum, leftOK := splitNodeID(left)
	rightPrefix, rightNum, rightOK := splitNodeID(right)
	if leftOK && rightOK && leftPrefix != rightPrefix {
		return leftPrefix < rightPrefix
	}
	if leftOK && rightOK && leftNum != rightNum {
		return leftNum < rightNum
	}
	return left < right
}

func splitNodeID(id string) (string, int, bool) {
	idx := strings.LastIndexByte(id, '-')
	if idx < 0 || idx == len(id)-1 {
		return id, 0, false
	}
	n, err := strconv.Atoi(id[idx+1:])
	return id[:idx], n, err == nil
}

func (e *Engine) tenantList(tenants map[string]*types.TenantConfig) []*types.TenantConfig {
	list := make([]*types.TenantConfig, 0, len(tenants))
	for _, t := range tenants {
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })
	return list
}
