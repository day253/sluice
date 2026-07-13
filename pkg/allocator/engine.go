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

	// 7. Distribute each tenant's workers across active nodes.
	nodeAllocs := e.distributeAcrossNodes(finalAlloc, activeNodes)

	// 8. Write to Raft.
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
// evenly across all active nodes.
func (e *Engine) distributeAcrossNodes(
	tenantAlloc map[string]int,
	nodes []*types.NodeInfo,
) []*types.NodeAllocation {
	nodeCount := len(nodes)
	result := make([]*types.NodeAllocation, nodeCount)
	for i, n := range nodes {
		result[i] = &types.NodeAllocation{
			NodeID:  n.ID,
			Tenants: make(map[string]int, len(tenantAlloc)),
		}
	}

	for tenantID, total := range tenantAlloc {
		perNode := total / nodeCount
		remainder := total % nodeCount

		for i, na := range result {
			count := perNode
			if i < remainder {
				count++
			}
			if count > 0 {
				na.Tenants[tenantID] = count
			}
		}
	}

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
	return active
}

func (e *Engine) tenantList(tenants map[string]*types.TenantConfig) []*types.TenantConfig {
	list := make([]*types.TenantConfig, 0, len(tenants))
	for _, t := range tenants {
		list = append(list, t)
	}
	return list
}
