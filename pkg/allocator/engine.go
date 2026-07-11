// Package allocator implements the max-min fairness worker allocation
// algorithm.  It runs on the Raft leader and writes the computed allocation
// plan to the Raft log so every node sees the same view.
package allocator

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/distributed-rate-limiting/pkg/raft"
	"github.com/distributed-rate-limiting/pkg/types"
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
		return nil // silently ignore on non-leader
	}
	return e.Reconcile()
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

// Reconcile computes the fair allocation and writes it to Raft.
func (e *Engine) Reconcile() error {
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
		return nil
	}

	// 3. Max-min fairness per tenant.
	tenantAlloc := e.maxMinFairness(tenantList, totalClusterWorkers)

	// 4. Distribute each tenant's workers across active nodes.
	nodeAllocs := e.distributeAcrossNodes(tenantAlloc, activeNodes)

	// 5. Write to Raft.
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

	e.logger.Info("allocator: plan committed",
		zap.Int("nodes", len(activeNodes)),
		zap.Int("tenants", len(tenantList)),
		zap.Int("total_workers", totalClusterWorkers),
	)
	return nil
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
		// Collect tenants that still have headroom.
		var unsatisfied []*types.TenantConfig
		for _, t := range tenants {
			if alloc[t.ID] < t.MaxWorkers {
				unsatisfied = append(unsatisfied, t)
			}
		}
		if len(unsatisfied) == 0 {
			// Everyone is at their limit — distribute remaining worker slots
			// by ignoring caps.  (Shouldn't normally happen in oversubscribed
			// scenarios, but handles edge cases gracefully.)
			break
		}

		fair := remaining / len(unsatisfied)
		if fair == 0 {
			// Fewer workers left than tenants — give 1 to each in order
			// until exhausted.
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

		// Safety valve — if no progress was made, exit.
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

// min returns the smaller of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
