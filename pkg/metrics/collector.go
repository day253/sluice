package metrics

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

// Collector periodically reads the FSM and records metric samples.
type Collector struct {
	fsm    *raftpkg.FSM
	logger *zap.Logger

	mu      sync.RWMutex
	vars    map[string]*VarHistory // key → history
}

func NewCollector(fsm *raftpkg.FSM, logger *zap.Logger) *Collector {
	return &Collector{
		fsm:    fsm,
		logger: logger,
		vars:   make(map[string]*VarHistory),
	}
}

// Start begins the collection loop (every 1 second).
func (c *Collector) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

func (c *Collector) collect() {
	state := c.fsm.GetState()

	// ---- inflight per tenant ----
	inflight := make(map[string]int64)
	for _, t := range state.Tasks {
		inflight[t.TenantID]++
	}

	c.mu.Lock()
	for tid, cnt := range inflight {
		v := c.ensureVar("inflight:"+tid, map[string]string{"tenant": tid})
		v.Record(cnt)
		v.Snapshot()
	}

	// ---- total inflight ----
	total := int64(len(state.Tasks))
	tv := c.ensureVar("inflight:total", nil)
	tv.Record(total)
	tv.Snapshot()

	// ---- worker allocation per tenant ----
	allocCounts := make(map[string]int64)
	for _, alloc := range state.Allocations {
		for tid, cnt := range alloc.Tenants {
			allocCounts[tid] += int64(cnt)
		}
	}
	for tid, cnt := range allocCounts {
		v := c.ensureVar("alloc:"+tid, map[string]string{"tenant": tid})
		v.Record(cnt)
		v.Snapshot()
	}

	// ---- node count ----
	activeNodes := int64(0)
	for _, n := range state.Nodes {
		if n.Status == "up" {
			activeNodes++
		}
	}
	nv := c.ensureVar("nodes:active", nil)
	nv.Record(activeNodes)
	nv.Snapshot()

	c.mu.Unlock()
}

func (c *Collector) ensureVar(name string, labels map[string]string) *VarHistory {
	if v, ok := c.vars[name]; ok {
		return v
	}
	v := NewVarHistory(name, labels)
	c.vars[name] = v
	return v
}

// Query returns historical data for a metric, or all metrics if name is empty.
func (c *Collector) Query(name string) ([]VarHistoryData, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if name != "" {
		if v, ok := c.vars[name]; ok {
			return []VarHistoryData{v.Query()}, 1
		}
		return nil, 0
	}

	out := make([]VarHistoryData, 0, len(c.vars))
	for _, v := range c.vars {
		out = append(out, v.Query())
	}
	return out, len(out)
}

// ListNames returns all tracked metric names.
func (c *Collector) ListNames() []string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	names := make([]string, 0, len(c.vars))
	for name := range c.vars {
		names = append(names, name)
	}
	return names
}
