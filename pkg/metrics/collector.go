package metrics

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

// Collector samples FSM metrics every second into VarHistory ring buffers.
type Collector struct {
	fsm    *raftpkg.FSM
	logger *zap.Logger

	mu   sync.RWMutex
	vars map[string]*VarHistory
}

func NewCollector(fsm *raftpkg.FSM, logger *zap.Logger) *Collector {
	return &Collector{
		fsm:    fsm,
		logger: logger,
		vars:   make(map[string]*VarHistory),
	}
}

func (c *Collector) Start(ctx context.Context) {
	c.logger.Info("metrics: collector started (1s tick)")
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

	c.mu.Lock()
	defer c.mu.Unlock()

	// ---- inflight per tenant ----
	perTenant := map[string]int64{}
	for _, t := range state.Tasks {
		perTenant[t.TenantID]++
	}
	totalInflight := int64(len(state.Tasks))
	c.ensure("inflight:total", nil).Record(totalInflight)
	for tid, cnt := range perTenant {
		c.ensure("inflight:"+tid, map[string]string{"tenant": tid}).Record(cnt)
	}

	// ---- allocation per tenant ----
	allocCounts := map[string]int64{}
	for _, alloc := range state.Allocations {
		for tid, cnt := range alloc.Tenants {
			allocCounts[tid] += int64(cnt)
		}
	}
	for tid, cnt := range allocCounts {
		c.ensure("alloc:"+tid, map[string]string{"tenant": tid}).Record(cnt)
	}

	// ---- active nodes ----
	active := int64(0)
	for _, n := range state.Nodes {
		if n.Status == "up" {
			active++
		}
	}
	c.ensure("nodes:active", nil).Record(active)

	// ---- Tick all vars ----
	for _, v := range c.vars {
		v.Tick()
	}
}

func (c *Collector) ensure(name string, labels map[string]string) *VarHistory {
	if v, ok := c.vars[name]; ok {
		return v
	}
	v := &VarHistory{}
	c.vars[name] = v
	return v
}

// Query returns historical data.  If name is empty, returns all.
func (c *Collector) Query(name string) ([]VarData, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if name != "" {
		if v, ok := c.vars[name]; ok {
			return []VarData{v.Query()}, 1
		}
		return nil, 0
	}

	out := make([]VarData, 0, len(c.vars))
	// Return with names for the API.
	for n, v := range c.vars {
		d := v.Query()
		// We lose the name here — need NamedVarData.
		_ = n
		_ = d
		out = append(out, d)
	}
	return out, len(out)
}

// NamedVarData is the API response type with metric name + rings.
type NamedVarData struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Secs   []int64           `json:"secs"`
	Mins   []int64           `json:"mins"`
	Hours  []int64           `json:"hours"`
	Days   []int64           `json:"days"`
}

// QueryNamed returns named historical data.
func (c *Collector) QueryNamed(name string) []NamedVarData {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if name != "" {
		if v, ok := c.vars[name]; ok {
			return []NamedVarData{{Name: name, Secs: v.Query().Secs, Mins: v.Query().Mins, Hours: v.Query().Hours, Days: v.Query().Days}}
		}
		return nil
	}

	out := make([]NamedVarData, 0, len(c.vars))
	for n, v := range c.vars {
		d := v.Query()
		out = append(out, NamedVarData{Name: n, Secs: d.Secs, Mins: d.Mins, Hours: d.Hours, Days: d.Days})
	}
	return out
}
