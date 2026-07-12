package metrics

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

const maxHistory = 600 // 10 minutes at 1s interval

type varData struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Values []int64           `json:"values"`
}

// Collector samples FSM metrics every second into a ring buffer.
type Collector struct {
	fsm    *raftpkg.FSM
	logger *zap.Logger

	mu   sync.RWMutex
	vars map[string]*varData
}

func NewCollector(fsm *raftpkg.FSM, logger *zap.Logger) *Collector {
	return &Collector{
		fsm:    fsm,
		logger: logger,
		vars:   make(map[string]*varData),
	}
}

func (c *Collector) Start(ctx context.Context) {
	c.logger.Info("metrics: collector started (1s interval)")
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
	push(c, "inflight:total", nil, totalInflight)
	for tid, cnt := range perTenant {
		push(c, "inflight:"+tid, map[string]string{"tenant": tid}, cnt)
	}

	// ---- allocation per tenant ----
	allocCounts := map[string]int64{}
	for _, alloc := range state.Allocations {
		for tid, cnt := range alloc.Tenants {
			allocCounts[tid] += int64(cnt)
		}
	}
	for tid, cnt := range allocCounts {
		push(c, "alloc:"+tid, map[string]string{"tenant": tid}, cnt)
	}

	// ---- active nodes ----
	active := int64(0)
	for _, n := range state.Nodes {
		if n.Status == "up" {
			active++
		}
	}
	push(c, "nodes:active", nil, active)

	// ---- total workers ----
	totalW := int64(0)
	for _, n := range state.Nodes {
		totalW += int64(n.TotalWorkers)
	}
	push(c, "workers:total", nil, totalW)
}

func push(c *Collector, name string, labels map[string]string, val int64) {
	v, ok := c.vars[name]
	if !ok {
		v = &varData{Name: name, Labels: labels}
		c.vars[name] = v
	}
	v.Values = append(v.Values, val)
	if len(v.Values) > maxHistory {
		v.Values = v.Values[len(v.Values)-maxHistory:]
	}
}

// Query returns historical data.  If name is empty, returns all.
func (c *Collector) Query(name string) ([]varData, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if name != "" {
		if v, ok := c.vars[name]; ok {
			return []varData{*v}, 1
		}
		return nil, 0
	}

	out := make([]varData, 0, len(c.vars))
	for _, v := range c.vars {
		out = append(out, *v)
	}
	return out, len(out)
}
