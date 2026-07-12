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
	snapshot := c.fsm.GetMetricsSnapshot()

	c.mu.Lock()
	defer c.mu.Unlock()

	// ---- unfinished task history ----
	var totalUnfinished int64
	for tenantID, count := range snapshot.Unfinished {
		totalUnfinished += count
		c.ensure("unfinished:"+tenantID, map[string]string{"tenant": tenantID}).Record(count)
	}
	c.ensure("unfinished:total", nil).Record(totalUnfinished)

	// ---- allocated worker history ----
	var totalAllocatedWorkers int64
	for tenantID, count := range snapshot.AllocatedWorkersByTenant {
		c.ensure("allocated-workers:tenant:"+tenantID, map[string]string{"tenant": tenantID}).Record(count)
	}
	for nodeID, count := range snapshot.AllocatedWorkersByNode {
		totalAllocatedWorkers += count
		c.ensure("allocated-workers:node:"+nodeID, map[string]string{"node": nodeID}).Record(count)
	}
	c.ensure("allocated-workers:total", nil).Record(totalAllocatedWorkers)

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
