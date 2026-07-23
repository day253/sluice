package metrics

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

// Collector samples FSM metrics every second into VarHistory ring buffers.
type Collector struct {
	fsm    *raftpkg.FSM
	logger *zap.Logger
	perf   *Performance

	mu   sync.RWMutex
	vars map[string]*VarHistory
}

// SetPerformance attaches process-local control-plane observations. They are
// sampled into bounded history but never written into the Raft FSM.
func (c *Collector) SetPerformance(perf *Performance) {
	c.mu.Lock()
	c.perf = perf
	c.mu.Unlock()
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
	c.mu.RLock()
	perf := c.perf
	c.mu.RUnlock()
	var performance performanceWindow
	if perf != nil {
		performance = perf.sample()
	}

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

	// ---- leader-local control-plane performance history ----
	for op, operation := range performance.Raft {
		prefix := "performance:raft:" + op + ":"
		c.ensure(prefix+"apply-rate", map[string]string{"op": op}).Record(int64(operation.Applies))
		c.ensure(prefix+"item-rate", map[string]string{"op": op}).Record(int64(operation.Items))
		c.ensure(prefix+"errors", map[string]string{"op": op}).Record(int64(operation.Errors))
		c.ensure(prefix+"batch-size", map[string]string{"op": op}).Record(divideUint64(operation.Items, operation.Applies))
		c.ensure(prefix+"apply-us", map[string]string{"op": op}).Record(divideInt64(operation.TotalMicros, operation.Applies))
		c.ensure(prefix+"apply-max-us", map[string]string{"op": op}).Record(operation.MaxMicros)
	}
	scheduler := performance.Scheduler
	c.ensure("performance:scheduler:selection-rate", nil).Record(int64(scheduler.Selections))
	c.ensure("performance:scheduler:pending-scanned", nil).Record(int64(scheduler.PendingScanned))
	c.ensure("performance:scheduler:tasks-selected", nil).Record(int64(scheduler.TasksSelected))
	c.ensure("performance:scheduler:load-aware-requests", nil).Record(int64(scheduler.LoadAwareRequests))
	c.ensure("performance:scheduler:load-throttled-requests", nil).Record(int64(scheduler.LoadThrottledRequests))
	c.ensure("performance:scheduler:load-unavailable-requests", nil).Record(int64(scheduler.LoadUnavailableRequests))
	c.ensure("performance:scheduler:stale-load-requests", nil).Record(int64(scheduler.StaleLoadRequests))
	c.ensure("performance:scheduler:worker-cpu-max-millis", nil).Record(scheduler.MaxWorkerCPUMillis)
	c.ensure("performance:scheduler:reporting-workers", nil).Record(scheduler.ReportingWorkers)
	c.ensure("performance:scheduler:select-us", nil).Record(divideInt64(scheduler.TotalSelectMicros, scheduler.Selections))
	c.ensure("performance:scheduler:select-max-us", nil).Record(scheduler.MaxSelectMicros)
	c.ensure("performance:scheduler:assignment-queue-depth", nil).Record(scheduler.AssignmentQueueDepth)
	c.ensure("performance:scheduler:completion-queue-depth", nil).Record(scheduler.CompletionQueueDepth)

	// ---- Tick all vars ----
	for _, v := range c.vars {
		v.Tick()
	}
}

// PerformanceDiagnostics returns current totals plus only the bounded
// performance histories. Workload and allocation histories remain available
// from the existing /api/v1/metrics endpoint.
func (c *Collector) PerformanceDiagnostics(nodeID string) PerformanceDiagnostics {
	diagnostics := PerformanceDiagnostics{
		NodeID: nodeID, CollectedAt: time.Now().UTC(), History: make(map[string]VarData),
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.perf != nil {
		diagnostics.Current = c.perf.Snapshot()
	}
	for name, history := range c.vars {
		if strings.HasPrefix(name, "performance:") {
			diagnostics.History[name] = history.Query()
		}
	}
	return diagnostics
}

// PerformanceCurrent returns the same cumulative snapshot as
// PerformanceDiagnostics without copying the 174-point histories. It is used
// by frequent UI polling; the full diagnostics endpoint remains the default
// for operators and external consumers.
func (c *Collector) PerformanceCurrent(nodeID string) PerformanceDiagnostics {
	diagnostics := PerformanceDiagnostics{
		NodeID: nodeID, CollectedAt: time.Now().UTC(), History: make(map[string]VarData),
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.perf != nil {
		diagnostics.Current = c.perf.Snapshot()
	}
	return diagnostics
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

// QueryNamed returns named historical data. An optional prefix excludes
// matching series before their ring buffers are copied.
func (c *Collector) QueryNamed(name string, excludePrefix ...string) []NamedVarData {
	excluded := ""
	if len(excludePrefix) > 0 {
		excluded = excludePrefix[0]
	}
	return c.QueryNamedFiltered(name, "", excluded)
}

// QueryNamedFiltered returns either one exact metric or every metric matching
// includePrefix. Both prefix filters are applied before copying ring buffers.
func (c *Collector) QueryNamedFiltered(name, includePrefix, excludePrefix string) []NamedVarData {
	c.mu.RLock()
	defer c.mu.RUnlock()
	excluded := func(metricName string) bool {
		return excludePrefix != "" && strings.HasPrefix(metricName, excludePrefix)
	}
	included := func(metricName string) bool {
		return includePrefix == "" || strings.HasPrefix(metricName, includePrefix)
	}

	if name != "" {
		if excluded(name) || !included(name) {
			return nil
		}
		if v, ok := c.vars[name]; ok {
			return []NamedVarData{{Name: name, Secs: v.Query().Secs, Mins: v.Query().Mins, Hours: v.Query().Hours, Days: v.Query().Days}}
		}
		return nil
	}

	out := make([]NamedVarData, 0, len(c.vars))
	for n, v := range c.vars {
		if excluded(n) || !included(n) {
			continue
		}
		d := v.Query()
		out = append(out, NamedVarData{Name: n, Secs: d.Secs, Mins: d.Mins, Hours: d.Hours, Days: d.Days})
	}
	return out
}
