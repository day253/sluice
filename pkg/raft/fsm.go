package raft

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/types"
)

// maxRetainedTaskResults bounds the queryable result window in Raft snapshots.
// Aggregate task counters remain durable after an individual result is evicted.
const maxRetainedTaskResults = 10000

// FSM is the Raft finite state machine.  All mutations to cluster state flow
// through Apply(), which is called by the Raft library on the leader's
// goroutine — it is inherently single-threaded for writes.
//
// Reads from other goroutines must acquire the read lock via exported accessors.
type FSM struct {
	mu     sync.RWMutex
	state  *types.FSMState
	logger *zap.Logger
}

// NewFSM creates a ready-to-use FSM with an empty state.
func NewFSM(logger *zap.Logger) *FSM {
	return &FSM{
		state:  types.NewFSMState(),
		logger: logger,
	}
}

// ---------------------------------------------------------------------------
// raft.FSM implementation
// ---------------------------------------------------------------------------

// Apply executes a single Raft log entry against the state machine.  The
// returned value is available via ApplyFuture.Response().
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		f.logger.Error("fsm: unmarshal command", zap.Error(err))
		return fmt.Errorf("fsm unmarshal: %w", err)
	}

	f.state.Version++

	switch cmd.Op {
	case OpUpsertTenant:
		return f.applyUpsertTenant(cmd.Data)
	case OpDeleteTenant:
		return f.applyDeleteTenant(cmd.Data)
	case OpNodeUp:
		return f.applyNodeUp(cmd.Data)
	case OpNodeDown:
		return f.applyNodeDown(cmd.Data)
	case OpCreateTask:
		return f.applyCreateTask(cmd.Data)
	case OpClaimTask:
		return f.applyClaimTask(cmd.Data)
	case OpClaimBatch:
		return f.applyClaimBatch(cmd.Data)
	case OpCompleteTask:
		return f.applyCompleteTask(cmd.Data)
	case OpFailTask:
		return f.applyFailTask(cmd.Data)
	case OpCompleteBatch:
		return f.applyCompleteBatch(cmd.Data)
	case OpRequeueTasks:
		return f.applyRequeueTasks(cmd.Data)
	case OpUpdateAllocation:
		return f.applyUpdateAllocation(cmd.Data)
	default:
		f.logger.Warn("fsm: unknown op", zap.String("op", cmd.Op))
		return fmt.Errorf("unknown op: %s", cmd.Op)
	}
}

// Snapshot returns a snapshot of the current state.  Called periodically by
// Raft to compact the log.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	data, err := json.Marshal(f.state)
	if err != nil {
		return nil, fmt.Errorf("fsm snapshot marshal: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore replaces the entire state from a snapshot.  Called on startup
// and when the leader installs a snapshot on a follower.
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var state types.FSMState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("fsm restore decode: %w", err)
	}

	// Ensure maps are non-nil (they may come back nil from JSON).
	if state.Nodes == nil {
		state.Nodes = make(map[string]*types.NodeInfo)
	}
	if state.Tenants == nil {
		state.Tenants = make(map[string]*types.TenantConfig)
	}
	if state.Allocations == nil {
		state.Allocations = make(map[string]*types.NodeAllocation)
	}
	if state.Tasks == nil {
		state.Tasks = make(map[string]*types.TaskRecord)
	}
	if state.Results == nil {
		state.Results = make(map[string]*types.TaskResult)
	}
	if len(state.ResultOrder) == 0 && len(state.Results) > 0 {
		state.ResultOrder = make([]string, 0, len(state.Results))
		for taskID := range state.Results {
			state.ResultOrder = append(state.ResultOrder, taskID)
		}
		sort.Slice(state.ResultOrder, func(i, j int) bool {
			a, b := state.Results[state.ResultOrder[i]], state.Results[state.ResultOrder[j]]
			if a.CompletedAt.Equal(b.CompletedAt) {
				return a.TaskID < b.TaskID
			}
			return a.CompletedAt.Before(b.CompletedAt)
		})
	}
	for len(state.ResultOrder) > maxRetainedTaskResults {
		delete(state.Results, state.ResultOrder[0])
		state.ResultOrder = state.ResultOrder[1:]
	}

	f.state = &state
	f.logger.Info("fsm: state restored from snapshot",
		zap.Uint64("version", state.Version),
		zap.Int("tenants", len(state.Tenants)),
		zap.Int("nodes", len(state.Nodes)),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Command handlers (caller holds f.mu)
// ---------------------------------------------------------------------------

func (f *FSM) applyUpsertTenant(data json.RawMessage) interface{} {
	var tc types.TenantConfig
	if err := json.Unmarshal(data, &tc); err != nil {
		return err
	}
	if tc.MaxWorkers < 1 {
		return fmt.Errorf("max_workers must be >= 1")
	}

	now := time.Now().UTC()
	if existing, ok := f.state.Tenants[tc.ID]; ok {
		tc.CreatedAt = existing.CreatedAt
	} else {
		tc.CreatedAt = now
	}
	tc.UpdatedAt = now
	f.state.Tenants[tc.ID] = &tc

	f.logger.Info("fsm: tenant upserted",
		zap.String("tenant", tc.ID),
		zap.Int("max_workers", tc.MaxWorkers),
	)
	return nil
}

func (f *FSM) applyDeleteTenant(data json.RawMessage) interface{} {
	var req DeleteTenantData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	delete(f.state.Tenants, req.ID)
	f.logger.Info("fsm: tenant deleted", zap.String("tenant", req.ID))
	return nil
}

func (f *FSM) applyNodeUp(data json.RawMessage) interface{} {
	var ni types.NodeInfo
	if err := json.Unmarshal(data, &ni); err != nil {
		return err
	}
	ni.Status = types.NodeStatusUp
	ni.LastHeartbeat = time.Now().UTC()
	f.state.Nodes[ni.ID] = &ni
	f.logger.Debug("fsm: node up", zap.String("node", ni.ID))
	return nil
}

func (f *FSM) applyNodeDown(data json.RawMessage) interface{} {
	var req NodeDownData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	node, ok := f.state.Nodes[req.ID]
	if !ok {
		return fmt.Errorf("node %s not found", req.ID)
	}
	node.Status = types.NodeStatusDown

	// Re-queue all inflight tasks that were assigned to this node.
	reQueued := 0
	for _, task := range f.state.Tasks {
		if task.NodeID == req.ID && task.Status == types.TaskStatusInflight {
			task.Status = types.TaskStatusPending
			task.NodeID = ""
			task.ClaimedAt = time.Time{}
			reQueued++
		}
	}

	f.logger.Warn("fsm: node down — tasks re-queued",
		zap.String("node", req.ID),
		zap.Int("re_queued", reQueued),
	)
	return nil
}

// applyCreateTask writes a new task as "pending" in the FSM so that any
// node's workers can claim it via the recovery / pending-task path.
func (f *FSM) applyCreateTask(data json.RawMessage) interface{} {
	var req CreateTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	// If a task with this ID already exists or has already completed
	// (idempotency), skip. A delayed duplicate submission must not resurrect a
	// completed task.
	if _, ok := f.state.Tasks[req.TaskID]; ok {
		return nil
	}
	if _, ok := f.state.Results[req.TaskID]; ok {
		return nil
	}
	f.state.Tasks[req.TaskID] = &types.TaskRecord{
		TaskID:    req.TaskID,
		TenantID:  req.TenantID,
		Status:    types.TaskStatusPending,
		Payload:   req.Payload,
		CreatedAt: time.Now().UTC(),
	}
	return nil
}

func (f *FSM) applyClaimTask(data json.RawMessage) interface{} {
	var req ClaimTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	now := time.Now().UTC()
	if _, completed := f.state.Results[req.TaskID]; completed {
		return fmt.Errorf("task %s already completed", req.TaskID)
	}

	// If the task was already claimed (by this node or another) reject.
	if existing, ok := f.state.Tasks[req.TaskID]; ok {
		if existing.Status == types.TaskStatusInflight {
			return fmt.Errorf("task %s already claimed by %s", req.TaskID, existing.NodeID)
		}
		// Task is in recovery-pending state — reclaim it.
		existing.Status = types.TaskStatusInflight
		existing.NodeID = req.NodeID
		existing.ClaimedAt = now
		existing.Payload = req.Payload
		return nil
	}

	// Fresh claim — the payload is being promoted from the local queue into
	// the Raft log, giving it cluster-wide durability.
	f.state.Tasks[req.TaskID] = &types.TaskRecord{
		TaskID:    req.TaskID,
		TenantID:  req.TenantID,
		Status:    types.TaskStatusInflight,
		NodeID:    req.NodeID,
		Payload:   req.Payload,
		CreatedAt: now,
		ClaimedAt: now,
	}
	return nil
}

func (f *FSM) applyCompleteTask(data json.RawMessage) interface{} {
	var req CompleteTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	f.finishTask(req, types.TaskStatusDone, time.Now().UTC())
	return nil
}

func (f *FSM) applyFailTask(data json.RawMessage) interface{} {
	var req CompleteTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	f.finishTask(req, types.TaskStatusFailed, time.Now().UTC())
	return nil
}

// ---------------------------------------------------------------------------
// Batch operations (streaming internal API)
// ---------------------------------------------------------------------------

func (f *FSM) applyClaimBatch(data json.RawMessage) interface{} {
	var req ClaimBatchData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	result := &ClaimBatchResult{
		Claimed: make([]string, 0, len(req.Tasks)),
		Failed:  make([]string, 0),
	}

	now := time.Now().UTC()
	for _, t := range req.Tasks {
		if _, completed := f.state.Results[t.TaskID]; completed {
			result.Failed = append(result.Failed, t.TaskID)
			continue
		}
		if existing, ok := f.state.Tasks[t.TaskID]; ok {
			if existing.Status == types.TaskStatusInflight {
				result.Failed = append(result.Failed, t.TaskID)
				continue
			}
			// Recovery-pending → reclaim.
			existing.Status = types.TaskStatusInflight
			existing.NodeID = t.NodeID
			existing.ClaimedAt = now
			existing.Payload = t.Payload
			result.Claimed = append(result.Claimed, t.TaskID)
			continue
		}
		// Fresh claim.
		f.state.Tasks[t.TaskID] = &types.TaskRecord{
			TaskID:    t.TaskID,
			TenantID:  t.TenantID,
			Status:    types.TaskStatusInflight,
			NodeID:    t.NodeID,
			Payload:   t.Payload,
			CreatedAt: now,
			ClaimedAt: now,
		}
		result.Claimed = append(result.Claimed, t.TaskID)
	}
	return result
}

func (f *FSM) applyCompleteBatch(data json.RawMessage) interface{} {
	var req CompleteBatchData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, t := range req.Tasks {
		status := types.TaskStatusDone
		if t.Status == types.TaskStatusFailed || t.Error != "" {
			status = types.TaskStatusFailed
		}
		f.finishTask(t, status, now)
	}
	return nil
}

func (f *FSM) applyRequeueTasks(data json.RawMessage) interface{} {
	var req RequeueTasksData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	requeued := 0
	for _, taskID := range req.TaskIDs {
		task, ok := f.state.Tasks[taskID]
		if !ok || task.Status != types.TaskStatusInflight {
			continue
		}
		task.Status = types.TaskStatusPending
		task.NodeID = ""
		task.ClaimedAt = time.Time{}
		requeued++
	}
	if requeued > 0 {
		f.logger.Warn("fsm: stale task claims re-queued", zap.Int("tasks", requeued))
	}
	return nil
}

// finishTask moves one task out of the current unfinished snapshot, updates
// durable counters, and retains only a bounded recent result for status reads.
// A repeated completion is ignored so counters remain idempotent.
func (f *FSM) finishTask(req CompleteTaskData, status string, completedAt time.Time) {
	task, exists := f.state.Tasks[req.TaskID]
	if !exists {
		return
	}
	delete(f.state.Tasks, req.TaskID)

	tenantID := task.TenantID
	if tenantID == "" {
		tenantID = req.TenantID
	}
	result := &types.TaskResult{
		TaskID:      req.TaskID,
		TenantID:    tenantID,
		Status:      status,
		CompletedAt: completedAt,
	}
	if status == types.TaskStatusFailed {
		result.Error = req.Error
	} else {
		result.Result = req.Result
	}
	f.state.Results[req.TaskID] = result
	f.state.ResultOrder = append(f.state.ResultOrder, req.TaskID)
	if len(f.state.ResultOrder) > maxRetainedTaskResults {
		oldest := f.state.ResultOrder[0]
		f.state.ResultOrder = f.state.ResultOrder[1:]
		delete(f.state.Results, oldest)
	}
}

func (f *FSM) applyUpdateAllocation(data json.RawMessage) interface{} {
	var allocs map[string]*types.NodeAllocation
	if err := json.Unmarshal(data, &allocs); err != nil {
		return err
	}
	f.state.Allocations = allocs
	f.logger.Debug("fsm: allocation updated",
		zap.Int("nodes", len(allocs)),
	)
	return nil
}

// ---------------------------------------------------------------------------
// Read accessors (safe for concurrent use)
// ---------------------------------------------------------------------------

// GetState returns a deep copy of the current FSM state.
func (f *FSM) GetState() *types.FSMState {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.copyState()
}

// GetTenant returns a copy of the tenant config, if it exists.
func (f *FSM) GetTenant(id string) (*types.TenantConfig, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.state.Tenants[id]
	if !ok {
		return nil, false
	}
	copyT := *t
	return &copyT, true
}

// GetAllTenants returns a copy of all tenant configs.
func (f *FSM) GetAllTenants() map[string]*types.TenantConfig {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*types.TenantConfig, len(f.state.Tenants))
	for k, v := range f.state.Tenants {
		copyV := *v
		out[k] = &copyV
	}
	return out
}

// GetActiveNodes returns nodes currently marked "up".
func (f *FSM) GetActiveNodes() []*types.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var nodes []*types.NodeInfo
	for _, n := range f.state.Nodes {
		if n.Status == types.NodeStatusUp {
			copyN := *n
			nodes = append(nodes, &copyN)
		}
	}
	return nodes
}

// GetAllNodes returns copies of all nodes regardless of status.
func (f *FSM) GetAllNodes() map[string]*types.NodeInfo {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*types.NodeInfo, len(f.state.Nodes))
	for k, v := range f.state.Nodes {
		copyV := *v
		out[k] = &copyV
	}
	return out
}

// GetAllocation returns the worker allocation for this node, if any.
func (f *FSM) GetAllocation(nodeID string) (*types.NodeAllocation, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a, ok := f.state.Allocations[nodeID]
	if !ok {
		return nil, false
	}
	copyA := *a
	copyA.Tenants = make(map[string]int, len(a.Tenants))
	for k, v := range a.Tenants {
		copyA.Tenants[k] = v
	}
	return &copyA, true
}

// GetAllAllocations returns a copy of all node allocations.
func (f *FSM) GetAllAllocations() map[string]*types.NodeAllocation {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]*types.NodeAllocation, len(f.state.Allocations))
	for k, v := range f.state.Allocations {
		copyV := *v
		copyV.Tenants = make(map[string]int, len(v.Tenants))
		for tk, tv := range v.Tenants {
			copyV.Tenants[tk] = tv
		}
		out[k] = &copyV
	}
	return out
}

// GetTask returns a task record.  Returns nil if not found.
func (f *FSM) GetTask(taskID string) *types.TaskRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	t, ok := f.state.Tasks[taskID]
	if !ok {
		return nil
	}
	copyT := *t
	return &copyT
}

// GetResult returns a completed task result.  Returns nil if not found.
func (f *FSM) GetResult(taskID string) *types.TaskResult {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r, ok := f.state.Results[taskID]
	if !ok {
		return nil
	}
	copyR := *r
	return &copyR
}

// TaskStatus returns the status of a task by checking both inflight and
// completed maps.
func (f *FSM) TaskStatus(taskID string) (string, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if t, ok := f.state.Tasks[taskID]; ok {
		return t.Status, true
	}
	if r, ok := f.state.Results[taskID]; ok {
		return r.Status, true
	}
	return "", false
}

// FindPendingTasks returns all tasks with status "pending" for the given
// tenant.  These are tasks that were re-queued after a node failure.
func (f *FSM) FindPendingTasks(tenantID string) []*types.TaskRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []*types.TaskRecord
	for _, t := range f.state.Tasks {
		if t.TenantID == tenantID && t.Status == types.TaskStatusPending {
			copyT := *t
			out = append(out, &copyT)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].TaskID < out[j].TaskID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out
}

// FindStaleInflightTaskIDs returns tasks whose claim lease predates before.
func (f *FSM) FindStaleInflightTaskIDs(before time.Time) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var taskIDs []string
	for taskID, task := range f.state.Tasks {
		if task.Status == types.TaskStatusInflight && !task.ClaimedAt.IsZero() && task.ClaimedAt.Before(before) {
			taskIDs = append(taskIDs, taskID)
		}
	}
	sort.Strings(taskIDs)
	return taskIDs
}

// CountUnfinishedPerTenant returns the number of inflight + pending tasks per
// tenant. Used by the allocator for idle detection and by the Web UI.
func (f *FSM) CountUnfinishedPerTenant() map[string]int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]int)
	for _, t := range f.state.Tasks {
		out[t.TenantID]++
	}
	return out
}

// CountPendingPerTenant returns pending (unclaimed) tasks per tenant.
func (f *FSM) CountPendingPerTenant() map[string]int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]int)
	for _, t := range f.state.Tasks {
		if t.Status == types.TaskStatusPending {
			out[t.TenantID]++
		}
	}
	return out
}

// MetricsSnapshot is a lightweight view used by the 1-second collector. It
// intentionally excludes task payloads and recent result bodies.
type MetricsSnapshot struct {
	Unfinished               map[string]int64
	AllocatedWorkersByTenant map[string]int64
	AllocatedWorkersByNode   map[string]int64
}

// GetMetricsSnapshot returns current gauges and cumulative counters under one
// read lock, without deep-copying the full FSM state on every metrics tick.
func (f *FSM) GetMetricsSnapshot() MetricsSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()

	snapshot := MetricsSnapshot{
		Unfinished:               make(map[string]int64, len(f.state.Tenants)),
		AllocatedWorkersByTenant: make(map[string]int64, len(f.state.Tenants)),
		AllocatedWorkersByNode:   make(map[string]int64, len(f.state.Nodes)),
	}
	for tenantID := range f.state.Tenants {
		snapshot.Unfinished[tenantID] = 0
		snapshot.AllocatedWorkersByTenant[tenantID] = 0
	}
	for nodeID := range f.state.Nodes {
		snapshot.AllocatedWorkersByNode[nodeID] = 0
	}
	for _, task := range f.state.Tasks {
		snapshot.Unfinished[task.TenantID]++
	}
	for nodeID, allocation := range f.state.Allocations {
		for tenantID, count := range allocation.Tenants {
			snapshot.AllocatedWorkersByNode[nodeID] += int64(count)
			if _, configured := f.state.Tenants[tenantID]; configured {
				snapshot.AllocatedWorkersByTenant[tenantID] += int64(count)
			}
		}
	}
	return snapshot
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func (f *FSM) copyState() *types.FSMState {
	s := &types.FSMState{
		Nodes:       make(map[string]*types.NodeInfo, len(f.state.Nodes)),
		Tenants:     make(map[string]*types.TenantConfig, len(f.state.Tenants)),
		Allocations: make(map[string]*types.NodeAllocation, len(f.state.Allocations)),
		Tasks:       make(map[string]*types.TaskRecord, len(f.state.Tasks)),
		Results:     make(map[string]*types.TaskResult, len(f.state.Results)),
		ResultOrder: append([]string(nil), f.state.ResultOrder...),
		Version:     f.state.Version,
	}
	for k, v := range f.state.Nodes {
		copyV := *v
		s.Nodes[k] = &copyV
	}
	for k, v := range f.state.Tenants {
		copyV := *v
		s.Tenants[k] = &copyV
	}
	for k, v := range f.state.Allocations {
		copyV := *v
		copyV.Tenants = make(map[string]int, len(v.Tenants))
		for tk, tv := range v.Tenants {
			copyV.Tenants[tk] = tv
		}
		s.Allocations[k] = &copyV
	}
	for k, v := range f.state.Tasks {
		copyV := *v
		s.Tasks[k] = &copyV
	}
	for k, v := range f.state.Results {
		copyV := *v
		s.Results[k] = &copyV
	}
	return s
}

// ---------------------------------------------------------------------------
// fsmSnapshot implements raft.FSMSnapshot
// ---------------------------------------------------------------------------

type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write(s.data)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("snapshot persist: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
