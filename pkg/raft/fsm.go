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
	mu      sync.RWMutex
	state   *types.FSMState
	pending *pendingIndex
	logger  *zap.Logger
}

// NewFSM creates a ready-to-use FSM with an empty state.
func NewFSM(logger *zap.Logger) *FSM {
	return &FSM{
		state:   types.NewFSMState(),
		pending: newPendingIndex(),
		logger:  logger,
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
	case OpWorkerOffline:
		return f.applyWorkerOffline(cmd.Data)
	case OpRetireNode:
		return f.applyRetireNode(cmd.Data)
	case OpSetControlNodes:
		return f.applySetControlNodes(cmd.Data)
	case OpCreateTask:
		return f.applyCreateTask(cmd.Data)
	case OpCreateTaskBatch:
		return f.applyCreateTaskBatch(cmd.Data)
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
	// Older releases could resurrect a completed task from a stale local queue,
	// leaving the same ID in both Tasks and Results. The completed result is
	// authoritative; keeping the unfinished copy would make workers reject and
	// retry it forever while starving newer pending work.
	repairedTasks := 0
	for taskID := range state.Results {
		if _, exists := state.Tasks[taskID]; exists {
			delete(state.Tasks, taskID)
			repairedTasks++
		}
	}

	f.state = &state
	f.pending = newPendingIndex()
	for _, task := range state.Tasks {
		f.pending.add(task)
	}
	f.logger.Info("fsm: state restored from snapshot",
		zap.Uint64("version", state.Version),
		zap.Int("tenants", len(state.Tenants)),
		zap.Int("nodes", len(state.Nodes)),
		zap.Int("repaired_completed_tasks", repairedTasks),
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
			f.pending.add(task)
			reQueued++
		}
	}

	f.logger.Warn("fsm: node down — tasks re-queued",
		zap.String("node", req.ID),
		zap.Int("re_queued", reQueued),
	)
	return nil
}

func (f *FSM) applyWorkerOffline(data json.RawMessage) interface{} {
	var req WorkerOfflineData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	node, ok := f.state.Nodes[req.ID]
	if !ok {
		return nil
	}
	// A delayed close from an older process incarnation must not take the
	// replacement worker offline.
	if req.SessionID != "" && node.SessionID != "" && node.SessionID != req.SessionID {
		return nil
	}
	node.Status = types.NodeStatusDown
	delete(f.state.Allocations, req.ID)
	f.logger.Warn("fsm: stateless worker offline",
		zap.String("node", req.ID), zap.String("session", req.SessionID))
	return nil
}

func (f *FSM) applyRetireNode(data json.RawMessage) interface{} {
	var req RetireNodeData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	delete(f.state.Allocations, req.ID)
	delete(f.state.Nodes, req.ID)
	// In-flight tasks deliberately retain their owner and lease. Lease expiry
	// is the only safe point to make them pending again.
	f.logger.Warn("fsm: node identity retired", zap.String("node", req.ID))
	return nil
}

func (f *FSM) applySetControlNodes(data json.RawMessage) interface{} {
	var req SetControlNodesData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	for _, nodeID := range req.NodeIDs {
		node := f.state.Nodes[nodeID]
		if node == nil {
			continue
		}
		node.Role = types.NodeRoleControl
		node.SessionID = ""
		node.TotalWorkers = 0
		delete(f.state.Allocations, nodeID)
	}
	return nil
}

// applyCreateTask writes a new task as "pending" in the FSM so that any
// node's workers can claim it via the recovery / pending-task path.
func (f *FSM) applyCreateTask(data json.RawMessage) interface{} {
	var req CreateTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	return f.insertPendingTask(req)
}

func (f *FSM) applyCreateTaskBatch(data json.RawMessage) interface{} {
	var req CreateTaskBatchData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}
	result := &CreateTaskBatchResult{Created: make([]string, 0, len(req.Tasks))}
	for _, task := range req.Tasks {
		if f.insertPendingTask(task) {
			result.Created = append(result.Created, task.TaskID)
		}
	}
	return result
}

// insertPendingTask applies the idempotent pending-task insertion shared by
// single and batch create commands. The caller holds f.mu through Apply.
func (f *FSM) insertPendingTask(req CreateTaskData) bool {
	// If a task with this ID already exists or has already completed
	// (idempotency), skip. A delayed duplicate submission must not resurrect a
	// completed task.
	if _, ok := f.state.Tasks[req.TaskID]; ok {
		return false
	}
	if _, ok := f.state.Results[req.TaskID]; ok {
		delete(f.state.Tasks, req.TaskID)
		return false
	}
	record := &types.TaskRecord{
		TaskID:      req.TaskID,
		TenantID:    req.TenantID,
		Status:      types.TaskStatusPending,
		QueueNodeID: req.QueueNodeID,
		Payload:     req.Payload,
		CreatedAt:   time.Now().UTC(),
	}
	f.state.Tasks[req.TaskID] = record
	f.pending.add(record)
	return true
}

func (f *FSM) applyClaimTask(data json.RawMessage) interface{} {
	var req ClaimTaskData
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	now := time.Now().UTC()
	if _, completed := f.state.Results[req.TaskID]; completed {
		f.pending.remove(f.state.Tasks[req.TaskID])
		delete(f.state.Tasks, req.TaskID)
		return fmt.Errorf("task %s already completed", req.TaskID)
	}

	// If the task was already claimed (by this node or another) reject.
	if existing, ok := f.state.Tasks[req.TaskID]; ok {
		if existing.Status == types.TaskStatusInflight {
			return fmt.Errorf("task %s already claimed by %s", req.TaskID, existing.NodeID)
		}
		// Task is in recovery-pending state — reclaim it.
		f.pending.remove(existing)
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
			f.pending.remove(f.state.Tasks[t.TaskID])
			delete(f.state.Tasks, t.TaskID)
			result.Failed = append(result.Failed, t.TaskID)
			continue
		}
		if existing, ok := f.state.Tasks[t.TaskID]; ok {
			if existing.Status == types.TaskStatusInflight {
				result.Failed = append(result.Failed, t.TaskID)
				continue
			}
			// Recovery-pending → reclaim.
			f.pending.remove(existing)
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
		f.pending.add(task)
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
	f.pending.remove(task)
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
	copyA.Borrowed = make(map[string]int, len(a.Borrowed))
	for k, v := range a.Borrowed {
		copyA.Borrowed[k] = v
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
		copyV.Borrowed = make(map[string]int, len(v.Borrowed))
		for tk, tv := range v.Borrowed {
			copyV.Borrowed[tk] = tv
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
// tenant, ordered by their original enqueue time. Pending tasks may be newly
// submitted or re-queued after a node failure.
func (f *FSM) FindPendingTasks(tenantID string) []*types.TaskRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*types.TaskRecord, 0)
	appendPendingInOrder(f.pending.byTenant[tenantID], f.state, &out, time.Time{})
	return out
}

// FindAllPendingTasks returns the global pending queue in FIFO order. Only
// the Raft leader's assignment service uses this method to choose concrete
// tasks for idle execution slots; workers never schedule from this snapshot.
func (f *FSM) FindAllPendingTasks() []*types.TaskRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]*types.TaskRecord, 0, f.pending.count)
	appendPendingInOrder(f.pending.all, f.state, &out, time.Time{})
	return out
}

// FindStealablePendingTasks supports the legacy rolling-upgrade claim path.
// It returns aged pending tasks belonging to tenants other than
// excludeTenantID, ordered by original enqueue time. New workers receive
// concrete assignments from the leader and do not call this method.
func (f *FSM) FindStealablePendingTasks(excludeTenantID string, before time.Time) []*types.TaskRecord {
	f.mu.RLock()
	defer f.mu.RUnlock()
	candidates := make([]*types.TaskRecord, 0)
	appendPendingInOrder(f.pending.all, f.state, &candidates, before)
	out := candidates[:0]
	for _, task := range candidates {
		if task.TenantID != excludeTenantID && !task.CreatedAt.IsZero() {
			out = append(out, task)
		}
	}
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

// OldestPendingCreatedAtByTenant returns the creation time of the oldest
// pending task for each tenant. It is a current scheduling signal only; the
// task history remains in the bounded task/result stores.
func (f *FSM) OldestPendingCreatedAtByTenant() map[string]time.Time {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make(map[string]time.Time)
	for _, task := range f.state.Tasks {
		if task.Status != types.TaskStatusPending {
			continue
		}
		oldest, ok := out[task.TenantID]
		if !ok || task.CreatedAt.Before(oldest) {
			out[task.TenantID] = task.CreatedAt
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
	for nodeID, node := range f.state.Nodes {
		if node.TotalWorkers > 0 {
			snapshot.AllocatedWorkersByNode[nodeID] = 0
		}
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
		copyV.Borrowed = make(map[string]int, len(v.Borrowed))
		for tk, tv := range v.Borrowed {
			copyV.Borrowed[tk] = tv
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
