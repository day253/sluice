package raft

import "encoding/json"

// FSM operation codes serialised into the Raft log.
const (
	OpUpsertTenant      = "upsert_tenant"
	OpDeleteTenant      = "delete_tenant"
	OpNodeUp            = "node_up"
	OpNodeDown          = "node_down"
	OpWorkerOffline     = "worker_offline"
	OpRetireNode        = "retire_node"
	OpSetControlNodes   = "set_control_nodes"
	OpSetWorkerCapacity = "set_worker_capacity"
	OpCreateTask        = "create_task"
	OpCreateTaskBatch   = "create_task_batch"
	OpClaimTask         = "claim_task"
	OpCompleteTask      = "complete_task"
	OpFailTask          = "fail_task"
	OpClaimBatch        = "claim_batch"
	OpCompleteBatch     = "complete_batch"
	OpRequeueTasks      = "requeue_tasks"
	OpUpdateAllocation  = "update_allocation"
)

// ---------------------------------------------------------------------------
// Raft-log command envelope
// ---------------------------------------------------------------------------

// Command is the top-level envelope for every Raft log entry.
type Command struct {
	Op   string          `json:"op"`
	Data json.RawMessage `json:"data"`
}

// ---------------------------------------------------------------------------
// Command payloads
// ---------------------------------------------------------------------------

// CreateTaskData is the payload for OpCreateTask — writes a task directly
// into the FSM as "pending" so any node's workers can claim it.
type CreateTaskData struct {
	TaskID      string `json:"task_id"`
	TenantID    string `json:"tenant_id"`
	Payload     string `json:"payload"`
	QueueNodeID string `json:"queue_node_id,omitempty"`
}

// CreateTaskBatchData is the payload for OpCreateTaskBatch. All tasks in the
// batch are persisted by one Raft log entry, reducing consensus overhead for
// high-volume submissions.
type CreateTaskBatchData struct {
	Tasks []CreateTaskData `json:"tasks"`
}

// CreateTaskBatchResult identifies only newly inserted tasks. Idempotent
// retries still return their stable task IDs to callers, but must not append
// duplicate best-effort queue hints.
type CreateTaskBatchResult struct {
	Created []string
}

// ClaimTaskData is the payload for OpClaimTask.
type ClaimTaskData struct {
	TaskID   string `json:"task_id"`
	TenantID string `json:"tenant_id"`
	NodeID   string `json:"node_id"`
	Payload  string `json:"payload"`
	Steal    bool   `json:"steal,omitempty"`
}

// CompleteTaskData is the payload for OpCompleteTask.
type CompleteTaskData struct {
	TaskID   string `json:"task_id"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status,omitempty"`
	Result   string `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
}

// DeleteTenantData is the payload for OpDeleteTenant.
type DeleteTenantData struct {
	ID string `json:"id"`
}

// ClaimBatchData is the payload for OpClaimBatch — claims multiple tasks
// in a single Raft log entry.
type ClaimBatchData struct {
	Tasks []ClaimTaskData `json:"tasks"`
}

// CompleteBatchData is the payload for OpCompleteBatch — completes
// multiple tasks in a single Raft log entry.
type CompleteBatchData struct {
	Tasks []CompleteTaskData `json:"tasks"`
}

// RequeueTasksData moves expired inflight claims back to pending.
type RequeueTasksData struct {
	TaskIDs []string `json:"task_ids"`
}

// ClaimBatchResult is returned by OpClaimBatch.
type ClaimBatchResult struct {
	Claimed []string `json:"claimed"` // task IDs successfully claimed
	Failed  []string `json:"failed"`  // task IDs that were duplicate/already claimed
}

// NodeDownData is the payload for OpNodeDown.
type NodeDownData struct {
	ID string `json:"id"`
}

// WorkerOfflineData removes stateless worker capacity without immediately
// re-queuing in-flight work. Existing claims retain the normal lease so a
// transient controller disconnect cannot cause instant duplicate execution.
type WorkerOfflineData struct {
	ID        string `json:"id"`
	SessionID string `json:"session_id,omitempty"`
}

// SetWorkerCapacityData changes the effective Processor concurrency of one
// live stateless Worker instance.
type SetWorkerCapacityData struct {
	NodeID       string `json:"node_id"`
	TotalWorkers int    `json:"total_workers"`
}

// RetireNodeData permanently removes a process identity which no longer
// belongs to the configured topology. In-flight claims keep their normal
// lease; retirement must not create a second owner while the old process may
// still be finishing work.
type RetireNodeData struct {
	ID string `json:"id"`
}

// SetControlNodesData migrates retained Raft members out of the legacy
// combined execution role without changing their liveness or addresses.
type SetControlNodesData struct {
	NodeIDs []string `json:"node_ids"`
}

// RaftApplier is an interface for applying commands to the Raft cluster.
// This allows other packages to depend on a narrow interface rather than
// the concrete *raft.Raft type.
type RaftApplier interface {
	Apply(cmd []byte, timeoutMs int) ApplyResult
	IsLeader() bool
	LeaderAddr() string
}

// ApplyResult mirrors raft.ApplyFuture for testing.
type ApplyResult interface {
	Error() error
	Response() interface{}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// MustMarshalCommand is a convenience helper that panics on error (only
// suitable for values known to be serialisable).
func MustMarshalCommand(op string, data interface{}) []byte {
	d, err := json.Marshal(data)
	if err != nil {
		panic("marshal command data: " + err.Error())
	}
	cmd := Command{Op: op, Data: d}
	b, err := json.Marshal(cmd)
	if err != nil {
		panic("marshal command: " + err.Error())
	}
	return b
}
