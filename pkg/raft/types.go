package raft

import "encoding/json"

// FSM operation codes serialised into the Raft log.
const (
	OpUpsertTenant     = "upsert_tenant"
	OpDeleteTenant     = "delete_tenant"
	OpNodeUp           = "node_up"
	OpNodeDown         = "node_down"
	OpClaimTask        = "claim_task"
	OpCompleteTask     = "complete_task"
	OpFailTask         = "fail_task"
	OpUpdateAllocation = "update_allocation"
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

// ClaimTaskData is the payload for OpClaimTask.
type ClaimTaskData struct {
	TaskID   string `json:"task_id"`
	TenantID string `json:"tenant_id"`
	NodeID   string `json:"node_id"`
	Payload  string `json:"payload"`
}

// CompleteTaskData is the payload for OpCompleteTask.
type CompleteTaskData struct {
	TaskID   string `json:"task_id"`
	TenantID string `json:"tenant_id"`
	Result   string `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
}

// DeleteTenantData is the payload for OpDeleteTenant.
type DeleteTenantData struct {
	ID string `json:"id"`
}

// NodeDownData is the payload for OpNodeDown.
type NodeDownData struct {
	ID string `json:"id"`
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
