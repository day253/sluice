// Package types defines the core data structures shared across all packages.
package types

import (
	"encoding/json"
	"time"
)

// Task status constants.
const (
	TaskStatusPending  = "pending"
	TaskStatusInflight = "inflight"
	TaskStatusDone     = "done"
	TaskStatusFailed   = "failed"
)

// Node status constants.
const (
	NodeStatusUp   = "up"
	NodeStatusDown = "down"
)

// ---------------------------------------------------------------------------
// Cluster & node
// ---------------------------------------------------------------------------

// NodeInfo represents a cluster node registered in the Raft FSM.
type NodeInfo struct {
	ID            string    `json:"id"`
	Address       string    `json:"address"`        // HTTP API address
	RaftAddress   string    `json:"raft_address"`   // Raft transport address
	Status        string    `json:"status"`         // "up" | "down"
	TotalWorkers  int       `json:"total_workers"`  // worker capacity of this node
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// ---------------------------------------------------------------------------
// Tenant
// ---------------------------------------------------------------------------

// TenantConfig defines the rate-limit configuration for a single tenant.
type TenantConfig struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	MaxWorkers int       `json:"max_workers"` // maximum concurrent workers for this tenant
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Allocation
// ---------------------------------------------------------------------------

// NodeAllocation maps how many workers a single node should run per tenant.
type NodeAllocation struct {
	NodeID  string         `json:"node_id"`
	Tenants map[string]int `json:"tenants"` // tenantID → worker count
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

// TaskRecord represents a task that is either pending (recovery queue) or
// inflight (currently being processed).  Once finished the task is moved into
// TaskResult.
type TaskRecord struct {
	TaskID    string    `json:"task_id"`
	TenantID  string    `json:"tenant_id"`
	Status    string    `json:"status"` // "pending" | "inflight"
	NodeID    string    `json:"node_id,omitempty"`
	Payload   string    `json:"payload,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	ClaimedAt time.Time `json:"claimed_at,omitempty"`
}

// TaskResult is the final outcome of a task.
type TaskResult struct {
	TaskID      string    `json:"task_id"`
	TenantID    string    `json:"tenant_id"`
	Status      string    `json:"status"` // "done" | "failed"
	Result      string    `json:"result,omitempty"`
	Error       string    `json:"error,omitempty"`
	CompletedAt time.Time `json:"completed_at"`
}

// ---------------------------------------------------------------------------
// FSM state (the entire state machine)
// ---------------------------------------------------------------------------

// FSMState is the complete Raft state machine payload.  It is serialised
// for snapshots and replicated via the Raft log.
type FSMState struct {
	Nodes       map[string]*NodeInfo       `json:"nodes"`
	Tenants     map[string]*TenantConfig   `json:"tenants"`
	Allocations map[string]*NodeAllocation `json:"allocations"`
	Tasks       map[string]*TaskRecord     `json:"tasks"`   // inflight + recovery-pending
	Results     map[string]*TaskResult     `json:"results"`  // completed tasks (LRU, externally pruned)
	Version     uint64                     `json:"version"`
}

// NewFSMState returns a properly initialised empty state.
func NewFSMState() *FSMState {
	return &FSMState{
		Nodes:       make(map[string]*NodeInfo),
		Tenants:     make(map[string]*TenantConfig),
		Allocations: make(map[string]*NodeAllocation),
		Tasks:       make(map[string]*TaskRecord),
		Results:     make(map[string]*TaskResult),
	}
}

// ---------------------------------------------------------------------------
// API types
// ---------------------------------------------------------------------------

// TaskSubmitRequest is the JSON body for POST /api/v1/tasks.
type TaskSubmitRequest struct {
	TenantID       string          `json:"tenant_id"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
}

// TaskResponse is the JSON body returned for task endpoints.
type TaskResponse struct {
	TaskID   string `json:"task_id"`
	TenantID string `json:"tenant_id"`
	Status   string `json:"status"`
	Result   string `json:"result,omitempty"`
	Error    string `json:"error,omitempty"`
}

// AllocationResponse is returned by the admin allocations endpoint.
type AllocationResponse struct {
	Nodes   []*NodeAllocation       `json:"nodes"`
	Tenants map[string]*TenantConfig `json:"tenants"`
}

// ErrorResponse is a generic error payload.
type ErrorResponse struct {
	Error   string `json:"error"`
	Code    int    `json:"code"`
}
