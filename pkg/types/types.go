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

// Node role constants separate replicated control-plane members from
// stateless execution capacity. Empty is retained for rolling compatibility
// with snapshots written before roles were introduced.
const (
	NodeRoleControl = "control"
	NodeRoleWorker  = "worker"
)

// MaxWorkerCapacityPerInstance bounds the API-configurable Processor
// concurrency of one stateless Worker process. The bound prevents an
// accidental control-plane write from creating an unbounded goroutine pool.
const MaxWorkerCapacityPerInstance = 1000

// ---------------------------------------------------------------------------
// Cluster & node
// ---------------------------------------------------------------------------

// NodeInfo represents a cluster node registered in the Raft FSM.
type NodeInfo struct {
	ID           string `json:"id"`
	Role         string `json:"role,omitempty"`       // "control" | "worker"
	SessionID    string `json:"session_id,omitempty"` // worker process incarnation
	Address      string `json:"address"`              // HTTP API address
	RaftAddress  string `json:"raft_address"`         // Raft transport address
	Status       string `json:"status"`               // "up" | "down"
	TotalWorkers int    `json:"total_workers"`        // effective worker capacity of this node
	// CapacityOverride is durable current configuration. Zero means the
	// instance uses the capacity reported by its startup configuration.
	CapacityOverride int       `json:"capacity_override,omitempty"`
	LastHeartbeat    time.Time `json:"last_heartbeat"`
}

// WorkerCapacityResponse is returned after an instance capacity mutation has
// committed through Raft.
type WorkerCapacityResponse struct {
	NodeID           string `json:"node_id"`
	TotalWorkers     int    `json:"total_workers"`
	CapacityOverride int    `json:"capacity_override"`
}

// WorkerLoadSnapshot is ephemeral execution-plane feedback attached to an
// idle-slot request. It is never written to Raft or an FSM snapshot.
type WorkerLoadSnapshot struct {
	CPUUtilizationMillis int32
	CPUValid             bool
	RunningTasks         int
	WorkerCapacity       int
}

// TaskPressureSnapshot is one coherent read of the replicated unfinished-task
// mirror. It is current state, not a historical series.
type TaskPressureSnapshot struct {
	UnfinishedTasks int64
	PendingTasks    int64
	RunningTasks    int64
	OldestPendingAt time.Time
}

// AutoscalingSnapshot is the read-only control-plane signal consumed by the
// Worker autoscaler. Replicated task/allocation fields and Leader-local
// execution telemetry are intentionally identified separately so missing soft
// metrics can block scale-down without blocking queue-driven scale-up.
type AutoscalingSnapshot struct {
	ObservedAt             time.Time `json:"observed_at"`
	UnfinishedTasks        int64     `json:"unfinished_tasks"`
	PendingTasks           int64     `json:"pending_tasks"`
	RunningTasks           int64     `json:"running_tasks"`
	OldestPendingAgeMillis int64     `json:"oldest_pending_age_ms"`
	TaskBreakdownValid     bool      `json:"task_breakdown_valid"`
	AllocatedWorkers       int64     `json:"allocated_workers"`
	WorkerCapacity         int64     `json:"worker_capacity"`
	WorkerInstances        int64     `json:"worker_instances"`
	ExecutionSignalsValid  bool      `json:"execution_signals_valid"`
	ReportingWorkers       int64     `json:"reporting_workers"`
	ExecutingTasks         int64     `json:"executing_tasks"`
	AverageWorkerCPUMillis int64     `json:"average_worker_cpu_millis"`
	MaxWorkerCPUMillis     int64     `json:"max_worker_cpu_millis"`
	RateCountersValid      bool      `json:"rate_counters_valid"`
	TelemetrySource        string    `json:"telemetry_source,omitempty"`
	TelemetryStartedAt     time.Time `json:"telemetry_started_at,omitempty"`
	SubmittedTasksTotal    int64     `json:"submitted_tasks_total"`
	CompletedTasksTotal    int64     `json:"completed_tasks_total"`
	ExecutionSignalsError  string    `json:"execution_signals_error,omitempty"`
}

// ---------------------------------------------------------------------------
// Tenant
// ---------------------------------------------------------------------------

// TenantConfig defines the rate-limit configuration for a single tenant.
type TenantConfig struct {
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	MaxWorkers int       `json:"max_workers"` // normal guaranteed concurrent workers; idle borrowing may exceed it
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Allocation
// ---------------------------------------------------------------------------

// NodeAllocation maps how many workers a single node should run per tenant.
type NodeAllocation struct {
	NodeID   string         `json:"node_id"`
	Tenants  map[string]int `json:"tenants"`            // tenantID → effective worker count
	Borrowed map[string]int `json:"borrowed,omitempty"` // tenantID → workers above max_workers
}

// ---------------------------------------------------------------------------
// Task
// ---------------------------------------------------------------------------

// TaskRecord represents a task that is either pending (recovery queue) or
// inflight (currently being processed).  Once finished the task is moved into
// TaskResult.
type TaskRecord struct {
	TaskID      string    `json:"task_id"`
	TenantID    string    `json:"tenant_id"`
	Status      string    `json:"status"` // "pending" | "inflight"
	NodeID      string    `json:"node_id,omitempty"`
	QueueNodeID string    `json:"queue_node_id,omitempty"` // node-local queue origin while pending
	Payload     string    `json:"payload,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	ClaimedAt   time.Time `json:"claimed_at,omitempty"`
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
	Tasks       map[string]*TaskRecord     `json:"tasks"`        // unfinished task snapshot
	Results     map[string]*TaskResult     `json:"results"`      // bounded recent result cache
	ResultOrder []string                   `json:"result_order"` // oldest to newest
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

// BatchTaskSubmitRequest is the JSON body for POST /api/v1/tasks/batch.
type BatchTaskSubmitRequest struct {
	Tasks []TaskSubmitRequest `json:"tasks"`
}

// BatchTaskResponse is returned by the batch submission endpoint. Results
// preserve the input order so callers can correlate task IDs deterministically.
type BatchTaskResponse struct {
	Tasks []TaskResponse `json:"tasks"`
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
	Nodes   []*NodeAllocation        `json:"nodes"`
	Tenants map[string]*TenantConfig `json:"tenants"`
}

// ErrorResponse is a generic error payload.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}
