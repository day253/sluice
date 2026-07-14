// Package queue defines the local durable queue abstraction used to buffer
// tasks before they are claimed by a worker and replicated into Raft.
package queue

import (
	"encoding/json"
	"time"
)

// ---------------------------------------------------------------------------
// Task envelope
// ---------------------------------------------------------------------------

// TaskEnvelope wraps a submitted task that sits in the local queue until a
// worker picks it up.
type TaskEnvelope struct {
	TaskID              string          `json:"task_id"`
	TenantID            string          `json:"tenant_id"`
	Payload             json.RawMessage `json:"payload"`
	IdempotencyKey      string          `json:"idempotency_key,omitempty"`
	EstimatedDurationMs int64           `json:"estimated_duration_ms,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Queue interface
// ---------------------------------------------------------------------------

// Queue is the local per-tenant durable FIFO queue.
//
// Implementations must be safe for concurrent use.  The canonical
// production implementation uses PebbleDB; an in-memory variant is
// provided for testing.
type Queue interface {
	// Enqueue appends a task to the given tenant's queue and returns once
	// the write is durable.
	Enqueue(tenantID string, task *TaskEnvelope) error

	// Dequeue atomically removes and returns the oldest task for the given
	// tenant.  When the queue is empty (nil, nil) is returned.
	Dequeue(tenantID string) (*TaskEnvelope, error)

	// Len returns the number of tasks waiting for a tenant.
	Len(tenantID string) (int, error)

	// ListPending returns all tasks for a tenant (for recovery / inspection).
	ListPending(tenantID string) ([]*TaskEnvelope, error)

	// Remove deletes a specific task from a tenant queue.
	Remove(tenantID, taskID string) error

	// Close releases all resources held by the queue.
	Close() error
}
