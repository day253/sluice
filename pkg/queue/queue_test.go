package queue

import (
	"encoding/json"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newTestTask(id, tenant string) *TaskEnvelope {
	return &TaskEnvelope{
		TaskID:    id,
		TenantID:  tenant,
		Payload:   json.RawMessage(`{"test":true}`),
		CreatedAt: time.Now().UTC(),
	}
}

// ---------------------------------------------------------------------------
// Queue interface compliance tests — runs against any Queue implementation.
// ---------------------------------------------------------------------------

func testQueueEnqueueDequeue(t *testing.T, q Queue) {
	t.Helper()

	task := newTestTask("t1", "tenant-a")
	if err := q.Enqueue("tenant-a", task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := q.Dequeue("tenant-a")
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got == nil {
		t.Fatal("expected task, got nil")
	}
	if got.TaskID != "t1" {
		t.Errorf("TaskID = %s, want t1", got.TaskID)
	}
}

func testQueueDequeueEmpty(t *testing.T, q Queue) {
	t.Helper()

	got, err := q.Dequeue("nonexistent")
	if err != nil {
		t.Fatalf("dequeue empty: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil from empty queue, got %v", got)
	}
}

func testQueueFIFO(t *testing.T, q Queue) {
	t.Helper()

	for i := range 5 {
		if err := q.Enqueue("t", newTestTask(string(rune('a'+i)), "t")); err != nil {
			t.Fatal(err)
		}
	}

	for i := range 5 {
		task, err := q.Dequeue("t")
		if err != nil {
			t.Fatal(err)
		}
		if task.TaskID != string(rune('a'+i)) {
			t.Errorf("FIFO violation: position %d got %s, want %c", i, task.TaskID, rune('a'+i))
		}
	}
}

func testQueueLen(t *testing.T, q Queue) {
	t.Helper()

	n, err := q.Len("tenant-a")
	if err != nil {
		t.Fatalf("len: %v", err)
	}
	if n != 0 {
		t.Errorf("empty queue len = %d, want 0", n)
	}

	q.Enqueue("tenant-a", newTestTask("t1", "tenant-a"))
	q.Enqueue("tenant-a", newTestTask("t2", "tenant-a"))

	n, _ = q.Len("tenant-a")
	if n != 2 {
		t.Errorf("len after 2 enqueues = %d, want 2", n)
	}
}

func testQueueListPending(t *testing.T, q Queue) {
	t.Helper()

	q.Enqueue("t", newTestTask("t1", "t"))
	q.Enqueue("t", newTestTask("t2", "t"))

	tasks, err := q.ListPending("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 2 {
		t.Errorf("ListPending: got %d tasks, want 2", len(tasks))
	}
}

func testQueueRemove(t *testing.T, q Queue) {
	t.Helper()

	q.Enqueue("t", newTestTask("keep", "t"))
	q.Enqueue("t", newTestTask("remove-me", "t"))

	if err := q.Remove("t", "remove-me"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	n, _ := q.Len("t")
	if n != 1 {
		t.Errorf("after remove: len = %d, want 1", n)
	}

	task, _ := q.Dequeue("t")
	if task.TaskID != "keep" {
		t.Errorf("remaining task = %s, want keep", task.TaskID)
	}
}

func testQueueMultiTenant(t *testing.T, q Queue) {
	t.Helper()

	q.Enqueue("a", newTestTask("a1", "a"))
	q.Enqueue("b", newTestTask("b1", "b"))
	q.Enqueue("a", newTestTask("a2", "a"))

	// Tenant a should have its own FIFO.
	t1, _ := q.Dequeue("a")
	if t1.TaskID != "a1" {
		t.Errorf("tenant a first = %s, want a1", t1.TaskID)
	}

	// Tenant b should have its own.
	t2, _ := q.Dequeue("b")
	if t2.TaskID != "b1" {
		t.Errorf("tenant b first = %s, want b1", t2.TaskID)
	}
}

// ---------------------------------------------------------------------------
// MemoryQueue tests
// ---------------------------------------------------------------------------

func TestMemoryQueue_EnqueueDequeue(t *testing.T) {
	testQueueEnqueueDequeue(t, NewMemoryQueue())
}

func TestMemoryQueue_DequeueEmpty(t *testing.T) {
	testQueueDequeueEmpty(t, NewMemoryQueue())
}

func TestMemoryQueue_FIFO(t *testing.T) {
	testQueueFIFO(t, NewMemoryQueue())
}

func TestMemoryQueue_Len(t *testing.T) {
	testQueueLen(t, NewMemoryQueue())
}

func TestMemoryQueue_ListPending(t *testing.T) {
	testQueueListPending(t, NewMemoryQueue())
}

func TestMemoryQueue_Remove(t *testing.T) {
	testQueueRemove(t, NewMemoryQueue())
}

func TestMemoryQueue_MultiTenant(t *testing.T) {
	testQueueMultiTenant(t, NewMemoryQueue())
}

func TestMemoryQueue_Close(t *testing.T) {
	q := NewMemoryQueue()
	if err := q.Close(); err != nil {
		t.Errorf("close should be a no-op, got %v", err)
	}
}
