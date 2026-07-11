package queue

import (
	"container/list"
	"fmt"
	"sync"
)

// MemoryQueue is a purely in-memory Queue implementation intended for
// testing and local development only.  Tasks are lost when the process
// restarts.
type MemoryQueue struct {
	mu   sync.Mutex
	q    map[string]*list.List // tenantID → FIFO list
}

// NewMemoryQueue returns an empty in-memory queue.
func NewMemoryQueue() *MemoryQueue {
	return &MemoryQueue{
		q: make(map[string]*list.List),
	}
}

func (m *MemoryQueue) getList(tenantID string) *list.List {
	l, ok := m.q[tenantID]
	if !ok {
		l = list.New()
		m.q[tenantID] = l
	}
	return l
}

// Enqueue adds a task to the end of the tenant queue.
func (m *MemoryQueue) Enqueue(tenantID string, task *TaskEnvelope) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getList(tenantID).PushBack(task)
	return nil
}

// Dequeue removes and returns the oldest task.
func (m *MemoryQueue) Dequeue(tenantID string) (*TaskEnvelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.getList(tenantID)
	if l.Len() == 0 {
		return nil, nil
	}
	front := l.Remove(l.Front())
	return front.(*TaskEnvelope), nil
}

// Len returns the number of waiting tasks.
func (m *MemoryQueue) Len(tenantID string) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.q[tenantID]
	if !ok {
		return 0, nil
	}
	return l.Len(), nil
}

// ListPending returns all waiting tasks.
func (m *MemoryQueue) ListPending(tenantID string) ([]*TaskEnvelope, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := m.getList(tenantID)
	out := make([]*TaskEnvelope, 0, l.Len())
	for e := l.Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*TaskEnvelope))
	}
	return out, nil
}

// Remove deletes a specific task.
func (m *MemoryQueue) Remove(tenantID, taskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.q[tenantID]
	if !ok {
		return nil
	}
	for e := l.Front(); e != nil; e = e.Next() {
		task := e.Value.(*TaskEnvelope)
		if task.TaskID == taskID {
			l.Remove(e)
			return nil
		}
	}
	return fmt.Errorf("task %s not found in tenant %s", taskID, tenantID)
}

// Close is a no-op for the in-memory queue.
func (m *MemoryQueue) Close() error {
	return nil
}
