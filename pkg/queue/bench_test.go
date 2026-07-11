package queue

import (
	"encoding/json"
	"fmt"
	"testing"
)

// ---------------------------------------------------------------------------
// Benchmark: queue throughput
// ---------------------------------------------------------------------------

func BenchmarkMemoryQueue_Enqueue(b *testing.B) {
	q := NewMemoryQueue()
	task := &TaskEnvelope{TaskID: "t", TenantID: "a", Payload: json.RawMessage(`{}`)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Enqueue("a", task)
	}
}

func BenchmarkMemoryQueue_Dequeue(b *testing.B) {
	q := NewMemoryQueue()
	// Pre-fill.
	for i := 0; i < b.N; i++ {
		_ = q.Enqueue("a", &TaskEnvelope{
			TaskID: fmt.Sprintf("t-%d", i), TenantID: "a", Payload: json.RawMessage(`{}`),
		})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = q.Dequeue("a")
	}
}

func BenchmarkMemoryQueue_EnqueueDequeue(b *testing.B) {
	q := NewMemoryQueue()
	task := &TaskEnvelope{TaskID: "t", TenantID: "a", Payload: json.RawMessage(`{}`)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = q.Enqueue("a", task)
		_, _ = q.Dequeue("a")
	}
}

func BenchmarkMemoryQueue_MultiTenant(b *testing.B) {
	q := NewMemoryQueue()
	tenants := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	task := &TaskEnvelope{TaskID: "t", TenantID: "", Payload: json.RawMessage(`{}`)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tid := tenants[i%len(tenants)]
		task.TenantID = tid
		_ = q.Enqueue(tid, task)
		_, _ = q.Dequeue(tid)
	}
}

func BenchmarkMemoryQueue_Concurrent(b *testing.B) {
	q := NewMemoryQueue()
	b.RunParallel(func(pb *testing.PB) {
		task := &TaskEnvelope{TaskID: "t", TenantID: "a", Payload: json.RawMessage(`{}`)}
		for pb.Next() {
			_ = q.Enqueue("a", task)
			_, _ = q.Dequeue("a")
		}
	})
}
