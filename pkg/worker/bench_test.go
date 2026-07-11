package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/queue"
)

// ---------------------------------------------------------------------------
// Benchmark: worker pool throughput & scheduling overhead
// ---------------------------------------------------------------------------

// instantProcessor completes instantly, measuring only scheduling overhead.
type instantProcessor struct {
	processed atomic.Int64
}

func (p *instantProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	p.processed.Add(1)
	return "ok", nil
}

func BenchmarkPool_SingleTenant_1Worker(b *testing.B) {
	benchPool(b, 1, 1, b.N)
}

func BenchmarkPool_SingleTenant_10Workers(b *testing.B) {
	benchPool(b, 1, 10, b.N)
}

func BenchmarkPool_SingleTenant_100Workers(b *testing.B) {
	benchPool(b, 1, 100, b.N)
}

func BenchmarkPool_MultiTenant_10x10(b *testing.B) {
	// 10 tenants, 10 workers each.
	benchPoolMulti(b, 10, 10, b.N/10)
}

func benchPool(b *testing.B, tenants, workersPerTenant, tasks int) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &instantProcessor{}
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	alloc := make(map[string]int)
	for i := 0; i < tenants; i++ {
		tid := "tenant"
		if tenants > 1 {
			tid = string(rune('a' + i))
		}
		alloc[tid] = workersPerTenant
		for j := 0; j < tasks; j++ {
			_ = q.Enqueue(tid, &queue.TaskEnvelope{
				TaskID: tid + "-task", TenantID: tid, Payload: json.RawMessage(`{}`),
			})
		}
	}

	pool.Reconcile(alloc)

	b.ResetTimer()
	// Wait until all tasks processed.
	for proc.processed.Load() < int64(tasks*tenants) {
		time.Sleep(5 * time.Millisecond)
	}
	b.StopTimer()

	_ = pool.Shutdown(5 * time.Second)
	b.ReportMetric(float64(proc.processed.Load())/b.Elapsed().Seconds(), "tasks/s")
}

func benchPoolMulti(b *testing.B, tenants, workersPerTenant, tasksPerTenant int) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &instantProcessor{}
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	alloc := make(map[string]int, tenants)
	for i := 0; i < tenants; i++ {
		tid := string(rune('a' + i))
		alloc[tid] = workersPerTenant
		for j := 0; j < tasksPerTenant; j++ {
			_ = q.Enqueue(tid, &queue.TaskEnvelope{
				TaskID: tid + "-task", TenantID: tid, Payload: json.RawMessage(`{}`),
			})
		}
	}

	pool.Reconcile(alloc)

	b.ResetTimer()
	total := int64(tenants * tasksPerTenant)
	for proc.processed.Load() < total {
		time.Sleep(5 * time.Millisecond)
	}
	b.StopTimer()

	_ = pool.Shutdown(5 * time.Second)
	b.ReportMetric(float64(total)/b.Elapsed().Seconds(), "tasks/s")
}

// BenchmarkPool_ReconcileOverhead measures the cost of Reconcile when
// nothing changes (steady state).
func BenchmarkPool_ReconcileNoop(b *testing.B) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &instantProcessor{}
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	alloc := map[string]int{"a": 10, "b": 10, "c": 10}
	pool.Reconcile(alloc) // initial spawn

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Reconcile(alloc) // no change
	}
	b.StopTimer()
	_ = pool.Shutdown(1 * time.Second)
}

// BenchmarkPool_StartStopWorker measures how fast a worker goroutine
// spins up and shuts down.
func BenchmarkPool_StartStopWorker(b *testing.B) {
	q := queue.NewMemoryQueue()
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &instantProcessor{}
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pool.Reconcile(map[string]int{"a": 1})
		pool.Reconcile(map[string]int{})
	}
	b.StopTimer()
	_ = pool.Shutdown(1 * time.Second)
}

// ---- concurrent claim/complete simulation ----

func BenchmarkClaimCompleteSequence(b *testing.B) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &mockRaftApplier{}
	proc := &instantProcessor{}
	q := queue.NewMemoryQueue()

	n := b.N
	if n > 100000 {
		n = 100000
	}
	for i := 0; i < n; i++ {
		_ = q.Enqueue("a", &queue.TaskEnvelope{
			TaskID: fmt.Sprintf("t-%d", i), TenantID: "a", Payload: json.RawMessage(`{}`),
		})
	}

	nCPU := runtime.GOMAXPROCS(0)
	pool := NewPool("n1", q, fsm, raft, proc, zap.NewNop())
	pool.Reconcile(map[string]int{"a": nCPU * 2})

	b.ResetTimer()
	for proc.processed.Load() < int64(n) {
		time.Sleep(5 * time.Millisecond)
	}
	b.StopTimer()

	_ = pool.Shutdown(5 * time.Second)
	b.ReportMetric(float64(proc.processed.Load())/b.Elapsed().Seconds(), "tasks/s")
}
