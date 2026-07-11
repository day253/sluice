package integration

import (
	"fmt"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Cluster throughput benchmarks (heavy — not for CI, use locally)
// ---------------------------------------------------------------------------

// BenchmarkCluster_1Node measures 1-node throughput through full Raft pipeline.
func BenchmarkCluster_1Node(b *testing.B) {
	benchClusterN(b, 1, b.N)
}

// BenchmarkCluster_3Node measures 3-node throughput.
func BenchmarkCluster_3Node(b *testing.B) {
	benchClusterN(b, 3, b.N)
}

func benchClusterN(b *testing.B, nodes, tasks int) {
	b.Helper()
	if tasks > 2000 {
		tasks = 2000
	}

	tc := newBenchCluster(b, nodes, 50)
	defer tc.shutdown()

	tc.addTenant("alice", 200)
	tc.waitAllocation(20 * time.Second)
	time.Sleep(3 * time.Second)

	for i := 0; i < tasks; i++ {
		tc.submitTask(0, "alice", fmt.Sprintf(`"b-%d"`, i))
	}

	b.ResetTimer()
	tc.waitProcessed(tasks, 60*time.Second)
	b.StopTimer()

	elapsed := b.Elapsed()
	b.ReportMetric(float64(tc.processedCount())/elapsed.Seconds(), "tasks/s")
	b.ReportMetric(elapsed.Seconds()/float64(tc.processedCount()), "s/task")
}

// newBenchCluster calls newTestCluster with *testing.B (which implements testing.TB).
func newBenchCluster(b *testing.B, n, workersPerNode int) *testCluster {
	return newTestCluster(b, n, workersPerNode)
}
