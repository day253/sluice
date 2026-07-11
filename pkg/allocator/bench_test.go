package allocator

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Benchmark: max-min fairness scaling with tenant count
// ---------------------------------------------------------------------------

// benchEngine pre-seeds an engine with n tenants and m nodes.
func benchEngine(nTenants, nNodes, workersPerNode int) *Engine {
	fsm := raftpkg.NewFSM(zap.NewNop())
	for i := 0; i < nTenants; i++ {
		benchApplyOp(fsm, raftpkg.OpUpsertTenant, types.TenantConfig{
			ID: fmt.Sprintf("tenant-%d", i), MaxWorkers: 50 + rand.Intn(200),
		})
	}
	for i := 0; i < nNodes; i++ {
		benchApplyOp(fsm, raftpkg.OpNodeUp, types.NodeInfo{
			ID: fmt.Sprintf("node-%d", i), Status: types.NodeStatusUp,
			TotalWorkers: workersPerNode,
		})
	}
	return NewEngine("n0", fsm, &fakeRaftApplier{fsm: fsm}, zap.NewNop())
}

func benchApplyOp(fsm *raftpkg.FSM, op string, data interface{}) {
	cmd := raftpkg.MustMarshalCommand(op, data)
	_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
}

// ---- max-min only (no raft apply) ----

func BenchmarkMaxMin_10(b *testing.B)   { benchMaxMin(b, 10) }
func BenchmarkMaxMin_100(b *testing.B)  { benchMaxMin(b, 100) }
func BenchmarkMaxMin_1000(b *testing.B) { benchMaxMin(b, 1000) }

func benchMaxMin(b *testing.B, n int) {
	e := benchEngine(n, 5, 100)
	tenants := e.tenantList(e.fsm.GetState().Tenants)
	totalWorkers := 5 * 100
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.maxMinFairness(tenants, totalWorkers)
	}
}

// ---- idle detection ----

func BenchmarkIdleDetection_100(b *testing.B)  { benchIdle(b, 100) }
func BenchmarkIdleDetection_1000(b *testing.B) { benchIdle(b, 1000) }

func benchIdle(b *testing.B, n int) {
	e := benchEngine(n, 5, 100)
	tenants := e.tenantList(e.fsm.GetState().Tenants)
	// Simulate mixed load: half active, half idle.
	inflight := make(map[string]int, n/2)
	for i := 0; i < n/2; i++ {
		inflight[tenants[i].ID] = rand.Intn(20) + 1
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.updateIdleState(tenants, inflight)
	}
}

// ---- full reconcile (read FSM → compute → apply) ----

func BenchmarkReconcile_10(b *testing.B)   { benchReconcile(b, 10) }
func BenchmarkReconcile_50(b *testing.B)   { benchReconcile(b, 50) }
func BenchmarkReconcile_100(b *testing.B)  { benchReconcile(b, 100) }

func benchReconcile(b *testing.B, n int) {
	e := benchEngine(n, 5, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.Reconcile()
	}
}

// ---- distribute across nodes ----

func BenchmarkDistribute_100(b *testing.B)  { benchDistribute(b, 100, 5) }
func BenchmarkDistribute_1000(b *testing.B) { benchDistribute(b, 1000, 10) }

func benchDistribute(b *testing.B, tenants, nodes int) {
	e := benchEngine(tenants, nodes, 100)
	tenantAlloc := make(map[string]int, tenants)
	for i := 0; i < tenants; i++ {
		tenantAlloc[fmt.Sprintf("tenant-%d", i)] = 10 + rand.Intn(50)
	}
	activeNodes := e.activeNodes(e.fsm.GetState().Nodes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.distributeAcrossNodes(tenantAlloc, activeNodes)
	}
}

// ---- node-count scaling ----

func BenchmarkDistribute_50nodes(b *testing.B)  { benchDistribute(b, 100, 50) }
func BenchmarkDistribute_100nodes(b *testing.B) { benchDistribute(b, 100, 100) }
func BenchmarkDistribute_500nodes(b *testing.B) { benchDistribute(b, 100, 500) }

func BenchmarkReconcile_10nodes(b *testing.B)  { benchReconcileNodes(b, 10) }
func BenchmarkReconcile_50nodes(b *testing.B)  { benchReconcileNodes(b, 50) }
func BenchmarkReconcile_100nodes(b *testing.B) { benchReconcileNodes(b, 100) }

func benchReconcileNodes(b *testing.B, nodes int) {
	e := benchEngine(20, nodes, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = e.Reconcile()
	}
}
