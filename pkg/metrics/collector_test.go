package metrics

import (
	"testing"

	hraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

func applyMetricCommand(t *testing.T, fsm *raftpkg.FSM, op string, data interface{}) {
	t.Helper()
	response := fsm.Apply(&hraft.Log{Data: raftpkg.MustMarshalCommand(op, data), Type: hraft.LogCommand})
	if err, ok := response.(error); ok {
		t.Fatalf("apply %s: %v", op, err)
	}
}

func TestCollectorStoresUnfinishedAndAllocatedWorkersByTenantAndNode(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyMetricCommand(t, fsm, raftpkg.OpUpsertTenant, types.TenantConfig{ID: "globex", MaxWorkers: 10})
	applyMetricCommand(t, fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{TaskID: "task-1", TenantID: "globex"})
	applyMetricCommand(t, fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"node-1": {NodeID: "node-1", Tenants: map[string]int{"globex": 7}},
	})

	collector := NewCollector(fsm, zap.NewNop())
	collector.collect()

	assertLatest := func(name string, want int64) {
		t.Helper()
		data := collector.QueryNamed(name)
		if len(data) != 1 {
			t.Fatalf("metric %s count = %d, want 1", name, len(data))
		}
		if got := data[0].Secs[len(data[0].Secs)-1]; got != want {
			t.Fatalf("metric %s latest = %d, want %d", name, got, want)
		}
	}

	assertLatest("unfinished:globex", 1)
	assertLatest("unfinished:total", 1)
	assertLatest("allocated-workers:tenant:globex", 7)
	assertLatest("allocated-workers:node:node-1", 7)
	assertLatest("allocated-workers:total", 7)
}
