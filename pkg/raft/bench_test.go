package raft

import (
	"testing"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Benchmark: FSM Apply latency per operation
// ---------------------------------------------------------------------------

func benchFSM() *FSM {
	fsm := NewFSM(zap.NewNop())
	cmd := MustMarshalCommand(OpNodeUp, types.NodeInfo{ID: "n1", TotalWorkers: 100})
	_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	cmd = MustMarshalCommand(OpUpsertTenant, types.TenantConfig{ID: "t1", MaxWorkers: 50})
	_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	return fsm
}

func BenchmarkFSM_ClaimTask(b *testing.B) {
	fsm := benchFSM()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := MustMarshalCommand(OpClaimTask, ClaimTaskData{
			TaskID: "task", TenantID: "t1", NodeID: "n1", Payload: `{}`,
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
		// Clean up for next iteration.
		cmd = MustMarshalCommand(OpCompleteTask, CompleteTaskData{
			TaskID: "task", TenantID: "t1", Result: "ok",
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	}
}

func BenchmarkFSM_CompleteTask(b *testing.B) {
	fsm := benchFSM()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := MustMarshalCommand(OpClaimTask, ClaimTaskData{
			TaskID: "task", TenantID: "t1", NodeID: "n1", Payload: `{}`,
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})

		cmd = MustMarshalCommand(OpCompleteTask, CompleteTaskData{
			TaskID: "task", TenantID: "t1", Result: "ok",
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	}
}

func BenchmarkFSM_NodeDownRequeue(b *testing.B) {
	fsm := benchFSM()
	// Pre-seed with inflight tasks on n1.
	for i := 0; i < 100; i++ {
		cmd := MustMarshalCommand(OpClaimTask, ClaimTaskData{
			TaskID: "inflight-task", TenantID: "t1", NodeID: "n1", Payload: `{}`,
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := MustMarshalCommand(OpNodeDown, NodeDownData{ID: "n1"})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
		// Re-register for next iteration.
		cmd = MustMarshalCommand(OpNodeUp, types.NodeInfo{ID: "n1", TotalWorkers: 100})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	}
}

func BenchmarkFSM_ReadConcurrent(b *testing.B) {
	fsm := benchFSM()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			fsm.GetTenant("t1")
			fsm.GetActiveNodes()
			fsm.CountInflightPerTenant()
		}
	})
}

func BenchmarkFSM_Snapshot(b *testing.B) {
	fsm := benchFSM()
	// Add some bulk.
	for i := 0; i < 1000; i++ {
		cmd := MustMarshalCommand(OpClaimTask, ClaimTaskData{
			TaskID: "t", TenantID: "t1", NodeID: "n1", Payload: `{}`,
		})
		_ = fsm.Apply(&raft.Log{Data: cmd, Type: raft.LogCommand})
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snap, _ := fsm.Snapshot()
		snap.Release()
	}
}
