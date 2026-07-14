package grpc

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	googlegrpc "google.golang.org/grpc"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

type internalTestRaft struct {
	fsm        *raftpkg.FSM
	leader     atomic.Bool
	applyCount atomic.Int32
}

func (r *internalTestRaft) Apply(cmd []byte, _ int) raftpkg.ApplyResult {
	r.applyCount.Add(1)
	response := r.fsm.Apply(&hashicorpraft.Log{Data: cmd, Type: hashicorpraft.LogCommand})
	return internalTestApplyResult{response: response}
}

func (r *internalTestRaft) IsLeader() bool     { return r.leader.Load() }
func (r *internalTestRaft) LeaderAddr() string { return "test:7000" }

type internalTestApplyResult struct{ response interface{} }

func (r internalTestApplyResult) Error() error          { return nil }
func (r internalTestApplyResult) Response() interface{} { return r.response }

func applyInternalTestCommand(fsm *raftpkg.FSM, op string, data interface{}) {
	fsm.Apply(&hashicorpraft.Log{
		Data: raftpkg.MustMarshalCommand(op, data), Type: hashicorpraft.LogCommand,
	})
}

func waitForWorkerClientDisconnected(t *testing.T, client *ClaimClient) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		disconnected := client.claimStream == nil && client.resultStream == nil
		client.mu.Unlock()
		if disconnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker client streams were not invalidated")
}

func TestWorkerStreamsCommitResultsAndReconnectAfterLeadershipLoss(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"worker-node": {NodeID: "worker-node", Tenants: map[string]int{"tenant-a": 1}},
	})
	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceInternalServer(server, NewInternalService("leader", fsm, raft, zap.NewNop()))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client := NewClaimClient("worker-node", zap.NewNop())
	client.SetLeader(listener.Addr().String())
	t.Cleanup(client.Close)

	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "task-1", TenantID: "tenant-a", Payload: `{}`,
	})
	claimed, err := client.Claim("task-1", "tenant-a", `{}`)
	if err != nil || !claimed {
		t.Fatalf("claim task-1: claimed=%v err=%v", claimed, err)
	}
	if err := client.Complete("task-1", "tenant-a", "ok", "", false); err != nil {
		t.Fatalf("complete task-1: %v", err)
	}
	if result := fsm.GetResult("task-1"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("task-1 result = %+v", result)
	}

	// The old leader rejects the next batch. The receive loop must invalidate
	// both streams so the periodic same-address SetLeader call can repair them.
	raft.leader.Store(false)
	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "task-2", TenantID: "tenant-a", Payload: `{}`,
	})
	if _, err := client.Claim("task-2", "tenant-a", `{}`); err == nil {
		t.Fatal("claim unexpectedly succeeded after leadership loss")
	}
	waitForWorkerClientDisconnected(t, client)

	raft.leader.Store(true)
	client.SetLeader(listener.Addr().String())
	claimed, err = client.Claim("task-2", "tenant-a", `{}`)
	if err != nil || !claimed {
		t.Fatalf("claim task-2 after reconnect: claimed=%v err=%v", claimed, err)
	}
	if err := client.Complete("task-2", "tenant-a", "", "boom", true); err != nil {
		t.Fatalf("complete failed task-2: %v", err)
	}
	if result := fsm.GetResult("task-2"); result == nil || result.Status != types.TaskStatusFailed || result.Error != "boom" {
		t.Fatalf("task-2 result = %+v", result)
	}

	// A stale queue copy arriving after completion must be acknowledged as a
	// rejected claim, never re-created as unfinished work.
	claimed, err = client.Claim("task-2", "tenant-a", `{}`)
	if err != nil {
		t.Fatalf("duplicate claim returned transport error: %v", err)
	}
	if claimed {
		t.Fatal("completed task was claimed again")
	}
	if task := fsm.GetTask("task-2"); task != nil {
		t.Fatalf("completed task was resurrected: %+v", task)
	}
}
