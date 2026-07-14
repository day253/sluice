package grpc

import (
	"context"
	"net"
	"testing"

	"go.uber.org/zap"
	googlegrpc "google.golang.org/grpc"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/queue"
	"github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

func TestLeaderAPIAddressUsesRegisteredNodeAddress(t *testing.T) {
	nodes := map[string]*types.NodeInfo{
		"node-1": {ID: "node-1", Address: "10.152.183.24:9090", RaftAddress: "10.152.183.24:7000"},
	}
	got, err := leaderAPIAddress("10.152.183.24:7000", nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.152.183.24:9090" {
		t.Fatalf("leader API address = %q, want %q", got, "10.152.183.24:9090")
	}
}

func TestLeaderAPIAddressFallsBackToRaftHost(t *testing.T) {
	got, err := leaderAPIAddress("10.0.0.8:7000", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.0.0.8:9090" {
		t.Fatalf("leader API address = %q, want %q", got, "10.0.0.8:9090")
	}
}

func TestSubmitForwardsBeforeFollowerTenantValidation(t *testing.T) {
	leaderFSM := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(leaderFSM, raft.OpUpsertTenant, types.TenantConfig{
		ID: "tenant-a", Name: "Tenant A", MaxWorkers: 2,
	})
	leaderRaft := &internalTestRaft{fsm: leaderFSM}
	leaderRaft.leader.Store(true)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceServer(server, NewService(
		"leader", queue.NewMemoryQueue(), leaderFSM, leaderRaft, nil, zap.NewNop(),
	))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	// The follower intentionally has no tenant yet, but it knows the leader's
	// API address through the replicated node registry.
	followerFSM := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(followerFSM, raft.OpNodeUp, types.NodeInfo{
		ID: "leader", Address: listener.Addr().String(), RaftAddress: "test:7000",
	})
	followerRaft := &internalTestRaft{fsm: followerFSM}
	follower := NewService("follower", queue.NewMemoryQueue(), followerFSM, followerRaft, nil, zap.NewNop())
	t.Cleanup(func() {
		follower.forwardMu.Lock()
		if follower.forwardConn != nil {
			_ = follower.forwardConn.Close()
		}
		follower.forwardMu.Unlock()
	})

	resp, err := follower.Submit(context.Background(), &grpcv1.SubmitRequest{
		TenantId: "tenant-a", Payload: []byte(`{"source":"test"}`),
	})
	if err != nil {
		t.Fatalf("follower submit: %v", err)
	}
	if resp.GetTaskId() == "" {
		t.Fatal("follower submit returned an empty task id")
	}
	if task := leaderFSM.GetTask(resp.GetTaskId()); task == nil || task.TenantID != "tenant-a" {
		t.Fatalf("leader task = %+v, want tenant-a", task)
	}
}
