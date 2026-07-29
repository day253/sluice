package node

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

func TestRaftLogRetentionRequiresConvergenceAfterPartialSnapshot(t *testing.T) {
	partial := raftpkg.LogRetentionResult{
		Needed:         true,
		SnapshotTaken:  true,
		Remaining:      true,
		RetainedBefore: 10258,
		RetainedAfter:  7920,
	}
	if raftLogRetentionConverged(partial) {
		t.Fatal("partial retention snapshot was treated as converged")
	}

	for _, converged := range []raftpkg.LogRetentionResult{
		{},
		{Needed: true, SnapshotTaken: true, Remaining: false},
	} {
		if !raftLogRetentionConverged(converged) {
			t.Fatalf("retention result %+v should be converged", converged)
		}
	}
}

func TestNodeRejectsUnsafeSubmissionApplyLimit(t *testing.T) {
	_, err := New(Config{
		MaxRaftVoters:        1,
		SubmissionApplyLimit: grpcpkg.MaxSubmissionApplyLimit + 1,
	}, nil, nil)
	if err == nil {
		t.Fatal("oversized submission Apply limit was accepted")
	}
}

func TestWaitForRaftLeaderKeepsProcessAliveAcrossRetryWindows(t *testing.T) {
	var waits atomic.Int32
	var warnings atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ok := waitForRaftLeader(
		ctx,
		time.Millisecond,
		func(time.Duration) bool {
			return waits.Add(1) == 3
		},
		func() {
			warnings.Add(1)
		},
	)
	if !ok {
		t.Fatal("leader wait stopped before a later quorum became available")
	}
	if waits.Load() != 3 || warnings.Load() != 2 {
		t.Fatalf("leader wait calls=%d warnings=%d, want 3 and 2", waits.Load(), warnings.Load())
	}
}

func TestWaitForRaftLeaderStopsOnlyWhenNodeContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var waits atomic.Int32
	ok := waitForRaftLeader(
		ctx,
		time.Millisecond,
		func(time.Duration) bool {
			if waits.Add(1) == 2 {
				cancel()
			}
			return false
		},
		func() {},
	)
	if ok || waits.Load() != 2 {
		t.Fatalf("cancelled leader wait returned ok=%t after %d calls", ok, waits.Load())
	}
}

func TestResolveLeaderAPIAddressUsesRegisteredOrRaftHost(t *testing.T) {
	tests := []struct {
		name     string
		nodes    map[string]*types.NodeInfo
		raft     string
		localAPI string
		want     string
	}{
		{
			name: "registered integration address",
			nodes: map[string]*types.NodeInfo{
				"node-1": {RaftAddress: "127.0.0.1:7001", Address: "127.0.0.1:9091"},
			},
			raft: "127.0.0.1:7001", localAPI: "127.0.0.1:9090", want: "127.0.0.1:9091",
		},
		{
			name: "Kubernetes wildcard advertise address",
			nodes: map[string]*types.NodeInfo{
				"sluice-2": {RaftAddress: "10.1.2.3:7000", Address: "0.0.0.0:9090"},
			},
			raft: "10.1.2.3:7000", localAPI: "0.0.0.0:9090", want: "10.1.2.3:9090",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveLeaderAPIAddress(test.raft, test.nodes, test.localAPI)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("leader API = %q, want %q", got, test.want)
			}
		})
	}
}

func TestWorkerRegistrationSameSessionIsNoOp(t *testing.T) {
	existing := &types.NodeInfo{
		ID: "worker-1", Role: types.NodeRoleWorker, SessionID: "session-1",
		Status: types.NodeStatusUp, Address: "127.0.0.1:9001", TotalWorkers: 100,
	}
	identical := *existing
	if workerRegistrationChanged(existing, &identical) {
		t.Fatal("identical worker session would create a Raft heartbeat log")
	}
	for name, mutate := range map[string]func(*types.NodeInfo){
		"new session": func(node *types.NodeInfo) { node.SessionID = "session-2" },
		"capacity":    func(node *types.NodeInfo) { node.TotalWorkers++ },
		"address":     func(node *types.NodeInfo) { node.Address = "127.0.0.1:9002" },
		"offline":     func(node *types.NodeInfo) { node.Status = types.NodeStatusDown },
	} {
		t.Run(name, func(t *testing.T) {
			next := *existing
			mutate(&next)
			if !workerRegistrationChanged(existing, &next) {
				t.Fatalf("changed registration was treated as no-op: %+v", next)
			}
		})
	}
}

func TestWorkerRegistrationKeepsDurableCapacityOverride(t *testing.T) {
	existing := &types.NodeInfo{
		ID: "worker-1", Role: types.NodeRoleWorker, SessionID: "session-1",
		Status: types.NodeStatusUp, Address: "127.0.0.1:9001",
		TotalWorkers: 250, CapacityOverride: 250,
	}
	startupDefault := *existing
	startupDefault.TotalWorkers = 100
	if workerRegistrationChanged(existing, &startupDefault) {
		t.Fatal("startup default mismatch would overwrite a durable capacity override")
	}
	startupDefault.SessionID = "session-2"
	if !workerRegistrationChanged(existing, &startupDefault) {
		t.Fatal("replacement process session was hidden by capacity override")
	}
}

func TestControlNodesNeedingMigrationUsesOnlyRaftMembersInStableOrder(t *testing.T) {
	status := raftpkg.MembershipStatus{
		Voters:    []string{"control-10", "control-2", "control-0"},
		Nonvoters: []string{"control-1"},
	}
	nodes := map[string]*types.NodeInfo{
		"control-0":  {ID: "control-0", Role: types.NodeRoleControl},
		"control-1":  {ID: "control-1", TotalWorkers: 100},
		"control-2":  {ID: "control-2", Role: types.NodeRoleControl, SessionID: "legacy"},
		"control-10": {ID: "control-10", Role: types.NodeRoleWorker},
		"worker-0":   {ID: "worker-0", Role: types.NodeRoleWorker, TotalWorkers: 100},
	}
	want := []string{"control-1", "control-10", "control-2"}
	if got := controlNodesNeedingMigration(status, nodes); !slices.Equal(got, want) {
		t.Fatalf("control migrations = %v, want %v", got, want)
	}
}
