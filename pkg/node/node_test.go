package node

import (
	"slices"
	"testing"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

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
