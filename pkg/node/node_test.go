package node

import (
	"testing"

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
