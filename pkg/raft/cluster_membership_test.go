package raft

import (
	"testing"

	hashicorpraft "github.com/hashicorp/raft"
)

func TestDesiredVoterIDsUseStableOrdinalOrder(t *testing.T) {
	configuration := hashicorpraft.Configuration{Servers: []hashicorpraft.Server{
		{ID: "sluice-10", Suffrage: hashicorpraft.Voter},
		{ID: "sluice-2", Suffrage: hashicorpraft.Voter},
		{ID: "sluice-1", Suffrage: hashicorpraft.Nonvoter},
		{ID: "sluice-11", Suffrage: hashicorpraft.Voter},
		{ID: "sluice-0", Suffrage: hashicorpraft.Voter},
		{ID: "sluice-3", Suffrage: hashicorpraft.Nonvoter},
	}}
	desired := desiredVoterIDs(configuration, 3)
	for _, id := range []hashicorpraft.ServerID{"sluice-0", "sluice-1", "sluice-2"} {
		if _, ok := desired[id]; !ok {
			t.Fatalf("desired voters %v omitted %s", desired, id)
		}
	}
	for _, id := range []hashicorpraft.ServerID{"sluice-3", "sluice-10", "sluice-11"} {
		if _, ok := desired[id]; ok {
			t.Fatalf("desired voters %v unexpectedly included %s", desired, id)
		}
	}
}

func TestValidateMaxVotersRejectsUnsafeEvenQuorum(t *testing.T) {
	for _, value := range []int{0, 2, 4, 6} {
		if err := validateMaxVoters(value); err == nil {
			t.Fatalf("max voters %d was accepted", value)
		}
	}
	for _, value := range []int{1, 3, 5} {
		if err := validateMaxVoters(value); err != nil {
			t.Fatalf("max voters %d: %v", value, err)
		}
	}
}
