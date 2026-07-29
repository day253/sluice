package raft

import (
	"bytes"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"
)

func TestCompactBoltStoreReclaimsDeletedRaftPagesAndPreservesLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.db")
	store, err := raftboltdb.New(raftboltdb.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}

	payload := bytes.Repeat([]byte("x"), 32<<10)
	const logCount = 128
	for index := uint64(1); index <= logCount; index++ {
		if err := store.StoreLog(&hashicorpraft.Log{
			Index: index,
			Term:  7,
			Type:  hashicorpraft.LogCommand,
			Data:  append([]byte(nil), payload...),
		}); err != nil {
			t.Fatalf("store log %d: %v", index, err)
		}
	}
	if err := store.DeleteRange(1, logCount-1); err != nil {
		t.Fatalf("delete obsolete logs: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close source store: %v", err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compactBoltStore(path, 1)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if !result.Compacted {
		t.Fatalf("compaction result = %+v, want compacted", result)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() >= before.Size()/2 {
		t.Fatalf("compacted size = %d, before = %d", after.Size(), before.Size())
	}
	if _, err := os.Stat(path + ".compact"); !os.IsNotExist(err) {
		t.Fatalf("temporary compact copy remains: %v", err)
	}

	reopened, err := raftboltdb.New(raftboltdb.Options{Path: path})
	if err != nil {
		t.Fatalf("reopen compacted store: %v", err)
	}
	defer reopened.Close()
	first, err := reopened.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := reopened.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	if first != logCount || last != logCount {
		t.Fatalf("retained log range = %d..%d, want %d..%d", first, last, logCount, logCount)
	}
	var retained hashicorpraft.Log
	if err := reopened.GetLog(logCount, &retained); err != nil {
		t.Fatalf("read retained log: %v", err)
	}
	if retained.Term != 7 || !bytes.Equal(retained.Data, payload) {
		t.Fatal("retained log changed during compaction")
	}
}

func TestCompactBoltStoreSkipsFileWithoutEnoughReclaimableSpace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft-log.db")
	store, err := raftboltdb.New(raftboltdb.Options{Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreLog(&hashicorpraft.Log{
		Index: 1,
		Term:  1,
		Type:  hashicorpraft.LogCommand,
		Data:  []byte("live"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := compactBoltStore(path, before.Size())
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if result.Compacted {
		t.Fatalf("live store was unnecessarily compacted: %+v", result)
	}
}

func TestOversizedLogWindowUsesBoundedTrailingAllowance(t *testing.T) {
	tests := []struct {
		name     string
		first    uint64
		last     uint64
		trailing uint64
		want     bool
	}{
		{name: "empty", trailing: 1024},
		{name: "at twice allowance", first: 1, last: 2048, trailing: 1024},
		{name: "above twice allowance", first: 1, last: 2049, trailing: 1024, want: true},
		{name: "nonzero first", first: 5000, last: 7048, trailing: 1024, want: true},
		{name: "disabled", first: 1, last: 100, trailing: 0},
		{name: "overflow safe", first: 1, last: ^uint64(0), trailing: ^uint64(0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := oversizedLogWindow(test.first, test.last, test.trailing); got != test.want {
				t.Fatalf("oversizedLogWindow(%d, %d, %d) = %v, want %v",
					test.first, test.last, test.trailing, got, test.want)
			}
		})
	}
}

func TestNewClusterUsesBoundedProductionSnapshotPolicy(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}

	cluster, err := NewCluster(ClusterConfig{
		NodeID:      "storage-policy",
		RaftAddress: address,
		DataDir:     t.TempDir(),
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Shutdown()

	config := cluster.GetRaft().ReloadableConfig()
	if config.TrailingLogs != 1024 ||
		config.SnapshotThreshold != 4096 ||
		config.SnapshotInterval != 30*time.Second {
		t.Fatalf("snapshot policy = %+v", config)
	}
}
