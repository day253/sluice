package raft

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	hashicorpraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
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
