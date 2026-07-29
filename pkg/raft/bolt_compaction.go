package raft

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const boltCompactionTransactionBytes int64 = 64 << 20

type boltCompactionResult struct {
	Compacted   bool
	BeforeBytes int64
	AfterBytes  int64
	Duration    time.Duration
}

// compactBoltStore reclaims free pages from a closed BoltDB file. The source
// remains authoritative until a complete copy has been synced and atomically
// renamed, so a crash or ENOSPC cannot replace the durable Raft log with a
// partial database.
func compactBoltStore(path string, thresholdBytes int64) (result boltCompactionResult, err error) {
	if thresholdBytes <= 0 {
		return result, nil
	}

	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("stat source: %w", err)
	}
	result.BeforeBytes = info.Size()
	if result.BeforeBytes < thresholdBytes {
		return result, nil
	}

	startedAt := time.Now()
	source, err := bolt.Open(path, info.Mode().Perm(), &bolt.Options{
		ReadOnly:        true,
		PreLoadFreelist: true,
		Timeout:         time.Second,
	})
	if err != nil {
		return result, fmt.Errorf("open source: %w", err)
	}
	sourceOpen := true
	defer func() {
		if sourceOpen {
			closeErr := source.Close()
			if err == nil && closeErr != nil {
				err = fmt.Errorf("close source: %w", closeErr)
			}
		}
	}()

	// Do not rewrite a genuinely large live log on every restart. Compaction is
	// useful only when at least thresholdBytes and one quarter of the file are
	// known free pages.
	allocatedBytes := int64(4 * source.Info().PageSize)
	if viewErr := source.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(_ []byte, bucket *bolt.Bucket) error {
			stats := bucket.Stats()
			allocatedBytes += int64(stats.BranchAlloc + stats.LeafAlloc)
			return nil
		})
	}); viewErr != nil {
		return result, fmt.Errorf("inspect source allocation: %w", viewErr)
	}
	// Bucket allocation plus a small allowance for Bolt metadata gives a
	// conservative live-size estimate. Unlike Stats.FreeAlloc this also sees
	// the unused tail left when Bolt grew its mmap in large steps.
	reclaimableBytes := result.BeforeBytes - allocatedBytes
	if reclaimableBytes < thresholdBytes || reclaimableBytes < result.BeforeBytes/4 {
		return result, nil
	}

	tempPath := path + ".compact"
	if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, fmt.Errorf("remove stale compact copy: %w", err)
	}
	keepTemp := false
	defer func() {
		if !keepTemp {
			_ = os.Remove(tempPath)
		}
	}()

	destination, err := bolt.Open(tempPath, info.Mode().Perm(), &bolt.Options{
		Timeout: time.Second,
	})
	if err != nil {
		return result, fmt.Errorf("open compact copy: %w", err)
	}
	if err := bolt.Compact(destination, source, boltCompactionTransactionBytes); err != nil {
		_ = destination.Close()
		return result, fmt.Errorf("compact: %w", err)
	}
	if err := destination.Sync(); err != nil {
		_ = destination.Close()
		return result, fmt.Errorf("sync compact copy: %w", err)
	}
	if err := destination.Close(); err != nil {
		return result, fmt.Errorf("close compact copy: %w", err)
	}

	compactedInfo, err := os.Stat(tempPath)
	if err != nil {
		return result, fmt.Errorf("stat compact copy: %w", err)
	}
	result.AfterBytes = compactedInfo.Size()
	if result.AfterBytes >= result.BeforeBytes {
		return result, nil
	}

	// Close the source mapping before replacing its pathname. Kubernetes uses
	// Linux today, but this also preserves portable rename semantics.
	if err := source.Close(); err != nil {
		return result, fmt.Errorf("close source before replace: %w", err)
	}
	sourceOpen = false
	if err := os.Rename(tempPath, path); err != nil {
		return result, fmt.Errorf("replace source: %w", err)
	}
	keepTemp = true
	if err := syncDirectory(filepath.Dir(path)); err != nil {
		return result, fmt.Errorf("sync parent directory: %w", err)
	}

	result.Compacted = true
	result.Duration = time.Since(startedAt)
	return result, nil
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
