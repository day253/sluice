package queue

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
)

// BoltQueue implements Queue on top of a local BoltDB instance.
// Each tenant gets its own bucket, and tasks are ordered by a
// monotonically-increasing sequence number encoded in the key.
//
// Bucket layout:
//
//	root / queue:{tenantID}
//	 key: 8-byte big-endian sequence number → JSON(TaskEnvelope)
type BoltQueue struct {
	db     *bbolt.DB
	logger *zap.Logger
}

// NewBoltQueue opens or creates a BoltDB database at the given path.
func NewBoltQueue(path string, logger *zap.Logger) (*BoltQueue, error) {
	db, err := bbolt.Open(path, 0o600, nil)
	if err != nil {
		return nil, fmt.Errorf("boltdb open: %w", err)
	}
	return &BoltQueue{db: db, logger: logger}, nil
}

// ---------------------------------------------------------------------------
// Queue interface
// ---------------------------------------------------------------------------

// Enqueue appends a task to the tenant queue.
func (bq *BoltQueue) Enqueue(tenantID string, task *TaskEnvelope) error {
	return bq.db.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("queue:" + tenantID))
		if err != nil {
			return fmt.Errorf("create bucket: %w", err)
		}

		seq, _ := bucket.NextSequence()
		key := encodeBoltKey(seq)
		value, err := json.Marshal(task)
		if err != nil {
			return fmt.Errorf("marshal task: %w", err)
		}
		return bucket.Put(key, value)
	})
}

// Dequeue atomically removes and returns the oldest task.  Returns nil
// when the queue is empty.
func (bq *BoltQueue) Dequeue(tenantID string) (*TaskEnvelope, error) {
	var task *TaskEnvelope

	err := bq.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("queue:" + tenantID))
		if bucket == nil {
			return nil // no bucket = empty
		}

		cursor := bucket.Cursor()
		key, value := cursor.First()
		if key == nil {
			return nil // empty
		}

		if err := json.Unmarshal(value, &task); err != nil {
			return fmt.Errorf("unmarshal task: %w", err)
		}
		return cursor.Delete()
	})
	if err != nil {
		return nil, err
	}
	return task, nil
}

// Len returns the number of waiting tasks for a tenant.
func (bq *BoltQueue) Len(tenantID string) (int, error) {
	var count int
	err := bq.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("queue:" + tenantID))
		if bucket == nil {
			return nil
		}
		count = bucket.Stats().KeyN
		return nil
	})
	return count, err
}

// ListPending returns all waiting tasks for a tenant.
func (bq *BoltQueue) ListPending(tenantID string) ([]*TaskEnvelope, error) {
	var tasks []*TaskEnvelope
	err := bq.db.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("queue:" + tenantID))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(_, v []byte) error {
			var task TaskEnvelope
			if err := json.Unmarshal(v, &task); err != nil {
				bq.logger.Warn("boltdb list: skipping corrupt value",
					zap.String("tenant", tenantID), zap.Error(err),
				)
				return nil
			}
			tasks = append(tasks, &task)
			return nil
		})
	})
	return tasks, err
}

// Remove deletes a specific task by scanning the tenant bucket.
func (bq *BoltQueue) Remove(tenantID, taskID string) error {
	return bq.db.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte("queue:" + tenantID))
		if bucket == nil {
			return nil
		}
		return bucket.ForEach(func(k, v []byte) error {
			var task TaskEnvelope
			if err := json.Unmarshal(v, &task); err != nil {
				return nil
			}
			if task.TaskID == taskID {
				return bucket.Delete(k)
			}
			return nil
		})
	})
}

// Close releases the BoltDB handle.
func (bq *BoltQueue) Close() error {
	return bq.db.Close()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func encodeBoltKey(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b
}
