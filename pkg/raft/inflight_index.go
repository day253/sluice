package raft

import (
	"sort"
	"time"

	"github.com/day253/sluice/pkg/types"
)

// inflightIndex is a rebuildable current-only lease index. The durable task
// map remains authoritative; this tree only avoids scanning every pending task
// when the allocator checks a usually small set of expired claims.
type inflightIndex struct {
	byClaimedAt *pendingTreeNode
	count       int
}

func newInflightIndex() *inflightIndex {
	return &inflightIndex{}
}

func inflightKeyFor(task *types.TaskRecord) pendingKey {
	return pendingKey{createdAt: task.ClaimedAt.UnixNano(), taskID: task.TaskID}
}

func (i *inflightIndex) add(task *types.TaskRecord) {
	if task == nil || task.Status != types.TaskStatusInflight || task.ClaimedAt.IsZero() {
		return
	}
	i.byClaimedAt = insertPendingTree(i.byClaimedAt, inflightKeyFor(task))
	i.count++
}

func (i *inflightIndex) remove(task *types.TaskRecord) {
	if task == nil || task.Status != types.TaskStatusInflight || task.ClaimedAt.IsZero() {
		return
	}
	i.byClaimedAt = deletePendingTree(i.byClaimedAt, inflightKeyFor(task))
	if i.count > 0 {
		i.count--
	}
}

func (i *inflightIndex) staleTaskIDs(before time.Time) []string {
	if before.IsZero() {
		return nil
	}
	taskIDs := make([]string, 0)
	appendInflightBefore(i.byClaimedAt, before.UnixNano(), &taskIDs)
	// Preserve the previous deterministic command order even though the index
	// is ordered by lease time.
	sort.Strings(taskIDs)
	return taskIDs
}

func appendInflightBefore(node *pendingTreeNode, before int64, taskIDs *[]string) {
	if node == nil {
		return
	}
	appendInflightBefore(node.left, before, taskIDs)
	if node.key.createdAt >= before {
		return
	}
	*taskIDs = append(*taskIDs, node.key.taskID)
	appendInflightBefore(node.right, before, taskIDs)
}
