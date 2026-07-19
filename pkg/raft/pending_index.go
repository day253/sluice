package raft

import (
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/day253/sluice/pkg/types"
)

const pendingWorkStealThreshold = 5 * time.Second

type pendingKey struct {
	createdAt int64
	taskID    string
}

func pendingKeyFor(task *types.TaskRecord) pendingKey {
	return pendingKey{createdAt: task.CreatedAt.UnixNano(), taskID: task.TaskID}
}

func (k pendingKey) less(other pendingKey) bool {
	if k.createdAt != other.createdAt {
		return k.createdAt < other.createdAt
	}
	return k.taskID < other.taskID
}

type pendingTreeNode struct {
	key      pendingKey
	priority uint64
	left     *pendingTreeNode
	right    *pendingTreeNode
}

func pendingPriority(key pendingKey) uint64 {
	h := fnv.New64a()
	var stamp [8]byte
	binary.LittleEndian.PutUint64(stamp[:], uint64(key.createdAt))
	_, _ = h.Write(stamp[:])
	_, _ = h.Write([]byte(key.taskID))
	return h.Sum64()
}

func rotatePendingRight(root *pendingTreeNode) *pendingTreeNode {
	next := root.left
	root.left = next.right
	next.right = root
	return next
}

func rotatePendingLeft(root *pendingTreeNode) *pendingTreeNode {
	next := root.right
	root.right = next.left
	next.left = root
	return next
}

func insertPendingTree(root *pendingTreeNode, key pendingKey) *pendingTreeNode {
	if root == nil {
		return &pendingTreeNode{key: key, priority: pendingPriority(key)}
	}
	if key == root.key {
		return root
	}
	if key.less(root.key) {
		root.left = insertPendingTree(root.left, key)
		if root.left.priority > root.priority {
			root = rotatePendingRight(root)
		}
	} else {
		root.right = insertPendingTree(root.right, key)
		if root.right.priority > root.priority {
			root = rotatePendingLeft(root)
		}
	}
	return root
}

func deletePendingTree(root *pendingTreeNode, key pendingKey) *pendingTreeNode {
	if root == nil {
		return nil
	}
	if key.less(root.key) {
		root.left = deletePendingTree(root.left, key)
		return root
	}
	if root.key.less(key) {
		root.right = deletePendingTree(root.right, key)
		return root
	}
	if root.left == nil {
		return root.right
	}
	if root.right == nil {
		return root.left
	}
	if root.left.priority > root.right.priority {
		root = rotatePendingRight(root)
		root.right = deletePendingTree(root.right, key)
	} else {
		root = rotatePendingLeft(root)
		root.left = deletePendingTree(root.left, key)
	}
	return root
}

type pendingNodeTenantKey struct {
	nodeID   string
	tenantID string
}

// pendingIndex is a derived, rebuildable view. It is never serialized in an
// FSM snapshot and never serves as task state truth.
type pendingIndex struct {
	all          *pendingTreeNode
	byTenant     map[string]*pendingTreeNode
	byNode       map[string]*pendingTreeNode
	byNodeTenant map[pendingNodeTenantKey]*pendingTreeNode
	count        int
}

func newPendingIndex() *pendingIndex {
	return &pendingIndex{
		byTenant:     make(map[string]*pendingTreeNode),
		byNode:       make(map[string]*pendingTreeNode),
		byNodeTenant: make(map[pendingNodeTenantKey]*pendingTreeNode),
	}
}

func (p *pendingIndex) add(task *types.TaskRecord) {
	if task == nil || task.Status != types.TaskStatusPending {
		return
	}
	key := pendingKeyFor(task)
	p.all = insertPendingTree(p.all, key)
	p.byTenant[task.TenantID] = insertPendingTree(p.byTenant[task.TenantID], key)
	if task.QueueNodeID != "" {
		p.byNode[task.QueueNodeID] = insertPendingTree(p.byNode[task.QueueNodeID], key)
		bucket := pendingNodeTenantKey{nodeID: task.QueueNodeID, tenantID: task.TenantID}
		p.byNodeTenant[bucket] = insertPendingTree(p.byNodeTenant[bucket], key)
	}
	p.count++
}

func (p *pendingIndex) remove(task *types.TaskRecord) {
	if task == nil || task.Status != types.TaskStatusPending {
		return
	}
	key := pendingKeyFor(task)
	p.all = deletePendingTree(p.all, key)
	p.byTenant[task.TenantID] = deletePendingTree(p.byTenant[task.TenantID], key)
	if p.byTenant[task.TenantID] == nil {
		delete(p.byTenant, task.TenantID)
	}
	if task.QueueNodeID != "" {
		p.byNode[task.QueueNodeID] = deletePendingTree(p.byNode[task.QueueNodeID], key)
		if p.byNode[task.QueueNodeID] == nil {
			delete(p.byNode, task.QueueNodeID)
		}
		bucket := pendingNodeTenantKey{nodeID: task.QueueNodeID, tenantID: task.TenantID}
		p.byNodeTenant[bucket] = deletePendingTree(p.byNodeTenant[bucket], key)
		if p.byNodeTenant[bucket] == nil {
			delete(p.byNodeTenant, bucket)
		}
	}
	if p.count > 0 {
		p.count--
	}
}

type pendingIterator struct {
	stack         []*pendingTreeNode
	createdBefore int64
}

func newPendingIterator(root *pendingTreeNode, createdBefore time.Time) *pendingIterator {
	it := &pendingIterator{}
	if !createdBefore.IsZero() {
		it.createdBefore = createdBefore.UnixNano()
	}
	it.pushLeft(root)
	return it
}

func (it *pendingIterator) pushLeft(node *pendingTreeNode) {
	for node != nil {
		it.stack = append(it.stack, node)
		node = node.left
	}
}

func (it *pendingIterator) next(state *types.FSMState, selected map[string]struct{}, inspected *int) *types.TaskRecord {
	for len(it.stack) > 0 {
		last := len(it.stack) - 1
		node := it.stack[last]
		it.stack = it.stack[:last]
		it.pushLeft(node.right)
		if it.createdBefore != 0 && node.key.createdAt >= it.createdBefore {
			it.stack = nil
			return nil
		}
		*inspected++
		if _, duplicate := selected[node.key.taskID]; duplicate {
			continue
		}
		task := state.Tasks[node.key.taskID]
		if task == nil || task.Status != types.TaskStatusPending {
			continue
		}
		selected[node.key.taskID] = struct{}{}
		copyTask := *task
		return &copyTask
	}
	return nil
}

// PendingSlot describes one idle execution slot without giving the Worker any
// authority to choose a concrete task.
type PendingSlot struct {
	NodeID   string
	TenantID string
}

func (f *FSM) SelectPendingForSlots(slots []PendingSlot, now time.Time) ([]*types.TaskRecord, int) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	selected := make(map[string]struct{}, len(slots))
	result := make([]*types.TaskRecord, len(slots))
	tenantIterators := make(map[string]*pendingIterator)
	nodeIterators := make(map[string]*pendingIterator)
	nodeTenantIterators := make(map[pendingNodeTenantKey]*pendingIterator)
	aged := newPendingIterator(f.pending.all, now.Add(-pendingWorkStealThreshold))
	inspected := 0
	for index, slot := range slots {
		bucket := pendingNodeTenantKey{nodeID: slot.NodeID, tenantID: slot.TenantID}
		nodeTenant := nodeTenantIterators[bucket]
		if nodeTenant == nil {
			nodeTenant = newPendingIterator(f.pending.byNodeTenant[bucket], time.Time{})
			nodeTenantIterators[bucket] = nodeTenant
		}
		if task := nodeTenant.next(f.state, selected, &inspected); task != nil {
			result[index] = task
			continue
		}
		tenant := tenantIterators[slot.TenantID]
		if tenant == nil {
			tenant = newPendingIterator(f.pending.byTenant[slot.TenantID], time.Time{})
			tenantIterators[slot.TenantID] = tenant
		}
		if task := tenant.next(f.state, selected, &inspected); task != nil {
			result[index] = task
			continue
		}
		node := nodeIterators[slot.NodeID]
		if node == nil {
			node = newPendingIterator(f.pending.byNode[slot.NodeID], time.Time{})
			nodeIterators[slot.NodeID] = node
		}
		if task := node.next(f.state, selected, &inspected); task != nil {
			result[index] = task
			continue
		}
		result[index] = aged.next(f.state, selected, &inspected)
	}
	return result, inspected
}

func appendPendingInOrder(root *pendingTreeNode, state *types.FSMState, out *[]*types.TaskRecord, before time.Time) {
	if root == nil {
		return
	}
	appendPendingInOrder(root.left, state, out, before)
	if before.IsZero() || root.key.createdAt < before.UnixNano() {
		if task := state.Tasks[root.key.taskID]; task != nil && task.Status == types.TaskStatusPending {
			copyTask := *task
			*out = append(*out, &copyTask)
		}
	}
	appendPendingInOrder(root.right, state, out, before)
}
