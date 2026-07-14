// Package integration contains MIT-6.824‑style multi‑node integration tests
// that exercise the full distributed rate‑limiting stack: Raft consensus,
// worker allocation, task processing, failover, and recovery.
//
// Each test spins up a real in‑memory cluster (3–5 nodes on loopback TCP),
// submits tasks, and verifies correct behaviour under both normal and
// failure conditions.
package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/node"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

// testCluster holds all the state needed for a multi-node integration test.
type testCluster struct {
	tb        testing.TB
	nodes     []*node.Node
	dirs      []string
	raftAddrs []string
	httpAddrs []string
	proc      *recordingProcessor
	workers   int

	mu      sync.Mutex
	results map[string]*types.TaskResult // taskID → final result (polled from FSM)
}

// newTestCluster creates n nodes connected in a single Raft cluster.
// Node 0 bootstraps; nodes 1..n-1 join by being added as voters on the
// leader once it is elected.  Accepts testing.TB so both *testing.T and
// *testing.B can use it.
func newTestCluster(tb testing.TB, n int, totalWorkersPerNode int) *testCluster {
	tb.Helper()

	if n < 1 {
		tb.Fatal("cluster must have at least 1 node")
	}

	tc := &testCluster{
		tb:        tb,
		nodes:     make([]*node.Node, n),
		dirs:      make([]string, n),
		raftAddrs: make([]string, n),
		httpAddrs: make([]string, n),
		proc:      newRecordingProcessor(),
		results:   make(map[string]*types.TaskResult),
		workers:   totalWorkersPerNode,
	}

	logger := zap.NewNop()
	if os.Getenv("SLUICE_TEST_LOGS") != "" {
		logger, _ = zap.NewDevelopment()
	}

	// ---- Allocate random loopback ports ----
	for i := 0; i < n; i++ {
		raftL, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("allocate raft port: %v", err)
		}
		tc.raftAddrs[i] = raftL.Addr().String()
		raftL.Close()

		httpL, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			tb.Fatalf("allocate http port: %v", err)
		}
		tc.httpAddrs[i] = httpL.Addr().String()
		httpL.Close()

		dir, err := os.MkdirTemp("", "rl-int-*")
		if err != nil {
			tb.Fatalf("temp dir: %v", err)
		}
		tc.dirs[i] = dir
	}

	// ---- Create node 0 (bootstrap) ----
	node0, err := node.New(node.Config{
		NodeID:       "node-0",
		APIAddress:   tc.httpAddrs[0],
		RaftAddress:  tc.raftAddrs[0],
		DataDir:      tc.dirs[0],
		Bootstrap:    true,
		TotalWorkers: totalWorkersPerNode,
	}, tc.proc, logger)
	if err != nil {
		tb.Fatalf("create node-0: %v", err)
	}
	tc.nodes[0] = node0

	// Run node 0 (Start blocks, so run in goroutine).
	go func() { _ = node0.Start() }()

	// Wait for node 0 to become leader and register in FSM.
	tc.waitLeader(0, 10*time.Second)
	tc.waitFor(func() bool {
		return len(node0.RaftCluster().FSM().GetActiveNodes()) > 0
	}, 5*time.Second, "node-0 registered in FSM")

	// ---- Create remaining nodes (add voter before Start) ----
	for i := 1; i < n; i++ {
		nodeID := fmt.Sprintf("node-%d", i)
		nd, err := node.New(node.Config{
			NodeID:       nodeID,
			APIAddress:   tc.httpAddrs[i],
			RaftAddress:  tc.raftAddrs[i],
			DataDir:      tc.dirs[i],
			Bootstrap:    false,
			TotalWorkers: totalWorkersPerNode,
		}, tc.proc, logger)
		if err != nil {
			tb.Fatalf("create node-%d: %v", i, err)
		}
		tc.nodes[i] = nd

		// Add as voter through the leader.
		tc.waitLeader(0, 5*time.Second)
		if err := node0.RaftCluster().AddVoter(nodeID, tc.raftAddrs[i]); err != nil {
			tb.Fatalf("add voter %s: %v", nodeID, err)
		}

		// Register the node in the FSM from the LEADER (raft.Apply
		// only works on the leader).
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpNodeUp, types.NodeInfo{
			ID: nodeID, Address: tc.httpAddrs[i],
			RaftAddress: tc.raftAddrs[i], Status: types.NodeStatusUp,
			TotalWorkers: totalWorkersPerNode,
		})
		if err := node0.RaftCluster().GetRaft().Apply(cmd, 5*time.Second).Error(); err != nil {
			tb.Fatalf("register %s: %v", nodeID, err)
		}

		// Now start the node — WaitForLeader should succeed quickly
		// because heartbeats are already arriving.
		go func(idx int) { _ = tc.nodes[idx].Start() }(i)

		// The RegisterNode call inside Start() will warn but is
		// non-fatal.  We wait for the node to appear active in the
		// FSM (already registered above).
	}
	tc.waitNodes(n, 5*time.Second)
	return tc
}

// shutdown cleanly stops all nodes and removes their data directories.
func (tc *testCluster) shutdown() {
	for i, nd := range tc.nodes {
		if nd == nil {
			continue
		}
		_ = nd.Shutdown(5 * time.Second)
		os.RemoveAll(tc.dirs[i])
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// waitLeader blocks until node i reports itself as the Raft leader.
func (tc *testCluster) waitLeader(i int, timeout time.Duration) {
	nodeID := fmt.Sprintf("node-%d", i)
	tc.waitFor(func() bool {
		return tc.nodes[i] != nil && tc.nodes[i].RaftCluster().IsLeader()
	}, timeout, nodeID+" becomes leader")
}

// waitAnyLeader waits for any running node to become leader and returns its index.
func (tc *testCluster) waitAnyLeader(timeout time.Duration) int {
	var leaderIdx int
	found := false
	tc.waitFor(func() bool {
		for i, nd := range tc.nodes {
			if nd != nil && nd.RaftCluster().IsLeader() {
				leaderIdx = i
				found = true
				return true
			}
		}
		return false
	}, timeout, "any node becomes leader")
	if !found {
		tc.tb.Fatal("no leader found")
	}
	return leaderIdx
}

// leaderIdx returns the index of the current leader, or -1.
func (tc *testCluster) leaderIdx() int {
	for i, nd := range tc.nodes {
		if nd != nil && nd.RaftCluster().IsLeader() {
			return i
		}
	}
	return -1
}

// waitNodes blocks until the FSM reports at least n active (up) nodes.
func (tc *testCluster) waitNodes(n int, timeout time.Duration) {
	tc.waitFor(func() bool {
		active := tc.nodes[0].RaftCluster().FSM().GetActiveNodes()
		return len(active) >= n
	}, timeout, fmt.Sprintf("%d nodes active", n))
}

// waitAllocation blocks until every active node has a non-empty worker
// allocation in the FSM.
func (tc *testCluster) waitAllocation(timeout time.Duration) {
	tc.waitFor(func() bool {
		fsm := tc.nodes[0].RaftCluster().FSM()
		allocs := fsm.GetAllAllocations()
		active := fsm.GetActiveNodes()
		if len(allocs) < len(active) {
			return false
		}
		for _, na := range allocs {
			if len(na.Tenants) == 0 {
				return false
			}
		}
		return true
	}, timeout, "allocation populated")
}

// waitFor is a polling helper.
func (tc *testCluster) waitFor(fn func() bool, timeout time.Duration, desc string) {
	deadline := time.After(timeout)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return
		}
		select {
		case <-deadline:
			tc.tb.Fatalf("timed out waiting for: %s", desc)
		case <-tick.C:
		}
	}
}

// addTenant upserts a tenant through node 0 and waits for it to appear in the
// FSM of every node.
func (tc *testCluster) addTenant(id string, maxWorkers int) {
	tc.tb.Helper()
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, types.TenantConfig{
		ID: id, Name: id, MaxWorkers: maxWorkers,
	})
	result := tc.nodes[0].RaftCluster().GetRaft().Apply(cmd, 5*time.Second)
	if err := result.Error(); err != nil {
		tc.tb.Fatalf("addTenant %s: %v", id, err)
	}
	// Give the allocator time to react.
	time.Sleep(500 * time.Millisecond)
}

// submitTask mirrors the production submit path: persist pending state through
// Raft, then enqueue locally as a best-effort fast path.
func (tc *testCluster) submitTask(nodeIdx int, tenantID string, payload string) string {
	tc.tb.Helper()
	taskID := fmt.Sprintf("task-%s-%d-%d", tenantID, nodeIdx, time.Now().UnixNano())
	leader := tc.leaderIdx()
	if leader < 0 {
		tc.tb.Fatal("submit task: no leader")
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: taskID, TenantID: tenantID, Payload: payload,
	})
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 5*time.Second).Error(); err != nil {
		tc.tb.Fatalf("persist task: %v", err)
	}

	env := &queue.TaskEnvelope{
		TaskID:    taskID,
		TenantID:  tenantID,
		Payload:   json.RawMessage(payload),
		CreatedAt: time.Now(),
	}
	if err := tc.nodes[nodeIdx].Queue().Enqueue(tenantID, env); err != nil {
		tc.tb.Fatalf("submit task: %v", err)
	}
	return taskID
}

// processedCount returns the total number of tasks processed by the
// recording processor across ALL nodes.
func (tc *testCluster) processedCount() int {
	return tc.proc.totalProcessed()
}

// waitProcessed blocks until the recording processor has seen at least n
// total completions.
func (tc *testCluster) waitProcessed(n int, timeout time.Duration) {
	tc.waitFor(func() bool {
		return tc.proc.totalProcessed() >= n
	}, timeout, fmt.Sprintf("%d tasks processed", n))
}

// killNode shuts down a node (simulating a crash).
func (tc *testCluster) killNode(i int) {
	tc.tb.Helper()
	if tc.nodes[i] == nil {
		return
	}
	tc.tb.Logf("killing node-%d", i)
	_ = tc.nodes[i].Shutdown(2 * time.Second)
	tc.nodes[i] = nil
}

// restartAll recreates every process over the existing Raft and queue data,
// mirroring a StatefulSet rollout or a full Kubernetes cluster restart.
func (tc *testCluster) restartAll() {
	tc.tb.Helper()
	for i, nd := range tc.nodes {
		if nd == nil {
			continue
		}
		if err := nd.Shutdown(5 * time.Second); err != nil {
			tc.tb.Fatalf("stop node-%d for restart: %v", i, err)
		}
		tc.nodes[i] = nil
	}

	logger := zap.NewNop()
	if os.Getenv("SLUICE_TEST_LOGS") != "" {
		logger, _ = zap.NewDevelopment()
	}
	for i := range tc.nodes {
		nd, err := node.New(node.Config{
			NodeID:       fmt.Sprintf("node-%d", i),
			APIAddress:   tc.httpAddrs[i],
			RaftAddress:  tc.raftAddrs[i],
			DataDir:      tc.dirs[i],
			Bootstrap:    i == 0,
			TotalWorkers: tc.workers,
		}, tc.proc, logger)
		if err != nil {
			tc.tb.Fatalf("recreate node-%d: %v", i, err)
		}
		tc.nodes[i] = nd
	}
	for i := range tc.nodes {
		go func(idx int) { _ = tc.nodes[idx].Start() }(i)
	}
	tc.waitAnyLeader(30 * time.Second)
}

// fsms returns the FSM of the first running node (for queries).
func (tc *testCluster) fsms() *raftpkg.FSM {
	for _, nd := range tc.nodes {
		if nd != nil {
			return nd.RaftCluster().FSM()
		}
	}
	tc.tb.Fatal("no running node available")
	return nil
}

// ---------------------------------------------------------------------------
// recordingProcessor — records every processed task for assertions.
// ---------------------------------------------------------------------------

type recordingProcessor struct {
	mu        sync.Mutex
	processed []processedRecord
}

type processedRecord struct {
	TaskID   string
	TenantID string
	NodeID   string
}

func newRecordingProcessor() *recordingProcessor {
	return &recordingProcessor{}
}

func (p *recordingProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	p.mu.Lock()
	p.processed = append(p.processed, processedRecord{
		TaskID: taskID, TenantID: tenantID,
	})
	p.mu.Unlock()
	// Simulate a small amount of work.
	time.Sleep(10 * time.Millisecond)
	return fmt.Sprintf(`{"echo":%s}`, string(payload)), nil
}

func (p *recordingProcessor) totalProcessed() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.processed)
}

func (p *recordingProcessor) processedByTenant(tenantID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, r := range p.processed {
		if r.TenantID == tenantID {
			n++
		}
	}
	return n
}

func (p *recordingProcessor) processedTaskCount(taskID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, record := range p.processed {
		if record.TaskID == taskID {
			n++
		}
	}
	return n
}

// ===================================================================
// MIT 6.824‑style integration tests
// ===================================================================

// TestBasicAgreement verifies that tasks submitted across the cluster are
// all processed via Raft consensus.
func TestBasicAgreement(t *testing.T) {
	tc := newTestCluster(t, 2, 50)
	defer tc.shutdown()

	tc.addTenant("alice", 100)
	tc.addTenant("bob", 50)
	tc.waitAllocation(10 * time.Second)
	time.Sleep(2 * time.Second) // let workers spawn

	// Submit all tasks through a follower's local worker queue. This verifies
	// both claim and completion forwarding through the leader streams.
	leader := tc.leaderIdx()
	submitNode := 0
	if submitNode == leader {
		submitNode = 1
	}
	const nTasks = 15
	taskIDs := make([]string, 0, nTasks)
	for i := 0; i < nTasks; i++ {
		taskIDs = append(taskIDs, tc.submitTask(submitNode, "alice", fmt.Sprintf(`"payload-%d"`, i)))
	}

	tc.waitProcessed(nTasks, 30*time.Second)
	tc.waitFor(func() bool {
		for _, taskID := range taskIDs {
			if result := tc.fsms().GetResult(taskID); result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 30*time.Second, "follower task results committed through leader")

	aliceCount := tc.proc.processedByTenant("alice")
	if aliceCount != nTasks {
		t.Errorf("alice: processed %d / %d tasks", aliceCount, nTasks)
	}
	t.Logf("basic agreement: %d/%d tasks processed", aliceCount, nTasks)
}

// TestHTTPSubmitThroughFollower covers the production API path that was
// previously missing from the integration suite. The request enters through
// a real follower HTTP listener, is forwarded to the leader, and is then
// processed and committed by the cluster.
func TestHTTPSubmitThroughFollower(t *testing.T) {
	tc := newTestCluster(t, 2, 20)
	defer tc.shutdown()

	tc.addTenant("http-tenant", 20)
	tc.waitAllocation(10 * time.Second)
	follower := tc.leaderIdx()
	if follower < 0 {
		t.Fatal("no leader found")
	}
	follower = 1 - follower

	body := []byte(`{"tenant_id":"http-tenant","payload":{"source":"follower"}}`)
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Post("http://"+tc.httpAddrs[follower]+"/api/v1/tasks", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST through follower: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST through follower status = %d, want 202; body=%s", resp.StatusCode, data)
	}
	var task types.TaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&task); err != nil {
		t.Fatalf("decode follower response: %v", err)
	}
	if task.TaskID == "" || task.TenantID != "http-tenant" {
		t.Fatalf("follower response = %+v, want task for http-tenant", task)
	}
	tc.waitProcessed(1, 30*time.Second)
	tc.waitFor(func() bool {
		result := tc.fsms().GetResult(task.TaskID)
		return result != nil && result.Status == types.TaskStatusDone
	}, 30*time.Second, "HTTP follower task result")
}

// TestHTTPBatchSubmitThroughFollower verifies the optimized submission path:
// one HTTP request entering through a follower creates all tasks with one
// create_task_batch Raft entry and every task is eventually processed.
func TestHTTPBatchSubmitThroughFollower(t *testing.T) {
	tc := newTestCluster(t, 2, 20)
	defer tc.shutdown()

	const taskCount = 24
	tc.addTenant("batch-http-tenant", taskCount)
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader found")
	}
	follower := 1 - leader

	request := types.BatchTaskSubmitRequest{Tasks: make([]types.TaskSubmitRequest, taskCount)}
	for i := range request.Tasks {
		request.Tasks[i] = types.TaskSubmitRequest{
			TenantID: "batch-http-tenant",
			Payload:  json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Post(
		"http://"+tc.httpAddrs[follower]+"/api/v1/tasks/batch", "application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("batch POST through follower: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(resp.Body)
		t.Fatalf("batch POST status = %d, want 202; body=%s", resp.StatusCode, data)
	}
	var result types.BatchTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode batch response: %v", err)
	}
	if len(result.Tasks) != taskCount {
		t.Fatalf("batch response tasks = %d, want %d", len(result.Tasks), taskCount)
	}
	tc.waitProcessed(taskCount, 30*time.Second)
	for _, task := range result.Tasks {
		if task.TaskID == "" {
			t.Fatal("batch response contained an empty task ID")
		}
		tc.waitFor(func() bool {
			completed := tc.fsms().GetResult(task.TaskID)
			return completed != nil && completed.Status == types.TaskStatusDone
		}, 30*time.Second, "batch task completion")
	}
}

// TestWorkStealUsesAgedPendingWork verifies the cross-tenant fallback path.
// The target tenant is deliberately removed from the current allocation
// mirror, so only an already allocated idle worker can finish its aged task.
func TestWorkStealUsesAgedPendingWork(t *testing.T) {
	tc := newTestCluster(t, 2, 10)
	defer tc.shutdown()

	tc.addTenant("steal-worker", 10)
	tc.addTenant("steal-target", 1)
	tc.waitAllocation(10 * time.Second)

	// Stop target workers in the current mirror while preserving one source
	// worker on every node. The allocator may restore the normal plan later,
	// but the aged task should be claimed before its next reconciliation tick.
	allocs := make(map[string]*types.NodeAllocation)
	for _, nodeInfo := range tc.fsms().GetActiveNodes() {
		allocs[nodeInfo.ID] = &types.NodeAllocation{
			NodeID:  nodeInfo.ID,
			Tenants: map[string]int{"steal-worker": 1},
		}
	}
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader found")
	}
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpUpdateAllocation, allocs), 5*time.Second,
	).Error(); err != nil {
		t.Fatalf("remove target allocation: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	taskID := "aged-steal-task"
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpCreateTask, raftpkg.CreateTaskData{
			TaskID: taskID, TenantID: "steal-target", Payload: `{"from":"steal"}`,
		}), 5*time.Second,
	).Error(); err != nil {
		t.Fatalf("create aged task: %v", err)
	}
	// Make the task old without sleeping through the five-second admission
	// threshold. This mutation is test-only; production CreatedAt is immutable.
	leaderFSM := tc.nodes[leader].RaftCluster().FSM()
	state := leaderFSM.GetState()
	if state.Tasks[taskID] == nil {
		t.Fatalf("created task %s missing from leader FSM", taskID)
	}
	state.Tasks[taskID].CreatedAt = time.Now().UTC().Add(-time.Minute)
	persisted, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := leaderFSM.Restore(io.NopCloser(bytes.NewReader(persisted))); err != nil {
		t.Fatalf("age task in test FSM: %v", err)
	}

	tc.waitFor(func() bool {
		result := tc.fsms().GetResult(taskID)
		return result != nil && result.Status == types.TaskStatusDone
	}, 10*time.Second, "aged task stolen by idle worker")
	if got := tc.proc.processedByTenant("steal-target"); got != 1 {
		t.Fatalf("steal-target processed count = %d, want 1", got)
	}
}

// TestFailover kills the leader and verifies a new leader is elected and
// can commit Raft log entries — the core guarantee of Raft consensus.
func TestFailover(t *testing.T) {
	tc := newTestCluster(t, 3, 50)
	defer tc.shutdown()

	tc.addTenant("alice", 100)
	tc.waitAllocation(10 * time.Second)

	oldLeader := tc.leaderIdx()
	if oldLeader < 0 {
		t.Fatal("no leader found")
	}
	t.Logf("initial leader: node-%d", oldLeader)

	// Write a value through the old leader.
	cmd1 := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant,
		types.TenantConfig{ID: "before-fail", MaxWorkers: 10})
	if err := tc.nodes[oldLeader].RaftCluster().GetRaft().Apply(cmd1, 5*time.Second).Error(); err != nil {
		t.Fatalf("apply before failover: %v", err)
	}

	// Kill the leader.
	tc.killNode(oldLeader)

	// Wait for a new leader.
	newLeader := tc.waitAnyLeader(30 * time.Second)
	t.Logf("new leader: node-%d", newLeader)

	// Verify the surviving cluster can commit NEW log entries.
	cmd2 := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant,
		types.TenantConfig{ID: "after-fail", MaxWorkers: 20})
	if err := tc.nodes[newLeader].RaftCluster().GetRaft().Apply(cmd2, 10*time.Second).Error(); err != nil {
		t.Fatalf("apply after failover: %v", err)
	}

	// Both entries must be visible in the new leader's FSM.
	leaderFSM := tc.nodes[newLeader].RaftCluster().FSM()
	if _, ok := leaderFSM.GetTenant("before-fail"); !ok {
		t.Error("before-fail tenant not found — log possibly lost")
	}
	if _, ok := leaderFSM.GetTenant("after-fail"); !ok {
		t.Error("after-fail tenant not found — new leader cannot commit")
	}

	// The remaining follower must notice that the leader address changed even
	// though it stayed a follower, then forward claim and result streams to it.
	follower := -1
	for i, node := range tc.nodes {
		if node != nil && i != newLeader {
			follower = i
			break
		}
	}
	if follower < 0 {
		t.Fatal("no surviving follower")
	}
	taskID := tc.submitTask(follower, "alice", `"after-leader-change"`)
	tc.waitFor(func() bool {
		result := tc.nodes[newLeader].RaftCluster().FSM().GetResult(taskID)
		return result != nil && result.Status == types.TaskStatusDone
	}, 30*time.Second, "task processed after follower-to-follower leader change")

	t.Logf("failover: Raft log preserved and new entries committed after leader kill")
}

// TestRecovery verifies that OpNodeDown re-queues inflight tasks for a
// failed node so they can be picked up by survivors.
func TestRecovery(t *testing.T) {
	tc := newTestCluster(t, 3, 50)
	defer tc.shutdown()

	tc.addTenant("alice", 100)
	tc.waitAllocation(10 * time.Second)

	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader found")
	}
	victim := -1
	for i, nd := range tc.nodes {
		if nd != nil && i != leader {
			victim = i
			break
		}
	}
	t.Logf("leader: node-%d, victim: node-%d", leader, victim)

	// Create inflight tasks directly via Raft (bypassing workers).
	// These simulate tasks that were claimed by the victim.
	taskIDs := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		taskID := fmt.Sprintf("rec-task-%d", i)
		taskIDs = append(taskIDs, taskID)
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
			TaskID:   taskID,
			TenantID: "alice",
			NodeID:   fmt.Sprintf("node-%d", victim),
			Payload:  `"recovery-test"`,
		})
		if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 5*time.Second).Error(); err != nil {
			t.Fatalf("create inflight task %d: %v", i, err)
		}
	}

	// Verify tasks are inflight on the victim.
	state := tc.fsms().GetState()
	inflightCount := 0
	for _, t := range state.Tasks {
		if t.NodeID == fmt.Sprintf("node-%d", victim) && t.Status == types.TaskStatusInflight {
			inflightCount++
		}
	}
	t.Logf("inflight tasks on victim: %d", inflightCount)
	if inflightCount < 5 {
		t.Fatalf("expected 5 inflight tasks, got %d", inflightCount)
	}

	// Kill the victim and mark it down.
	tc.killNode(victim)
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpNodeDown, raftpkg.NodeDownData{
		ID: fmt.Sprintf("node-%d", victim),
	})
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 5*time.Second).Error(); err != nil {
		t.Fatalf("mark node down: %v", err)
	}

	// Survivors must claim, process, and commit every re-queued task. Checking
	// only the intermediate pending state would miss broken result forwarding.
	tc.waitFor(func() bool {
		for _, taskID := range taskIDs {
			result := tc.fsms().GetResult(taskID)
			if result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 30*time.Second, "all node-down tasks recovered and committed")
	for _, taskID := range taskIDs {
		if count := tc.proc.processedTaskCount(taskID); count != 1 {
			t.Errorf("recovery task %s processed %d times, want once", taskID, count)
		}
	}

	t.Logf("recovery: inflight→pending→claim→complete pipeline verified")
}

// TestFullClusterRestartRecoversExpiredClaims preserves the production case
// where all Kubernetes Pods restart while work is pending and inflight. Pending
// work must resume immediately; abandoned inflight work must resume after its
// claim lease expires, with no task executed twice.
func TestFullClusterRestartRecoversExpiredClaims(t *testing.T) {
	tc := newTestCluster(t, 3, 20)
	defer tc.shutdown()

	tc.addTenant("restart-tenant", 30)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader before restart")
	}

	taskIDs := make([]string, 0, 6)
	for i := 0; i < 5; i++ {
		taskID := fmt.Sprintf("restart-pending-%d", i)
		taskIDs = append(taskIDs, taskID)
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTask, raftpkg.CreateTaskData{
			TaskID: taskID, TenantID: "restart-tenant", Payload: `"pending"`,
		})
		if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 5*time.Second).Error(); err != nil {
			t.Fatalf("create pending task: %v", err)
		}
	}
	const expiredTaskID = "restart-expired-claim"
	taskIDs = append(taskIDs, expiredTaskID)
	claim := raftpkg.MustMarshalCommand(raftpkg.OpClaimTask, raftpkg.ClaimTaskData{
		TaskID: expiredTaskID, TenantID: "restart-tenant", NodeID: "node-1", Payload: `"inflight"`,
	})
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(claim, 5*time.Second).Error(); err != nil {
		t.Fatalf("create inflight task: %v", err)
	}

	tc.restartAll()
	tc.waitAllocation(15 * time.Second)
	tc.waitFor(func() bool {
		for _, taskID := range taskIDs {
			result := tc.fsms().GetResult(taskID)
			if result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, taskClaimRecoveryTimeout(), "all work completed after full cluster restart")

	if unfinished := tc.fsms().CountUnfinishedPerTenant()["restart-tenant"]; unfinished != 0 {
		t.Fatalf("unfinished tasks after recovery = %d, want 0", unfinished)
	}
	for _, taskID := range taskIDs {
		if count := tc.proc.processedTaskCount(taskID); count != 1 {
			t.Errorf("task %s processed %d times across restart, want once", taskID, count)
		}
	}
	t.Log("full restart: pending and expired inflight work drained exactly once")
}

func taskClaimRecoveryTimeout() time.Duration {
	// Production lease is 30s and allocator reconciliation runs every 3s.
	// Keep enough election/CI headroom without hiding an unbounded wait.
	return 50 * time.Second
}

// TestDynamicTenant verifies that adding and modifying tenants at runtime
// causes the allocation to adapt.
func TestDynamicTenant(t *testing.T) {
	tc := newTestCluster(t, 2, 50) // 2 nodes, 100 total workers
	defer tc.shutdown()

	// Start with one tenant.
	tc.addTenant("alice", 50)
	tc.waitAllocation(10 * time.Second)

	fsm := tc.fsms()
	alloc := fsm.GetAllAllocations()

	aliceTotal := 0
	for _, na := range alloc {
		aliceTotal += na.Tenants["alice"]
	}
	t.Logf("alice workers after create: %d (max 50)", aliceTotal)

	// Add a second tenant and wait long enough for reallocation.
	tc.addTenant("bob", 50)
	// The allocator runs every 3s — wait 5s to ensure rebalance.
	time.Sleep(5 * time.Second)

	fsm = tc.fsms()
	alloc = fsm.GetAllAllocations()
	aliceTotal2 := 0
	bobTotal := 0
	for _, na := range alloc {
		aliceTotal2 += na.Tenants["alice"]
		bobTotal += na.Tenants["bob"]
	}
	t.Logf("after bob added: alice=%d bob=%d (100 total)", aliceTotal2, bobTotal)

	if bobTotal < 1 {
		t.Error("bob should get at least 1 worker")
	}
	// Alice should have lost some workers to Bob.
	if aliceTotal2 >= aliceTotal {
		t.Logf("alice unchanged (%d → %d) — may be fully allocated", aliceTotal, aliceTotal2)
	}
}

// TestAdaptiveIdleBorrowing verifies the end-to-end controller behavior:
// every aged backlog can probe above its configured limit, multiple tenants
// share spare capacity, and total effective workers never exceed the cluster.
func TestAdaptiveIdleBorrowing(t *testing.T) {
	tc := newTestCluster(t, 2, 5) // 10 total workers
	defer tc.shutdown()

	tc.addTenant("borrower", 1)
	tc.addTenant("other", 1)
	tc.waitFor(func() bool {
		allocations := tc.fsms().GetAllAllocations()
		tenantEntries := 0
		for _, allocation := range allocations {
			tenantEntries += len(allocation.Tenants)
		}
		return len(allocations) == 2 && tenantEntries >= 2
	}, 10*time.Second, "adaptive allocation mirror")

	createBacklog := func(tenantID, prefix string, count int) {
		t.Helper()
		leader := tc.leaderIdx()
		if leader < 0 {
			t.Fatal("no leader while creating backlog")
		}
		tasks := make([]raftpkg.CreateTaskData, count)
		for i := range tasks {
			tasks[i] = raftpkg.CreateTaskData{
				TaskID:   fmt.Sprintf("%s-%d", prefix, i),
				TenantID: tenantID,
				Payload:  `"adaptive-borrowing"`,
			}
		}
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks})
		if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 10*time.Second).Error(); err != nil {
			t.Fatalf("create %s backlog: %v", tenantID, err)
		}
	}

	// The first tenant has enough pending work to keep a probe meaningful
	// through several 3-second allocator cycles.
	createBacklog("borrower", "borrower-task", 2000)
	tc.waitFor(func() bool {
		borrowed := 0
		for _, allocation := range tc.fsms().GetAllAllocations() {
			borrowed += allocation.Borrowed["borrower"]
		}
		return borrowed > 0
	}, 12*time.Second, "borrowed workers for first backlogged tenant")

	createBacklog("other", "other-task", 2000)
	tc.waitFor(func() bool {
		borrowed := map[string]int{}
		effectiveTotal := 0
		for _, allocation := range tc.fsms().GetAllAllocations() {
			borrowed["borrower"] += allocation.Borrowed["borrower"]
			borrowed["other"] += allocation.Borrowed["other"]
			for _, workers := range allocation.Tenants {
				effectiveTotal += workers
			}
		}
		return borrowed["borrower"] > 0 && borrowed["other"] > 0 && effectiveTotal <= 10
	}, 12*time.Second, "spare workers shared by two backlogged tenants")
}

// TestOversubscription verifies the max-min fairness allocation when the sum
// of all tenant limits exceeds the total cluster capacity.
func TestOversubscription(t *testing.T) {
	// 2 nodes × 50 workers = 100 total
	// Tenant limits: alice=100, bob=50, carol=30 → sum=180 > 100
	tc := newTestCluster(t, 2, 50)
	defer tc.shutdown()

	tc.addTenant("alice", 100)
	tc.addTenant("bob", 50)
	tc.addTenant("carol", 30)
	time.Sleep(4 * time.Second)

	fsm := tc.fsms()
	alloc := fsm.GetAllAllocations()

	totals := map[string]int{}
	for _, na := range alloc {
		for tid, cnt := range na.Tenants {
			totals[tid] += cnt
		}
	}

	t.Logf("oversubscribed allocation: alice=%d bob=%d carol=%d (total=%d)",
		totals["alice"], totals["bob"], totals["carol"],
		totals["alice"]+totals["bob"]+totals["carol"])

	// Every tenant must have at least 1 worker.
	for _, tid := range []string{"alice", "bob", "carol"} {
		if totals[tid] < 1 {
			t.Errorf("%s has %d workers, want at least 1", tid, totals[tid])
		}
	}

	// No tenant should exceed its limit.
	if totals["alice"] > 100 {
		t.Errorf("alice exceeds limit: %d > 100", totals["alice"])
	}
	if totals["bob"] > 50 {
		t.Errorf("bob exceeds limit: %d > 50", totals["bob"])
	}
	if totals["carol"] > 30 {
		t.Errorf("carol exceeds limit: %d > 30", totals["carol"])
	}

	// Total should not exceed cluster capacity.
	grandTotal := totals["alice"] + totals["bob"] + totals["carol"]
	if grandTotal > 100 {
		t.Errorf("grand total %d exceeds cluster capacity 100", grandTotal)
	}

	// Under oversubscription, no single tenant should get its full limit
	// (unless all others also get theirs, which is impossible when sum>total).
	// At least one tenant should be below its limit.
	below := false
	if totals["alice"] < 100 {
		below = true
	}
	if totals["bob"] < 50 {
		below = true
	}
	if totals["carol"] < 30 {
		below = true
	}
	if !below {
		t.Error("at least one tenant should be below its limit under oversubscription")
	}

	// Submit tasks to all tenants.
	for i := 0; i < 10; i++ {
		tc.submitTask(0, "alice", `"a"`)
		tc.submitTask(0, "bob", `"b"`)
		tc.submitTask(0, "carol", `"c"`)
	}

	tc.waitProcessed(30, 30*time.Second)

	// Verify each tenant had its tasks processed.
	for _, tid := range []string{"alice", "bob", "carol"} {
		n := tc.proc.processedByTenant(tid)
		if n < 1 {
			t.Errorf("%s had no tasks processed", tid)
		}
		t.Logf("%s processed %d tasks", tid, n)
	}
}
