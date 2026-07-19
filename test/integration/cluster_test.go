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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	metricspkg "github.com/day253/sluice/pkg/metrics"
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
	tb                         testing.TB
	nodes                      []*node.Node
	dirs                       []string
	raftAddrs                  []string
	httpAddrs                  []string
	proc                       *recordingProcessor
	workers                    int
	maxVoters                  int
	disableVoterReconciliation bool

	mu      sync.Mutex
	results map[string]*types.TaskResult // taskID → final result (polled from FSM)
}

// newTestCluster creates n nodes connected in a single Raft cluster.
// Node 0 bootstraps; nodes 1..n-1 join by being added as voters on the
// leader once it is elected.  Accepts testing.TB so both *testing.T and
// *testing.B can use it.
func newTestCluster(tb testing.TB, n int, totalWorkersPerNode int) *testCluster {
	logger := zap.NewNop()
	if os.Getenv("SLUICE_TEST_LOGS") != "" {
		logger, _ = zap.NewDevelopment()
	}
	return newTestClusterWithLogger(tb, n, totalWorkersPerNode, logger)
}

func newClaimRejectCountingLogger(rejectedClaims *atomic.Int64) *zap.Logger {
	var sink io.Writer = io.Discard
	if os.Getenv("SLUICE_TEST_LOGS") != "" {
		sink = os.Stderr
	}
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(sink),
		zap.WarnLevel,
	)
	return zap.New(core, zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Message == "failed to claim task" {
			rejectedClaims.Add(1)
		}
		return nil
	}))
}

func newTestClusterWithLogger(tb testing.TB, n int, totalWorkersPerNode int, logger *zap.Logger) *testCluster {
	return newTestClusterWithMemberAdder(tb, n, totalWorkersPerNode, raftpkg.DefaultMaxVoters, false, logger,
		func(cluster *raftpkg.Cluster, nodeID, address string) error {
			return cluster.AddVoter(nodeID, address)
		})
}

func newTestClusterWithVoterLimit(tb testing.TB, n int, totalWorkersPerNode, maxVoters int, logger *zap.Logger) *testCluster {
	return newTestClusterWithMemberAdder(tb, n, totalWorkersPerNode, maxVoters, false, logger,
		func(cluster *raftpkg.Cluster, nodeID, address string) error {
			return cluster.AddServer(nodeID, address, maxVoters)
		})
}

func newAllVoterTestCluster(tb testing.TB, n int, totalWorkersPerNode int) *testCluster {
	return newTestClusterWithMemberAdder(tb, n, totalWorkersPerNode, n, true, zap.NewNop(),
		func(cluster *raftpkg.Cluster, nodeID, address string) error {
			return cluster.AddVoter(nodeID, address)
		})
}

func newTestClusterWithMemberAdder(
	tb testing.TB,
	n int,
	totalWorkersPerNode int,
	maxVoters int,
	disableVoterReconciliation bool,
	logger *zap.Logger,
	addMember func(*raftpkg.Cluster, string, string) error,
) *testCluster {
	tb.Helper()

	if n < 1 {
		tb.Fatal("cluster must have at least 1 node")
	}

	tc := &testCluster{
		tb:                         tb,
		nodes:                      make([]*node.Node, n),
		dirs:                       make([]string, n),
		raftAddrs:                  make([]string, n),
		httpAddrs:                  make([]string, n),
		proc:                       newRecordingProcessor(),
		results:                    make(map[string]*types.TaskResult),
		workers:                    totalWorkersPerNode,
		maxVoters:                  maxVoters,
		disableVoterReconciliation: disableVoterReconciliation,
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
		NodeID:                     "node-0",
		APIAddress:                 tc.httpAddrs[0],
		RaftAddress:                tc.raftAddrs[0],
		DataDir:                    tc.dirs[0],
		Bootstrap:                  true,
		TotalWorkers:               totalWorkersPerNode,
		MaxRaftVoters:              maxVoters,
		DisableVoterReconciliation: disableVoterReconciliation,
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
			NodeID:                     nodeID,
			APIAddress:                 tc.httpAddrs[i],
			RaftAddress:                tc.raftAddrs[i],
			DataDir:                    tc.dirs[i],
			Bootstrap:                  false,
			TotalWorkers:               totalWorkersPerNode,
			MaxRaftVoters:              maxVoters,
			DisableVoterReconciliation: disableVoterReconciliation,
		}, tc.proc, logger)
		if err != nil {
			tb.Fatalf("create node-%d: %v", i, err)
		}
		tc.nodes[i] = nd

		// Add as voter through the leader.
		tc.waitLeader(0, 5*time.Second)
		if err := addMember(node0.RaftCluster(), nodeID, tc.raftAddrs[i]); err != nil {
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

// waitAllocation blocks until every active follower has a non-empty worker
// allocation and the leader has none.
func (tc *testCluster) waitAllocation(timeout time.Duration) {
	tc.waitFor(func() bool {
		fsm := tc.nodes[0].RaftCluster().FSM()
		allocs := fsm.GetAllAllocations()
		active := fsm.GetActiveNodes()
		leader := tc.leaderIdx()
		if leader < 0 || len(allocs) != len(active)-1 {
			return false
		}
		leaderID := fmt.Sprintf("node-%d", leader)
		if _, ok := allocs[leaderID]; ok {
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
			NodeID:                     fmt.Sprintf("node-%d", i),
			APIAddress:                 tc.httpAddrs[i],
			RaftAddress:                tc.raftAddrs[i],
			DataDir:                    tc.dirs[i],
			Bootstrap:                  i == 0,
			TotalWorkers:               tc.workers,
			MaxRaftVoters:              tc.maxVoters,
			DisableVoterReconciliation: tc.disableVoterReconciliation,
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

	gateTenant  string
	gateStarted chan string
	gateRelease chan struct{}
	canceled    int
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
	gateStarted := p.gateStarted
	gateRelease := p.gateRelease
	gated := tenantID == p.gateTenant && gateStarted != nil && gateRelease != nil
	p.mu.Unlock()
	if gated {
		gateStarted <- taskID
		select {
		case <-gateRelease:
		case <-ctx.Done():
			p.mu.Lock()
			p.canceled++
			p.mu.Unlock()
			return "", ctx.Err()
		}
	}
	// Simulate a small amount of work.
	time.Sleep(10 * time.Millisecond)
	return fmt.Sprintf(`{"echo":%s}`, string(payload)), nil
}

func (p *recordingProcessor) gate(tenantID string) (<-chan string, func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gateTenant = tenantID
	p.gateStarted = make(chan string, 128)
	p.gateRelease = make(chan struct{})
	var once sync.Once
	return p.gateStarted, func() { once.Do(func() { close(p.gateRelease) }) }
}

func (p *recordingProcessor) canceledCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.canceled
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

func (p *recordingProcessor) processedTaskCounts() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	counts := make(map[string]int, len(p.processed))
	for _, record := range p.processed {
		counts[record.TaskID]++
	}
	return counts
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

// TestLeaderIsControlPlaneOnly locks the role boundary: the current leader
// owns assignment and Raft commits but has no allocation or live business
// workers. A follower still drains work end to end.
func TestLeaderIsControlPlaneOnly(t *testing.T) {
	tc := newTestCluster(t, 2, 20)
	defer tc.shutdown()

	tc.addTenant("control-plane-boundary", 20)
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader")
	}
	follower := 1 - leader
	leaderID := fmt.Sprintf("node-%d", leader)
	if allocation, ok := tc.fsms().GetAllocation(leaderID); ok {
		t.Fatalf("leader allocation = %+v, want none", allocation)
	}
	tc.waitFor(func() bool {
		return tc.nodes[leader].Pool().GetStatus()["control-plane-boundary"] == 0 &&
			tc.nodes[follower].Pool().GetStatus()["control-plane-boundary"] > 0
	}, 5*time.Second, "worker pools apply follower-only allocation")
	taskID := tc.submitTask(leader, "control-plane-boundary", `"leader-submission"`)
	tc.waitFor(func() bool {
		result := tc.fsms().GetResult(taskID)
		return result != nil && result.Status == types.TaskStatusDone
	}, 30*time.Second, "follower executes leader-submitted task")
	if count := tc.proc.processedTaskCount(taskID); count != 1 {
		t.Fatalf("task processed %d times, want once", count)
	}
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

// TestHTTPBatchSubmitThroughFollower verifies the maximum-size optimized
// submission path and SUBMIT-003: a retry after an unknown follower-forward
// outcome returns the same IDs and cannot create duplicate work.
func TestHTTPBatchSubmitThroughFollower(t *testing.T) {
	tc := newTestCluster(t, 2, 100)
	defer tc.shutdown()

	const taskCount = 1000
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
			TenantID:       "batch-http-tenant",
			Payload:        json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
			IdempotencyKey: fmt.Sprintf("batch-retry-%d", i),
		}
	}
	body, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 70 * time.Second}
	resp, err := client.Post(
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
	_ = resp.Body.Close()

	retryResp, err := client.Post(
		"http://"+tc.httpAddrs[follower]+"/api/v1/tasks/batch", "application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("retry batch POST through follower: %v", err)
	}
	defer retryResp.Body.Close()
	if retryResp.StatusCode != http.StatusAccepted {
		data, _ := io.ReadAll(retryResp.Body)
		t.Fatalf("retry batch POST status = %d, want 202; body=%s", retryResp.StatusCode, data)
	}
	var retryResult types.BatchTaskResponse
	if err := json.NewDecoder(retryResp.Body).Decode(&retryResult); err != nil {
		t.Fatalf("decode retry batch response: %v", err)
	}
	if len(retryResult.Tasks) != taskCount {
		t.Fatalf("retry batch response tasks = %d, want %d", len(retryResult.Tasks), taskCount)
	}
	for i := range result.Tasks {
		if result.Tasks[i].TaskID != retryResult.Tasks[i].TaskID {
			t.Fatalf("retry task[%d] id changed: %s != %s", i, result.Tasks[i].TaskID, retryResult.Tasks[i].TaskID)
		}
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
	if got := tc.proc.totalProcessed(); got != taskCount {
		t.Fatalf("idempotent batch retry processed %d tasks, want %d", got, taskCount)
	}
}

// TestPerformanceDiagnosticsProxyFromFollower covers OBS-001 through the
// production boundary: real follower HTTP forwarding, leader-owned assignment
// and completion streams, real Raft Apply, worker execution, and the
// follower-to-leader read-only diagnostics proxy.
func TestPerformanceDiagnosticsProxyFromFollower(t *testing.T) {
	tc := newTestCluster(t, 3, 20)
	defer tc.shutdown()
	tc.addTenant("observed", 60)
	tc.waitAllocation(20 * time.Second)

	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader")
	}
	follower := (leader + 1) % len(tc.nodes)
	const taskCount = 256
	batch := types.BatchTaskSubmitRequest{Tasks: make([]types.TaskSubmitRequest, taskCount)}
	for i := range batch.Tasks {
		batch.Tasks[i] = types.TaskSubmitRequest{
			TenantID: "observed", Payload: json.RawMessage(fmt.Sprintf(`{"index":%d}`, i)),
			IdempotencyKey: fmt.Sprintf("observed-%d", i),
		}
	}
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Post(
		"http://"+tc.httpAddrs[follower]+"/api/v1/tasks/batch",
		"application/json", bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("submit observed batch through follower: %v", err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		t.Fatalf("observed batch status = %d, want 202; body=%s", response.StatusCode, responseBody)
	}
	tc.waitProcessed(taskCount, 30*time.Second)
	tc.waitFor(func() bool {
		leader := tc.leaderIdx()
		return leader >= 0 && tc.nodes[leader].RaftCluster().FSM().CountUnfinishedPerTenant()["observed"] == 0
	}, 30*time.Second, "observed tasks commit final state")

	var diagnostics metricspkg.PerformanceDiagnostics
	tc.waitFor(func() bool {
		response, err := client.Get("http://" + tc.httpAddrs[follower] + "/api/v1/admin/performance")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		if err := json.NewDecoder(response.Body).Decode(&diagnostics); err != nil {
			return false
		}
		create := diagnostics.Current.Raft[raftpkg.OpCreateTaskBatch]
		claim := diagnostics.Current.Raft[raftpkg.OpClaimBatch]
		complete := diagnostics.Current.Raft[raftpkg.OpCompleteBatch]
		return diagnostics.NodeID == fmt.Sprintf("node-%d", tc.leaderIdx()) &&
			create.Items >= taskCount && claim.Items >= taskCount && complete.Items >= taskCount &&
			diagnostics.Current.Scheduler.PendingScanned >= taskCount &&
			diagnostics.Current.Scheduler.TasksSelected >= taskCount &&
			len(diagnostics.History) > 0
	}, 15*time.Second, "leader performance diagnostics through follower")

	if diagnostics.Current.Raft[raftpkg.OpCreateTaskBatch].Errors != 0 ||
		diagnostics.Current.Raft[raftpkg.OpClaimBatch].Errors != 0 ||
		diagnostics.Current.Raft[raftpkg.OpCompleteBatch].Errors != 0 {
		t.Fatalf("performance diagnostics reported Raft errors: %+v", diagnostics.Current.Raft)
	}

	currentResponse, err := client.Get("http://" + tc.httpAddrs[follower] + "/api/v1/admin/performance?history=0")
	if err != nil {
		t.Fatalf("query current-only performance diagnostics through follower: %v", err)
	}
	var currentOnly metricspkg.PerformanceDiagnostics
	decodeErr := json.NewDecoder(currentResponse.Body).Decode(&currentOnly)
	currentResponse.Body.Close()
	if currentResponse.StatusCode != http.StatusOK || decodeErr != nil {
		t.Fatalf("current-only performance diagnostics status=%d decode=%v", currentResponse.StatusCode, decodeErr)
	}
	if currentOnly.NodeID != diagnostics.NodeID ||
		currentOnly.Current.Raft[raftpkg.OpCompleteBatch].Items < taskCount ||
		len(currentOnly.History) != 0 {
		t.Fatalf("current-only performance diagnostics = %+v", currentOnly)
	}

	tc.waitFor(func() bool {
		response, err := client.Get("http://" + tc.httpAddrs[follower] + "/api/v1/metrics?performance=0")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return false
		}
		var histories []struct {
			Name string `json:"name"`
		}
		if err := json.NewDecoder(response.Body).Decode(&histories); err != nil {
			return false
		}
		foundWorkload := false
		for _, history := range histories {
			if strings.HasPrefix(history.Name, "performance:") {
				return false
			}
			if history.Name == "unfinished:observed" {
				foundWorkload = true
			}
		}
		return foundWorkload
	}, 15*time.Second, "follower workload histories without local performance series")

	dashboardResponse, err := client.Get("http://" + tc.httpAddrs[follower] + "/")
	if err != nil {
		t.Fatalf("GET dashboard through follower: %v", err)
	}
	dashboardBody, err := io.ReadAll(dashboardResponse.Body)
	dashboardResponse.Body.Close()
	if err != nil {
		t.Fatalf("read dashboard through follower: %v", err)
	}
	if dashboardResponse.StatusCode != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200", dashboardResponse.StatusCode)
	}
	for _, fragment := range []string{
		`id="performance-title"`,
		`id="performance-raft-chart"`,
		`id="performance-scheduler-chart"`,
		`href="/api/v1/admin/performance"`,
		`getJSON('/api/v1/metrics?performance=0')`,
		`getJSON('/api/v1/admin/performance')`,
		`.chart-tooltip{`,
		`canvas.addEventListener('pointermove',event=>moveChartHover(canvas,event))`,
		`Number.isFinite(selected.item.limit)`,
	} {
		if !strings.Contains(string(dashboardBody), fragment) {
			t.Errorf("production dashboard is missing performance fragment %q", fragment)
		}
	}
}

// TestWorkStealUsesAgedPendingWork verifies the cross-tenant fallback path.
// The target tenant is deliberately removed from the current allocation
// mirror, so only an already allocated idle worker can finish its aged task.
func TestWorkStealUsesAgedPendingWork(t *testing.T) {
	tc := newTestCluster(t, 2, 10)
	defer tc.shutdown()

	tc.addTenant("steal-worker", 10)
	tc.waitAllocation(10 * time.Second)

	// Stop target workers in the current mirror while preserving one source
	// worker on every follower. The allocator may restore the normal plan later,
	// but the aged task should be claimed before its next reconciliation tick.
	allocs := make(map[string]*types.NodeAllocation)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader found")
	}
	for _, nodeInfo := range tc.fsms().GetActiveNodes() {
		if nodeInfo.ID == fmt.Sprintf("node-%d", leader) {
			continue
		}
		allocs[nodeInfo.ID] = &types.NodeAllocation{
			NodeID:  nodeInfo.ID,
			Tenants: map[string]int{"steal-worker": 1},
		}
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
	// No target allocation exists, so the task remains pending until it crosses
	// the production five-second admission boundary.
	var createdAt time.Time
	tc.waitFor(func() bool {
		record := tc.fsms().GetTask(taskID)
		if record == nil {
			return false
		}
		createdAt = record.CreatedAt
		return !createdAt.IsZero()
	}, 5*time.Second, "created task replicated")
	tc.waitFor(func() bool { return time.Since(createdAt) > 5*time.Second }, 7*time.Second, "task reaches cross-node steal age")

	tc.waitFor(func() bool {
		result := tc.fsms().GetResult(taskID)
		return result != nil && result.Status == types.TaskStatusDone
	}, 10*time.Second, "aged task stolen by idle worker")
	if got := tc.proc.processedByTenant("steal-target"); got != 1 {
		t.Fatalf("steal-target processed count = %d, want 1", got)
	}
}

// TestLeaderAssignmentDrainsAgedBacklogWithoutClaimCompetition preserves
// SCHED-002. Every worker reports only an idle slot; the leader chooses each
// aged task once and commits the concrete node assignments in ClaimBatch.
func TestLeaderAssignmentDrainsAgedBacklogWithoutClaimCompetition(t *testing.T) {
	var rejectedClaims atomic.Int64
	tc := newTestClusterWithLogger(t, 3, 30, newClaimRejectCountingLogger(&rejectedClaims))
	defer tc.shutdown()

	tc.addTenant("steal-source", 90)
	tc.waitAllocation(10 * time.Second)

	const taskCount = 90
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID:   fmt.Sprintf("aged-steal-%d", i),
			TenantID: "unallocated-target",
			Payload:  `"aged-work-steal"`,
		}
	}
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader while creating aged steal backlog")
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks})
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 10*time.Second).Error(); err != nil {
		t.Fatalf("create aged steal backlog: %v", err)
	}
	var createdAt time.Time
	tc.waitFor(func() bool {
		record := tc.fsms().GetTask(tasks[0].TaskID)
		if record == nil {
			return false
		}
		createdAt = record.CreatedAt
		return !createdAt.IsZero()
	}, 5*time.Second, "aged steal backlog replicated")
	tc.waitFor(func() bool { return time.Since(createdAt) > 5*time.Second }, 7*time.Second, "backlog reaches cross-node steal age")

	tc.waitFor(func() bool {
		for _, task := range tasks {
			result := tc.fsms().GetResult(task.TaskID)
			if result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 15*time.Second, "aged steal backlog drained")
	for _, task := range tasks {
		if count := tc.proc.processedTaskCount(task.TaskID); count != 1 {
			t.Errorf("aged steal task %s processed %d times, want once", task.TaskID, count)
		}
	}
	if got := rejectedClaims.Load(); got != 0 {
		t.Fatalf("aged work stealing generated %d rejected claims, want 0", got)
	}
}

// TestFreshRecoveryDoesNotCauseCrossNodeClaimStorm preserves the production
// incident where every node scanned the same global pending set. Workers now
// report idle slots only; the leader selects and commits every concrete task
// assignment, so there is no worker-side claim race.
func TestFreshRecoveryDoesNotCauseCrossNodeClaimStorm(t *testing.T) {
	var rejectedClaims atomic.Int64
	tc := newTestClusterWithLogger(t, 3, 40, newClaimRejectCountingLogger(&rejectedClaims))
	defer tc.shutdown()

	tc.addTenant("recovery-owner", 120)
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader while checking recovery workers")
	}
	tc.waitFor(func() bool {
		for i, nd := range tc.nodes {
			if nd == nil {
				return false
			}
			workers := nd.Pool().GetStatus()["recovery-owner"]
			if i == leader && workers != 0 {
				return false
			}
			if i != leader && workers == 0 {
				return false
			}
		}
		return true
	}, 10*time.Second, "recovery workers only on followers")

	const taskCount = 120
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID:   fmt.Sprintf("fresh-recovery-%d", i),
			TenantID: "recovery-owner",
			Payload:  `"raft-only-pending"`,
		}
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks})
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(cmd, 10*time.Second).Error(); err != nil {
		t.Fatalf("create fresh recovery backlog: %v", err)
	}

	tc.waitFor(func() bool {
		for _, task := range tasks {
			result := tc.fsms().GetResult(task.TaskID)
			if result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 15*time.Second, "fresh recovery backlog drained")
	for _, task := range tasks {
		if count := tc.proc.processedTaskCount(task.TaskID); count != 1 {
			t.Errorf("recovery task %s processed %d times, want once", task.TaskID, count)
		}
	}
	if got := rejectedClaims.Load(); got != 0 {
		t.Fatalf("fresh recovery generated %d rejected claims, want 0", got)
	}
}

// TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery preserves SCHED-003 and
// RESULT-001. Assignment and completion traffic from all follower streams is
// aggregated before Raft Apply, so healthy work cannot time out and sit
// inflight until the 30-second lease repair cycle.
func TestGlobalLeaderBatchingDrainsWithoutLeaseRecovery(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	tc := newTestClusterWithLogger(t, 7, 60, zap.New(core))
	defer tc.shutdown()

	const taskCount = 360
	tc.addTenant("global-batch", taskCount)
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader while creating global batching backlog")
	}
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID: fmt.Sprintf("global-batch-%d", i), TenantID: "global-batch", Payload: `{"batch":true}`,
		}
	}
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks}),
		10*time.Second,
	).Error(); err != nil {
		t.Fatalf("create global batching backlog: %v", err)
	}

	started := time.Now()
	tc.waitFor(func() bool {
		for _, task := range tasks {
			if result := tc.fsms().GetResult(task.TaskID); result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 15*time.Second, "global assignment/result batches drain before lease recovery")
	if elapsed := time.Since(started); elapsed >= 30*time.Second {
		t.Fatalf("backlog drained in %s, crossed the claim lease boundary", elapsed)
	}
	for _, task := range tasks {
		if got := tc.proc.processedTaskCount(task.TaskID); got != 1 {
			t.Fatalf("global batch task %s processed %d times, want once", task.TaskID, got)
		}
	}
	for _, entry := range logs.All() {
		fields := entry.ContextMap()
		errText := fmt.Sprint(fields["error"])
		if entry.Message == "worker client stream invalidated" &&
			(strings.Contains(errText, "assignment timeout") || strings.Contains(errText, "completion timeout")) {
			t.Fatalf("healthy global batch invalidated a worker stream: %s", errText)
		}
		if entry.Message == "allocator: expired task claims returned to pending" {
			t.Fatalf("healthy global batch required claim lease recovery: %+v", fields)
		}
	}
}

// TestNodeCreditsDrainProductionWorkerFanoutWithoutLeaseRecovery preserves
// SCHED-004. The eight-node cluster has seven execution nodes with 700 workers
// each: the same 4,900-slot fanout that previously flooded the leader with one
// request per Worker, invalidated every shared stream after 15 seconds, and
// left committed claims waiting for the 30-second lease scanner.
func TestNodeCreditsDrainProductionWorkerFanoutWithoutLeaseRecovery(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	tc := newTestClusterWithLogger(t, 8, 700, zap.New(core))
	defer tc.shutdown()

	const (
		taskCount      = 4096
		executionSlots = 4900
	)
	tc.addTenant("credit-backpressure", executionSlots)
	tc.waitAllocation(15 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader while creating credit-backpressure backlog")
	}
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID: fmt.Sprintf("credit-backpressure-%d", i), TenantID: "credit-backpressure", Payload: `{"credit":true}`,
		}
	}
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks}),
		15*time.Second,
	).Error(); err != nil {
		t.Fatalf("create credit-backpressure backlog: %v", err)
	}

	started := time.Now()
	tc.waitFor(func() bool {
		for _, task := range tasks {
			if result := tc.fsms().GetResult(task.TaskID); result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 25*time.Second, "4,900 worker slots drain without a stream reconnect storm")
	if elapsed := time.Since(started); elapsed >= 30*time.Second {
		t.Fatalf("backlog drained in %s, crossed the claim lease boundary", elapsed)
	}
	processed := tc.proc.processedTaskCounts()
	for _, task := range tasks {
		if got := processed[task.TaskID]; got != 1 {
			t.Fatalf("credit-backpressure task %s processed %d times, want once", task.TaskID, got)
		}
	}
	assignmentBatches := 0
	completionBatches := 0
	for _, entry := range logs.All() {
		fields := entry.ContextMap()
		errText := fmt.Sprint(fields["error"])
		if entry.Message == "worker client stream invalidated" &&
			(strings.Contains(errText, "assignment timeout") || strings.Contains(errText, "completion timeout")) {
			t.Fatalf("worker fanout invalidated a shared stream: %s", errText)
		}
		if entry.Message == "allocator: expired task claims returned to pending" {
			t.Fatalf("worker fanout required claim lease recovery: %+v", fields)
		}
		if entry.Message == "assignment raft batch committed" {
			assignmentBatches++
			if size, ok := fields["tasks"].(int64); !ok || size < 1 || size > 128 {
				t.Fatalf("assignment Raft batch size = %v, want 1..128", fields["tasks"])
			}
		}
		if entry.Message == "completion raft batch committed" {
			completionBatches++
			if size, ok := fields["tasks"].(int64); !ok || size < 1 || size > 128 {
				t.Fatalf("completion Raft batch size = %v, want 1..128", fields["tasks"])
			}
		}
	}
	if assignmentBatches == 0 || completionBatches == 0 {
		t.Fatalf("observed assignment batches=%d completion batches=%d, want both", assignmentBatches, completionBatches)
	}
}

// TestAllocationScaleDownLetsInflightProcessorsFinish preserves SCHED-005.
// The real allocator/pool boundary may reduce borrowed workers while tasks are
// executing; retirement must stop future assignments without canceling the
// already claimed Processor or forcing a 30-second lease recovery.
func TestAllocationScaleDownLetsInflightProcessorsFinish(t *testing.T) {
	tc := newTestCluster(t, 3, 20)
	defer tc.shutdown()

	const tenantID = "graceful-scale-down"
	started, release := tc.proc.gate(tenantID)
	defer release()
	tc.addTenant(tenantID, 4)
	tc.waitAllocation(10 * time.Second)

	const taskCount = 4
	taskIDs := make([]string, 0, taskCount)
	for i := 0; i < taskCount; i++ {
		taskIDs = append(taskIDs, tc.submitTask(0, tenantID, fmt.Sprintf(`{"task":%d}`, i)))
	}
	seen := make(map[string]struct{}, taskCount)
	deadline := time.After(10 * time.Second)
	for len(seen) < taskCount {
		select {
		case taskID := <-started:
			seen[taskID] = struct{}{}
		case <-deadline:
			t.Fatalf("only %d/%d processors started", len(seen), taskCount)
		}
	}

	leader := tc.leaderIdx()
	for i, nd := range tc.nodes {
		if i != leader {
			nd.Pool().Reconcile(map[string]int{})
		}
	}
	time.Sleep(300 * time.Millisecond)
	if got := tc.proc.canceledCount(); got != 0 {
		t.Fatalf("allocation scale-down canceled %d in-flight processors, want 0", got)
	}
	for _, taskID := range taskIDs {
		if result := tc.fsms().GetResult(taskID); result != nil {
			t.Fatalf("task %s completed before gated processor release: %+v", taskID, result)
		}
	}

	release()
	tc.waitFor(func() bool {
		for _, taskID := range taskIDs {
			result := tc.fsms().GetResult(taskID)
			if result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 10*time.Second, "retiring workers commit in-flight results without lease recovery")
	processed := tc.proc.processedTaskCounts()
	for _, taskID := range taskIDs {
		if got := processed[taskID]; got != 1 {
			t.Fatalf("task %s processed %d times, want once", taskID, got)
		}
	}
}

// TestNewMembersBeyondVoterLimitAreNonvoters preserves PERF-001's steady-state
// join policy. All seven processes run the real FSM/API/worker stack, while
// only three participate in elections and commit acknowledgement.
func TestNewMembersBeyondVoterLimitAreNonvoters(t *testing.T) {
	tc := newTestClusterWithVoterLimit(t, 7, 80, 3, zap.NewNop())
	defer tc.shutdown()

	status, err := tc.nodes[tc.leaderIdx()].RaftCluster().MembershipStatus()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Voters) != 3 || len(status.Nonvoters) != 4 {
		t.Fatalf("membership = voters %v nonvoters %v, want 3/4", status.Voters, status.Nonvoters)
	}
	tc.addTenant("bounded-voters", 480)
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	const taskCount = 600
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID: fmt.Sprintf("bounded-voters-%d", i), TenantID: "bounded-voters", Payload: `{"voters":3}`,
		}
	}
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks}),
		10*time.Second,
	).Error(); err != nil {
		t.Fatalf("create bounded-voter backlog: %v", err)
	}
	tc.waitFor(func() bool {
		for _, task := range tasks {
			if result := tc.fsms().GetResult(task.TaskID); result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 15*time.Second, "bounded-voter cluster drains backlog")
	processed := tc.proc.processedTaskCounts()
	for _, task := range tasks {
		if got := processed[task.TaskID]; got != 1 {
			t.Fatalf("task %s processed %d times, want once", task.TaskID, got)
		}
	}
}

// TestBoundedVotersDrainTwentyThousandHTTPTasks preserves PERF-001's observed
// production shape: four tenants submit 20,000 tasks through a follower's real
// HTTP batch endpoint, then the real assignment/result streams drain them on a
// seven-replica cluster whose consensus quorum is bounded to three voters.
func TestBoundedVotersDrainTwentyThousandHTTPTasks(t *testing.T) {
	tc := newTestClusterWithVoterLimit(t, 7, 80, 3, zap.NewNop())
	defer tc.shutdown()

	tenants := []string{"perf-a", "perf-b", "perf-c", "perf-d"}
	for _, tenantID := range tenants {
		tc.addTenant(tenantID, 120)
	}
	tc.waitAllocation(10 * time.Second)
	leader := tc.leaderIdx()
	if leader < 0 {
		t.Fatal("no leader found")
	}
	follower := (leader + 1) % len(tc.nodes)

	const (
		taskCount = 20000
		batchSize = 1000
	)
	taskIDs := make([]string, 0, taskCount)
	client := &http.Client{Timeout: 15 * time.Second}
	submitStarted := time.Now()
	for batchStart := 0; batchStart < taskCount; batchStart += batchSize {
		request := types.BatchTaskSubmitRequest{Tasks: make([]types.TaskSubmitRequest, batchSize)}
		for i := range request.Tasks {
			index := batchStart + i
			request.Tasks[i] = types.TaskSubmitRequest{
				TenantID:       tenants[index%len(tenants)],
				Payload:        json.RawMessage(fmt.Sprintf(`{"index":%d}`, index)),
				IdempotencyKey: fmt.Sprintf("perf-001-%d", index),
			}
		}
		body, err := json.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Post(
			"http://"+tc.httpAddrs[follower]+"/api/v1/tasks/batch", "application/json", bytes.NewReader(body),
		)
		if err != nil {
			t.Fatalf("batch %d POST through follower: %v", batchStart/batchSize, err)
		}
		if resp.StatusCode != http.StatusAccepted {
			data, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			t.Fatalf("batch %d POST status = %d, want 202; body=%s", batchStart/batchSize, resp.StatusCode, data)
		}
		var result types.BatchTaskResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			_ = resp.Body.Close()
			t.Fatalf("decode batch %d response: %v", batchStart/batchSize, err)
		}
		_ = resp.Body.Close()
		if len(result.Tasks) != batchSize {
			t.Fatalf("batch %d response tasks = %d, want %d", batchStart/batchSize, len(result.Tasks), batchSize)
		}
		for _, task := range result.Tasks {
			taskIDs = append(taskIDs, task.TaskID)
		}
	}
	submitElapsed := time.Since(submitStarted)
	drainStarted := time.Now()

	tc.waitFor(func() bool {
		unfinished := tc.fsms().CountUnfinishedPerTenant()
		for _, tenantID := range tenants {
			if unfinished[tenantID] != 0 {
				return false
			}
		}
		return tc.proc.totalProcessed() >= taskCount
	}, 90*time.Second, "20,000 HTTP tasks drain through bounded voter quorum")
	t.Logf("PERF-001: submitted 20,000 tasks in %s and drained in %s", submitElapsed, time.Since(drainStarted))
	processed := tc.proc.processedTaskCounts()
	for _, taskID := range taskIDs {
		if got := processed[taskID]; got != 1 {
			t.Fatalf("task %s processed %d times, want once", taskID, got)
		}
	}
}

// TestOversizedVoterSetTransfersLeaderAndMigrates preserves PERF-001's upgrade
// path. It starts with the historical all-voter topology and deliberately
// places leadership outside the stable five-node target before reconciliation.
func TestOversizedVoterSetTransfersLeaderAndMigrates(t *testing.T) {
	tc := newAllVoterTestCluster(t, 7, 60)
	defer tc.shutdown()
	tc.addTenant("voter-migration", 300)
	tc.waitAllocation(10 * time.Second)

	before, err := tc.nodes[0].RaftCluster().MembershipStatus()
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Voters) != 7 {
		t.Fatalf("pre-migration voters = %v, want all 7", before.Voters)
	}
	configuration, err := tc.nodes[0].RaftCluster().Configuration()
	if err != nil {
		t.Fatal(err)
	}
	var transferTarget hashicorpraft.Server
	for _, server := range configuration.Servers {
		if server.ID == "node-6" {
			transferTarget = server
			break
		}
	}
	if transferTarget.ID == "" {
		t.Fatal("node-6 missing from Raft configuration")
	}
	if err := tc.nodes[0].RaftCluster().GetRaft().LeadershipTransferToServer(
		transferTarget.ID, transferTarget.Address,
	).Error(); err != nil {
		t.Fatalf("transfer leadership outside target set: %v", err)
	}
	tc.waitLeader(6, 10*time.Second)
	transfer, err := tc.nodes[6].RaftCluster().ReconcileVoters(raftpkg.DefaultMaxVoters)
	if err != nil {
		t.Fatalf("reconcile leadership transfer: %v", err)
	}
	if !transfer.LeadershipTransferred {
		t.Fatalf("reconcile result = %+v, want leadership transfer", transfer)
	}
	tc.waitLeader(0, 10*time.Second)
	leader := 0
	result, err := tc.nodes[leader].RaftCluster().ReconcileVoters(raftpkg.DefaultMaxVoters)
	if err != nil {
		t.Fatalf("reconcile voter demotions: %v", err)
	}
	if !result.Changed {
		t.Fatal("oversized voter set was not changed")
	}
	status, err := tc.nodes[leader].RaftCluster().MembershipStatus()
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Voters) != raftpkg.DefaultMaxVoters || len(status.Nonvoters) != 2 {
		t.Fatalf("post-migration membership = voters %v nonvoters %v", status.Voters, status.Nonvoters)
	}
	if !containsString(status.Voters, fmt.Sprintf("node-%d", leader)) {
		t.Fatalf("leader node-%d is outside voter set %v", leader, status.Voters)
	}

	const taskCount = 300
	tasks := make([]raftpkg.CreateTaskData, taskCount)
	for i := range tasks {
		tasks[i] = raftpkg.CreateTaskData{
			TaskID: fmt.Sprintf("voter-migration-%d", i), TenantID: "voter-migration", Payload: `{"migration":true}`,
		}
	}
	if err := tc.nodes[leader].RaftCluster().GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: tasks}),
		10*time.Second,
	).Error(); err != nil {
		t.Fatalf("create post-migration backlog: %v", err)
	}
	tc.waitFor(func() bool {
		for _, task := range tasks {
			if result := tc.fsms().GetResult(task.TaskID); result == nil || result.Status != types.TaskStatusDone {
				return false
			}
		}
		return true
	}, 15*time.Second, "post-migration backlog drains")
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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
	tc.waitFor(func() bool {
		_, allocated := tc.nodes[newLeader].RaftCluster().FSM().GetAllocation(fmt.Sprintf("node-%d", newLeader))
		return !allocated && tc.nodes[newLeader].Pool().GetStatus()["alice"] == 0
	}, 10*time.Second, "new leader leaves the execution plane")

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
	tc := newTestCluster(t, 2, 50) // one 50-worker follower executes
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
	t.Logf("after bob added: alice=%d bob=%d (50 execution workers)", aliceTotal2, bobTotal)

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
	tc := newTestCluster(t, 2, 5) // one 5-worker follower executes
	defer tc.shutdown()

	tc.addTenant("borrower", 1)
	tc.addTenant("other", 1)
	tc.waitFor(func() bool {
		allocations := tc.fsms().GetAllAllocations()
		tenantEntries := 0
		for _, allocation := range allocations {
			tenantEntries += len(allocation.Tenants)
		}
		return len(allocations) == 1 && tenantEntries >= 2
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
		return borrowed["borrower"] > 0 && borrowed["other"] > 0 && effectiveTotal <= 5
	}, 12*time.Second, "spare workers shared by two backlogged tenants")
}

// TestOversubscription verifies the max-min fairness allocation when the sum
// of all tenant limits exceeds the total cluster capacity.
func TestOversubscription(t *testing.T) {
	// One follower × 50 workers = 50 execution workers; the leader only
	// schedules. Tenant limits sum to 180, so the plan is oversubscribed.
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
	if grandTotal > 50 {
		t.Errorf("grand total %d exceeds execution capacity 50", grandTotal)
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
