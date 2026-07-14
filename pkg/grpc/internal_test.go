package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

type internalTestRaft struct {
	fsm        *raftpkg.FSM
	leader     atomic.Bool
	applyCount atomic.Int32
}

func (r *internalTestRaft) Apply(cmd []byte, _ int) raftpkg.ApplyResult {
	r.applyCount.Add(1)
	response := r.fsm.Apply(&hashicorpraft.Log{Data: cmd, Type: hashicorpraft.LogCommand})
	return internalTestApplyResult{response: response}
}

func (r *internalTestRaft) IsLeader() bool     { return r.leader.Load() }
func (r *internalTestRaft) LeaderAddr() string { return "test:7000" }

type internalTestApplyResult struct{ response interface{} }

func (r internalTestApplyResult) Error() error          { return nil }
func (r internalTestApplyResult) Response() interface{} { return r.response }

// legacyInternalService models an older leader during a rolling deployment:
// claim/result streams exist, but AssignmentStream is unimplemented.
type legacyInternalService struct {
	grpcv1.UnimplementedSluiceInternalServer
	delegate *InternalService
}

func (s *legacyInternalService) ClaimStream(stream grpcv1.SluiceInternal_ClaimStreamServer) error {
	return s.delegate.ClaimStream(stream)
}

func (s *legacyInternalService) ResultStream(stream grpcv1.SluiceInternal_ResultStreamServer) error {
	return s.delegate.ResultStream(stream)
}

func applyInternalTestCommand(fsm *raftpkg.FSM, op string, data interface{}) {
	fsm.Apply(&hashicorpraft.Log{
		Data: raftpkg.MustMarshalCommand(op, data), Type: hashicorpraft.LogCommand,
	})
}

func TestClaimBatchPreservesArrivalOrder(t *testing.T) {
	batch := []raftpkg.ClaimTaskData{
		{TaskID: "first"},
		{TaskID: "second"},
		{TaskID: "third"},
	}
	got := []string{batch[0].TaskID, batch[1].TaskID, batch[2].TaskID}
	want := []string{"first", "second", "third"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("claim order = %v, want %v", got, want)
		}
	}
}

func TestCanStealRequiresAgedPendingTask(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raftpkg.OpNodeUp, types.NodeInfo{ID: "worker-node", TotalWorkers: 1})
	applyInternalTestCommand(fsm, raftpkg.OpNodeUp, types.NodeInfo{ID: "other-node", TotalWorkers: 1})
	allocations := map[string]*types.NodeAllocation{
		"worker-node": {NodeID: "worker-node", Tenants: map[string]int{"target": 1}},
		"other-node":  {NodeID: "other-node", Tenants: map[string]int{"target": 1}},
	}
	applyInternalTestCommand(fsm, raftpkg.OpUpdateAllocation, allocations)
	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "old-task", TenantID: "target", Payload: `{}`,
	})
	state := fsm.GetState()
	state.Tasks["old-task"].CreatedAt = time.Now().UTC().Add(-time.Minute)
	persisted, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsm.Restore(io.NopCloser(bytes.NewReader(persisted))); err != nil {
		t.Fatal(err)
	}

	service := NewInternalService("leader", fsm, &internalTestRaft{}, zap.NewNop())
	if !service.canSteal(raftpkg.ClaimTaskData{TaskID: "old-task", TenantID: "target", NodeID: "worker-node", Steal: true}) {
		t.Fatal("aged pending task was not admitted for stealing")
	}
	if service.canSteal(raftpkg.ClaimTaskData{TaskID: "old-task", TenantID: "target", NodeID: "worker-node"}) {
		t.Fatal("claim without steal flag was admitted")
	}
	if service.canSteal(raftpkg.ClaimTaskData{TaskID: "old-task", TenantID: "wrong", NodeID: "worker-node", Steal: true}) {
		t.Fatal("wrong tenant was admitted for stealing")
	}
	// A steal request must not bypass ownership merely because the node also
	// has a normal target-tenant allocation. The explicit steal path still
	// checks task status, tenant, and age.
	if !service.canClaim(raftpkg.ClaimTaskData{TaskID: "old-task", TenantID: "target", NodeID: "worker-node", Steal: true}, allocations) {
		t.Fatal("valid aged steal was rejected")
	}
	if !service.canClaim(raftpkg.ClaimTaskData{TaskID: "old-task", TenantID: "target", NodeID: "worker-node"}, allocations) {
		t.Fatal("normal allocated claim was rejected")
	}

	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "fresh-task", TenantID: "target", QueueNodeID: "worker-node", Payload: `{}`,
	})
	if !service.canSteal(raftpkg.ClaimTaskData{TaskID: "fresh-task", TenantID: "target", NodeID: "worker-node", Steal: true}) {
		t.Fatal("same-node pending task was not admitted for stealing")
	}
	if service.canSteal(raftpkg.ClaimTaskData{TaskID: "fresh-task", TenantID: "target", NodeID: "other-node", Steal: true}) {
		t.Fatal("fresh cross-node task was admitted for stealing")
	}
}

func TestSelectPendingForSlot_PreservesLocalityAndAgeBoundary(t *testing.T) {
	now := time.Now().UTC()
	pending := []*types.TaskRecord{
		{TaskID: "preferred-remote", TenantID: "preferred", QueueNodeID: "node-b", CreatedAt: now.Add(-time.Minute)},
		{TaskID: "local-other", TenantID: "other", QueueNodeID: "node-a", CreatedAt: now.Add(-time.Second)},
		{TaskID: "preferred-local", TenantID: "preferred", QueueNodeID: "node-a", CreatedAt: now},
	}
	selected := map[string]struct{}{}
	first := selectPendingForSlot(pending, selected, "node-a", "preferred", now)
	if first == nil || first.TaskID != "preferred-local" {
		t.Fatalf("first assignment = %+v, want preferred-local", first)
	}
	selected[first.TaskID] = struct{}{}
	second := selectPendingForSlot(pending, selected, "node-a", "preferred", now)
	if second == nil || second.TaskID != "preferred-remote" {
		t.Fatalf("second assignment = %+v, want preferred-remote", second)
	}

	freshRemote := []*types.TaskRecord{{
		TaskID: "fresh-remote", TenantID: "other", QueueNodeID: "node-b", CreatedAt: now.Add(-time.Second),
	}}
	if got := selectPendingForSlot(freshRemote, map[string]struct{}{}, "node-a", "preferred", now); got != nil {
		t.Fatalf("fresh cross-node fallback = %+v, want nil", got)
	}
	freshRemote[0].CreatedAt = now.Add(-workStealThreshold - time.Second)
	if got := selectPendingForSlot(freshRemote, map[string]struct{}{}, "node-a", "preferred", now); got == nil || got.TaskID != "fresh-remote" {
		t.Fatalf("aged cross-node fallback = %+v, want fresh-remote", got)
	}
}

func TestAssignmentStreamBatchesDistinctLeaderCommittedTasks(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"worker-a": {NodeID: "worker-a", Tenants: map[string]int{"tenant-a": 1}},
		"worker-b": {NodeID: "worker-b", Tenants: map[string]int{"tenant-a": 1}},
		// Even a stale/malformed mirror entry must never let the leader execute.
		"leader": {NodeID: "leader", Tenants: map[string]int{"tenant-a": 1}},
	})
	for _, taskID := range []string{"assigned-1", "assigned-2"} {
		applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
			TaskID: taskID, TenantID: "tenant-a", Payload: `{"source":"assignment"}`,
		})
	}
	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	service := NewInternalService("leader", fsm, raft, zap.NewNop())
	service.assignmentWindow = 50 * time.Millisecond
	grpcv1.RegisterSluiceInternalServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	clients := map[string]*ClaimClient{
		"worker-a": NewClaimClient("worker-a", zap.NewNop()),
		"worker-b": NewClaimClient("worker-b", zap.NewNop()),
	}
	for _, client := range clients {
		client.SetLeader(listener.Addr().String())
		t.Cleanup(client.Close)
	}
	start := make(chan struct{})
	type assignedResult struct {
		node string
		task *types.TaskRecord
	}
	assigned := make(chan assignedResult, len(clients))
	errs := make(chan error, len(clients))
	var wg sync.WaitGroup
	for nodeID, client := range clients {
		wg.Add(1)
		go func(nodeID string, client *ClaimClient) {
			defer wg.Done()
			<-start
			task, supported, err := client.Assign("tenant-a")
			if err != nil {
				errs <- err
				return
			}
			if !supported || task == nil {
				errs <- fmt.Errorf("assignment supported=%v task=%+v", supported, task)
				return
			}
			assigned <- assignedResult{node: nodeID, task: task}
		}(nodeID, client)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	close(assigned)
	seen := map[string]bool{}
	for result := range assigned {
		seen[result.task.TaskID] = true
		record := fsm.GetTask(result.task.TaskID)
		if record == nil || record.Status != types.TaskStatusInflight || record.NodeID != result.node {
			t.Fatalf("committed assignment = %+v, want inflight on %s", record, result.node)
		}
	}
	if len(seen) != 2 || !seen["assigned-1"] || !seen["assigned-2"] {
		t.Fatalf("assigned tasks = %v, want two distinct tasks", seen)
	}
	if got := raft.applyCount.Load(); got != 1 {
		t.Fatalf("Raft assignment applies = %d, want one batch", got)
	}

	leaderClient := NewClaimClient("leader", zap.NewNop())
	leaderClient.SetLeader(listener.Addr().String())
	t.Cleanup(leaderClient.Close)
	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "leader-must-not-run", TenantID: "tenant-a", Payload: `{}`,
	})
	if task, supported, err := leaderClient.Assign("tenant-a"); err != nil || !supported || task != nil {
		t.Fatalf("leader assignment = task=%+v supported=%v err=%v, want empty supported response", task, supported, err)
	}
	if task := fsm.GetTask("leader-must-not-run"); task == nil || task.Status != types.TaskStatusPending {
		t.Fatalf("leader-only task state = %+v, want pending", task)
	}
}

func TestResultStreamBatchesCompletionsAcrossNodeStreams(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	for _, task := range []raftpkg.CreateTaskData{
		{TaskID: "done-a", TenantID: "tenant-a", Payload: `{}`},
		{TaskID: "done-b", TenantID: "tenant-a", Payload: `{}`},
	} {
		applyInternalTestCommand(fsm, raftpkg.OpCreateTask, task)
	}
	applyInternalTestCommand(fsm, raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{Tasks: []raftpkg.ClaimTaskData{
		{TaskID: "done-a", TenantID: "tenant-a", NodeID: "worker-a", Payload: `{}`},
		{TaskID: "done-b", TenantID: "tenant-a", NodeID: "worker-b", Payload: `{}`},
	}})

	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	service := NewInternalService("leader", fsm, raft, zap.NewNop())
	service.completionWindow = 50 * time.Millisecond
	grpcv1.RegisterSluiceInternalServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	clients := map[string]*ClaimClient{
		"done-a": NewClaimClient("worker-a", zap.NewNop()),
		"done-b": NewClaimClient("worker-b", zap.NewNop()),
	}
	for _, client := range clients {
		client.SetLeader(listener.Addr().String())
		t.Cleanup(client.Close)
	}
	start := make(chan struct{})
	errs := make(chan error, len(clients))
	var wg sync.WaitGroup
	for taskID, client := range clients {
		wg.Add(1)
		go func(taskID string, client *ClaimClient) {
			defer wg.Done()
			<-start
			if err := client.Complete(taskID, "tenant-a", "ok", "", false); err != nil {
				errs <- err
			}
		}(taskID, client)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := raft.applyCount.Load(); got != 1 {
		t.Fatalf("Raft completion applies = %d, want one cross-stream batch", got)
	}
	for taskID := range clients {
		result := fsm.GetResult(taskID)
		if result == nil || result.Status != types.TaskStatusDone {
			t.Fatalf("completion %s = %+v, want done", taskID, result)
		}
	}
}

func TestDispatchCompletionsDoesNotAcknowledgeCanceledJob(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)
	service := NewInternalService("leader", fsm, raft, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcomes := make(chan completionOutcome, 1)
	service.dispatchCompletions([]completionJob{{
		ctx: ctx,
		task: raftpkg.CompleteTaskData{
			TaskID: "canceled", TenantID: "tenant-a", Status: types.TaskStatusDone,
		},
		outcome: outcomes,
	}})
	if got := raft.applyCount.Load(); got != 0 {
		t.Fatalf("canceled completion caused %d Raft applies, want 0", got)
	}
	select {
	case outcome := <-outcomes:
		if status.Code(outcome.err) != codes.Canceled {
			t.Fatalf("canceled completion outcome = %v, want Canceled", outcome.err)
		}
	default:
		// The dispatcher may drop the error because the originating stream is
		// already canceled; it must never emit a successful acknowledgement.
	}
}

func waitForWorkerClientDisconnected(t *testing.T, client *ClaimClient) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		client.mu.Lock()
		disconnected := client.claimStream == nil && client.resultStream == nil
		client.mu.Unlock()
		if disconnected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("worker client streams were not invalidated")
}

func TestClaimClientFallsBackOnlyForLegacyLeader(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"worker-node": {NodeID: "worker-node", Tenants: map[string]int{"tenant-a": 1}},
	})
	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "legacy-task", TenantID: "tenant-a", Payload: `{}`,
	})
	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceInternalServer(server, &legacyInternalService{
		delegate: NewInternalService("leader", fsm, raft, zap.NewNop()),
	})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client := NewClaimClient("worker-node", zap.NewNop())
	client.SetLeader(listener.Addr().String())
	t.Cleanup(client.Close)
	if task, supported, err := client.Assign("tenant-a"); err != nil || supported || task != nil {
		t.Fatalf("legacy assignment = task=%+v supported=%v err=%v, want unsupported without error", task, supported, err)
	}
	claimed, err := client.Claim("legacy-task", "tenant-a", `{}`)
	if err != nil || !claimed {
		t.Fatalf("legacy ClaimStream after assignment fallback: claimed=%v err=%v", claimed, err)
	}
}

func TestWorkerStreamsCommitResultsAndReconnectAfterLeadershipLoss(t *testing.T) {
	fsm := raftpkg.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raftpkg.OpUpdateAllocation, map[string]*types.NodeAllocation{
		"worker-node": {NodeID: "worker-node", Tenants: map[string]int{"tenant-a": 1}},
	})
	raft := &internalTestRaft{fsm: fsm}
	raft.leader.Store(true)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceInternalServer(server, NewInternalService("leader", fsm, raft, zap.NewNop()))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	client := NewClaimClient("worker-node", zap.NewNop())
	client.SetLeader(listener.Addr().String())
	t.Cleanup(client.Close)

	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "task-1", TenantID: "tenant-a", Payload: `{}`,
	})
	claimed, err := client.Claim("task-1", "tenant-a", `{}`)
	if err != nil || !claimed {
		t.Fatalf("claim task-1: claimed=%v err=%v", claimed, err)
	}
	if err := client.Complete("task-1", "tenant-a", "ok", "", false); err != nil {
		t.Fatalf("complete task-1: %v", err)
	}
	if result := fsm.GetResult("task-1"); result == nil || result.Status != types.TaskStatusDone {
		t.Fatalf("task-1 result = %+v", result)
	}

	// The old leader rejects the next batch. The receive loop must invalidate
	// both streams so the periodic same-address SetLeader call can repair them.
	raft.leader.Store(false)
	applyInternalTestCommand(fsm, raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: "task-2", TenantID: "tenant-a", Payload: `{}`,
	})
	if _, err := client.Claim("task-2", "tenant-a", `{}`); err == nil {
		t.Fatal("claim unexpectedly succeeded after leadership loss")
	}
	waitForWorkerClientDisconnected(t, client)

	raft.leader.Store(true)
	client.SetLeader(listener.Addr().String())
	claimed, err = client.Claim("task-2", "tenant-a", `{}`)
	if err != nil || !claimed {
		t.Fatalf("claim task-2 after reconnect: claimed=%v err=%v", claimed, err)
	}
	if err := client.Complete("task-2", "tenant-a", "", "boom", true); err != nil {
		t.Fatalf("complete failed task-2: %v", err)
	}
	if result := fsm.GetResult("task-2"); result == nil || result.Status != types.TaskStatusFailed || result.Error != "boom" {
		t.Fatalf("task-2 result = %+v", result)
	}

	// A stale queue copy arriving after completion must be acknowledged as a
	// rejected claim, never re-created as unfinished work.
	claimed, err = client.Claim("task-2", "tenant-a", `{}`)
	if err != nil {
		t.Fatalf("duplicate claim returned transport error: %v", err)
	}
	if claimed {
		t.Fatal("completed task was claimed again")
	}
	if task := fsm.GetTask("task-2"); task != nil {
		t.Fatalf("completed task was resurrected: %+v", task)
	}
}
