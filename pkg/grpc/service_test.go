package grpc

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/day253/sluice/pkg/queue"
	"github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

type deadlineSluiceService struct {
	grpcv1.UnimplementedSluiceServer
}

type batchSpyQueue struct {
	*queue.MemoryQueue
	enqueueCalls int
}

type recordingSubmissionObserver struct {
	mu       sync.Mutex
	batches  int
	requests int
	tasks    int
	depth    int
}

func (o *recordingSubmissionObserver) ObserveSubmissionBatch(requests, tasks int, _ time.Duration) {
	o.mu.Lock()
	o.batches++
	o.requests += requests
	o.tasks += tasks
	o.mu.Unlock()
}

func (o *recordingSubmissionObserver) SetSubmissionQueueDepth(depth int) {
	o.mu.Lock()
	o.depth = depth
	o.mu.Unlock()
}

func (o *recordingSubmissionObserver) snapshot() (batches, requests, tasks, depth int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.batches, o.requests, o.tasks, o.depth
}

type blockingCreateRaft struct {
	fsm           *raft.FSM
	createStarted chan struct{}
	createRelease chan struct{}
	createOnce    sync.Once
	deleteApplies atomic.Int32
}

func (r *blockingCreateRaft) Apply(cmd []byte, _ int) raft.ApplyResult {
	var envelope raft.Command
	if err := json.Unmarshal(cmd, &envelope); err != nil {
		return internalTestApplyResult{response: err}
	}
	if envelope.Op == raft.OpCreateTaskBatch {
		r.createOnce.Do(func() { close(r.createStarted) })
		<-r.createRelease
	}
	if envelope.Op == raft.OpDeleteAllTenants {
		r.deleteApplies.Add(1)
	}
	response := r.fsm.Apply(&hashicorpraft.Log{
		Data: cmd, Type: hashicorpraft.LogCommand,
	})
	return internalTestApplyResult{response: response}
}

func (r *blockingCreateRaft) IsLeader() bool     { return true }
func (r *blockingCreateRaft) LeaderAddr() string { return "test:7000" }

func newBatchSpyQueue() *batchSpyQueue {
	return &batchSpyQueue{MemoryQueue: queue.NewMemoryQueue()}
}

func (q *batchSpyQueue) Enqueue(tenantID string, task *queue.TaskEnvelope) error {
	q.enqueueCalls++
	return q.MemoryQueue.Enqueue(tenantID, task)
}

func (deadlineSluiceService) SubmitBatch(ctx context.Context, _ *grpcv1.SubmitBatchRequest) (*grpcv1.SubmitBatchResponse, error) {
	<-ctx.Done()
	return nil, status.Error(codes.DeadlineExceeded, ctx.Err().Error())
}

func TestLeaderAPIAddressUsesRegisteredNodeAddress(t *testing.T) {
	nodes := map[string]*types.NodeInfo{
		"node-1": {ID: "node-1", Address: "10.152.183.24:9090", RaftAddress: "10.152.183.24:7000"},
	}
	got, err := leaderAPIAddress("10.152.183.24:7000", nodes)
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.152.183.24:9090" {
		t.Fatalf("leader API address = %q, want %q", got, "10.152.183.24:9090")
	}
}

func TestLeaderAPIAddressFallsBackToRaftHost(t *testing.T) {
	got, err := leaderAPIAddress("10.0.0.8:7000", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got != "10.0.0.8:9090" {
		t.Fatalf("leader API address = %q, want %q", got, "10.0.0.8:9090")
	}
}

func TestSubmitForwardsBeforeFollowerTenantValidation(t *testing.T) {
	leaderFSM := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(leaderFSM, raft.OpUpsertTenant, types.TenantConfig{
		ID: "tenant-a", Name: "Tenant A", MaxWorkers: 2,
	})
	leaderRaft := &internalTestRaft{fsm: leaderFSM}
	leaderRaft.leader.Store(true)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceServer(server, NewService(
		"leader", queue.NewMemoryQueue(), leaderFSM, leaderRaft, nil, zap.NewNop(),
	))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	// The follower intentionally has no tenant yet, but it knows the leader's
	// API address through the replicated node registry.
	followerFSM := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(followerFSM, raft.OpNodeUp, types.NodeInfo{
		ID: "leader", Address: listener.Addr().String(), RaftAddress: "test:7000",
	})
	followerRaft := &internalTestRaft{fsm: followerFSM}
	follower := NewService("follower", queue.NewMemoryQueue(), followerFSM, followerRaft, nil, zap.NewNop())
	t.Cleanup(func() {
		follower.forwardMu.Lock()
		if follower.forwardConn != nil {
			_ = follower.forwardConn.Close()
		}
		follower.forwardMu.Unlock()
	})

	resp, err := follower.Submit(context.Background(), &grpcv1.SubmitRequest{
		TenantId: "tenant-a", Payload: []byte(`{"source":"test"}`),
	})
	if err != nil {
		t.Fatalf("follower submit: %v", err)
	}
	if resp.GetTaskId() == "" {
		t.Fatal("follower submit returned an empty task id")
	}
	if task := leaderFSM.GetTask(resp.GetTaskId()); task == nil || task.TenantID != "tenant-a" {
		t.Fatalf("leader task = %+v, want tenant-a", task)
	}
}

func TestDeleteAllTenantsCannotOvertakeValidatedSubmission(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{
		ID: "tenant-a", MaxWorkers: 2,
	})
	testRaft := &blockingCreateRaft{
		fsm:           fsm,
		createStarted: make(chan struct{}),
		createRelease: make(chan struct{}),
	}
	service := NewService(
		"leader",
		queue.NewMemoryQueue(),
		fsm,
		testRaft,
		nil,
		zap.NewNop(),
	)

	submitted := make(chan error, 1)
	go func() {
		_, err := service.Submit(context.Background(), &grpcv1.SubmitRequest{
			TenantId: "tenant-a", Payload: []byte(`{}`),
		})
		submitted <- err
	}()
	select {
	case <-testRaft.createStarted:
	case <-time.After(time.Second):
		t.Fatal("submission did not reach the blocked Raft Apply")
	}

	cleared := make(chan error, 1)
	go func() {
		_, err := service.DeleteAllTenants(
			context.Background(),
			&grpcv1.DeleteAllTenantsRequest{},
		)
		cleared <- err
	}()
	select {
	case err := <-cleared:
		t.Fatalf("bulk delete overtook the uncommitted submission: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if got := testRaft.deleteApplies.Load(); got != 0 {
		t.Fatalf("bulk delete reached Raft %d times before Create committed", got)
	}

	close(testRaft.createRelease)
	if err := <-submitted; err != nil {
		t.Fatalf("submit: %v", err)
	}
	if err := <-cleared; status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("bulk delete error = %v, want FailedPrecondition", err)
	}
	if got := testRaft.deleteApplies.Load(); got != 1 {
		t.Fatalf("bulk delete Raft applies = %d, want 1", got)
	}
	if got := len(fsm.GetAllTenants()); got != 1 {
		t.Fatalf("bulk delete removed tenant despite unfinished work: %d remain", got)
	}
	if got := fsm.TaskPressureSnapshot().UnfinishedTasks; got != 1 {
		t.Fatalf("unfinished tasks = %d, want 1", got)
	}
}

func TestSubmitBatchFollowerUsesConfiguredForwardTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := googlegrpc.NewServer()
	grpcv1.RegisterSluiceServer(server, deadlineSluiceService{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})

	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpNodeUp, types.NodeInfo{
		ID: "leader", Address: listener.Addr().String(), RaftAddress: "test:7000",
	})
	followerRaft := &internalTestRaft{fsm: fsm}
	follower := NewService("follower", queue.NewMemoryQueue(), fsm, followerRaft, nil, zap.NewNop())
	follower.submitForwardTimeout = 50 * time.Millisecond
	t.Cleanup(func() {
		follower.forwardMu.Lock()
		if follower.forwardConn != nil {
			_ = follower.forwardConn.Close()
		}
		follower.forwardMu.Unlock()
	})

	started := time.Now()
	_, err = follower.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{
		{TenantId: "tenant-a", Payload: []byte(`{}`)},
	}})
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("forward timeout error = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("configured 50ms forward timeout took %s", elapsed)
	}
}

func TestSubmitBatchUsesOneRaftApply(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 10})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())

	resp, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{
		{TenantId: "tenant-a", Payload: []byte(`{"n":1}`)},
		{TenantId: "tenant-a", Payload: []byte(`{"n":2}`)},
		{TenantId: "tenant-a", Payload: []byte(`{"n":3}`)},
	}})
	if err != nil {
		t.Fatalf("submit batch: %v", err)
	}
	if len(resp.GetTasks()) != 3 {
		t.Fatalf("batch response length = %d, want 3", len(resp.GetTasks()))
	}
	if got := testRaft.applyCount.Load(); got != 1 {
		t.Fatalf("Raft Apply calls = %d, want one batch entry", got)
	}
	for _, task := range resp.GetTasks() {
		if task.GetTaskId() == "" || fsm.GetTask(task.GetTaskId()) == nil {
			t.Fatalf("batch task was not persisted: %+v", task)
		}
	}
}

func TestConcurrentSubmitBatchesShareOneLeaderRaftApply(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 1000})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())
	svc.submissionWindow = time.Second
	observer := &recordingSubmissionObserver{}
	svc.SetSubmissionPerformanceObserver(observer)

	const (
		requests        = 4
		tasksPerRequest = 250
	)
	start := make(chan struct{})
	results := make(chan error, requests)
	var wg sync.WaitGroup
	for requestIndex := 0; requestIndex < requests; requestIndex++ {
		requestIndex := requestIndex
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			tasks := make([]*grpcv1.SubmitRequest, tasksPerRequest)
			for taskIndex := range tasks {
				tasks[taskIndex] = &grpcv1.SubmitRequest{
					TenantId:       "tenant-a",
					Payload:        []byte(`{"batch":true}`),
					IdempotencyKey: fmt.Sprintf("request-%d-task-%d", requestIndex, taskIndex),
				}
			}
			response, err := svc.SubmitBatch(
				context.Background(),
				&grpcv1.SubmitBatchRequest{Tasks: tasks},
			)
			if err == nil && len(response.GetTasks()) != tasksPerRequest {
				err = fmt.Errorf("response tasks = %d, want %d", len(response.GetTasks()), tasksPerRequest)
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}

	if got := testRaft.applyCount.Load(); got != 1 {
		t.Fatalf("concurrent request Raft Apply calls = %d, want one global batch", got)
	}
	if got := len(fsm.FindAllPendingTasks()); got != requests*tasksPerRequest {
		t.Fatalf("pending tasks = %d, want %d", got, requests*tasksPerRequest)
	}
	batches, observedRequests, observedTasks, depth := observer.snapshot()
	if batches != 1 || observedRequests != requests ||
		observedTasks != requests*tasksPerRequest || depth != 0 {
		t.Fatalf(
			"submission observation = batches:%d requests:%d tasks:%d depth:%d",
			batches, observedRequests, observedTasks, depth,
		)
	}
}

func TestConcurrentSubmitBatchRejectsUnknownTenantWithoutPoisoningValidJobs(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 10})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())
	svc.submissionWindow = 20 * time.Millisecond

	start := make(chan struct{})
	validResult := make(chan error, 1)
	invalidResult := make(chan error, 1)
	go func() {
		<-start
		_, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{
			Tasks: []*grpcv1.SubmitRequest{{
				TenantId: "tenant-a", IdempotencyKey: "valid",
			}},
		})
		validResult <- err
	}()
	go func() {
		<-start
		_, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{
			Tasks: []*grpcv1.SubmitRequest{{
				TenantId: "missing", IdempotencyKey: "invalid",
			}},
		})
		invalidResult <- err
	}()
	close(start)

	if err := <-validResult; err != nil {
		t.Fatalf("valid job failed because a coalesced request was invalid: %v", err)
	}
	if err := <-invalidResult; status.Code(err) != codes.NotFound {
		t.Fatalf("unknown tenant error = %v, want NotFound", err)
	}
	if got := testRaft.applyCount.Load(); got != 1 {
		t.Fatalf("valid job Raft Apply calls = %d, want 1", got)
	}
	pending := fsm.FindAllPendingTasks()
	if len(pending) != 1 || pending[0].TenantID != "tenant-a" {
		t.Fatalf("pending tasks = %+v, want only valid tenant job", pending)
	}
}

func TestSubmitBatchNotifiesWorkOnlyAfterDurableApply(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 10})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())
	notified := make(chan int, 1)
	svc.SetWorkAvailableFunc(func(tenantIDs []string) {
		if len(tenantIDs) != 2 || tenantIDs[0] != "tenant-a" || tenantIDs[1] != "tenant-a" {
			t.Errorf("notified tenants = %v, want submitted tenant IDs", tenantIDs)
		}
		notified <- len(fsm.FindAllPendingTasks())
	})

	if _, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{
		{TenantId: "tenant-a", Payload: []byte(`{"n":1}`)},
		{TenantId: "tenant-a", Payload: []byte(`{"n":2}`)},
	}}); err != nil {
		t.Fatalf("submit batch: %v", err)
	}
	select {
	case pending := <-notified:
		if pending != 2 {
			t.Fatalf("pending tasks visible at notification = %d, want 2 durable tasks", pending)
		}
	case <-time.After(time.Second):
		t.Fatal("durable submission did not notify the allocator")
	}
}

func TestSubmitBatchDoesNotDuplicateRaftPendingIntoLocalQueue(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 10})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	q := newBatchSpyQueue()
	svc := NewService("leader", q, fsm, testRaft, nil, zap.NewNop())
	tasks := make([]*grpcv1.SubmitRequest, maxSubmitBatchTasks)
	for i := range tasks {
		tasks[i] = &grpcv1.SubmitRequest{TenantId: "tenant-a", Payload: []byte(`{"batch":true}`)}
	}

	if _, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: tasks}); err != nil {
		t.Fatal(err)
	}
	if q.enqueueCalls != 0 {
		t.Fatalf("local queue writes = %d, want 0", q.enqueueCalls)
	}
	if got, err := q.Len("tenant-a"); err != nil || got != 0 {
		t.Fatalf("local queue records = %d, err=%v, want 0", got, err)
	}
}

func TestSubmitBatchIdempotencyKeysReuseTaskIDs(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	applyInternalTestCommand(fsm, raft.OpUpsertTenant, types.TenantConfig{ID: "tenant-a", MaxWorkers: 10})
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	q := queue.NewMemoryQueue()
	svc := NewService("leader", q, fsm, testRaft, nil, zap.NewNop())
	request := &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{
		{TenantId: "tenant-a", Payload: []byte(`{"n":1}`), IdempotencyKey: "retry-1"},
		{TenantId: "tenant-a", Payload: []byte(`{"n":2}`), IdempotencyKey: "retry-2"},
	}}

	first, err := svc.SubmitBatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.SubmitBatch(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for i := range first.Tasks {
		if first.Tasks[i].TaskId != second.Tasks[i].TaskId {
			t.Fatalf("retry task[%d] id changed: %s != %s", i, first.Tasks[i].TaskId, second.Tasks[i].TaskId)
		}
	}
	if got := len(fsm.FindAllPendingTasks()); got != len(request.Tasks) {
		t.Fatalf("pending tasks after retry = %d, want %d unique tasks", got, len(request.Tasks))
	}
	if got, err := q.Len("tenant-a"); err != nil || got != 0 {
		t.Fatalf("local queue records after retry = %d, err=%v, want 0", got, err)
	}
}

func TestSubmitBatchRejectsUnknownTenantAtomically(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())

	_, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{
		{TenantId: "missing", Payload: []byte(`{}`)},
	}})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown tenant error = %v, want NotFound", err)
	}
	if got := testRaft.applyCount.Load(); got != 0 {
		t.Fatalf("unknown tenant caused %d Raft Apply calls", got)
	}
}

func TestSubmitBatchRejectsOversizedRequest(t *testing.T) {
	fsm := raft.NewFSM(zap.NewNop())
	testRaft := &internalTestRaft{fsm: fsm}
	testRaft.leader.Store(true)
	svc := NewService("leader", queue.NewMemoryQueue(), fsm, testRaft, nil, zap.NewNop())
	tasks := make([]*grpcv1.SubmitRequest, maxSubmitBatchTasks+1)
	for i := range tasks {
		tasks[i] = &grpcv1.SubmitRequest{TenantId: "tenant-a"}
	}
	_, err := svc.SubmitBatch(context.Background(), &grpcv1.SubmitBatchRequest{Tasks: tasks})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized batch error = %v, want InvalidArgument", err)
	}
	if got := testRaft.applyCount.Load(); got != 0 {
		t.Fatalf("oversized batch caused %d Raft Apply calls", got)
	}
}
