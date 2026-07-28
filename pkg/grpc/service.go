// Package grpc provides the gRPC API layer for Sluice.  It implements
// the generated SluiceServer interface (all unary) by delegating to the
// existing queue / FSM / raft / worker-pool components.
//
// Streaming (batch claim, allocation push) is handled separately by the
// internal service (internal.go).
package grpc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// ---------------------------------------------------------------------------
// Service — implements grpcv1.SluiceServer (all unary)
// ---------------------------------------------------------------------------

type Service struct {
	grpcv1.UnimplementedSluiceServer

	nodeID string
	queue  queue.Queue
	fsm    *raftpkg.FSM
	raft   raftpkg.RaftApplier
	pool   *worker.Pool
	logger *zap.Logger

	workAvailable func([]string)

	submitForwardTimeout time.Duration
	tenantMutationMu     sync.RWMutex
	submissionOnce       sync.Once
	submissionJobs       chan submissionJob
	submissionWindow     time.Duration
	submissionMax        int
	submissionObserver   SubmissionPerformanceObserver
	submissionApplySlots chan struct{}
	submissionWaitSlots  chan struct{}
	submissionApplyLimit int

	forwardMu     sync.Mutex
	forwardAddr   string
	forwardConn   *googlegrpc.ClientConn
	forwardClient grpcv1.SluiceClient
}

// SubmissionPerformanceObserver receives process-local ingress batching
// observations. They are diagnostics only and must never feed scheduling or
// replicated state.
type SubmissionPerformanceObserver interface {
	ObserveSubmissionBatch(requests, tasks int, queueWait time.Duration)
	ObserveSubmissionBackpressure(wait time.Duration, rejected bool)
	SetSubmissionQueueDepth(depth int)
	SetSubmissionApplyPressure(inFlight, waiting, limit int)
}

type submissionJob struct {
	create     []raftpkg.CreateTaskData
	response   *grpcv1.SubmitBatchResponse
	receivedAt time.Time
	outcome    chan<- submissionOutcome
}

type submissionOutcome struct {
	response *grpcv1.SubmitBatchResponse
	err      error
}

// SetWorkAvailableFunc installs the control-plane notification invoked only
// after submitted tasks are durably committed by Raft. The callback is wired
// during node construction, before the service starts handling requests.
func (s *Service) SetWorkAvailableFunc(fn func([]string)) {
	s.workAvailable = fn
}

func (s *Service) SetSubmissionPerformanceObserver(observer SubmissionPerformanceObserver) {
	s.submissionObserver = observer
	s.setSubmissionApplyPressure()
}

// SetSubmissionApplyLimit bounds concurrent, unresolved CreateTaskBatch Raft
// Apply futures on this process. Only the current Leader uses the slots;
// followers forward complete requests before reaching this boundary.
func (s *Service) SetSubmissionApplyLimit(limit int) {
	if limit < 1 {
		limit = DefaultSubmissionApplyLimit
	}
	s.submissionApplyLimit = limit
	s.submissionApplySlots = make(chan struct{}, limit)
	s.submissionWaitSlots = make(chan struct{}, limit*submissionWaiterMultiplier)
	s.setSubmissionApplyPressure()
}

const (
	DefaultSubmissionApplyLimit = 16
	MaxSubmissionApplyLimit     = 128
	submissionWaiterMultiplier  = 4
)

func NewService(
	nodeID string,
	q queue.Queue,
	fsm *raftpkg.FSM,
	raft raftpkg.RaftApplier,
	pool *worker.Pool,
	logger *zap.Logger,
) *Service {
	return &Service{
		nodeID: nodeID, queue: q, fsm: fsm,
		raft: raft, pool: pool, logger: logger,
		submitForwardTimeout: 60 * time.Second,
		submissionJobs:       make(chan submissionJob, 1024),
		submissionWindow:     2 * time.Millisecond,
		submissionMax:        maxSubmitBatchTasks,
		submissionApplySlots: make(chan struct{}, DefaultSubmissionApplyLimit),
		submissionWaitSlots:  make(chan struct{}, DefaultSubmissionApplyLimit*submissionWaiterMultiplier),
		submissionApplyLimit: DefaultSubmissionApplyLimit,
	}
}

// ---------------------------------------------------------------------------
// Submit — unary, returns task_id immediately
// ---------------------------------------------------------------------------

func (s *Service) Submit(ctx context.Context, req *grpcv1.SubmitRequest) (*grpcv1.SubmitResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	resp, err := s.SubmitBatch(ctx, &grpcv1.SubmitBatchRequest{Tasks: []*grpcv1.SubmitRequest{req}})
	if err != nil {
		return nil, err
	}
	return resp.Tasks[0], nil
}

const maxSubmitBatchTasks = 1000

// SubmitBatch persists pending tasks through the Leader-wide submission path.
// Partial concurrent requests from every HTTP/gRPC client and every forwarding
// follower can share one bounded Raft log entry. Full requests bypass the
// partial-request dispatcher so concurrent callers retain Raft ingress
// pipelining. Tenant validation happens only on the leader: a follower may
// have a briefly stale FSM snapshot and must forward the complete request
// before validating it.
func (s *Service) SubmitBatch(ctx context.Context, req *grpcv1.SubmitBatchRequest) (*grpcv1.SubmitBatchResponse, error) {
	if req == nil || len(req.Tasks) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one task is required")
	}
	if len(req.Tasks) > maxSubmitBatchTasks {
		return nil, status.Errorf(codes.InvalidArgument, "batch exceeds maximum of %d tasks", maxSubmitBatchTasks)
	}
	if !s.raft.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		forwardCtx, cancel := context.WithTimeout(ctx, s.submitForwardTimeout)
		defer cancel()
		return client.SubmitBatch(forwardCtx, req)
	}

	create := make([]raftpkg.CreateTaskData, len(req.Tasks))
	resp := &grpcv1.SubmitBatchResponse{Tasks: make([]*grpcv1.SubmitResponse, len(req.Tasks))}
	for i, item := range req.Tasks {
		if item == nil || item.TenantId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "tasks[%d].tenant_id is required", i)
		}
		taskID := uuid.New().String()
		if item.IdempotencyKey != "" {
			// Stable IDs turn an unknown follower-forward outcome into a safe
			// retry while the task/result remains in the bounded FSM window.
			taskID = uuid.NewSHA1(uuid.NameSpaceOID, []byte(item.TenantId+"\x00"+item.IdempotencyKey)).String()
		}
		create[i] = raftpkg.CreateTaskData{
			TaskID:      taskID,
			TenantID:    item.TenantId,
			Payload:     string(item.Payload),
			QueueNodeID: "",
		}
		resp.Tasks[i] = &grpcv1.SubmitResponse{TaskId: taskID, TenantId: item.TenantId, Status: types.TaskStatusPending}
	}

	outcome := make(chan submissionOutcome, 1)
	job := submissionJob{
		create: create, response: resp,
		receivedAt: time.Now(), outcome: outcome,
	}
	// A request that already fills the replicated 1000-task limit cannot
	// benefit from coalescing. Apply it directly so concurrent full batches
	// retain Hashicorp Raft's ingress pipelining instead of waiting behind the
	// dispatcher's previous ApplyFuture.
	if len(create) == s.submissionMax {
		s.applySubmissionJobs(ctx, []submissionJob{job})
		result := <-outcome
		return result.response, result.err
	}

	s.submissionOnce.Do(func() { go s.runSubmissionDispatcher() })
	select {
	case s.submissionJobs <- job:
		s.setSubmissionQueueDepth(0)
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	select {
	case result := <-outcome:
		return result.response, result.err
	case <-ctx.Done():
		// The queued job may still commit after the caller disappears. A retry
		// with idempotency keys resolves that intentionally unknown outcome.
		return nil, status.FromContextError(ctx.Err()).Err()
	}
}

func (s *Service) runSubmissionDispatcher() {
	pending := make([]submissionJob, 0, 1)
	for {
		var first submissionJob
		if len(pending) > 0 {
			first = pending[0]
			pending = pending[:0]
		} else {
			first = <-s.submissionJobs
		}
		jobs := []submissionJob{first}
		tasks := len(first.create)
		if tasks < s.submissionMax {
			timer := time.NewTimer(s.submissionWindow)
		collect:
			for tasks < s.submissionMax {
				select {
				case next := <-s.submissionJobs:
					if tasks+len(next.create) > s.submissionMax {
						pending = append(pending, next)
						break collect
					}
					jobs = append(jobs, next)
					tasks += len(next.create)
				case <-timer.C:
					break collect
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		s.setSubmissionQueueDepth(len(pending))
		s.applySubmissionJobs(context.Background(), jobs)
	}
}

func (s *Service) applySubmissionJobs(ctx context.Context, jobs []submissionJob) {
	if err := s.acquireSubmissionApply(ctx); err != nil {
		for _, job := range jobs {
			job.outcome <- submissionOutcome{err: err}
		}
		return
	}
	defer s.releaseSubmissionApply()

	// Keep tenant validation and durable Create in one Leader-local read
	// section. DeleteAllTenants takes the write side so no validated job can
	// commit after the atomic clear and create orphaned work.
	s.tenantMutationMu.RLock()
	defer s.tenantMutationMu.RUnlock()

	tenantExists := make(map[string]bool)
	for _, job := range jobs {
		for _, task := range job.create {
			if _, checked := tenantExists[task.TenantID]; checked {
				continue
			}
			_, tenantExists[task.TenantID] = s.fsm.GetTenant(task.TenantID)
		}
	}
	valid := make([]submissionJob, 0, len(jobs))
	create := make([]raftpkg.CreateTaskData, 0, s.submissionMax)
	oldest := time.Now()
	for _, job := range jobs {
		missing := ""
		for _, task := range job.create {
			if !tenantExists[task.TenantID] {
				missing = task.TenantID
				break
			}
		}
		if missing != "" {
			job.outcome <- submissionOutcome{
				err: status.Error(codes.NotFound, "tenant not found: "+missing),
			}
			continue
		}
		if len(valid) == 0 || job.receivedAt.Before(oldest) {
			oldest = job.receivedAt
		}
		valid = append(valid, job)
		create = append(create, job.create...)
	}
	if len(valid) == 0 {
		return
	}
	if s.submissionObserver != nil {
		s.submissionObserver.ObserveSubmissionBatch(
			len(valid), len(create), time.Since(oldest),
		)
	}

	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: create})
	result := s.raft.Apply(cmd, 5000)
	if err := result.Error(); err != nil {
		s.logger.Error(
			"submit batch raft apply failed",
			zap.Error(err), zap.Int("tasks", len(create)), zap.Int("requests", len(valid)),
		)
		applyErr := status.Error(codes.Internal, "failed to create task batch")
		for _, job := range valid {
			job.outcome <- submissionOutcome{err: applyErr}
		}
		return
	}
	if s.workAvailable != nil {
		tenantIDs := make([]string, len(create))
		for i := range create {
			tenantIDs[i] = create[i].TenantID
		}
		s.workAvailable(tenantIDs)
	}
	if _, ok := result.Response().(*raftpkg.CreateTaskBatchResult); !ok {
		applyErr := status.Error(codes.Internal, "create task batch returned an invalid response")
		for _, job := range valid {
			job.outcome <- submissionOutcome{err: applyErr}
		}
		return
	}
	for _, job := range valid {
		job.outcome <- submissionOutcome{response: job.response}
	}
	// The replicated pending record is the only durable queue. Duplicating this
	// batch into the Leader's local Bolt queue adds one fsync and later one
	// delete scan per task, while providing no locality because Leaders execute
	// no business work. Legacy workers recover pending tasks from the FSM.
}

func (s *Service) setSubmissionQueueDepth(extra int) {
	if s.submissionObserver != nil {
		s.submissionObserver.SetSubmissionQueueDepth(len(s.submissionJobs) + extra)
	}
}

func (s *Service) acquireSubmissionApply(ctx context.Context) error {
	select {
	case s.submissionApplySlots <- struct{}{}:
		s.setSubmissionApplyPressure()
		return nil
	default:
	}

	select {
	case s.submissionWaitSlots <- struct{}{}:
	default:
		if s.submissionObserver != nil {
			s.submissionObserver.ObserveSubmissionBackpressure(0, true)
		}
		return status.Error(
			codes.ResourceExhausted,
			"submission apply backlog is full; retry with idempotency keys",
		)
	}
	s.setSubmissionApplyPressure()
	started := time.Now()
	defer func() {
		<-s.submissionWaitSlots
		s.setSubmissionApplyPressure()
	}()

	select {
	case s.submissionApplySlots <- struct{}{}:
		if s.submissionObserver != nil {
			s.submissionObserver.ObserveSubmissionBackpressure(time.Since(started), false)
		}
		s.setSubmissionApplyPressure()
		return nil
	case <-ctx.Done():
		if s.submissionObserver != nil {
			s.submissionObserver.ObserveSubmissionBackpressure(time.Since(started), false)
		}
		return status.FromContextError(ctx.Err()).Err()
	}
}

func (s *Service) releaseSubmissionApply() {
	<-s.submissionApplySlots
	s.setSubmissionApplyPressure()
}

func (s *Service) setSubmissionApplyPressure() {
	if s.submissionObserver != nil {
		s.submissionObserver.SetSubmissionApplyPressure(
			len(s.submissionApplySlots),
			len(s.submissionWaitSlots),
			s.submissionApplyLimit,
		)
	}
}

// ---------------------------------------------------------------------------
// GetTask — unary status query
// ---------------------------------------------------------------------------

func (s *Service) GetTask(ctx context.Context, req *grpcv1.GetTaskRequest) (*grpcv1.TaskStatus, error) {
	if task := s.fsm.GetTask(req.TaskId); task != nil {
		return &grpcv1.TaskStatus{
			TaskId: task.TaskID, TenantId: task.TenantID, Status: task.Status,
		}, nil
	}
	if result := s.fsm.GetResult(req.TaskId); result != nil {
		return &grpcv1.TaskStatus{
			TaskId: result.TaskID, TenantId: result.TenantID,
			Status: result.Status, Result: result.Result, Error: result.Error,
		}, nil
	}
	return nil, status.Error(codes.NotFound, "task not found: "+req.TaskId)
}

// ---------------------------------------------------------------------------
// WaitTask — unary, blocks until done or timeout
// ---------------------------------------------------------------------------

func (s *Service) WaitTask(ctx context.Context, req *grpcv1.WaitTaskRequest) (*grpcv1.TaskStatus, error) {
	timeout := 30 * time.Second
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			if task := s.fsm.GetTask(req.TaskId); task != nil {
				return &grpcv1.TaskStatus{
					TaskId: task.TaskID, TenantId: task.TenantID, Status: task.Status,
				}, nil
			}
			return nil, status.Error(codes.DeadlineExceeded, "timeout waiting for task")
		case <-ticker.C:
			if result := s.fsm.GetResult(req.TaskId); result != nil {
				return &grpcv1.TaskStatus{
					TaskId: result.TaskID, TenantId: result.TenantID,
					Status: result.Status, Result: result.Result, Error: result.Error,
				}, nil
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Admin RPCs
// ---------------------------------------------------------------------------

func (s *Service) UpsertTenant(ctx context.Context, req *grpcv1.UpsertTenantRequest) (*grpcv1.UpsertTenantResponse, error) {
	if req.MaxWorkers < 1 {
		return nil, status.Error(codes.InvalidArgument, "max_workers must be >= 1")
	}
	if !s.raft.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return client.UpsertTenant(forwardCtx, req)
	}
	s.tenantMutationMu.Lock()
	defer s.tenantMutationMu.Unlock()
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, types.TenantConfig{
		ID: req.TenantId, Name: req.Name, MaxWorkers: int(req.MaxWorkers),
	})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	return &grpcv1.UpsertTenantResponse{Ok: true}, nil
}

func (s *Service) DeleteTenant(ctx context.Context, req *grpcv1.DeleteTenantRequest) (*grpcv1.DeleteTenantResponse, error) {
	if !s.raft.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return client.DeleteTenant(forwardCtx, req)
	}
	s.tenantMutationMu.Lock()
	defer s.tenantMutationMu.Unlock()
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpDeleteTenant, raftpkg.DeleteTenantData{ID: req.TenantId})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	return &grpcv1.DeleteTenantResponse{Ok: true}, nil
}

func (s *Service) DeleteAllTenants(ctx context.Context, req *grpcv1.DeleteAllTenantsRequest) (*grpcv1.DeleteAllTenantsResponse, error) {
	if !s.raft.IsLeader() {
		client, err := s.leaderClient()
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		forwardCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return client.DeleteAllTenants(forwardCtx, req)
	}

	s.tenantMutationMu.Lock()
	defer s.tenantMutationMu.Unlock()
	cmd := raftpkg.MustMarshalCommand(
		raftpkg.OpDeleteAllTenants,
		raftpkg.DeleteAllTenantsData{},
	)
	result := s.raft.Apply(cmd, 5000)
	if err := result.Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	outcome, ok := result.Response().(*raftpkg.DeleteAllTenantsResult)
	if !ok {
		return nil, status.Error(codes.Internal, "delete all tenants returned an invalid response")
	}
	if outcome.Unfinished > 0 {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"%d unfinished tasks must drain before deleting all tenants",
			outcome.Unfinished,
		)
	}
	return &grpcv1.DeleteAllTenantsResponse{Deleted: int32(outcome.Deleted)}, nil
}

// leaderClient returns a cached gRPC client to the current leader. External
// requests arrive through a load-balanced Kubernetes Service, so followers
// must forward writes instead of calling raft.Apply locally.
func (s *Service) leaderClient() (grpcv1.SluiceClient, error) {
	addr, err := leaderAPIAddress(s.raft.LeaderAddr(), s.fsm.GetAllNodes())
	if err != nil {
		return nil, err
	}

	s.forwardMu.Lock()
	defer s.forwardMu.Unlock()
	if s.forwardClient != nil && s.forwardAddr == addr {
		return s.forwardClient, nil
	}
	if s.forwardConn != nil {
		_ = s.forwardConn.Close()
	}
	conn, err := googlegrpc.NewClient(addr, googlegrpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to leader %s: %w", addr, err)
	}
	s.forwardAddr = addr
	s.forwardConn = conn
	s.forwardClient = grpcv1.NewSluiceClient(conn)
	return s.forwardClient, nil
}

func leaderAPIAddress(raftAddr string, nodes map[string]*types.NodeInfo) (string, error) {
	if raftAddr == "" {
		return "", fmt.Errorf("raft leader is not available")
	}
	for _, node := range nodes {
		if node.RaftAddress != raftAddr || node.Address == "" {
			continue
		}
		host, _, err := net.SplitHostPort(node.Address)
		if err == nil && host != "" && host != "0.0.0.0" && host != "::" {
			return node.Address, nil
		}
	}
	host, _, err := net.SplitHostPort(raftAddr)
	if err != nil {
		return "", fmt.Errorf("parse raft leader address %q: %w", raftAddr, err)
	}
	return net.JoinHostPort(host, "9090"), nil
}

func (s *Service) ListTenants(ctx context.Context, req *grpcv1.ListTenantsRequest) (*grpcv1.ListTenantsResponse, error) {
	tenants := s.fsm.GetAllTenants()
	outstanding := s.fsm.CountUnfinishedPerTenant()
	resp := &grpcv1.ListTenantsResponse{}
	for _, t := range tenants {
		resp.Tenants = append(resp.Tenants, &grpcv1.TenantInfo{
			TenantId: t.ID, Name: t.Name,
			MaxWorkers: int32(t.MaxWorkers),
			Inflight:   int32(outstanding[t.ID]),
		})
	}
	return resp, nil
}

func (s *Service) ClusterStatus(ctx context.Context, req *grpcv1.ClusterStatusRequest) (*grpcv1.ClusterStatusResponse, error) {
	nodes := s.fsm.GetAllNodes()
	allocs := s.fsm.GetAllAllocations()
	resp := &grpcv1.ClusterStatusResponse{LeaderAddress: s.raft.LeaderAddr()}
	for _, n := range nodes {
		resp.Nodes = append(resp.Nodes, &grpcv1.NodeInfo{
			NodeId: n.ID, Address: n.Address, RaftAddress: n.RaftAddress,
			Status: n.Status, TotalWorkers: int32(n.TotalWorkers),
		})
	}
	for _, a := range allocs {
		na := &grpcv1.NodeAllocation{NodeId: a.NodeID}
		for tid, cnt := range a.Tenants {
			na.Tenants = append(na.Tenants, &grpcv1.TenantAllocation{
				TenantId: tid, Workers: int32(cnt),
			})
		}
		resp.Allocations = append(resp.Allocations, na)
	}
	return resp, nil
}

// AllocationSnapshot exposes the current allocation mirror for the REST
// adapter. The effective worker counts and borrowed counts are current FSM
// state, not historical data; callers that need history should use metrics.
func (s *Service) AllocationSnapshot() (map[string]*types.NodeAllocation, map[string]*types.TenantConfig) {
	return s.fsm.GetAllAllocations(), s.fsm.GetAllTenants()
}

// TaskPressureSnapshot exposes one coherent read-only FSM task snapshot for
// the HTTP autoscaling signal endpoint.
func (s *Service) TaskPressureSnapshot() types.TaskPressureSnapshot {
	return s.fsm.TaskPressureSnapshot()
}

// NodeSnapshot exposes the replicated role-aware node mirror to the REST UI.
func (s *Service) NodeSnapshot() (map[string]*types.NodeInfo, string) {
	return s.fsm.GetAllNodes(), s.raft.LeaderAddr()
}

func (s *Service) Health(ctx context.Context, req *grpcv1.HealthRequest) (*grpcv1.HealthResponse, error) {
	return &grpcv1.HealthResponse{
		Status: "ok", NodeId: s.nodeID, Leader: s.raft.LeaderAddr(),
	}, nil
}
