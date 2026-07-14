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

	forwardMu     sync.Mutex
	forwardAddr   string
	forwardConn   *googlegrpc.ClientConn
	forwardClient grpcv1.SluiceClient
}

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

// SubmitBatch persists multiple pending tasks in one Raft log entry. Tenant
// validation happens only on the leader: a follower may have a briefly stale
// FSM snapshot and must forward the complete request before validating it.
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
		forwardCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return client.SubmitBatch(forwardCtx, req)
	}

	create := make([]raftpkg.CreateTaskData, len(req.Tasks))
	resp := &grpcv1.SubmitBatchResponse{Tasks: make([]*grpcv1.SubmitResponse, len(req.Tasks))}
	for i, item := range req.Tasks {
		if item == nil || item.TenantId == "" {
			return nil, status.Errorf(codes.InvalidArgument, "tasks[%d].tenant_id is required", i)
		}
		// Validate tenant state on the leader only. Followers can lag the
		// replicated tenant snapshot and must not return a transient 404.
		if _, ok := s.fsm.GetTenant(item.TenantId); !ok {
			return nil, status.Error(codes.NotFound, "tenant not found: "+item.TenantId)
		}
		taskID := uuid.New().String()
		create[i] = raftpkg.CreateTaskData{
			TaskID:              taskID,
			TenantID:            item.TenantId,
			Payload:             string(item.Payload),
			EstimatedDurationMs: item.EstimatedDurationMs,
		}
		resp.Tasks[i] = &grpcv1.SubmitResponse{TaskId: taskID, TenantId: item.TenantId, Status: types.TaskStatusPending}
	}

	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{Tasks: create})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		s.logger.Error("submit batch raft apply failed", zap.Error(err), zap.Int("tasks", len(create)))
		return nil, status.Error(codes.Internal, "failed to create task batch")
	}
	for i, item := range req.Tasks {
		// Also enqueue locally so local workers pick tasks up quickly
		// (best-effort); the batch Raft entry remains the durable source.
		_ = s.queue.Enqueue(item.TenantId, &queue.TaskEnvelope{
			TaskID:              create[i].TaskID,
			TenantID:            item.TenantId,
			Payload:             item.Payload,
			EstimatedDurationMs: item.EstimatedDurationMs,
			CreatedAt:           time.Now().UTC(),
		})
	}
	return resp, nil
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
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpDeleteTenant, raftpkg.DeleteTenantData{ID: req.TenantId})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	return &grpcv1.DeleteTenantResponse{Ok: true}, nil
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

func (s *Service) Health(ctx context.Context, req *grpcv1.HealthRequest) (*grpcv1.HealthResponse, error) {
	return &grpcv1.HealthResponse{
		Status: "ok", NodeId: s.nodeID, Leader: s.raft.LeaderAddr(),
	}, nil
}
