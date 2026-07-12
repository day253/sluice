// Package grpc provides the gRPC API layer for Sluice.  It implements
// the generated SluiceServer interface (all unary) by delegating to the
// existing queue / FSM / raft / worker-pool components.
//
// Streaming (batch claim, allocation push) is handled separately by the
// internal service (internal.go).
package grpc

import (
	"context"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/queue"
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
	if req.TenantId == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if _, ok := s.fsm.GetTenant(req.TenantId); !ok {
		return nil, status.Error(codes.NotFound, "tenant not found: "+req.TenantId)
	}

	taskID := uuid.New().String()
	payloadStr := string(req.Payload)

	// Write directly to Raft FSM as "pending". Any node's workers can
	// claim it — no local-queue routing problem.
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpCreateTask, raftpkg.CreateTaskData{
		TaskID: taskID, TenantID: req.TenantId, Payload: payloadStr,
	})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		s.logger.Error("submit raft apply failed", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to create task")
	}

	// Also enqueue locally so local workers pick it up quickly (best-effort).
	_ = s.queue.Enqueue(req.TenantId, &queue.TaskEnvelope{
		TaskID: taskID, TenantID: req.TenantId, Payload: req.Payload, CreatedAt: time.Now().UTC(),
	})

	return &grpcv1.SubmitResponse{
		TaskId: taskID, TenantId: req.TenantId, Status: types.TaskStatusPending,
	}, nil
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
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, types.TenantConfig{
		ID: req.TenantId, Name: req.Name, MaxWorkers: int(req.MaxWorkers),
	})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	return &grpcv1.UpsertTenantResponse{Ok: true}, nil
}

func (s *Service) DeleteTenant(ctx context.Context, req *grpcv1.DeleteTenantRequest) (*grpcv1.DeleteTenantResponse, error) {
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpDeleteTenant, raftpkg.DeleteTenantData{ID: req.TenantId})
	if err := s.raft.Apply(cmd, 5000).Error(); err != nil {
		return nil, status.Errorf(codes.Internal, "raft apply: %v", err)
	}
	return &grpcv1.DeleteTenantResponse{Ok: true}, nil
}

func (s *Service) ListTenants(ctx context.Context, req *grpcv1.ListTenantsRequest) (*grpcv1.ListTenantsResponse, error) {
	tenants := s.fsm.GetAllTenants()
	inflight := s.fsm.CountInflightPerTenant()
	pending := s.fsm.CountPendingPerTenant()
	resp := &grpcv1.ListTenantsResponse{}
	for _, t := range tenants {
		resp.Tenants = append(resp.Tenants, &grpcv1.TenantInfo{
			TenantId: t.ID, Name: t.Name,
			MaxWorkers: int32(t.MaxWorkers),
			Inflight:   int32(inflight[t.ID] + pending[t.ID]),
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
