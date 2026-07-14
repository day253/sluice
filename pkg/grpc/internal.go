package grpc

import (
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	raftpkg "github.com/day253/sluice/pkg/raft"
)

// ---------------------------------------------------------------------------
// InternalService — node-to-node streaming for high throughput
// ---------------------------------------------------------------------------

// InternalService implements grpcv1.SluiceInternalServer.  It is only
// exposed on the Raft leader.  Worker nodes connect to the leader to
// batch-claim tasks and receive allocation push updates.
type InternalService struct {
	grpcv1.UnimplementedSluiceInternalServer

	nodeID string
	raft   raftpkg.RaftApplier
	fsm    *raftpkg.FSM
	logger *zap.Logger

	// allocation subscribers
	subMu sync.RWMutex
	subs  map[string]chan<- *grpcv1.AllocationPlan // nodeID → push channel
}

// NewInternalService creates the internal gRPC service.
func NewInternalService(
	nodeID string,
	fsm *raftpkg.FSM,
	raft raftpkg.RaftApplier,
	logger *zap.Logger,
) *InternalService {
	return &InternalService{
		nodeID: nodeID,
		raft:   raft,
		fsm:    fsm,
		logger: logger,
		subs:   make(map[string]chan<- *grpcv1.AllocationPlan),
	}
}

// ---------------------------------------------------------------------------
// ClaimStream — bidirectional batch claim
// ---------------------------------------------------------------------------

const (
	claimBatchWindow  = 5 * time.Millisecond // accumulate window
	claimBatchMaxSize = 128                  // max claims per Raft entry
)

func (s *InternalService) ClaimStream(stream grpcv1.SluiceInternal_ClaimStreamServer) error {
	if !s.raft.IsLeader() {
		return status.Error(codes.FailedPrecondition, "claim stream is only available on the leader")
	}
	s.logger.Info("internal: ClaimStream opened")
	defer s.logger.Info("internal: ClaimStream closed")

	// Accumulation loop.
	var (
		batch   = make([]raftpkg.ClaimTaskData, 0, claimBatchMaxSize)
		timer   = time.NewTimer(claimBatchWindow)
		timerOn = false
	)
	if !timer.Stop() {
		<-timer.C
	}

	stopTimer := func() {
		if timerOn && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerOn = false
	}

	// Read goroutine sends claims to a channel.
	type claimWithIndex struct {
		req raftpkg.ClaimTaskData
		idx int // position in this batch
	}
	claimCh := make(chan claimWithIndex, 256)
	readErr := make(chan error, 1)

	go func() {
		defer close(claimCh)
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				readErr <- err
				return
			}
			claimCh <- claimWithIndex{
				req: raftpkg.ClaimTaskData{
					TaskID: req.TaskId, TenantID: req.TenantId,
					NodeID: req.NodeId, Payload: string(req.Payload), Steal: req.Steal,
				},
			}
		}
	}()

	// Process loop: accumulate and flush.
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		if !s.raft.IsLeader() {
			return status.Error(codes.FailedPrecondition, "leadership changed")
		}

		// Filter: only grant claims for nodes that are allocated.
		alloc := s.fsm.GetAllAllocations()
		// Preserve the worker arrival order. Scheduling is based on actual
		// pending age and observed completion, not client-provided estimates.
		granted := make([]raftpkg.ClaimTaskData, 0, len(batch))
		var failedIDs []string

		for _, t := range batch {
			na, ok := alloc[t.NodeID]
			if !ok || na.Tenants[t.TenantID] <= 0 {
				if !s.canSteal(t) {
					failedIDs = append(failedIDs, t.TaskID)
					continue
				}
			}
			granted = append(granted, t)
		}

		claimedIDs := make([]string, 0, len(granted))
		if len(granted) > 0 {
			cmd := raftpkg.MustMarshalCommand(raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{
				Tasks: granted,
			})
			result := s.raft.Apply(cmd, 5000)
			if err := result.Error(); err != nil {
				s.logger.Error("claim batch raft apply failed", zap.Error(err))
				return status.Errorf(codes.Unavailable, "claim batch commit failed: %v", err)
			} else if resp, ok := result.Response().(*raftpkg.ClaimBatchResult); ok {
				claimedIDs = append(claimedIDs, resp.Claimed...)
				failedIDs = append(failedIDs, resp.Failed...)
			} else {
				return status.Error(codes.Internal, "claim batch returned an invalid response")
			}
		}

		if err := stream.Send(&grpcv1.ClaimBatch{
			TaskIds:   claimedIDs,
			FailedIds: failedIDs,
		}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-stream.Context().Done():
			_ = flush() // best effort; the client is already gone
			return stream.Context().Err()
		case err := <-readErr:
			_ = flush()
			return err
		case c, ok := <-claimCh:
			if !ok {
				return flush()
			}
			batch = append(batch, c.req)
			if len(batch) == 1 {
				stopTimer()
				timer = time.NewTimer(claimBatchWindow)
				timerOn = true
			}
			if len(batch) >= claimBatchMaxSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			timerOn = false
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// workStealThreshold is intentionally shared with the worker-side age check.
// The leader remains authoritative so a stale or malicious client cannot
// bypass tenant allocation by setting the steal bit on a fresh task.
const workStealThreshold = 5 * time.Second

func (s *InternalService) canSteal(task raftpkg.ClaimTaskData) bool {
	if !task.Steal {
		return false
	}
	record := s.fsm.GetTask(task.TaskID)
	if record == nil || record.Status != "pending" || record.TenantID != task.TenantID {
		return false
	}
	return !record.CreatedAt.IsZero() && record.CreatedAt.Before(time.Now().UTC().Add(-workStealThreshold))
}

// ---------------------------------------------------------------------------
// ResultStream — bidirectional batch completion
// ---------------------------------------------------------------------------

func (s *InternalService) ResultStream(stream grpcv1.SluiceInternal_ResultStreamServer) error {
	if !s.raft.IsLeader() {
		return status.Error(codes.FailedPrecondition, "result stream is only available on the leader")
	}
	s.logger.Info("internal: ResultStream opened")
	defer s.logger.Info("internal: ResultStream closed")

	batch := make([]raftpkg.CompleteTaskData, 0, claimBatchMaxSize)
	timer := time.NewTimer(claimBatchWindow)
	if !timer.Stop() {
		<-timer.C
	}
	timerOn := false

	stopTimer := func() {
		if timerOn && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerOn = false
	}

	resultCh := make(chan raftpkg.CompleteTaskData, 256)
	readErr := make(chan error, 1)
	go func() {
		defer close(resultCh)
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				readErr <- err
				return
			}
			resultCh <- raftpkg.CompleteTaskData{
				TaskID: req.TaskId, TenantID: req.TenantId,
				Status: req.Status, Result: req.Result, Error: req.Error,
			}
		}
	}()

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		if !s.raft.IsLeader() {
			return status.Error(codes.FailedPrecondition, "leadership changed")
		}

		cmd := raftpkg.MustMarshalCommand(raftpkg.OpCompleteBatch, raftpkg.CompleteBatchData{
			Tasks: batch,
		})
		result := s.raft.Apply(cmd, 5000)
		if err := result.Error(); err != nil {
			s.logger.Error("complete batch raft apply failed", zap.Error(err))
			return status.Errorf(codes.Unavailable, "complete batch commit failed: %v", err)
		}
		committed := make([]string, len(batch))
		for i, t := range batch {
			committed[i] = t.TaskID
		}
		if err := stream.Send(&grpcv1.ResultBatch{CommittedIds: committed}); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}

	for {
		select {
		case <-stream.Context().Done():
			_ = flush()
			return stream.Context().Err()
		case err := <-readErr:
			_ = flush()
			return err
		case <-timer.C:
			timerOn = false
			if err := flush(); err != nil {
				return err
			}
		case result, ok := <-resultCh:
			if !ok {
				return flush()
			}
			batch = append(batch, result)
			if len(batch) == 1 {
				stopTimer()
				timer = time.NewTimer(claimBatchWindow)
				timerOn = true
			}
			if len(batch) >= claimBatchMaxSize {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// AllocationPush — server-streaming allocation updates
// ---------------------------------------------------------------------------

func (s *InternalService) AllocationPush(
	req *grpcv1.AllocationSubscribe,
	stream grpcv1.SluiceInternal_AllocationPushServer,
) error {
	ch := make(chan *grpcv1.AllocationPlan, 8)

	s.subMu.Lock()
	s.subs[req.NodeId] = ch
	s.subMu.Unlock()

	defer func() {
		s.subMu.Lock()
		delete(s.subs, req.NodeId)
		s.subMu.Unlock()
		close(ch)
	}()

	// Send current allocation immediately.
	if alloc, ok := s.fsm.GetAllocation(req.NodeId); ok {
		plan := &grpcv1.AllocationPlan{NodeId: req.NodeId}
		for tid, cnt := range alloc.Tenants {
			plan.Tenants = append(plan.Tenants, &grpcv1.TenantWorkerCount{
				TenantId: tid, Workers: int32(cnt),
			})
		}
		_ = stream.Send(plan)
	}

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case plan, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(plan); err != nil {
				return err
			}
		}
	}
}

// PushAllocation broadcasts an allocation change to all subscribers.
// Called by the allocator after committing a new plan.
func (s *InternalService) PushAllocation(plan *grpcv1.AllocationPlan) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()
	ch, ok := s.subs[plan.NodeId]
	if !ok {
		return
	}
	select {
	case ch <- plan:
	default:
		// subscriber is slow; drop (next reconcile will catch up)
	}
}
