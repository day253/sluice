package grpc

import (
	"io"
	"sync"
	"time"

	"go.uber.org/zap"

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
	subMu   sync.RWMutex
	subs    map[string]chan<- *grpcv1.AllocationPlan // nodeID → push channel
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
	s.logger.Info("internal: ClaimStream opened")
	defer s.logger.Info("internal: ClaimStream closed")

	// Accumulation loop.
	var (
		batch   = make([]raftpkg.ClaimTaskData, 0, claimBatchMaxSize)
		timer   = time.NewTimer(claimBatchWindow)
		timerOn = false
	)

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
					NodeID: req.NodeId, Payload: string(req.Payload),
				},
			}
		}
	}()

	// Process loop: accumulate and flush.
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.logger.Debug("internal: flushing claim batch", zap.Int("size", len(batch)))
		stopTimer()

		cmd := raftpkg.MustMarshalCommand(raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{
			Tasks: batch,
		})
		result := s.raft.Apply(cmd, 5000)
		if err := result.Error(); err != nil {
			s.logger.Error("claim batch raft apply failed", zap.Error(err))
			batch = batch[:0]
			return
		}

		resp, ok := result.Response().(*raftpkg.ClaimBatchResult)
		if !ok {
			batch = batch[:0]
			return
		}

		_ = stream.Send(&grpcv1.ClaimBatch{
			TaskIds:   resp.Claimed,
			FailedIds: resp.Failed,
		})
		batch = batch[:0]
	}

	for {
		select {
		case <-stream.Context().Done():
			flush() // flush remaining before exit
			return stream.Context().Err()
		case err := <-readErr:
			flush()
			return err
		case c, ok := <-claimCh:
			if !ok {
				flush()
				return nil
			}
			batch = append(batch, c.req)
			if len(batch) == 1 {
				stopTimer()
				timer = time.NewTimer(claimBatchWindow)
				timerOn = true
			}
			if len(batch) >= claimBatchMaxSize {
				flush()
			}
		case <-timer.C:
			timerOn = false
			flush()
		}
	}
}

// ---------------------------------------------------------------------------
// ResultStream — bidirectional batch completion
// ---------------------------------------------------------------------------

func (s *InternalService) ResultStream(stream grpcv1.SluiceInternal_ResultStreamServer) error {
	batch := make([]raftpkg.CompleteTaskData, 0, claimBatchMaxSize)
	timer := time.NewTimer(claimBatchWindow)
	timerOn := true

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if timerOn && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerOn = false

		cmd := raftpkg.MustMarshalCommand(raftpkg.OpCompleteBatch, raftpkg.CompleteBatchData{
			Tasks: batch,
		})
		result := s.raft.Apply(cmd, 5000)
		if err := result.Error(); err != nil {
			s.logger.Error("complete batch raft apply failed", zap.Error(err))
		}
		committed := make([]string, len(batch))
		for i, t := range batch {
			committed[i] = t.TaskID
		}
		_ = stream.Send(&grpcv1.ResultBatch{CommittedIds: committed})
		batch = batch[:0]
		timer = time.NewTimer(claimBatchWindow)
		timerOn = true
	}

	for {
		select {
		case <-stream.Context().Done():
			flush()
			return stream.Context().Err()
		case <-timer.C:
			flush()
			timer = time.NewTimer(claimBatchWindow)
			timerOn = true
		default:
			req, err := stream.Recv()
			if err == io.EOF {
				flush()
				return nil
			}
			if err != nil {
				flush()
				return err
			}
			batch = append(batch, raftpkg.CompleteTaskData{
				TaskID: req.TaskId, TenantID: req.TenantId,
				Result: req.Result, Error: req.Error,
			})
			if len(batch) >= claimBatchMaxSize {
				flush()
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
