package grpc

import (
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
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
	queue  queue.Queue

	// claimMu makes the leader the single task-assignment authority across all
	// node streams. Selection and the corresponding Raft ClaimBatch commit are
	// one serialized critical section.
	claimMu sync.Mutex

	// allocation subscribers
	subMu sync.RWMutex
	subs  map[string]chan<- *grpcv1.AllocationPlan // nodeID → push channel
}

// SetQueue lets the leader remove obsolete local queue hints after a durable
// assignment. Raft pending state remains the source of truth.
func (s *InternalService) SetQueue(q queue.Queue) { s.queue = q }

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
		s.claimMu.Lock()
		defer s.claimMu.Unlock()

		// Filter: only grant claims for nodes that are allocated.
		alloc := s.fsm.GetAllAllocations()
		// Preserve the worker arrival order. Scheduling is based on actual
		// pending age and observed completion, not client-provided estimates.
		granted := make([]raftpkg.ClaimTaskData, 0, len(batch))
		var failedIDs []string

		for _, t := range batch {
			if !s.canClaim(t, alloc) {
				failedIDs = append(failedIDs, t.TaskID)
				continue
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

// ---------------------------------------------------------------------------
// AssignmentStream — leader-owned pull scheduling
// ---------------------------------------------------------------------------

// AssignmentStream batches idle-slot requests from one worker node. All
// streams share claimMu, so the leader selects each pending task once and
// commits the concrete node assignment before returning the payload.
func (s *InternalService) AssignmentStream(stream grpcv1.SluiceInternal_AssignmentStreamServer) error {
	if !s.raft.IsLeader() {
		return status.Error(codes.FailedPrecondition, "assignment stream is only available on the leader")
	}
	s.logger.Info("internal: AssignmentStream opened")
	defer s.logger.Info("internal: AssignmentStream closed")

	requestCh := make(chan *grpcv1.AssignmentRequest, 256)
	readErr := make(chan error, 1)
	go func() {
		defer close(requestCh)
		for {
			request, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				readErr <- err
				return
			}
			requestCh <- request
		}
	}()

	batch := make([]*grpcv1.AssignmentRequest, 0, claimBatchMaxSize)
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

	type selectedAssignment struct {
		request *grpcv1.AssignmentRequest
		task    *types.TaskRecord
	}
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		stopTimer()
		if !s.raft.IsLeader() {
			return status.Error(codes.FailedPrecondition, "leadership changed")
		}

		s.claimMu.Lock()
		defer s.claimMu.Unlock()

		allocations := s.fsm.GetAllAllocations()
		pending := s.fsm.FindAllPendingTasks()
		selectedIDs := make(map[string]struct{}, len(batch))
		selected := make([]selectedAssignment, 0, len(batch))
		emptyRequestIDs := make([]string, 0)
		now := time.Now().UTC()
		for _, request := range batch {
			if request.RequestId == "" || request.NodeId == "" || request.PreferredTenantId == "" {
				emptyRequestIDs = append(emptyRequestIDs, request.RequestId)
				continue
			}
			if request.NodeId == s.nodeID {
				emptyRequestIDs = append(emptyRequestIDs, request.RequestId)
				continue
			}
			allocation, ok := allocations[request.NodeId]
			if !ok || allocation.Tenants[request.PreferredTenantId] <= 0 {
				emptyRequestIDs = append(emptyRequestIDs, request.RequestId)
				continue
			}
			task := selectPendingForSlot(pending, selectedIDs, request.NodeId, request.PreferredTenantId, now)
			if task == nil {
				emptyRequestIDs = append(emptyRequestIDs, request.RequestId)
				continue
			}
			selectedIDs[task.TaskID] = struct{}{}
			selected = append(selected, selectedAssignment{request: request, task: task})
		}

		claimed := make(map[string]struct{}, len(selected))
		if len(selected) > 0 {
			claims := make([]raftpkg.ClaimTaskData, 0, len(selected))
			for _, assignment := range selected {
				claims = append(claims, raftpkg.ClaimTaskData{
					TaskID: assignment.task.TaskID, TenantID: assignment.task.TenantID,
					NodeID: assignment.request.NodeId, Payload: assignment.task.Payload,
				})
			}
			result := s.raft.Apply(raftpkg.MustMarshalCommand(raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{Tasks: claims}), 5000)
			if err := result.Error(); err != nil {
				return status.Errorf(codes.Unavailable, "assignment batch commit failed: %v", err)
			}
			response, ok := result.Response().(*raftpkg.ClaimBatchResult)
			if !ok {
				return status.Error(codes.Internal, "assignment batch returned an invalid response")
			}
			for _, taskID := range response.Claimed {
				claimed[taskID] = struct{}{}
			}
		}

		response := &grpcv1.AssignmentBatch{EmptyRequestIds: emptyRequestIDs}
		for _, assignment := range selected {
			if _, ok := claimed[assignment.task.TaskID]; !ok {
				response.EmptyRequestIds = append(response.EmptyRequestIds, assignment.request.RequestId)
				continue
			}
			response.Tasks = append(response.Tasks, &grpcv1.AssignedTask{
				RequestId: assignment.request.RequestId,
				TaskId:    assignment.task.TaskID, TenantId: assignment.task.TenantID,
				Payload: []byte(assignment.task.Payload), QueueNodeId: assignment.task.QueueNodeID,
			})
			if s.queue != nil && assignment.task.QueueNodeID == s.nodeID {
				if err := s.queue.Remove(assignment.task.TenantID, assignment.task.TaskID); err != nil {
					s.logger.Warn("assignment: remove local queue hint failed",
						zap.String("task_id", assignment.task.TaskID), zap.Error(err))
				}
			}
		}
		if err := stream.Send(response); err != nil {
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
		case request, ok := <-requestCh:
			if !ok {
				return flush()
			}
			batch = append(batch, request)
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

// selectPendingForSlot applies the leader's scheduling policy to one idle
// execution slot. The global pending slice is already FIFO, while priority
// classes preserve tenant allocation and node-local queue affinity.
func selectPendingForSlot(
	pending []*types.TaskRecord,
	selected map[string]struct{},
	nodeID, preferredTenantID string,
	now time.Time,
) *types.TaskRecord {
	bestClass := 4
	var best *types.TaskRecord
	stealBefore := now.Add(-workStealThreshold)
	for _, task := range pending {
		if task == nil {
			continue
		}
		if _, ok := selected[task.TaskID]; ok {
			continue
		}
		class := 4
		switch {
		case task.TenantID == preferredTenantID && task.QueueNodeID == nodeID:
			class = 0
		case task.TenantID == preferredTenantID:
			class = 1
		case task.QueueNodeID != "" && task.QueueNodeID == nodeID:
			class = 2
		case !task.CreatedAt.IsZero() && task.CreatedAt.Before(stealBefore):
			class = 3
		}
		if class < bestClass {
			bestClass = class
			best = task
			if class == 0 {
				return best
			}
		}
	}
	return best
}

// workStealThreshold is intentionally shared with the worker-side age check.
// The leader remains authoritative so a stale or malicious client cannot
// bypass tenant allocation by setting the steal bit on a fresh task.
const workStealThreshold = 5 * time.Second

func (s *InternalService) canClaim(task raftpkg.ClaimTaskData, allocations map[string]*types.NodeAllocation) bool {
	// A steal request always uses the stricter steal admission path, even if
	// this node also happens to have a normal allocation for the target tenant.
	// Otherwise an idle worker can bypass locality/age/ownership validation.
	if task.Steal {
		return s.canSteal(task)
	}
	allocation, ok := allocations[task.NodeID]
	return ok && allocation.Tenants[task.TenantID] > 0
}

func (s *InternalService) canSteal(task raftpkg.ClaimTaskData) bool {
	if !task.Steal {
		return false
	}
	record := s.fsm.GetTask(task.TaskID)
	if record == nil || record.Status != "pending" || record.TenantID != task.TenantID {
		return false
	}
	// A worker may immediately steal a task sitting in another tenant's queue
	// on the same node. Cross-node steals remain age-gated so idle workers do
	// not stampede the leader's recovery scan on fresh submissions.
	if record.QueueNodeID != "" && record.QueueNodeID == task.NodeID {
		return true
	}
	if record.CreatedAt.IsZero() || !record.CreatedAt.Before(time.Now().UTC().Add(-workStealThreshold)) {
		return false
	}
	return true
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
