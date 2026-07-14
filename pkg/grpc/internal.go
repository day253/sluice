package grpc

import (
	"context"
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

	// claimMu serializes the new leader-owned assignment path with the legacy
	// ClaimStream path during a rolling upgrade.
	claimMu sync.Mutex

	// Assignment and completion requests are aggregated across every node
	// stream before Raft Apply. Per-stream batching caused one node to hold
	// claimMu while all other nodes waited behind a separate Raft round trip.
	assignmentOnce   sync.Once
	assignmentJobs   chan assignmentJob
	assignmentWindow time.Duration
	assignmentMax    int
	completionOnce   sync.Once
	completionJobs   chan completionJob
	completionWindow time.Duration
	completionMax    int

	// allocation subscribers
	subMu sync.RWMutex
	subs  map[string]chan<- *grpcv1.AllocationPlan // nodeID → push channel
}

type assignmentJob struct {
	ctx     context.Context
	request *grpcv1.AssignmentRequest
	outcome chan<- assignmentOutcome
}

type assignmentOutcome struct {
	requestID string
	task      *grpcv1.AssignedTask
	err       error
}

type completionJob struct {
	ctx     context.Context
	task    raftpkg.CompleteTaskData
	outcome chan<- completionOutcome
}

type completionOutcome struct {
	taskID string
	err    error
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
		nodeID:           nodeID,
		raft:             raft,
		fsm:              fsm,
		logger:           logger,
		assignmentJobs:   make(chan assignmentJob, 16384),
		assignmentWindow: claimBatchWindow,
		assignmentMax:    2048,
		completionJobs:   make(chan completionJob, 16384),
		completionWindow: claimBatchWindow,
		completionMax:    2048,
		subs:             make(map[string]chan<- *grpcv1.AllocationPlan),
	}
}

// ---------------------------------------------------------------------------
// ClaimStream — bidirectional batch claim
// ---------------------------------------------------------------------------

const (
	claimBatchWindow  = 5 * time.Millisecond // accumulate window
	claimBatchMaxSize = 128                  // max claims per Raft entry
)

func newStoppedTimer() *time.Timer {
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	return timer
}

func stopTimer(timer *time.Timer, active *bool) {
	if *active && !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	*active = false
}

func resetTimer(timer *time.Timer, active *bool, window time.Duration) {
	stopTimer(timer, active)
	timer.Reset(window)
	*active = true
}

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

// AssignmentStream forwards idle-slot requests into one leader-wide batcher.
// Responses remain on the originating node stream, but task selection and the
// Raft ClaimBatch are shared across all streams.
func (s *InternalService) AssignmentStream(stream grpcv1.SluiceInternal_AssignmentStreamServer) error {
	if !s.raft.IsLeader() {
		return status.Error(codes.FailedPrecondition, "assignment stream is only available on the leader")
	}
	s.assignmentOnce.Do(func() { go s.runAssignmentDispatcher() })
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
				select {
				case readErr <- err:
				case <-stream.Context().Done():
				}
				return
			}
			select {
			case requestCh <- request:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	outcomes := make(chan assignmentOutcome, 4096)
	response := &grpcv1.AssignmentBatch{}
	timer := newStoppedTimer()
	timerOn := false
	flush := func() error {
		if len(response.Tasks) == 0 && len(response.EmptyRequestIds) == 0 {
			return nil
		}
		stopTimer(timer, &timerOn)
		if err := stream.Send(response); err != nil {
			return err
		}
		response = &grpcv1.AssignmentBatch{}
		return nil
	}

	pendingResponses := 0
	receiving := true

	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-readErr:
			return err
		case request, ok := <-requestCh:
			if !ok {
				receiving = false
				requestCh = nil
				if pendingResponses == 0 {
					return flush()
				}
				continue
			}
			job := assignmentJob{ctx: stream.Context(), request: request, outcome: outcomes}
			select {
			case s.assignmentJobs <- job:
				pendingResponses++
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		case outcome := <-outcomes:
			if pendingResponses > 0 {
				pendingResponses--
			}
			if outcome.err != nil {
				return outcome.err
			}
			if outcome.task != nil {
				response.Tasks = append(response.Tasks, outcome.task)
			} else {
				response.EmptyRequestIds = append(response.EmptyRequestIds, outcome.requestID)
			}
			if len(response.Tasks)+len(response.EmptyRequestIds) == 1 {
				resetTimer(timer, &timerOn, claimBatchWindow)
			}
			if len(response.Tasks)+len(response.EmptyRequestIds) >= claimBatchMaxSize {
				if err := flush(); err != nil {
					return err
				}
			}
			if !receiving && pendingResponses == 0 {
				return flush()
			}
		case <-timer.C:
			timerOn = false
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (s *InternalService) runAssignmentDispatcher() {
	for first := range s.assignmentJobs {
		batch := []assignmentJob{first}
		timer := time.NewTimer(s.assignmentWindow)
	collect:
		for len(batch) < s.assignmentMax {
			select {
			case job := <-s.assignmentJobs:
				batch = append(batch, job)
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
		s.dispatchAssignments(batch)
	}
}

func (s *InternalService) dispatchAssignments(batch []assignmentJob) {
	type selectedAssignment struct {
		index   int
		request *grpcv1.AssignmentRequest
		task    *types.TaskRecord
	}
	outcomes := make([]assignmentOutcome, len(batch))
	for i, job := range batch {
		if job.request != nil {
			outcomes[i].requestID = job.request.RequestId
		}
	}
	if !s.raft.IsLeader() {
		err := status.Error(codes.FailedPrecondition, "leadership changed")
		for i := range outcomes {
			outcomes[i].err = err
		}
		s.deliverAssignmentOutcomes(batch, outcomes)
		return
	}

	s.claimMu.Lock()
	allocations := s.fsm.GetAllAllocations()
	pending := s.fsm.FindAllPendingTasks()
	selectedIDs := make(map[string]struct{}, len(batch))
	selected := make([]selectedAssignment, 0, len(batch))
	now := time.Now().UTC()
	for i, job := range batch {
		request := job.request
		if job.ctx.Err() != nil || request == nil || request.RequestId == "" ||
			request.NodeId == "" || request.PreferredTenantId == "" || request.NodeId == s.nodeID {
			continue
		}
		allocation, ok := allocations[request.NodeId]
		if !ok || allocation.Tenants[request.PreferredTenantId] <= 0 {
			continue
		}
		task := selectPendingForSlot(pending, selectedIDs, request.NodeId, request.PreferredTenantId, now)
		if task == nil {
			continue
		}
		selectedIDs[task.TaskID] = struct{}{}
		selected = append(selected, selectedAssignment{index: i, request: request, task: task})
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
		result := s.raft.Apply(
			raftpkg.MustMarshalCommand(raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{Tasks: claims}),
			5000,
		)
		if err := result.Error(); err != nil {
			rpcErr := status.Errorf(codes.Unavailable, "assignment batch commit failed: %v", err)
			for i := range outcomes {
				outcomes[i].err = rpcErr
			}
			s.claimMu.Unlock()
			s.deliverAssignmentOutcomes(batch, outcomes)
			return
		}
		response, ok := result.Response().(*raftpkg.ClaimBatchResult)
		if !ok {
			err := status.Error(codes.Internal, "assignment batch returned an invalid response")
			for i := range outcomes {
				outcomes[i].err = err
			}
			s.claimMu.Unlock()
			s.deliverAssignmentOutcomes(batch, outcomes)
			return
		}
		for _, taskID := range response.Claimed {
			claimed[taskID] = struct{}{}
		}
	}
	s.claimMu.Unlock()

	for _, assignment := range selected {
		if _, ok := claimed[assignment.task.TaskID]; !ok {
			continue
		}
		outcomes[assignment.index].task = &grpcv1.AssignedTask{
			RequestId: assignment.request.RequestId,
			TaskId:    assignment.task.TaskID, TenantId: assignment.task.TenantID,
			Payload: []byte(assignment.task.Payload), QueueNodeId: assignment.task.QueueNodeID,
		}
		if s.queue != nil && assignment.task.QueueNodeID == s.nodeID {
			if err := s.queue.Remove(assignment.task.TenantID, assignment.task.TaskID); err != nil {
				s.logger.Warn("assignment: remove local queue hint failed",
					zap.String("task_id", assignment.task.TaskID), zap.Error(err))
			}
		}
	}
	s.deliverAssignmentOutcomes(batch, outcomes)
}

func (s *InternalService) deliverAssignmentOutcomes(batch []assignmentJob, outcomes []assignmentOutcome) {
	for i, job := range batch {
		select {
		case job.outcome <- outcomes[i]:
		case <-job.ctx.Done():
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
	s.completionOnce.Do(func() { go s.runCompletionDispatcher() })
	s.logger.Info("internal: ResultStream opened")
	defer s.logger.Info("internal: ResultStream closed")

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
				select {
				case readErr <- err:
				case <-stream.Context().Done():
				}
				return
			}
			result := raftpkg.CompleteTaskData{
				TaskID: req.TaskId, TenantID: req.TenantId,
				Status: req.Status, Result: req.Result, Error: req.Error,
			}
			select {
			case resultCh <- result:
			case <-stream.Context().Done():
				return
			}
		}
	}()

	outcomes := make(chan completionOutcome, 4096)
	committedIDs := make([]string, 0, claimBatchMaxSize)
	timer := newStoppedTimer()
	timerOn := false
	flush := func() error {
		if len(committedIDs) == 0 {
			return nil
		}
		stopTimer(timer, &timerOn)
		if err := stream.Send(&grpcv1.ResultBatch{CommittedIds: committedIDs}); err != nil {
			return err
		}
		committedIDs = make([]string, 0, claimBatchMaxSize)
		return nil
	}

	pendingResponses := 0
	receiving := true
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-readErr:
			return err
		case result, ok := <-resultCh:
			if !ok {
				receiving = false
				resultCh = nil
				if pendingResponses == 0 {
					return flush()
				}
				continue
			}
			job := completionJob{ctx: stream.Context(), task: result, outcome: outcomes}
			select {
			case s.completionJobs <- job:
				pendingResponses++
			case <-stream.Context().Done():
				return stream.Context().Err()
			}
		case outcome := <-outcomes:
			if pendingResponses > 0 {
				pendingResponses--
			}
			if outcome.err != nil {
				return outcome.err
			}
			committedIDs = append(committedIDs, outcome.taskID)
			if len(committedIDs) == 1 {
				resetTimer(timer, &timerOn, claimBatchWindow)
			}
			if len(committedIDs) >= claimBatchMaxSize {
				if err := flush(); err != nil {
					return err
				}
			}
			if !receiving && pendingResponses == 0 {
				return flush()
			}
		case <-timer.C:
			timerOn = false
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

func (s *InternalService) runCompletionDispatcher() {
	for first := range s.completionJobs {
		batch := []completionJob{first}
		timer := time.NewTimer(s.completionWindow)
	collect:
		for len(batch) < s.completionMax {
			select {
			case job := <-s.completionJobs:
				batch = append(batch, job)
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
		s.dispatchCompletions(batch)
	}
}

func (s *InternalService) dispatchCompletions(batch []completionJob) {
	outcomes := make([]completionOutcome, len(batch))
	active := make([]raftpkg.CompleteTaskData, 0, len(batch))
	activeIndexes := make([]int, 0, len(batch))
	for i, job := range batch {
		outcomes[i].taskID = job.task.TaskID
		if job.ctx.Err() == nil {
			active = append(active, job.task)
			activeIndexes = append(activeIndexes, i)
		} else {
			outcomes[i].err = status.Error(codes.Canceled, "completion stream canceled before commit")
		}
	}
	if !s.raft.IsLeader() {
		err := status.Error(codes.FailedPrecondition, "leadership changed")
		for i := range outcomes {
			outcomes[i].err = err
		}
		s.deliverCompletionOutcomes(batch, outcomes)
		return
	}
	if len(active) > 0 {
		result := s.raft.Apply(
			raftpkg.MustMarshalCommand(raftpkg.OpCompleteBatch, raftpkg.CompleteBatchData{Tasks: active}),
			5000,
		)
		if err := result.Error(); err != nil {
			s.logger.Error("complete batch raft apply failed", zap.Error(err))
			rpcErr := status.Errorf(codes.Unavailable, "complete batch commit failed: %v", err)
			for _, index := range activeIndexes {
				outcomes[index].err = rpcErr
			}
		}
	}
	s.deliverCompletionOutcomes(batch, outcomes)
}

func (s *InternalService) deliverCompletionOutcomes(batch []completionJob, outcomes []completionOutcome) {
	for i, job := range batch {
		select {
		case job.outcome <- outcomes[i]:
		case <-job.ctx.Done():
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
