package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/types"
)

// A global assignment/result batch may include every execution node and one
// Raft round trip. Keep this above the normal 5s apply budget so a healthy,
// large voter set does not abandon already committed work and wait for lease
// recovery merely because the response arrives just after Apply completes.
const workerStreamTimeout = 15 * time.Second

// ClaimClient owns the worker-to-leader claim and result streams. Both
// streams are replaced together whenever leadership or connectivity changes.
type ClaimClient struct {
	nodeID string
	logger *zap.Logger

	mu           sync.Mutex
	leaderAddr   string
	conn         *grpc.ClientConn
	claimStream  grpcv1.SluiceInternal_ClaimStreamClient
	assignStream grpcv1.SluiceInternal_AssignmentStreamClient
	resultStream grpcv1.SluiceInternal_ResultStreamClient
	streamCtx    context.Context
	streamCancel context.CancelFunc
	generation   uint64
	closed       bool
	assignLegacy bool // current leader does not implement AssignmentStream

	claimSendMu  sync.Mutex
	assignSendMu sync.Mutex
	resultSendMu sync.Mutex
	assignSeq    atomic.Uint64

	pendingClaims    map[string]chan claimResult
	pendingClaimsMu  sync.Mutex
	pendingAssign    map[string]chan assignmentResult
	pendingAssignMu  sync.Mutex
	pendingResults   map[string]chan struct{}
	pendingResultsMu sync.Mutex
}

type claimResult struct{ claimed bool }

type assignmentResult struct {
	task      *types.TaskRecord
	supported bool
}

func NewClaimClient(nodeID string, logger *zap.Logger) *ClaimClient {
	return &ClaimClient{
		nodeID:         nodeID,
		logger:         logger,
		pendingClaims:  make(map[string]chan claimResult),
		pendingAssign:  make(map[string]chan assignmentResult),
		pendingResults: make(map[string]chan struct{}),
	}
}

// Assign reports one idle execution slot to the leader. supported=false is
// returned only while rolling against an older leader that does not implement
// AssignmentStream; callers may use the legacy claim path in that case.
func (c *ClaimClient) Assign(preferredTenantID string) (*types.TaskRecord, bool, error) {
	c.mu.Lock()
	stream, streamCtx, generation := c.assignStream, c.streamCtx, c.generation
	legacy := c.assignLegacy
	c.mu.Unlock()
	if legacy {
		return nil, false, nil
	}
	if stream == nil || streamCtx == nil {
		return nil, true, fmt.Errorf("assignment client: not connected")
	}

	requestID := fmt.Sprintf("%s-%d", c.nodeID, c.assignSeq.Add(1))
	ch := make(chan assignmentResult, 1)
	c.pendingAssignMu.Lock()
	c.pendingAssign[requestID] = ch
	c.pendingAssignMu.Unlock()
	defer func() {
		c.pendingAssignMu.Lock()
		delete(c.pendingAssign, requestID)
		c.pendingAssignMu.Unlock()
	}()

	c.assignSendMu.Lock()
	err := stream.Send(&grpcv1.AssignmentRequest{
		RequestId: requestID, NodeId: c.nodeID, PreferredTenantId: preferredTenantID,
	})
	c.assignSendMu.Unlock()
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			c.disableAssignments(generation)
			return nil, false, nil
		}
		c.invalidate(generation, err)
		return nil, true, err
	}

	timer := time.NewTimer(workerStreamTimeout)
	defer timer.Stop()
	select {
	case result := <-ch:
		return result.task, result.supported, nil
	case <-timer.C:
		err := fmt.Errorf("assignment timeout")
		c.invalidate(generation, err)
		return nil, true, err
	case <-streamCtx.Done():
		return nil, true, streamCtx.Err()
	}
}

// SetLeader is idempotent and also repairs a broken stream when the leader
// address has not changed. Node leadership tracking calls it periodically.
func (c *ClaimClient) SetLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	if addr == c.leaderAddr && c.claimStream != nil && c.resultStream != nil &&
		(c.assignStream != nil || c.assignLegacy) {
		return
	}
	if addr != c.leaderAddr {
		c.logger.Info("worker client: new leader", zap.String("addr", addr))
	}
	c.leaderAddr = addr
	c.reconnectLocked()
}

func (c *ClaimClient) Claim(taskID, tenantID, payload string) (bool, error) {
	return c.claim(taskID, tenantID, payload, false)
}

// ClaimSteal claims an aged pending task for another tenant using an existing
// idle worker. The leader validates the age before admitting the claim.
func (c *ClaimClient) ClaimSteal(taskID, tenantID, payload string) (bool, error) {
	return c.claim(taskID, tenantID, payload, true)
}

func (c *ClaimClient) claim(taskID, tenantID, payload string, steal bool) (bool, error) {
	ch := make(chan claimResult, 1)
	c.pendingClaimsMu.Lock()
	if _, exists := c.pendingClaims[taskID]; exists {
		c.pendingClaimsMu.Unlock()
		return false, fmt.Errorf("claim already pending for task %s", taskID)
	}
	c.pendingClaims[taskID] = ch
	c.pendingClaimsMu.Unlock()
	defer func() {
		c.pendingClaimsMu.Lock()
		delete(c.pendingClaims, taskID)
		c.pendingClaimsMu.Unlock()
	}()

	c.mu.Lock()
	stream, streamCtx, generation := c.claimStream, c.streamCtx, c.generation
	c.mu.Unlock()
	if stream == nil || streamCtx == nil {
		return false, fmt.Errorf("claim client: not connected")
	}

	c.claimSendMu.Lock()
	err := stream.Send(&grpcv1.ClaimRequest{
		TaskId: taskID, TenantId: tenantID,
		NodeId: c.nodeID, Payload: []byte(payload), Steal: steal,
	})
	c.claimSendMu.Unlock()
	if err != nil {
		c.invalidate(generation, err)
		return false, err
	}

	timer := time.NewTimer(workerStreamTimeout)
	defer timer.Stop()
	select {
	case result := <-ch:
		return result.claimed, nil
	case <-timer.C:
		err := fmt.Errorf("claim timeout")
		c.invalidate(generation, err)
		return false, err
	case <-streamCtx.Done():
		return false, streamCtx.Err()
	}
}

// Complete commits a processed task through the leader's batched result
// stream and waits for the Raft acknowledgement.
func (c *ClaimClient) Complete(taskID, tenantID, result, errStr string, failed bool) error {
	ch := make(chan struct{}, 1)
	c.pendingResultsMu.Lock()
	if _, exists := c.pendingResults[taskID]; exists {
		c.pendingResultsMu.Unlock()
		return fmt.Errorf("completion already pending for task %s", taskID)
	}
	c.pendingResults[taskID] = ch
	c.pendingResultsMu.Unlock()
	defer func() {
		c.pendingResultsMu.Lock()
		delete(c.pendingResults, taskID)
		c.pendingResultsMu.Unlock()
	}()

	c.mu.Lock()
	stream, streamCtx, generation := c.resultStream, c.streamCtx, c.generation
	c.mu.Unlock()
	if stream == nil || streamCtx == nil {
		return fmt.Errorf("result client: not connected")
	}

	status := "done"
	if failed {
		status = "failed"
	}
	c.resultSendMu.Lock()
	err := stream.Send(&grpcv1.ResultRequest{
		TaskId: taskID, TenantId: tenantID, Status: status,
		Result: result, Error: errStr,
	})
	c.resultSendMu.Unlock()
	if err != nil {
		c.invalidate(generation, err)
		return err
	}

	timer := time.NewTimer(workerStreamTimeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-timer.C:
		err := fmt.Errorf("completion timeout")
		c.invalidate(generation, err)
		return err
	case <-streamCtx.Done():
		return streamCtx.Err()
	}
}

func (c *ClaimClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.generation++
	c.closeStreamsLocked()
}

func (c *ClaimClient) reconnectLocked() {
	c.generation++
	generation := c.generation
	c.closeStreamsLocked()
	c.assignLegacy = false
	if c.leaderAddr == "" || c.closed {
		return
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	conn, err := grpc.NewClient(c.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		streamCancel()
		c.logger.Warn("worker client dial failed", zap.String("addr", c.leaderAddr), zap.Error(err))
		return
	}
	client := grpcv1.NewSluiceInternalClient(conn)
	claimStream, err := client.ClaimStream(streamCtx)
	if err != nil {
		streamCancel()
		_ = conn.Close()
		c.logger.Warn("claim stream open failed", zap.Error(err))
		return
	}
	resultStream, err := client.ResultStream(streamCtx)
	if err != nil {
		streamCancel()
		_ = conn.Close()
		c.logger.Warn("result stream open failed", zap.Error(err))
		return
	}
	assignStream, assignErr := client.AssignmentStream(streamCtx)

	c.conn = conn
	c.claimStream = claimStream
	c.assignStream = assignStream
	c.resultStream = resultStream
	c.streamCtx = streamCtx
	c.streamCancel = streamCancel
	c.assignLegacy = status.Code(assignErr) == codes.Unimplemented
	go c.recvClaimLoop(claimStream, generation)
	go c.recvResultLoop(resultStream, generation)
	if assignErr == nil {
		go c.recvAssignmentLoop(assignStream, generation)
	} else if !c.assignLegacy {
		c.logger.Warn("assignment stream open failed", zap.Error(assignErr))
	}
	c.logger.Info("worker client connected", zap.String("addr", c.leaderAddr))
}

func (c *ClaimClient) closeStreamsLocked() {
	if c.streamCancel != nil {
		c.streamCancel()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = nil
	c.claimStream = nil
	c.assignStream = nil
	c.resultStream = nil
	c.streamCtx = nil
	c.streamCancel = nil
}

func (c *ClaimClient) disableAssignments(generation uint64) {
	c.mu.Lock()
	if c.closed || generation != c.generation {
		c.mu.Unlock()
		return
	}
	c.assignStream = nil
	c.assignLegacy = true
	c.mu.Unlock()

	c.pendingAssignMu.Lock()
	for _, ch := range c.pendingAssign {
		select {
		case ch <- assignmentResult{supported: false}:
		default:
		}
	}
	c.pendingAssignMu.Unlock()
}

func (c *ClaimClient) invalidate(generation uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || generation != c.generation {
		return
	}
	c.logger.Warn("worker client stream invalidated", zap.Error(err))
	c.generation++
	c.closeStreamsLocked()
}

func (c *ClaimClient) recvClaimLoop(stream grpcv1.SluiceInternal_ClaimStreamClient, generation uint64) {
	for {
		batch, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.logger.Debug("claim recv done", zap.Error(err))
			}
			c.invalidate(generation, err)
			return
		}
		for _, taskID := range batch.TaskIds {
			c.notifyClaim(taskID, true)
		}
		for _, taskID := range batch.FailedIds {
			c.notifyClaim(taskID, false)
		}
	}
}

func (c *ClaimClient) recvAssignmentLoop(stream grpcv1.SluiceInternal_AssignmentStreamClient, generation uint64) {
	for {
		batch, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.Unimplemented {
				c.disableAssignments(generation)
				return
			}
			if err != io.EOF {
				c.logger.Debug("assignment recv done", zap.Error(err))
			}
			c.invalidate(generation, err)
			return
		}
		for _, task := range batch.Tasks {
			c.notifyAssignment(task.RequestId, assignmentResult{
				supported: true,
				task: &types.TaskRecord{
					TaskID: task.TaskId, TenantID: task.TenantId,
					Status: types.TaskStatusInflight, NodeID: c.nodeID,
					QueueNodeID: task.QueueNodeId, Payload: string(task.Payload),
				},
			})
		}
		for _, requestID := range batch.EmptyRequestIds {
			c.notifyAssignment(requestID, assignmentResult{supported: true})
		}
	}
}

func (c *ClaimClient) recvResultLoop(stream grpcv1.SluiceInternal_ResultStreamClient, generation uint64) {
	for {
		batch, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.logger.Debug("result recv done", zap.Error(err))
			}
			c.invalidate(generation, err)
			return
		}
		for _, taskID := range batch.CommittedIds {
			c.notifyResult(taskID)
		}
	}
}

func (c *ClaimClient) notifyClaim(taskID string, claimed bool) {
	c.pendingClaimsMu.Lock()
	ch, ok := c.pendingClaims[taskID]
	c.pendingClaimsMu.Unlock()
	if ok {
		select {
		case ch <- claimResult{claimed: claimed}:
		default:
		}
	}
}

func (c *ClaimClient) notifyAssignment(requestID string, result assignmentResult) {
	c.pendingAssignMu.Lock()
	ch, ok := c.pendingAssign[requestID]
	c.pendingAssignMu.Unlock()
	if ok {
		select {
		case ch <- result:
		default:
		}
	}
}

func (c *ClaimClient) notifyResult(taskID string) {
	c.pendingResultsMu.Lock()
	ch, ok := c.pendingResults[taskID]
	c.pendingResultsMu.Unlock()
	if ok {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
