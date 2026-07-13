package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
)

const workerStreamTimeout = 5 * time.Second

// ClaimClient owns the worker-to-leader claim and result streams. Both
// streams are replaced together whenever leadership or connectivity changes.
type ClaimClient struct {
	nodeID string
	logger *zap.Logger

	mu           sync.Mutex
	leaderAddr   string
	conn         *grpc.ClientConn
	claimStream  grpcv1.SluiceInternal_ClaimStreamClient
	resultStream grpcv1.SluiceInternal_ResultStreamClient
	streamCtx    context.Context
	streamCancel context.CancelFunc
	generation   uint64
	closed       bool

	claimSendMu  sync.Mutex
	resultSendMu sync.Mutex

	pendingClaims    map[string]chan claimResult
	pendingClaimsMu  sync.Mutex
	pendingResults   map[string]chan struct{}
	pendingResultsMu sync.Mutex
}

type claimResult struct{ claimed bool }

func NewClaimClient(nodeID string, logger *zap.Logger) *ClaimClient {
	return &ClaimClient{
		nodeID:         nodeID,
		logger:         logger,
		pendingClaims:  make(map[string]chan claimResult),
		pendingResults: make(map[string]chan struct{}),
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
	if addr == c.leaderAddr && c.claimStream != nil && c.resultStream != nil {
		return
	}
	if addr != c.leaderAddr {
		c.logger.Info("worker client: new leader", zap.String("addr", addr))
	}
	c.leaderAddr = addr
	c.reconnectLocked()
}

func (c *ClaimClient) Claim(taskID, tenantID, payload string) (bool, error) {
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
		NodeId: c.nodeID, Payload: []byte(payload),
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

	c.conn = conn
	c.claimStream = claimStream
	c.resultStream = resultStream
	c.streamCtx = streamCtx
	c.streamCancel = streamCancel
	go c.recvClaimLoop(claimStream, generation)
	go c.recvResultLoop(resultStream, generation)
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
	c.resultStream = nil
	c.streamCtx = nil
	c.streamCancel = nil
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
