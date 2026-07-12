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

// ClaimClient streams claim requests to the leader and receives batch
// responses.  Workers on ANY node use this to claim tasks — the leader
// enforces the allocation plan.
type ClaimClient struct {
	nodeID string
	logger *zap.Logger

	mu         sync.Mutex
	leaderAddr string
	conn       *grpc.ClientConn
	stream     grpcv1.SluiceInternal_ClaimStreamClient

	ctx    context.Context
	cancel context.CancelFunc

	pending   map[string]chan claimResult
	pendingMu sync.Mutex
}

type claimResult struct{ claimed bool }

func NewClaimClient(nodeID string, logger *zap.Logger) *ClaimClient {
	ctx, cancel := context.WithCancel(context.Background())
	return &ClaimClient{
		nodeID:  nodeID,
		logger:  logger,
		pending: make(map[string]chan claimResult),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (c *ClaimClient) SetLeader(addr string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if addr == c.leaderAddr && c.stream != nil {
		return
	}
	c.logger.Info("claim client: new leader", zap.String("addr", addr))
	c.leaderAddr = addr
	c.reconnectLocked()
}

func (c *ClaimClient) Claim(taskID, tenantID, payload string) (bool, error) {
	ch := make(chan claimResult, 1)
	c.pendingMu.Lock()
	c.pending[taskID] = ch
	c.pendingMu.Unlock()
	defer func() { c.pendingMu.Lock(); delete(c.pending, taskID); c.pendingMu.Unlock() }()

	c.mu.Lock()
	s := c.stream
	c.mu.Unlock()
	if s == nil {
		return false, fmt.Errorf("claim client: not connected")
	}

	if err := s.Send(&grpcv1.ClaimRequest{
		TaskId: taskID, TenantId: tenantID,
		NodeId: c.nodeID, Payload: []byte(payload),
	}); err != nil {
		c.reconnect()
		return false, err
	}

	select {
	case r := <-ch:
		return r.claimed, nil
	case <-time.After(10 * time.Second):
		return false, fmt.Errorf("claim timeout")
	case <-c.ctx.Done():
		return false, c.ctx.Err()
	}
}

func (c *ClaimClient) Close() {
	c.cancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *ClaimClient) reconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reconnectLocked()
}

func (c *ClaimClient) reconnectLocked() {
	if c.cancel != nil {
		c.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	if c.conn != nil {
		c.conn.Close()
	}
	if c.leaderAddr == "" {
		return
	}

	conn, err := grpc.NewClient(c.leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		c.logger.Warn("claim dial failed", zap.String("addr", c.leaderAddr), zap.Error(err))
		return
	}
	c.conn = conn

	stream, err := grpcv1.NewSluiceInternalClient(conn).ClaimStream(ctx)
	if err != nil {
		c.logger.Warn("claim stream open failed", zap.Error(err))
		return
	}
	c.stream = stream
	go c.recvLoop(stream)
	c.logger.Info("claim client connected", zap.String("addr", c.leaderAddr))
}

func (c *ClaimClient) recvLoop(stream grpcv1.SluiceInternal_ClaimStreamClient) {
	for {
		batch, err := stream.Recv()
		if err != nil {
			if err != io.EOF {
				c.logger.Debug("claim recv done", zap.Error(err))
			}
			return
		}
		for _, tid := range batch.TaskIds {
			c.notify(tid, true)
		}
		for _, tid := range batch.FailedIds {
			c.notify(tid, false)
		}
	}
}

func (c *ClaimClient) notify(taskID string, claimed bool) {
	c.pendingMu.Lock()
	ch, ok := c.pending[taskID]
	c.pendingMu.Unlock()
	if ok {
		select {
		case ch <- claimResult{claimed: claimed}:
		default:
		}
	}
}
