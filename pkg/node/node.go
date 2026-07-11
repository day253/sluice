// Package node is the central orchestrator that assembles all subsystems
// (Raft, queue, worker pool, allocator, API) into a single running process.
package node

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/allocator"
	"github.com/day253/sluice/pkg/api"
	grpcpkg "github.com/day253/sluice/pkg/grpc"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/tenant"
	"github.com/day253/sluice/pkg/worker"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Config holds all parameters needed to start a node.
type Config struct {
	NodeID       string
	HTTPAddress  string
	GRPCAddress         string // gRPC listen address (empty = no gRPC)
	GRPCInternalAddress string // internal gRPC (empty = no internal gRPC)
	RaftAddress         string
	DataDir      string
	Bootstrap    bool
	JoinAddress  string // HTTP address of an existing node to join
	TotalWorkers int    // max concurrent workers on this node
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

// Node is a single cluster member.  It owns all long-lived components.
type Node struct {
	cfg    Config
	logger *zap.Logger

	raftCluster *raftpkg.Cluster
	queue       queue.Queue
	pool        *worker.Pool
	allocEngine *allocator.Engine
	tenantMgr   *tenant.Manager
	apiServer   *api.Server
	grpcServer         *grpcpkg.Server
	grpcInternalServer *grpcpkg.Server
	grpcInternalSvc    *grpcpkg.InternalService
	handler            *api.Handler

	// cancellation
	ctx    context.Context
	cancel context.CancelFunc
}

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

// New creates and initialises all node components but does not start serving.
func New(cfg Config, processor worker.Processor, logger *zap.Logger) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:    cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// ---- 1. Create data directory ----
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		cancel()
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// ---- 2. Raft cluster ----
	raftCfg := raftpkg.ClusterConfig{
		NodeID:      cfg.NodeID,
		RaftAddress: cfg.RaftAddress,
		DataDir:     cfg.DataDir + "/raft",
		Bootstrap:   cfg.Bootstrap,
		Logger:      logger,
	}
	cluster, err := raftpkg.NewCluster(raftCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("raft cluster: %w", err)
	}
	n.raftCluster = cluster

	// ---- 3. Local durable queue ----
	qPath := cfg.DataDir + "/queue"
	if err := os.MkdirAll(qPath, 0o755); err != nil {
		cancel()
		_ = cluster.Shutdown()
		return nil, fmt.Errorf("create queue dir: %w", err)
	}
	q, err := queue.NewBoltQueue(qPath+"/queue.db", logger)
	if err != nil {
		cancel()
		_ = cluster.Shutdown()
		return nil, fmt.Errorf("pebble queue: %w", err)
	}
	n.queue = q

	// ---- 4. Worker pool ----
	pool := worker.NewPool(
		cfg.NodeID,
		q,
		cluster.FSM(),
		&raftApplierBridge{cluster: cluster},
		processor,
		logger,
	)
	n.pool = pool

	// ---- 5. Allocator engine ----
	n.allocEngine = allocator.NewEngine(
		cfg.NodeID,
		cluster.FSM(),
		&raftApplierBridge{cluster: cluster},
		logger,
	)

	// ---- 6. Tenant manager ----
	n.tenantMgr = tenant.NewManager(
		cluster.FSM(),
		&raftApplierBridge{cluster: cluster},
		logger,
	)

	// ---- 7. API handler & server ----
	n.handler = api.NewHandler(
		cfg.NodeID,
		q,
		cluster.FSM(),
		&raftApplierBridge{cluster: cluster},
		pool,
		logger,
	)
	n.apiServer = api.NewServer(cfg.HTTPAddress, n.handler, logger)

	// ---- 8. gRPC server (external) ----
	if cfg.GRPCAddress != "" {
		grpcSvc := grpcpkg.NewService(cfg.NodeID, q, cluster.FSM(),
			&raftApplierBridge{cluster: cluster}, pool, logger)
		n.grpcServer, err = grpcpkg.NewServer(cfg.GRPCAddress, grpcSvc, logger)
		if err != nil {
			cancel()
			_ = cluster.Shutdown()
			_ = q.Close()
			return nil, fmt.Errorf("grpc server: %w", err)
		}
	}

	// ---- 9. gRPC internal server (node-to-node streaming) ----
	if cfg.GRPCInternalAddress != "" {
		n.grpcInternalSvc = grpcpkg.NewInternalService(cfg.NodeID, cluster.FSM(),
			&raftApplierBridge{cluster: cluster}, logger)
		n.grpcInternalServer, err = grpcpkg.NewInternalServer(
			cfg.GRPCInternalAddress, n.grpcInternalSvc, logger)
		if err != nil {
			cancel()
			_ = cluster.Shutdown()
			_ = q.Close()
			return nil, fmt.Errorf("grpc internal server: %w", err)
		}
	}

	return n, nil
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

// Start begins all background services and blocks until a shutdown signal is
// received or the context is cancelled.
func (n *Node) Start() error {
	// ---- 0. Wait for Raft leadership to be established ----
	n.logger.Info("waiting for raft leader election...")
	if !n.tenantMgr.WaitForLeader(30 * time.Second) {
		return fmt.Errorf("timed out waiting for raft leader")
	}

	// ---- 1. Register this node in the FSM ----
	if err := n.raftCluster.RegisterNode(n.cfg.HTTPAddress, n.cfg.TotalWorkers); err != nil {
		n.logger.Warn("register node (non-fatal, may already exist)", zap.Error(err))
	}

	// ---- 2. Start allocator reconciliation loop ----
	n.allocEngine.Start(n.ctx, 3*time.Second)

	// ---- 3. Leader observation goroutine ----
	go n.watchLeadership()

	// ---- 4. Worker reconciliation loop ----
	go n.watchAllocations()

	// ---- 5. Start HTTP server ----
	httpErrCh := make(chan error, 1)
	go func() {
		httpErrCh <- n.apiServer.Start()
	}()

	// ---- 5b. Start gRPC server (nil chan means disabled) ----
	var grpcErrCh <-chan error
	if n.grpcServer != nil {
		ch := make(chan error, 1)
		grpcErrCh = ch
		go func() {
			ch <- n.grpcServer.Start()
		}()
	}

	// ---- 5c. Start internal gRPC server ----
	var grpcInternalErrCh <-chan error
	if n.grpcInternalServer != nil {
		ch := make(chan error, 1)
		grpcInternalErrCh = ch
		go func() {
			ch <- n.grpcInternalServer.Start()
		}()
	}

	// ---- 6. Wait for shutdown signal ----
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	n.logger.Info("node started",
		zap.String("node_id", n.cfg.NodeID),
		zap.String("http", n.cfg.HTTPAddress),
		zap.String("raft", n.cfg.RaftAddress),
	)

	select {
	case sig := <-sigCh:
		n.logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-httpErrCh:
		if err != nil {
			n.logger.Error("http server error", zap.Error(err))
		}
	case err := <-grpcErrCh:
		if err != nil {
			n.logger.Error("grpc server error", zap.Error(err))
		}
	case err := <-grpcInternalErrCh:
		if err != nil {
			n.logger.Error("grpc internal server error", zap.Error(err))
		}
	case <-n.ctx.Done():
		n.logger.Info("context cancelled, shutting down")
	}

	// ---- 7. Graceful shutdown ----
	return n.Shutdown(30 * time.Second)
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Shutdown gracefully stops all components.
func (n *Node) Shutdown(timeout time.Duration) error {
	n.logger.Info("shutting down node...")

	// Cancel context first to stop background loops.
	n.cancel()

	var errs []error

	// 1. Stop gRPC servers first.
	if n.grpcServer != nil {
		n.grpcServer.Stop()
	}
	if n.grpcInternalServer != nil {
		n.grpcInternalServer.Stop()
	}

	// 2. Stop HTTP server.
	if err := n.apiServer.Shutdown(timeout); err != nil {
		errs = append(errs, fmt.Errorf("api shutdown: %w", err))
	}

	// 2. Stop worker pool (wait for in-flight tasks).
	if err := n.pool.Shutdown(timeout); err != nil {
		errs = append(errs, fmt.Errorf("pool shutdown: %w", err))
	}

	// 3. Close local queue.
	if err := n.queue.Close(); err != nil {
		errs = append(errs, fmt.Errorf("queue close: %w", err))
	}

	// 4. Shutdown Raft.
	if err := n.raftCluster.Shutdown(); err != nil {
		errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}

	n.logger.Info("node shut down cleanly")
	return nil
}

// ---------------------------------------------------------------------------
// Background loops
// ---------------------------------------------------------------------------

// watchLeadership tracks Raft leadership changes and notifies the allocator.
func (n *Node) watchLeadership() {
	// LeaderCh() only fires on state changes — check initial state first.
	if n.raftCluster.IsLeader() {
		n.allocEngine.SetLeader(true)
		_ = n.allocEngine.ReconcileNow()
	}

	ch := n.raftCluster.LeaderCh()
	for {
		select {
		case <-n.ctx.Done():
			return
		case isLeader, ok := <-ch:
			if !ok {
				return
			}
			n.allocEngine.SetLeader(isLeader)
			if isLeader {
				_ = n.allocEngine.ReconcileNow()
			}
		}
	}
}

// watchAllocations polls the FSM for allocation updates and reconciles the
// local worker pool.
func (n *Node) watchAllocations() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastVersion uint64

	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			alloc, ok := n.raftCluster.FSM().GetAllocation(n.cfg.NodeID)
			if !ok {
				continue
			}

			state := n.raftCluster.FSM().GetState()
			if state.Version == lastVersion {
				continue
			}
			lastVersion = state.Version

			n.pool.Reconcile(alloc.Tenants)
			n.logger.Debug("allocation reconciled",
				zap.Any("tenants", alloc.Tenants),
			)
		}
	}
}

// ---------------------------------------------------------------------------
// Accessors (for tests / external use)
// ---------------------------------------------------------------------------

// RaftCluster returns the Raft cluster handle.
func (n *Node) RaftCluster() *raftpkg.Cluster { return n.raftCluster }

// Queue returns the local queue.
func (n *Node) Queue() queue.Queue { return n.queue }

// Pool returns the worker pool.
func (n *Node) Pool() *worker.Pool { return n.pool }

// AllocEngine returns the allocation engine.
func (n *Node) AllocEngine() *allocator.Engine { return n.allocEngine }

// TenantManager returns the tenant manager.
func (n *Node) TenantManager() *tenant.Manager { return n.tenantMgr }

// APIServer returns the HTTP server.
func (n *Node) APIServer() *api.Server { return n.apiServer }

// ---------------------------------------------------------------------------
// raftApplierBridge adapts *raftpkg.Cluster to the RaftApplier interface.
// ---------------------------------------------------------------------------

type raftApplierBridge struct {
	cluster *raftpkg.Cluster
}

func (b *raftApplierBridge) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	future := b.cluster.GetRaft().Apply(cmd, time.Duration(timeoutMs)*time.Millisecond)
	return &applyResultBridge{future: future}
}

func (b *raftApplierBridge) IsLeader() bool {
	return b.cluster.IsLeader()
}

func (b *raftApplierBridge) LeaderAddr() string {
	return b.cluster.LeaderAddr()
}

type applyResultBridge struct {
	future hashicorpraft.ApplyFuture
}

func (r *applyResultBridge) Error() error           { return r.future.Error() }
func (r *applyResultBridge) Response() interface{} { return r.future.Response() }
