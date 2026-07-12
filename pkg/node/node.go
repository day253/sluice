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
	"github.com/day253/sluice/pkg/metrics"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/queue"
	"github.com/day253/sluice/pkg/tenant"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	NodeID       string
	APIAddress   string // cmux: HTTP+gRPC on single port (e.g. :9090)
	RaftAddress  string
	DataDir      string
	Bootstrap    bool
	JoinAddress  string
	TotalWorkers int
}

// ---------------------------------------------------------------------------
// Node
// ---------------------------------------------------------------------------

type Node struct {
	cfg    Config
	logger *zap.Logger

	raftCluster *raftpkg.Cluster
	queue       queue.Queue
	pool        *worker.Pool
	allocEngine *allocator.Engine
	tenantMgr   *tenant.Manager
	muxServer   *grpcpkg.MultiplexServer
	apiServer   *api.Server
	claimClient *grpcpkg.ClaimClient
	collector   *metrics.Collector

	ctx    context.Context
	cancel context.CancelFunc
}

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func New(cfg Config, processor worker.Processor, logger *zap.Logger) (*Node, error) {
	ctx, cancel := context.WithCancel(context.Background())

	n := &Node{
		cfg:    cfg,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		cancel()
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	// ---- Raft cluster ----
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

	// ---- Local durable queue ----
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
		return nil, fmt.Errorf("bolt queue: %w", err)
	}
	n.queue = q

	bridge := &raftApplierBridge{cluster: cluster}

	// ---- Worker pool (only leader processes) ----
	n.pool = worker.NewPool(cfg.NodeID, q, cluster.FSM(), bridge, processor, logger)
	n.claimClient = grpcpkg.NewClaimClient(cfg.NodeID, logger)
	n.pool.SetClaimer(n.claimClient)

	// ---- Allocator engine ----
	n.allocEngine = allocator.NewEngine(cfg.NodeID, cluster.FSM(), bridge, logger)

	// ---- Tenant manager ----
	n.tenantMgr = tenant.NewManager(cluster.FSM(), bridge, logger)

	// ---- gRPC services (shared by HTTP adapter + gRPC server) ----
	grpcSvc := grpcpkg.NewService(cfg.NodeID, q, cluster.FSM(), bridge, n.pool, logger)
	internalSvc := grpcpkg.NewInternalService(cfg.NodeID, cluster.FSM(), bridge, logger)

	// ---- Metrics collector (server-side history) ----
	n.collector = metrics.NewCollector(cluster.FSM(), logger)

	// ---- HTTP handler (adapts gRPC service) ----
	httpHandler := api.NewHandler(cfg.NodeID, grpcSvc, logger)
	httpHandler.SetCollector(metricsAdapter{n.collector})
	httpHandler.SetJoinFunc(func(nodeID, raftAddr, httpAddr string, workers int) error {
		if err := cluster.AddVoter(nodeID, raftAddr); err != nil {
			return err
		}
		// Also register in FSM (AddVoter only updates Raft config).
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpNodeUp, types.NodeInfo{
			ID: nodeID, Address: httpAddr, RaftAddress: raftAddr,
			Status: types.NodeStatusUp, TotalWorkers: workers,
		})
		return cluster.GetRaft().Apply(cmd, 5*time.Second).Error()
	})

	// ---- API server (cmux or legacy HTTP) ----
	if cfg.APIAddress != "" {
		n.muxServer, err = grpcpkg.NewMultiplexServer(
			cfg.APIAddress,
			api.NewRouter(httpHandler),
			grpcSvc, internalSvc,
			logger,
		)
		if err != nil {
			cancel()
			_ = cluster.Shutdown()
			_ = q.Close()
			return nil, fmt.Errorf("cmux server: %w", err)
		}
	} else {
		n.apiServer = api.NewServer(":0", httpHandler, logger)
	}

	return n, nil
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

func (n *Node) Start() error {
	n.logger.Info("waiting for raft leader election...")
	if !n.tenantMgr.WaitForLeader(30 * time.Second) {
		return fmt.Errorf("timed out waiting for raft leader")
	}

	if err := n.raftCluster.RegisterNode(
		n.cfg.APIAddress, n.cfg.TotalWorkers,
	); err != nil {
		n.logger.Warn("register node (non-fatal)", zap.Error(err))
	}

	n.allocEngine.Start(n.ctx, 3*time.Second)
	go n.collector.Start(n.ctx)
	go n.watchLeadership()
	go n.watchAllocations()

	// ---- API server ----
	errCh := make(chan error, 1)
	if n.muxServer != nil {
		go func() { errCh <- n.muxServer.Start() }()
	} else {
		go func() { errCh <- n.apiServer.Start() }()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	n.logger.Info("node started",
		zap.String("node_id", n.cfg.NodeID),
		zap.String("api", n.cfg.APIAddress),
		zap.String("raft", n.cfg.RaftAddress),
	)

	select {
	case sig := <-sigCh:
		n.logger.Info("received signal, shutting down", zap.String("signal", sig.String()))
	case err := <-errCh:
		if err != nil {
			n.logger.Error("api server error", zap.Error(err))
		}
	case <-n.ctx.Done():
		n.logger.Info("context cancelled, shutting down")
	}

	return n.Shutdown(30 * time.Second)
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

func (n *Node) Shutdown(timeout time.Duration) error {
	n.logger.Info("shutting down node...")
	n.cancel()

	var errs []error

	// 1. API server.
	if n.muxServer != nil {
		n.muxServer.Stop()
	}
	if n.apiServer != nil {
		_ = n.apiServer.Shutdown(timeout)
	}

	// 2. Worker pool.
	if err := n.pool.Shutdown(timeout); err != nil {
		errs = append(errs, fmt.Errorf("pool shutdown: %w", err))
	}

	// 3. Queue.
	if err := n.queue.Close(); err != nil {
		errs = append(errs, fmt.Errorf("queue close: %w", err))
	}

	// 4. Claim client.
	if n.claimClient != nil {
		n.claimClient.Close()
	}
	// 5. Raft.
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

func (n *Node) watchLeadership() {
	updateClaim := func() {
		if addr := n.raftCluster.LeaderAddr(); addr != "" {
			apiAddr := addr[:len(addr)-5] + ":9090"
			n.claimClient.SetLeader(apiAddr)
		}
	}
	updateClaim()

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
			updateClaim()
		}
	}
}

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
		}
	}
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

func (n *Node) RaftCluster() *raftpkg.Cluster { return n.raftCluster }
func (n *Node) Queue() queue.Queue             { return n.queue }
func (n *Node) Pool() *worker.Pool             { return n.pool }
func (n *Node) AllocEngine() *allocator.Engine  { return n.allocEngine }
func (n *Node) TenantManager() *tenant.Manager  { return n.tenantMgr }

// ---------------------------------------------------------------------------
// raftApplierBridge
// ---------------------------------------------------------------------------

type raftApplierBridge struct {
	cluster *raftpkg.Cluster
}

func (b *raftApplierBridge) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	future := b.cluster.GetRaft().Apply(cmd, time.Duration(timeoutMs)*time.Millisecond)
	return &applyResultBridge{future: future}
}

func (b *raftApplierBridge) IsLeader() bool      { return b.cluster.IsLeader() }
func (b *raftApplierBridge) LeaderAddr() string   { return b.cluster.LeaderAddr() }

type applyResultBridge struct {
	future hashicorpraft.ApplyFuture
}

func (r *applyResultBridge) Error() error          { return r.future.Error() }
func (r *applyResultBridge) Response() interface{}  { return r.future.Response() }

// metricsAdapter bridges metrics.Collector → api.MetricsData for HTTP.
type metricsAdapter struct{ c *metrics.Collector }

func (a metricsAdapter) Query(name string) ([]api.MetricsData, int) {
	data, n := a.c.Query(name)
	out := make([]api.MetricsData, len(data))
	for i, d := range data {
		out[i] = api.MetricsData{
			Name: d.Name, Labels: d.Labels,
			Secs: d.Secs, Mins: d.Mins, Hours: d.Hours, Days: d.Days,
		}
	}
	return out, n
}
