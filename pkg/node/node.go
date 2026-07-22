// Package node is the central orchestrator that assembles all subsystems
// (Raft, queue, worker pool, allocator, API) into a single running process.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	hashicorpraft "github.com/hashicorp/raft"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/allocator"
	"github.com/day253/sluice/pkg/api"
	grpcpkg "github.com/day253/sluice/pkg/grpc"
	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/metrics"
	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/tenant"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Role            string // empty = legacy combined node; "control" = Raft/controller only
	NodeID          string
	APIAddress      string // cmux: HTTP+gRPC on single port (e.g. :9090)
	RaftAddress     string // stable address advertised to peers
	RaftBindAddress string // local listen address; defaults to RaftAddress
	DataDir         string
	Bootstrap       bool
	JoinAddress     string
	TotalWorkers    int
	MaxRaftVoters   int // odd voter cap; remaining members replicate as non-voters
	MaxRaftMembers  int // zero keeps legacy membership; production control plane sets a fixed bound
	// DisableVoterReconciliation is reserved for externally managed embedded
	// clusters and protocol tests. Production leaves it false.
	DisableVoterReconciliation bool
	// AllocatorInterval overrides the 3s periodic liveness tick. Production
	// normally leaves it zero; protocol tests may extend it to prove event wakes.
	AllocatorInterval time.Duration
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
	performance *metrics.Performance
	internalSvc *grpcpkg.InternalService

	ctx    context.Context
	cancel context.CancelFunc

	shutdownOnce sync.Once
	shutdownErr  error

	voterReconcileRunning atomic.Bool
	voterReconcileDone    atomic.Bool
}

// ---------------------------------------------------------------------------
// New
// ---------------------------------------------------------------------------

func New(cfg Config, processor worker.Processor, logger *zap.Logger) (*Node, error) {
	if cfg.MaxRaftVoters == 0 {
		cfg.MaxRaftVoters = raftpkg.DefaultMaxVoters
	}
	if cfg.MaxRaftVoters < 1 || cfg.MaxRaftVoters%2 == 0 {
		return nil, fmt.Errorf("max Raft voters must be a positive odd number, got %d", cfg.MaxRaftVoters)
	}
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
		NodeID:          cfg.NodeID,
		RaftAddress:     cfg.RaftAddress,
		RaftBindAddress: cfg.RaftBindAddress,
		DataDir:         cfg.DataDir + "/raft",
		Bootstrap:       cfg.Bootstrap,
		Logger:          logger,
	}
	cluster, err := raftpkg.NewCluster(raftCfg)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("raft cluster: %w", err)
	}
	n.raftCluster = cluster
	n.performance = metrics.NewPerformance()

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

	bridge := &raftApplierBridge{cluster: cluster, performance: n.performance}

	// ---- Worker pool (followers execute; leader is control-plane only) ----
	n.pool = worker.NewPool(cfg.NodeID, q, cluster.FSM(), bridge, processor, logger)
	n.claimClient = grpcpkg.NewClaimClient(cfg.NodeID, logger)
	n.pool.SetClaimer(n.claimClient)
	n.pool.SetCompleter(n.claimClient)
	if cfg.Role == types.NodeRoleControl {
		n.pool.SetWorkerGuard(func() bool { return false })
	} else {
		n.pool.SetWorkerGuard(func() bool { return !cluster.IsLeader() })
	}

	// ---- Allocator engine ----
	n.allocEngine = allocator.NewEngine(cfg.NodeID, cluster.FSM(), bridge, logger)

	// ---- Tenant manager ----
	n.tenantMgr = tenant.NewManager(cluster.FSM(), bridge, logger)

	// ---- gRPC services (shared by HTTP adapter + gRPC server) ----
	grpcSvc := grpcpkg.NewService(cfg.NodeID, q, cluster.FSM(), bridge, n.pool, logger)
	grpcSvc.SetWorkAvailableFunc(n.allocEngine.NotifyWorkAvailable)
	internalSvc := grpcpkg.NewInternalService(cfg.NodeID, cluster.FSM(), bridge, logger)
	internalSvc.SetPerformanceObserver(n.performance)
	n.internalSvc = internalSvc

	// ---- Metrics collector (server-side history) ----
	n.collector = metrics.NewCollector(cluster.FSM(), logger)
	n.collector.SetPerformance(n.performance)

	// ---- HTTP handler (adapts gRPC service) ----
	httpHandler := api.NewHandler(cfg.NodeID, grpcSvc, logger)
	httpHandler.SetCollector(metricsAdapter{n.collector})
	httpHandler.SetRaftStatusFunc(cluster.MembershipStatus)
	httpHandler.SetPerformanceFunc(n.performanceDiagnostics)
	httpHandler.SetJoinFunc(func(nodeID, raftAddr, httpAddr string, workers int) error {
		if err := cluster.AddServer(nodeID, raftAddr, cfg.MaxRaftVoters); err != nil {
			return err
		}
		// Also register in FSM (AddVoter only updates Raft config).
		role := ""
		if workers == 0 {
			role = types.NodeRoleControl
		}
		cmd := raftpkg.MustMarshalCommand(raftpkg.OpNodeUp, types.NodeInfo{
			ID: nodeID, Address: httpAddr, RaftAddress: raftAddr,
			Role: role, Status: types.NodeStatusUp, TotalWorkers: workers,
		})
		return cluster.GetRaft().Apply(cmd, 5*time.Second).Error()
	})
	httpHandler.SetWorkerRegisterFunc(n.registerWorker)

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

	if err := n.raftCluster.RegisterNodeWithRole(
		n.cfg.APIAddress, n.cfg.TotalWorkers, n.cfg.Role,
	); err != nil {
		n.logger.Warn("register node (non-fatal)", zap.Error(err))
	}

	allocatorInterval := n.cfg.AllocatorInterval
	if allocatorInterval <= 0 {
		allocatorInterval = 3 * time.Second
	}
	n.allocEngine.Start(n.ctx, allocatorInterval)
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
	n.shutdownOnce.Do(func() {
		n.shutdownErr = n.shutdown(timeout)
	})
	return n.shutdownErr
}

func (n *Node) shutdown(timeout time.Duration) error {
	n.logger.Info("shutting down node...")
	n.cancel()

	var errs []error

	// 1. Close outbound streams before waiting for workers. Cancellation leaves
	// interrupted claims for the leader's lease scanner to recover.
	if n.claimClient != nil {
		n.claimClient.Close()
	}

	// 2. Worker pool.
	if err := n.pool.Shutdown(timeout); err != nil {
		errs = append(errs, fmt.Errorf("pool shutdown: %w", err))
	}

	// 3. API server. Internal streams are deliberately stopped after workers,
	// so local completions get their final chance to flush.
	if n.muxServer != nil {
		n.muxServer.Stop()
	}
	if n.apiServer != nil {
		_ = n.apiServer.Shutdown(timeout)
	}

	// 4. Queue.
	if err := n.queue.Close(); err != nil {
		errs = append(errs, fmt.Errorf("queue close: %w", err))
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
			// Prefer the registered API address. Integration clusters use
			// dynamic ports, while Kubernetes falls back to the stable Raft
			// service host with the shared 9090 port.
			for _, member := range n.raftCluster.FSM().GetState().Nodes {
				if member.RaftAddress != addr || member.Address == "" {
					continue
				}
				host, _, err := net.SplitHostPort(member.Address)
				if err == nil && host != "0.0.0.0" && host != "::" && host != "" {
					n.claimClient.SetLeader(member.Address)
					return
				}
			}
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				n.logger.Warn("could not parse raft leader address", zap.String("addr", addr), zap.Error(err))
				return
			}
			n.claimClient.SetLeader(net.JoinHostPort(host, "9090"))
		}
	}
	updateClaim()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	if n.raftCluster.IsLeader() {
		n.pool.Reconcile(map[string]int{})
		n.allocEngine.SetLeader(true)
		n.internalSvc.SetLeader(true)
		_ = n.allocEngine.ReconcileNow()
		n.reconcileVotersAsync()
	}

	ch := n.raftCluster.LeaderCh()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			// LeaderCh only reports this node's boolean leadership transitions.
			// Polling the address also catches follower A -> follower B changes.
			updateClaim()
			if n.raftCluster.IsLeader() {
				n.reconcileVotersAsync()
			}
		case isLeader, ok := <-ch:
			if !ok {
				return
			}
			n.allocEngine.SetLeader(isLeader)
			n.internalSvc.SetLeader(isLeader)
			if isLeader {
				n.voterReconcileDone.Store(false)
				// Stop the data plane before publishing the follower-only plan.
				n.pool.Reconcile(map[string]int{})
				_ = n.allocEngine.ReconcileNow()
				n.reconcileVotersAsync()
			} else {
				n.voterReconcileDone.Store(false)
			}
			updateClaim()
		}
	}
}

func (n *Node) reconcileVotersAsync() {
	if n.voterReconcileDone.Load() || !n.raftCluster.IsLeader() ||
		!n.voterReconcileRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer n.voterReconcileRunning.Store(false)
		var result raftpkg.VoterReconcileResult
		if !n.cfg.DisableVoterReconciliation {
			var err error
			result, err = n.raftCluster.ReconcileVoters(n.cfg.MaxRaftVoters)
			if err != nil {
				if err != hashicorpraft.ErrNotLeader {
					n.logger.Warn("raft voter reconciliation failed", zap.Error(err))
				}
				return
			}
			if result.LeadershipTransferred {
				n.logger.Info("raft leadership transferred into bounded voter set",
					zap.String("leader", result.Status.LeaderID))
				return
			}
		}
		if !n.cfg.DisableVoterReconciliation && n.cfg.MaxRaftMembers > 0 {
			pruned, pruneErr := n.raftCluster.PruneMembers(n.cfg.MaxRaftMembers)
			if pruneErr != nil {
				if pruneErr != hashicorpraft.ErrNotLeader {
					n.logger.Warn("raft member pruning failed", zap.Error(pruneErr))
				}
				return
			}
			for _, nodeID := range pruned.Removed {
				if n.raftCluster.FSM().GetAllNodes()[nodeID] == nil {
					continue
				}
				future := n.raftCluster.GetRaft().Apply(raftpkg.MustMarshalCommand(
					raftpkg.OpRetireNode,
					raftpkg.RetireNodeData{ID: nodeID},
				), 5*time.Second)
				if err := future.Error(); err != nil {
					n.logger.Warn("retired replica capacity cleanup failed",
						zap.String("node", nodeID), zap.Error(err))
					return
				}
			}
			if pruned.Changed {
				n.logger.Info("raft replication set pruned",
					zap.Int("members", n.cfg.MaxRaftMembers), zap.Strings("removed", pruned.Removed))
			}
		}
		if n.cfg.Role == types.NodeRoleControl {
			status, statusErr := n.raftCluster.MembershipStatus()
			if statusErr != nil {
				n.logger.Warn("control mirror membership read failed", zap.Error(statusErr))
				return
			}
			migrations := controlNodesNeedingMigration(status, n.raftCluster.FSM().GetAllNodes())
			if len(migrations) > 0 {
				future := n.raftCluster.GetRaft().Apply(raftpkg.MustMarshalCommand(
					raftpkg.OpSetControlNodes,
					raftpkg.SetControlNodesData{NodeIDs: migrations},
				), 5*time.Second)
				if err := future.Error(); err != nil {
					n.logger.Warn("control mirror migration failed", zap.Error(err))
					return
				}
				if err := n.allocEngine.ReconcileNow(); err != nil {
					n.logger.Warn("allocation reconcile after control migration failed", zap.Error(err))
				}
			}
		}
		n.voterReconcileDone.Store(true)
		if result.Changed {
			n.logger.Info("raft voter set reconciled",
				zap.Int("voters", len(result.Status.Voters)),
				zap.Int("nonvoters", len(result.Status.Nonvoters)))
		}
	}()
}

func controlNodesNeedingMigration(status raftpkg.MembershipStatus, nodes map[string]*types.NodeInfo) []string {
	members := append(append([]string(nil), status.Voters...), status.Nonvoters...)
	sort.Strings(members)
	result := make([]string, 0, len(members))
	for _, nodeID := range members {
		node := nodes[nodeID]
		if node != nil && (node.Role != types.NodeRoleControl || node.TotalWorkers != 0 || node.SessionID != "") {
			result = append(result, nodeID)
		}
	}
	return result
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
			state := n.raftCluster.FSM().GetState()
			if state.Version == lastVersion {
				continue
			}
			lastVersion = state.Version
			if n.raftCluster.IsLeader() {
				for _, allocation := range state.Allocations {
					plan := &grpcv1.AllocationPlan{NodeId: allocation.NodeID}
					for tenantID, count := range allocation.Tenants {
						plan.Tenants = append(plan.Tenants, &grpcv1.TenantWorkerCount{
							TenantId: tenantID, Workers: int32(count),
						})
					}
					n.internalSvc.PushAllocation(plan)
				}
			}
			if n.cfg.Role == types.NodeRoleControl {
				n.pool.Reconcile(map[string]int{})
				continue
			}
			alloc, ok := n.raftCluster.FSM().GetAllocation(n.cfg.NodeID)
			if !ok || n.raftCluster.IsLeader() {
				n.pool.Reconcile(map[string]int{})
				continue
			}
			n.pool.Reconcile(alloc.Tenants)
		}
	}
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

func (n *Node) RaftCluster() *raftpkg.Cluster  { return n.raftCluster }
func (n *Node) Queue() queue.Queue             { return n.queue }
func (n *Node) Pool() *worker.Pool             { return n.pool }
func (n *Node) AllocEngine() *allocator.Engine { return n.allocEngine }
func (n *Node) TenantManager() *tenant.Manager { return n.tenantMgr }

func (n *Node) performanceDiagnostics(ctx context.Context, localOnly, includeHistory bool) (metrics.PerformanceDiagnostics, error) {
	if n.raftCluster.IsLeader() {
		if !includeHistory {
			return n.collector.PerformanceCurrent(n.cfg.NodeID), nil
		}
		return n.collector.PerformanceDiagnostics(n.cfg.NodeID), nil
	}
	if localOnly {
		return metrics.PerformanceDiagnostics{}, fmt.Errorf("local node %s is not the leader", n.cfg.NodeID)
	}
	leaderRaft := n.raftCluster.LeaderAddr()
	if leaderRaft == "" {
		return metrics.PerformanceDiagnostics{}, fmt.Errorf("raft leader is unknown")
	}
	leaderAPI, err := resolveLeaderAPIAddress(leaderRaft, n.raftCluster.FSM().GetAllNodes(), n.cfg.APIAddress)
	if err != nil {
		return metrics.PerformanceDiagnostics{}, err
	}
	query := "?local=1"
	if !includeHistory {
		query += "&history=0"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://"+leaderAPI+"/api/v1/admin/performance"+query, nil)
	if err != nil {
		return metrics.PerformanceDiagnostics{}, err
	}
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return metrics.PerformanceDiagnostics{}, fmt.Errorf("query leader %s: %w", leaderAPI, err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return metrics.PerformanceDiagnostics{}, fmt.Errorf("leader %s returned %s", leaderAPI, response.Status)
	}
	var diagnostics metrics.PerformanceDiagnostics
	if err := json.NewDecoder(response.Body).Decode(&diagnostics); err != nil {
		return metrics.PerformanceDiagnostics{}, fmt.Errorf("decode leader diagnostics: %w", err)
	}
	return diagnostics, nil
}

func resolveLeaderAPIAddress(leaderRaft string, nodes map[string]*types.NodeInfo, localAPI string) (string, error) {
	for _, member := range nodes {
		if member.RaftAddress != leaderRaft {
			continue
		}
		if host, port, err := net.SplitHostPort(member.Address); err == nil &&
			host != "" && host != "0.0.0.0" && host != "::" && port != "" {
			return member.Address, nil
		}
	}
	leaderHost, _, err := net.SplitHostPort(leaderRaft)
	if err != nil || leaderHost == "" {
		return "", fmt.Errorf("invalid raft leader address %q", leaderRaft)
	}
	_, apiPort, err := net.SplitHostPort(localAPI)
	if err != nil || apiPort == "" {
		return "", fmt.Errorf("invalid local API address %q", localAPI)
	}
	return net.JoinHostPort(leaderHost, apiPort), nil
}

func (n *Node) registerWorker(info types.NodeInfo) error {
	info.Role = types.NodeRoleWorker
	info.Status = types.NodeStatusUp
	info.RaftAddress = ""
	if !n.raftCluster.IsLeader() {
		leaderAPI, err := resolveLeaderAPIAddress(
			n.raftCluster.LeaderAddr(), n.raftCluster.FSM().GetAllNodes(), n.cfg.APIAddress,
		)
		if err != nil {
			return err
		}
		body, err := json.Marshal(map[string]interface{}{
			"node_id": info.ID, "session_id": info.SessionID,
			"http_address": info.Address, "total_workers": info.TotalWorkers,
		})
		if err != nil {
			return err
		}
		request, err := http.NewRequest(http.MethodPost,
			"http://"+leaderAPI+"/api/v1/cluster/workers/register", bytes.NewReader(body))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
		if err != nil {
			return fmt.Errorf("forward worker registration to %s: %w", leaderAPI, err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("leader worker registration returned %s", response.Status)
		}
		return nil
	}

	existing := n.raftCluster.FSM().GetAllNodes()[info.ID]
	if !workerRegistrationChanged(existing, &info) {
		return nil
	}
	result := n.raftCluster.GetRaft().Apply(
		raftpkg.MustMarshalCommand(raftpkg.OpNodeUp, info), 5*time.Second,
	)
	if err := result.Error(); err != nil {
		return err
	}
	go func() {
		if err := n.allocEngine.ReconcileNow(); err != nil {
			n.logger.Warn("reconcile after worker registration failed", zap.Error(err))
		}
	}()
	return nil
}

func workerRegistrationChanged(existing, next *types.NodeInfo) bool {
	return existing == nil || next == nil || existing.Role != next.Role ||
		existing.SessionID != next.SessionID || existing.Status != next.Status ||
		existing.TotalWorkers != next.TotalWorkers || existing.Address != next.Address
}

// ---------------------------------------------------------------------------
// raftApplierBridge
// ---------------------------------------------------------------------------

type raftApplierBridge struct {
	cluster     *raftpkg.Cluster
	performance *metrics.Performance
}

func (b *raftApplierBridge) Apply(cmd []byte, timeoutMs int) raftpkg.ApplyResult {
	started := time.Now()
	future := b.cluster.GetRaft().Apply(cmd, time.Duration(timeoutMs)*time.Millisecond)
	return &applyResultBridge{future: future, observe: func(err error) {
		if b.performance != nil {
			b.performance.ObserveRaftApply(cmd, time.Since(started), err)
		}
	}}
}

func (b *raftApplierBridge) IsLeader() bool     { return b.cluster.IsLeader() }
func (b *raftApplierBridge) LeaderAddr() string { return b.cluster.LeaderAddr() }

type applyResultBridge struct {
	future  hashicorpraft.ApplyFuture
	once    sync.Once
	observe func(error)
}

func (r *applyResultBridge) Error() error {
	err := r.future.Error()
	r.record(err)
	return err
}

func (r *applyResultBridge) Response() interface{} {
	response := r.future.Response()
	r.record(r.future.Error())
	return response
}

func (r *applyResultBridge) record(err error) {
	r.once.Do(func() {
		if r.observe != nil {
			r.observe(err)
		}
	})
}

// metricsAdapter bridges metrics.Collector → api.MetricsData for HTTP.
type metricsAdapter struct{ c *metrics.Collector }

func (a metricsAdapter) Query(name, includePrefix, excludePrefix string) ([]api.MetricsData, int) {
	data := a.c.QueryNamedFiltered(name, includePrefix, excludePrefix)
	out := make([]api.MetricsData, len(data))
	for i, d := range data {
		out[i] = api.MetricsData{
			Name: d.Name, Labels: d.Labels,
			Secs: d.Secs, Mins: d.Mins, Hours: d.Hours, Days: d.Days,
		}
	}
	return out, len(out)
}
