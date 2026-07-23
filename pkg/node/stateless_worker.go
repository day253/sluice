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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	grpcpkg "github.com/day253/sluice/pkg/grpc"
	grpcv1 "github.com/day253/sluice/pkg/grpc/v1"
	"github.com/day253/sluice/pkg/types"
	"github.com/day253/sluice/pkg/worker"
)

// StatelessWorkerConfig contains only connection and execution capacity. A
// stateless Worker deliberately has no Raft address or data directory.
type StatelessWorkerConfig struct {
	NodeID            string
	SessionID         string
	APIAddress        string
	ControllerAddress string
	TotalWorkers      int
	// LoadSampler is optional test/integration injection. Production uses the
	// process/container CPU sampler when it is nil.
	LoadSampler worker.CPULoadSampler
}

// StatelessWorker is the execution-plane process. Its only durable state is
// held by the control-plane Raft shard.
type StatelessWorker struct {
	cfg    StatelessWorkerConfig
	logger *zap.Logger

	pool        *worker.Pool
	claimClient *grpcpkg.ClaimClient
	httpServer  *http.Server
	listener    net.Listener

	ctx      context.Context
	cancel   context.CancelFunc
	draining atomic.Bool

	shutdownOnce sync.Once
	shutdownErr  error
}

func NewStatelessWorker(cfg StatelessWorkerConfig, processor worker.Processor, logger *zap.Logger) (*StatelessWorker, error) {
	if cfg.NodeID == "" || cfg.ControllerAddress == "" || cfg.TotalWorkers < 1 {
		return nil, fmt.Errorf("stateless worker requires id, controller and positive workers")
	}
	if cfg.SessionID == "" {
		cfg.SessionID = uuid.NewString()
	}
	if cfg.APIAddress == "" {
		cfg.APIAddress = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", cfg.APIAddress)
	if err != nil {
		return nil, fmt.Errorf("worker health listen %s: %w", cfg.APIAddress, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	claimClient := grpcpkg.NewClaimClient(cfg.NodeID, logger)
	pool := worker.NewPool(cfg.NodeID, nil, nil, nil, processor, logger)
	pool.DisableLegacyScheduling()
	pool.SetClaimer(claimClient)
	pool.SetCompleter(claimClient)
	loadSampler := cfg.LoadSampler
	if loadSampler == nil {
		loadSampler = worker.NewProcessCPULoadSampler()
	}
	claimClient.SetLoadProvider(func() types.WorkerLoadSnapshot {
		cpuMillis, valid := loadSampler.Sample()
		running, capacity := pool.ExecutionSnapshot()
		return types.WorkerLoadSnapshot{
			CPUUtilizationMillis: cpuMillis,
			CPUValid:             valid,
			RunningTasks:         running,
			WorkerCapacity:       capacity,
		}
	})

	w := &StatelessWorker{
		cfg: cfg, logger: logger, pool: pool, claimClient: claimClient,
		listener: listener, ctx: ctx, cancel: cancel,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/health", func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(response).Encode(map[string]interface{}{
			"status": "ok", "role": types.NodeRoleWorker,
			"node_id": cfg.NodeID, "session_id": cfg.SessionID,
		})
	})
	w.httpServer = &http.Server{Handler: mux}
	return w, nil
}

func (w *StatelessWorker) Start() error {
	errCh := make(chan error, 2)
	go func() {
		if err := w.httpServer.Serve(w.listener); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go w.runControlLoop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	w.logger.Info("stateless worker started",
		zap.String("node_id", w.cfg.NodeID), zap.String("session_id", w.cfg.SessionID),
		zap.String("controller", w.cfg.ControllerAddress), zap.Int("workers", w.cfg.TotalWorkers))

	select {
	case sig := <-sigCh:
		w.logger.Info("stateless worker received signal", zap.String("signal", sig.String()))
	case err := <-errCh:
		w.logger.Error("stateless worker server failed", zap.Error(err))
	case <-w.ctx.Done():
	}
	return w.Shutdown(25 * time.Second)
}

func (w *StatelessWorker) Shutdown(timeout time.Duration) error {
	w.shutdownOnce.Do(func() {
		w.draining.Store(true)
		// Keep the Leader streams alive while already-started Processor calls
		// finish and their results are durably acknowledged.
		drainErr := w.pool.Drain(timeout)
		w.cancel()
		w.claimClient.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		serverErr := w.httpServer.Shutdown(shutdownCtx)
		if drainErr != nil {
			w.shutdownErr = drainErr
		} else if serverErr != nil {
			w.shutdownErr = serverErr
		}
	})
	return w.shutdownErr
}

func (w *StatelessWorker) Pool() *worker.Pool { return w.pool }

func (w *StatelessWorker) APIAddress() string { return w.listener.Addr().String() }

func (w *StatelessWorker) runControlLoop() {
	for w.ctx.Err() == nil {
		leaderAPI, err := w.discoverLeaderAPI()
		if err != nil {
			w.waitReconnect(err)
			continue
		}
		if err := w.register(leaderAPI); err != nil {
			w.waitReconnect(err)
			continue
		}
		w.claimClient.SetLeader(leaderAPI)
		if err := w.consumeAllocations(leaderAPI); err != nil && w.ctx.Err() == nil {
			if !w.draining.Load() {
				w.pool.Reconcile(map[string]int{})
			}
			w.waitReconnect(err)
		}
	}
}

func (w *StatelessWorker) waitReconnect(err error) {
	w.logger.Debug("worker control reconnect", zap.Error(err))
	select {
	case <-w.ctx.Done():
	case <-time.After(time.Second):
	}
}

func (w *StatelessWorker) discoverLeaderAPI() (string, error) {
	request, err := http.NewRequestWithContext(w.ctx, http.MethodGet,
		"http://"+w.cfg.ControllerAddress+"/api/v1/health", nil)
	if err != nil {
		return "", err
	}
	response, err := (&http.Client{Timeout: 3 * time.Second}).Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("controller health returned %s", response.Status)
	}
	var health struct {
		Leader string `json:"leader"`
	}
	if err := json.NewDecoder(response.Body).Decode(&health); err != nil {
		return "", err
	}
	// Dynamic-port integration clusters publish each control node's concrete
	// API address. Prefer that mapping; Kubernetes control nodes intentionally
	// advertise 0.0.0.0 and use the stable Raft-service host fallback below.
	nodesRequest, err := http.NewRequestWithContext(w.ctx, http.MethodGet,
		"http://"+w.cfg.ControllerAddress+"/api/v1/admin/nodes", nil)
	if err == nil {
		if nodesResponse, nodesErr := (&http.Client{Timeout: 3 * time.Second}).Do(nodesRequest); nodesErr == nil {
			var snapshot struct {
				Nodes []struct {
					Address     string `json:"address"`
					RaftAddress string `json:"raft_address"`
				} `json:"nodes"`
			}
			if nodesResponse.StatusCode == http.StatusOK && json.NewDecoder(nodesResponse.Body).Decode(&snapshot) == nil {
				for _, node := range snapshot.Nodes {
					host, port, splitErr := net.SplitHostPort(node.Address)
					if node.RaftAddress == health.Leader && splitErr == nil && host != "" &&
						host != "0.0.0.0" && host != "::" && port != "" {
						nodesResponse.Body.Close()
						return node.Address, nil
					}
				}
			}
			nodesResponse.Body.Close()
		}
	}
	host, _, err := net.SplitHostPort(health.Leader)
	if err != nil || host == "" {
		return "", fmt.Errorf("controller returned invalid leader %q", health.Leader)
	}
	_, controllerPort, err := net.SplitHostPort(w.cfg.ControllerAddress)
	if err != nil {
		return "", fmt.Errorf("invalid controller address %q", w.cfg.ControllerAddress)
	}
	return net.JoinHostPort(host, controllerPort), nil
}

func (w *StatelessWorker) register(leaderAPI string) error {
	body, err := json.Marshal(map[string]interface{}{
		"node_id": w.cfg.NodeID, "session_id": w.cfg.SessionID,
		"http_address": w.APIAddress(), "total_workers": w.cfg.TotalWorkers,
	})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(w.ctx, http.MethodPost,
		"http://"+leaderAPI+"/api/v1/cluster/workers/register", bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := (&http.Client{Timeout: 5 * time.Second}).Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("worker registration returned %s", response.Status)
	}
	return nil
}

func (w *StatelessWorker) consumeAllocations(leaderAPI string) error {
	conn, err := grpc.NewClient(leaderAPI, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	stream, err := grpcv1.NewSluiceInternalClient(conn).AllocationPush(
		w.ctx, &grpcv1.AllocationSubscribe{NodeId: w.cfg.NodeID},
	)
	if err != nil {
		return err
	}
	for {
		plan, err := stream.Recv()
		if err != nil {
			return err
		}
		if w.draining.Load() {
			continue
		}
		desired := make(map[string]int, len(plan.Tenants))
		for _, tenant := range plan.Tenants {
			if tenant.Workers > 0 {
				desired[tenant.TenantId] = int(tenant.Workers)
			}
		}
		w.pool.Reconcile(desired)
	}
}
