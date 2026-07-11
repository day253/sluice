// Command rate-limiter starts a single node of the distributed rate-limiting
// cluster.  It can bootstrap a new cluster or join an existing one.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/day253/sluice/pkg/node"
)

// ---------------------------------------------------------------------------
// Flags
// ---------------------------------------------------------------------------

var (
	nodeID       = flag.String("id", "node-1", "Unique node identifier")
	httpAddr     = flag.String("http", "127.0.0.1:8080", "HTTP API listen address")
	grpcAddr     = flag.String("grpc", "127.0.0.1:9090", "gRPC listen address (empty to disable)")
	grpcInternal = flag.String("grpc-internal", "127.0.0.1:9091", "Internal gRPC (empty to disable)")
	raftAddr     = flag.String("raft", "127.0.0.1:7000", "Raft transport address")
	dataDir      = flag.String("data", "./data", "Data directory")
	bootstrap    = flag.Bool("bootstrap", false, "Bootstrap a new single-node cluster")
	joinAddr     = flag.String("join", "", "HTTP address of an existing node to join")
	totalWorkers = flag.Int("workers", 100, "Total worker capacity on this node")
	logLevel     = flag.String("log-level", "info", "Log level: debug, info, warn, error")
)

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	flag.Parse()

	logger := newLogger(*logLevel)
	defer logger.Sync()

	// Print startup banner.
	logger.Info("distributed-rate-limiting starting",
		zap.String("id", *nodeID),
		zap.String("http", *httpAddr),
		zap.String("raft", *raftAddr),
		zap.Bool("bootstrap", *bootstrap),
		zap.Int("workers", *totalWorkers),
	)

	// ---- Build node config ----
	cfg := node.Config{
		NodeID:       *nodeID,
		HTTPAddress:  *httpAddr,
		GRPCAddress:         *grpcAddr,
		GRPCInternalAddress: *grpcInternal,
		RaftAddress:         *raftAddr,
		DataDir:      *dataDir,
		Bootstrap:    *bootstrap,
		JoinAddress:  *joinAddr,
		TotalWorkers: *totalWorkers,
	}

	// ---- Use the demo processor (replace with your own) ----
	processor := &DemoProcessor{logger: logger}

	// ---- Create node ----
	n, err := node.New(cfg, processor, logger)
	if err != nil {
		logger.Fatal("failed to create node", zap.Error(err))
	}

	// ---- Handle join (if --join is set) ----
	if *joinAddr != "" {
		if err := joinExistingCluster(*joinAddr, cfg, logger); err != nil {
			logger.Warn("join cluster attempt failed (cluster may already know this node)",
				zap.Error(err),
			)
		}
	}

	// ---- Run ----
	if err := n.Start(); err != nil {
		logger.Fatal("node exited with error", zap.Error(err))
	}

	logger.Info("goodbye")
}

// ---------------------------------------------------------------------------
// Demo processor — replace with real business logic
// ---------------------------------------------------------------------------

// DemoProcessor is a trivial task processor for demonstration purposes.  In
// a real deployment you would replace this with something that does actual
// work (HTTP call, DB query, etc.).
type DemoProcessor struct {
	logger *zap.Logger
}

// Process implements worker.Processor.
func (p *DemoProcessor) Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (string, error) {
	p.logger.Info("processing task",
		zap.String("task_id", taskID),
		zap.String("tenant", tenantID),
		zap.String("payload", string(payload)),
	)

	// Simulate work.
	select {
	case <-ctx.Done():
		return "", fmt.Errorf("task cancelled")
	default:
	}

	// The demo just returns the payload as the result.
	return fmt.Sprintf(`{"echo": %s}`, string(payload)), nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newLogger(level string) *zap.Logger {
	var lvl zapcore.Level
	switch level {
	case "debug":
		lvl = zapcore.DebugLevel
	case "info":
		lvl = zapcore.InfoLevel
	case "warn":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	default:
		lvl = zapcore.InfoLevel
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(lvl)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	logger, err := cfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	return logger
}

// joinExistingCluster sends a join request to an existing member.
func joinExistingCluster(joinAddr string, cfg node.Config, logger *zap.Logger) error {
	// In a real implementation this would make an HTTP POST to
	// http://<joinAddr>/api/v1/cluster/join with the node's details.
	// For now we log that joining is handled externally (e.g. via
	// the admin API or manual raft.AddVoter).
	logger.Info("join requested — use admin API or raft CLI to add this node as a voter",
		zap.String("join_addr", joinAddr),
		zap.String("my_raft_addr", cfg.RaftAddress),
		zap.String("my_node_id", cfg.NodeID),
	)
	return nil
}
