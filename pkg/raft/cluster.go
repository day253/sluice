package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"

	"github.com/distributed-rate-limiting/pkg/types"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// ClusterConfig holds all parameters needed to bootstrap a Raft node.
type ClusterConfig struct {
	NodeID      string // unique node identifier
	RaftAddress string // e.g. "192.168.1.10:7000" — Raft RPC
	DataDir     string // directory for Raft logs, stable store, snapshots
	Bootstrap   bool   // true = create a single-node cluster
	Logger      *zap.Logger
}

// ---------------------------------------------------------------------------
// Cluster wraps hashicorp/raft
// ---------------------------------------------------------------------------

// Cluster manages the lifecycle of a Raft node.
type Cluster struct {
	raft      *raft.Raft
	fsm       *FSM
	transport *raft.NetworkTransport
	logStore  raft.LogStore
	stable    raft.StableStore
	snapDir   string
	dataDir   string
	config    ClusterConfig
	logger    *zap.Logger
}

// NewCluster creates and optionally bootstraps a Raft node.
func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	logger := cfg.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	// ---- directories ----
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir datadir: %w", err)
	}

	snapDir := filepath.Join(cfg.DataDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snapdir: %w", err)
	}

	// ---- FSM ----
	fsm := NewFSM(logger)

	// ---- log store (BoltDB) ----
	logStorePath := filepath.Join(cfg.DataDir, "raft-log.db")
	logStore, err := raftboltdb.New(raftboltdb.Options{Path: logStorePath})
	if err != nil {
		return nil, fmt.Errorf("bolt log store: %w", err)
	}

	// ---- stable store (BoltDB) ----
	stablePath := filepath.Join(cfg.DataDir, "raft-stable.db")
	stableStore, err := raftboltdb.New(raftboltdb.Options{Path: stablePath})
	if err != nil {
		_ = logStore.Close()
		return nil, fmt.Errorf("bolt stable store: %w", err)
	}

	// ---- snapshot store ----
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 3, os.Stderr)
	if err != nil {
		_ = logStore.Close()
		_ = stableStore.Close()
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// ---- transport ----
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddress)
	if err != nil {
		return nil, fmt.Errorf("resolve raft address %s: %w", cfg.RaftAddress, err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddress, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("tcp transport: %w", err)
	}

	// ---- Raft config ----
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.LogLevel = "INFO"

	// ---- create Raft ----
	rf, err := raft.NewRaft(raftCfg, fsm, logStore, stableStore, snapStore, transport)
	if err != nil {
		_ = transport.Close()
		_ = logStore.Close()
		_ = stableStore.Close()
		return nil, fmt.Errorf("new raft: %w", err)
	}

	c := &Cluster{
		raft:      rf,
		fsm:       fsm,
		transport: transport,
		logStore:  logStore,
		stable:    stableStore,
		snapDir:   snapDir,
		dataDir:   cfg.DataDir,
		config:    cfg,
		logger:    logger,
	}

	// ---- bootstrap ----
	if cfg.Bootstrap {
		cfgFuture := rf.BootstrapCluster(raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(cfg.NodeID),
					Address: transport.LocalAddr(),
				},
			},
		})
		if err := cfgFuture.Error(); err != nil && err != raft.ErrCantBootstrap {
			_ = c.Shutdown()
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
		logger.Info("raft: cluster bootstrapped (single node)")
	}

	return c, nil
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

// Raft returns the underlying *raft.Raft handle.
func (c *Cluster) Raft() *raft.Raft { return c.raft }

// FSM returns the state machine.
func (c *Cluster) FSM() *FSM { return c.fsm }

// IsLeader reports whether the local node is the current Raft leader.
func (c *Cluster) IsLeader() bool {
	return c.raft.State() == raft.Leader
}

// LeaderAddr returns the address of the current leader, or empty string.
func (c *Cluster) LeaderAddr() string {
	return string(c.raft.Leader())
}

// LocalAddr returns the local Raft transport address.
func (c *Cluster) LocalAddr() string {
	return string(c.transport.LocalAddr())
}

// GetRaft returns the raft instance for observers.
func (c *Cluster) GetRaft() *raft.Raft {
	return c.raft
}

// ---------------------------------------------------------------------------
// Apply helper
// ---------------------------------------------------------------------------

// ApplyCommand marshals and applies a command to the Raft log.  It blocks
// until the command is committed or the timeout is reached.
func (c *Cluster) ApplyCommand(op string, data interface{}, timeout time.Duration) (interface{}, error) {
	cmdBytes := MustMarshalCommand(op, data)
	future := c.raft.Apply(cmdBytes, timeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf("raft apply %s: %w", op, err)
	}
	return future.Response(), nil
}

// RegisterNode applies an OpNodeUp to announce this node's presence.
func (c *Cluster) RegisterNode(httpAddr string, totalWorkers int) error {
	ni := types.NodeInfo{
		ID:           c.config.NodeID,
		Address:      httpAddr,
		RaftAddress:  c.config.RaftAddress,
		Status:       types.NodeStatusUp,
		TotalWorkers: totalWorkers,
	}
	_, err := c.ApplyCommand(OpNodeUp, ni, 5*time.Second)
	return err
}

// AddVoter adds a new voting member to the cluster.  Call this on the
// leader when a new node wants to join.
func (c *Cluster) AddVoter(nodeID, raftAddr string) error {
	future := c.raft.AddVoter(
		raft.ServerID(nodeID),
		raft.ServerAddress(raftAddr),
		0, 0,
	)
	return future.Error()
}

// RemoveServer removes a server from the cluster.
func (c *Cluster) RemoveServer(nodeID string) error {
	future := c.raft.RemoveServer(raft.ServerID(nodeID), 0, 0)
	return future.Error()
}

// ---------------------------------------------------------------------------
// Join
// ---------------------------------------------------------------------------

// JoinRequest is sent via HTTP to an existing cluster member to request
// joining the Raft cluster.
type JoinRequest struct {
	NodeID      string `json:"node_id"`
	RaftAddress string `json:"raft_address"`
	HTTPAddress string `json:"http_address"`
	TotalWorkers int   `json:"total_workers"`
}

// JoinResponse is the reply from the leader.
type JoinResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// MarshalJoinRequest serialises a join request to JSON.
func MarshalJoinRequest(req JoinRequest) []byte {
	b, _ := json.Marshal(req)
	return b
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

// Shutdown gracefully stops the Raft node.
func (c *Cluster) Shutdown() error {
	var errs []error

	if err := c.raft.Shutdown().Error(); err != nil {
		errs = append(errs, fmt.Errorf("raft shutdown: %w", err))
	}
	if err := c.transport.Close(); err != nil {
		errs = append(errs, fmt.Errorf("transport close: %w", err))
	}
	if err := c.logStore.(*raftboltdb.BoltStore).Close(); err != nil {
		errs = append(errs, fmt.Errorf("logstore close: %w", err))
	}
	if err := c.stable.(*raftboltdb.BoltStore).Close(); err != nil {
		errs = append(errs, fmt.Errorf("stable close: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}

// LeaderCh returns a channel that receives true when this node becomes
// leader and false when it steps down.
func (c *Cluster) LeaderCh() <-chan bool {
	return c.raft.LeaderCh()
}
