package raft

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/types"
)

// DefaultMaxVoters keeps one Raft shard within the conventional 3-5 voter
// range. Additional execution replicas receive every log as non-voters but do
// not extend the quorum critical path.
const DefaultMaxVoters = 5

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

// ClusterConfig holds all parameters needed to bootstrap a Raft node.
type ClusterConfig struct {
	NodeID          string // unique node identifier
	RaftAddress     string // stable Raft RPC address advertised to peers
	RaftBindAddress string // local listen address; defaults to RaftAddress
	DataDir         string // directory for Raft logs, stable store, snapshots
	Bootstrap       bool   // true = create a single-node cluster
	Logger          *zap.Logger
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

	membershipMu sync.Mutex
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
	bindAddress := cfg.RaftBindAddress
	if bindAddress == "" {
		bindAddress = cfg.RaftAddress
	}
	transport, err := raft.NewTCPTransport(bindAddress, addr, 3, 10*time.Second, os.Stderr)
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
					ID: raft.ServerID(cfg.NodeID),
					// Keep the configured address instead of the local bind address.
					// In Kubernetes this is a stable per-Pod Service IP; persisting
					// the resolved Pod IP breaks Raft after a rollout.
					Address: raft.ServerAddress(cfg.RaftAddress),
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
	return c.RegisterNodeWithRole(httpAddr, totalWorkers, "")
}

// RegisterNodeWithRole publishes the current control-plane identity without
// coupling Raft membership to execution capacity.
func (c *Cluster) RegisterNodeWithRole(httpAddr string, totalWorkers int, role string) error {
	ni := types.NodeInfo{
		ID:           c.config.NodeID,
		Role:         role,
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

// AddServer adds or repairs a member while bounding the voting quorum.
// Existing suffrage is preserved across address refreshes. A genuinely new
// member becomes a voter only while the configured voter set has capacity.
func (c *Cluster) AddServer(nodeID, raftAddr string, maxVoters int) error {
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	if err := validateMaxVoters(maxVoters); err != nil {
		return err
	}
	configuration, err := c.configurationLocked()
	if err != nil {
		return err
	}
	voters := 0
	for _, server := range configuration.Servers {
		if server.Suffrage == raft.Voter {
			voters++
		}
		if server.ID != raft.ServerID(nodeID) {
			continue
		}
		if server.Address == raft.ServerAddress(raftAddr) {
			return nil
		}
		if server.Suffrage == raft.Voter {
			return c.raft.AddVoter(server.ID, raft.ServerAddress(raftAddr), 0, 0).Error()
		}
		return c.raft.AddNonvoter(server.ID, raft.ServerAddress(raftAddr), 0, 0).Error()
	}
	if voters < maxVoters {
		return c.raft.AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0).Error()
	}
	return c.raft.AddNonvoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, 0).Error()
}

// MembershipStatus is the read-only Raft configuration exposed for
// operations and regression assertions.
type MembershipStatus struct {
	LeaderID  string   `json:"leader_id"`
	Voters    []string `json:"voters"`
	Nonvoters []string `json:"nonvoters"`
}

// VoterReconcileResult describes one bounded-voter reconciliation attempt.
// A leadership transfer intentionally ends the attempt; the selected new
// leader performs the demotions on its next reconciliation tick.
type VoterReconcileResult struct {
	Changed               bool
	LeadershipTransferred bool
	Status                MembershipStatus
}

// MemberPruneResult identifies replicas removed after execution has been
// split into stateless Workers.
type MemberPruneResult struct {
	Changed bool
	Removed []string
}

func validateMaxVoters(maxVoters int) error {
	if maxVoters < 1 || maxVoters%2 == 0 {
		return fmt.Errorf("max Raft voters must be a positive odd number, got %d", maxVoters)
	}
	return nil
}

func (c *Cluster) configurationLocked() (raft.Configuration, error) {
	future := c.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return future.Configuration(), nil
}

// Configuration returns the local node's latest Raft membership view.
func (c *Cluster) Configuration() (raft.Configuration, error) {
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	return c.configurationLocked()
}

func trailingOrdinal(id string) (prefix string, ordinal int, ok bool) {
	index := strings.LastIndexByte(id, '-')
	if index < 0 || index == len(id)-1 {
		return "", 0, false
	}
	value, err := strconv.Atoi(id[index+1:])
	if err != nil {
		return "", 0, false
	}
	return id[:index], value, true
}

func stableServerLess(a, b raft.Server) bool {
	ap, ai, aok := trailingOrdinal(string(a.ID))
	bp, bi, bok := trailingOrdinal(string(b.ID))
	if aok && bok && ap == bp && ai != bi {
		return ai < bi
	}
	return a.ID < b.ID
}

func sortedServers(configuration raft.Configuration) []raft.Server {
	servers := append([]raft.Server(nil), configuration.Servers...)
	sort.Slice(servers, func(i, j int) bool { return stableServerLess(servers[i], servers[j]) })
	return servers
}

func desiredVoterIDs(configuration raft.Configuration, maxVoters int) map[raft.ServerID]struct{} {
	servers := sortedServers(configuration)
	if len(servers) > maxVoters {
		servers = servers[:maxVoters]
	}
	desired := make(map[raft.ServerID]struct{}, len(servers))
	for _, server := range servers {
		desired[server.ID] = struct{}{}
	}
	return desired
}

func membershipStatus(configuration raft.Configuration, leaderID raft.ServerID) MembershipStatus {
	status := MembershipStatus{LeaderID: string(leaderID)}
	for _, server := range sortedServers(configuration) {
		if server.Suffrage == raft.Voter {
			status.Voters = append(status.Voters, string(server.ID))
		} else {
			status.Nonvoters = append(status.Nonvoters, string(server.ID))
		}
	}
	return status
}

// MembershipStatus returns voter and non-voter IDs in stable ordinal order.
func (c *Cluster) MembershipStatus() (MembershipStatus, error) {
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	configuration, err := c.configurationLocked()
	if err != nil {
		return MembershipStatus{}, err
	}
	_, leaderID := c.raft.LeaderWithID()
	return membershipStatus(configuration, leaderID), nil
}

// ReconcileVoters migrates an existing oversized voter set without removing
// FSM replicas. Stable lowest-ordinal members remain voters; all others become
// non-voters. If this leader is outside the desired set, leadership is first
// transferred to a desired voter and the next leader finishes reconciliation.
func (c *Cluster) ReconcileVoters(maxVoters int) (VoterReconcileResult, error) {
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	if err := validateMaxVoters(maxVoters); err != nil {
		return VoterReconcileResult{}, err
	}
	if !c.IsLeader() {
		return VoterReconcileResult{}, raft.ErrNotLeader
	}
	configuration, err := c.configurationLocked()
	if err != nil {
		return VoterReconcileResult{}, err
	}
	desired := desiredVoterIDs(configuration, maxVoters)
	servers := sortedServers(configuration)
	changed := false
	for _, server := range servers {
		if _, keep := desired[server.ID]; !keep || server.Suffrage == raft.Voter {
			continue
		}
		if err := c.raft.AddVoter(server.ID, server.Address, 0, 0).Error(); err != nil {
			return VoterReconcileResult{}, fmt.Errorf("promote desired voter %s: %w", server.ID, err)
		}
		changed = true
	}
	_, leaderID := c.raft.LeaderWithID()
	if _, keep := desired[leaderID]; !keep {
		for _, server := range servers {
			if _, target := desired[server.ID]; !target {
				continue
			}
			if err := c.raft.LeadershipTransferToServer(server.ID, server.Address).Error(); err != nil {
				return VoterReconcileResult{}, fmt.Errorf("transfer leadership to %s: %w", server.ID, err)
			}
			return VoterReconcileResult{
				Changed: true, LeadershipTransferred: true,
				Status: membershipStatus(configuration, server.ID),
			}, nil
		}
	}
	for _, server := range servers {
		if server.Suffrage != raft.Voter {
			continue
		}
		if _, keep := desired[server.ID]; keep {
			continue
		}
		if err := c.raft.DemoteVoter(server.ID, 0, 0).Error(); err != nil {
			return VoterReconcileResult{}, fmt.Errorf("demote voter %s: %w", server.ID, err)
		}
		changed = true
	}
	configuration, err = c.configurationLocked()
	if err != nil {
		return VoterReconcileResult{}, err
	}
	_, leaderID = c.raft.LeaderWithID()
	return VoterReconcileResult{Changed: changed, Status: membershipStatus(configuration, leaderID)}, nil
}

// PruneMembers bounds total Raft replication independently from Worker count.
// Stable lowest-ordinal members are retained; callers must run voter
// reconciliation first so the retained set already contains a healthy quorum.
func (c *Cluster) PruneMembers(maxMembers int) (MemberPruneResult, error) {
	if maxMembers < 1 {
		return MemberPruneResult{}, fmt.Errorf("max Raft members must be positive, got %d", maxMembers)
	}
	c.membershipMu.Lock()
	defer c.membershipMu.Unlock()
	if !c.IsLeader() {
		return MemberPruneResult{}, raft.ErrNotLeader
	}
	configuration, err := c.configurationLocked()
	if err != nil {
		return MemberPruneResult{}, err
	}
	servers := sortedServers(configuration)
	if len(servers) <= maxMembers {
		return MemberPruneResult{}, nil
	}
	keep := make(map[raft.ServerID]struct{}, maxMembers)
	for _, server := range servers[:maxMembers] {
		keep[server.ID] = struct{}{}
	}
	_, leaderID := c.raft.LeaderWithID()
	if _, ok := keep[leaderID]; !ok {
		return MemberPruneResult{}, fmt.Errorf("refusing to prune current leader %s outside retained set", leaderID)
	}
	result := MemberPruneResult{}
	for _, server := range servers[maxMembers:] {
		if server.Suffrage == raft.Voter {
			if err := c.raft.DemoteVoter(server.ID, 0, 0).Error(); err != nil {
				return result, fmt.Errorf("demote pruned voter %s: %w", server.ID, err)
			}
		}
		if err := c.raft.RemoveServer(server.ID, 0, 0).Error(); err != nil {
			return result, fmt.Errorf("remove Raft replica %s: %w", server.ID, err)
		}
		result.Changed = true
		result.Removed = append(result.Removed, string(server.ID))
	}
	return result, nil
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
	NodeID       string `json:"node_id"`
	RaftAddress  string `json:"raft_address"`
	HTTPAddress  string `json:"http_address"`
	TotalWorkers int    `json:"total_workers"`
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
