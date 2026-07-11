// Package tenant provides an abstraction over tenant CRUD operations that
// are routed through the Raft log.
package tenant

import (
	"time"

	"go.uber.org/zap"

	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// Manager provides tenant lifecycle operations.  All mutations go through
// the Raft leader; reads are served from the local FSM.
type Manager struct {
	fsm    *raftpkg.FSM
	raft   raftpkg.RaftApplier
	logger *zap.Logger
}

// NewManager creates a tenant manager.
func NewManager(fsm *raftpkg.FSM, raft raftpkg.RaftApplier, logger *zap.Logger) *Manager {
	return &Manager{fsm: fsm, raft: raft, logger: logger}
}

// Upsert creates or updates a tenant configuration.  The call blocks until
// the Raft log entry is committed.
func (m *Manager) Upsert(id, name string, maxWorkers int) error {
	tc := types.TenantConfig{
		ID:         id,
		Name:       name,
		MaxWorkers: maxWorkers,
	}
	result := m.raft.Apply(raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, tc), 5000)
	if err := result.Error(); err != nil {
		return err
	}
	m.logger.Info("tenant upserted via raft",
		zap.String("tenant", id),
		zap.Int("max_workers", maxWorkers),
	)
	return nil
}

// Delete removes a tenant.  In-flight tasks for this tenant will complete
// normally; no new tasks will be accepted.
func (m *Manager) Delete(id string) error {
	data := raftpkg.DeleteTenantData{ID: id}
	result := m.raft.Apply(raftpkg.MustMarshalCommand(raftpkg.OpDeleteTenant, data), 5000)
	return result.Error()
}

// Get returns a tenant configuration (local read, no Raft overhead).
func (m *Manager) Get(id string) (*types.TenantConfig, bool) {
	return m.fsm.GetTenant(id)
}

// List returns all tenant configurations.
func (m *Manager) List() map[string]*types.TenantConfig {
	return m.fsm.GetAllTenants()
}

// GetRaft returns the raft applier for direct use.
func (m *Manager) Raft() raftpkg.RaftApplier {
	return m.raft
}

// FSM returns the FSM for direct reads.
func (m *Manager) FSM() *raftpkg.FSM {
	return m.fsm
}

// WaitForLeader blocks until there is a Raft leader or the timeout expires.
func (m *Manager) WaitForLeader(timeout time.Duration) bool {
	deadline := time.After(timeout)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			return false
		case <-tick.C:
			if m.raft.IsLeader() || m.raft.LeaderAddr() != "" {
				return true
			}
		}
	}
}
