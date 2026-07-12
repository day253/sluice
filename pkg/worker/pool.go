// Package worker manages the per-tenant goroutine pools that dequeue tasks,
// claim them via Raft, execute the business logic, and publish results.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/day253/sluice/pkg/queue"
	raftpkg "github.com/day253/sluice/pkg/raft"
	"github.com/day253/sluice/pkg/types"
)

// ---------------------------------------------------------------------------
// Processor — pluggable business logic
// ---------------------------------------------------------------------------

// Processor is the interface a consumer must implement.  Given a task
// payload (raw JSON as delivered by the client), it returns a result string
// or an error.
type Processor interface {
	Process(ctx context.Context, taskID, tenantID string, payload json.RawMessage) (result string, err error)
}

// ---------------------------------------------------------------------------
// Pool
// ---------------------------------------------------------------------------

// Claimer abstracts task claiming.  A local raft.Apply works on the
// leader; a gRPC streaming client works from any node.
type Claimer interface {
	Claim(taskID, tenantID, payload string) (bool, error)
}

// Pool manages worker goroutines organised by tenant.
type Pool struct {
	nodeID string

	mu     sync.Mutex
	groups map[string]*tenantGroup // tenantID → group

	queue     queue.Queue
	fsm       *raftpkg.FSM
	raft        raftpkg.RaftApplier
	claimer     Claimer     // nil = use raft.Apply directly
	workerGuard func() bool // nil = always run
	processor   Processor
	logger    *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// tenantGroup tracks the desired and actual worker count for one tenant on
// this node.
type tenantGroup struct {
	tenantID string
	desired  int
	current  int
	cancels  []context.CancelFunc // one per running worker
}

// ---------------------------------------------------------------------------
// NewPool
// ---------------------------------------------------------------------------

// NewPool creates a worker pool ready to be reconciled.
func NewPool(
	nodeID string,
	q queue.Queue,
	fsm *raftpkg.FSM,
	raft raftpkg.RaftApplier,
	proc Processor,
	logger *zap.Logger,
) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		nodeID:    nodeID,
		groups:    make(map[string]*tenantGroup),
		queue:     q,
		fsm:       fsm,
		raft:      raft,
		processor: proc,
		logger:    logger,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// SetClaimer sets a streaming claim client.  When set, workers use this
// instead of raft.Apply so followers' workers can claim via the leader.
func (p *Pool) SetClaimer(c Claimer) { p.claimer = c }

// SetWorkerGuard gates worker processing.  Followers pass `IsLeader` so
// only the leader processes tasks.
func (p *Pool) SetWorkerGuard(fn func() bool) { p.workerGuard = fn }

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

// Reconcile adjusts the running worker count so that it matches the supplied
// desired allocation map (tenantID → count).
//
// It is safe to call Reconcile concurrently and repeatedly; only the delta
// between desired and current is acted upon.
func (p *Pool) Reconcile(desired map[string]int) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Mark absent tenants as desired=0 so their workers get killed.
	for tid, grp := range p.groups {
		if _, ok := desired[tid]; !ok {
			grp.desired = 0
		}
	}

	// Spawn or kill workers per tenant.
	for tenantID, want := range desired {
		grp, ok := p.groups[tenantID]
		if !ok {
			grp = &tenantGroup{tenantID: tenantID}
			p.groups[tenantID] = grp
		}
		grp.desired = want
	}
	// Also handle absent tenants (desired=0 set above).
	for _, grp := range p.groups {
		delta := grp.desired - grp.current
		if delta > 0 {
			p.spawnWorkers(grp, delta)
		} else if delta < 0 {
			p.killWorkers(grp, -delta)
		}
	}

	// Remove groups that have been fully drained.
	for tenantID, grp := range p.groups {
		if grp.desired == 0 && grp.current == 0 {
			delete(p.groups, tenantID)
		}
	}
}

// ---------------------------------------------------------------------------
// GetStatus
// ---------------------------------------------------------------------------

// GetStatus returns the current worker count per tenant.
func (p *Pool) GetStatus() map[string]int {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]int, len(p.groups))
	for tid, g := range p.groups {
		out[tid] = g.current
	}
	return out
}

// ---------------------------------------------------------------------------
// Shutdown
// ---------------------------------------------------------------------------

// Shutdown gracefully stops all workers and waits for in-flight tasks to
// complete (subject to context timeout).
func (p *Pool) Shutdown(timeout time.Duration) error {
	p.cancel()

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("worker pool shutdown timed out after %s", timeout)
	}
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func (p *Pool) spawnWorkers(grp *tenantGroup, count int) {
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithCancel(p.ctx)
		grp.cancels = append(grp.cancels, cancel)
		grp.current++

		p.wg.Add(1)
		go p.workerLoop(ctx, cancel, grp.tenantID)
	}
}

func (p *Pool) killWorkers(grp *tenantGroup, count int) {
	// Cancel the oldest workers; they will finish their current task and exit.
	for i := 0; i < count && len(grp.cancels) > 0; i++ {
		// Cancel the last (newest) worker.
		cancel := grp.cancels[len(grp.cancels)-1]
		grp.cancels = grp.cancels[:len(grp.cancels)-1]
		cancel()
		grp.current--
	}
}

// workerLoop is the main goroutine for a single worker.
func (p *Pool) workerLoop(ctx context.Context, cancel context.CancelFunc, tenantID string) {
	defer func() {
		cancel()
		p.wg.Done()
	}()

	logger := p.logger.With(zap.String("tenant", tenantID), zap.String("node", p.nodeID))
	logger.Debug("worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Debug("worker exiting")
			return
		default:
		}

		// 1. Try local queue first.
		task := p.dequeueLocal(tenantID)

		// 2. If local is empty, scan FSM for recovery-pending tasks.
		if task == nil {
			task = p.findRecoveryTask(tenantID)
		}

		if task == nil {
			// Nothing to do — sleep and retry.
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		// 3. Claim the task via Raft.
		if err := p.claimTask(task); err != nil {
			logger.Warn("failed to claim task", zap.String("task_id", task.TaskID), zap.Error(err))
			continue
		}

		// 4. Process the task (context-aware).
		result, err := p.processor.Process(ctx, task.TaskID, task.TenantID, json.RawMessage(task.Payload))

		// 5. Publish result via Raft.
		if err != nil {
			p.completeTask(task.TaskID, task.TenantID, "", err.Error(), true)
			logger.Warn("task failed",
				zap.String("task_id", task.TaskID),
				zap.Error(err),
			)
		} else {
			p.completeTask(task.TaskID, task.TenantID, result, "", false)
			logger.Debug("task completed", zap.String("task_id", task.TaskID))
		}
	}
}

// dequeueLocal tries to dequeue a task from the local queue.
func (p *Pool) dequeueLocal(tenantID string) *types.TaskRecord {
	env, err := p.queue.Dequeue(tenantID)
	if err != nil {
		p.logger.Error("dequeue error", zap.String("tenant", tenantID), zap.Error(err))
		return nil
	}
	if env == nil {
		return nil
	}
	raw, _ := json.Marshal(env.Payload)
	return &types.TaskRecord{
		TaskID:   env.TaskID,
		TenantID: env.TenantID,
		Payload:  string(raw),
	}
}

// findRecoveryTask scans the FSM for tasks that were re-queued after a node
// failure and tries to claim one.
func (p *Pool) findRecoveryTask(tenantID string) *types.TaskRecord {
	tasks := p.fsm.FindPendingTasks(tenantID)
	if len(tasks) == 0 {
		return nil
	}
	// Try the first one (oldest).
	return tasks[0]
}

// claimTask claims a task.  Uses streaming claim client if available
// (works from any node); falls back to direct raft.Apply (leader only).
func (p *Pool) claimTask(task *types.TaskRecord) error {
	if p.claimer != nil {
		ok, err := p.claimer.Claim(task.TaskID, task.TenantID, task.Payload)
		if err != nil {
			p.logger.Warn("claim stream failed, trying raft", zap.Error(err))
		} else if ok {
			return nil
		}
	}
	data := raftpkg.ClaimTaskData{
		TaskID:   task.TaskID,
		TenantID: task.TenantID,
		NodeID:   p.nodeID,
		Payload:  task.Payload,
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpClaimTask, data)
	return p.raft.Apply(cmd, 5000).Error()
}

// completeTask publishes the final result (done or failed) to Raft.
func (p *Pool) completeTask(taskID, tenantID, result, errStr string, failed bool) {
	op := raftpkg.OpCompleteTask
	if failed {
		op = raftpkg.OpFailTask
	}
	data := raftpkg.CompleteTaskData{
		TaskID:   taskID,
		TenantID: tenantID,
		Result:   result,
		Error:    errStr,
	}
	cmd := raftpkg.MustMarshalCommand(op, data)
	_ = p.raft.Apply(cmd, 5000) // best-effort; log errors if needed
}
