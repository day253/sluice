// Package worker manages the per-tenant goroutine pools that dequeue tasks,
// claim them via Raft, execute the business logic, and publish results.
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
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

// StealableClaimer is implemented by the streaming claimer. It lets an idle
// worker explicitly ask the leader to admit an aged task from another tenant.
type StealableClaimer interface {
	ClaimSteal(taskID, tenantID, payload string) (bool, error)
}

const workStealThreshold = 5 * time.Second

// TaskAssigner is the production scheduling boundary. Workers report an idle
// slot; the leader chooses and durably claims the concrete task. supported is
// false only during a rolling upgrade against a legacy leader.
type TaskAssigner interface {
	Assign(ctx context.Context, preferredTenantID string) (task *types.TaskRecord, supported bool, err error)
}

// Completer publishes task results through the current Raft leader.
type Completer interface {
	Complete(taskID, tenantID, result, errStr string, failed bool) error
}

// Pool manages worker goroutines organised by tenant.
type Pool struct {
	nodeID string

	mu     sync.Mutex
	groups map[string]*tenantGroup // tenantID → group

	queue            queue.Queue
	fsm              *raftpkg.FSM
	raft             raftpkg.RaftApplier
	claimer          Claimer     // nil = use raft.Apply directly
	completer        Completer   // nil = use raft.Apply directly
	workerGuard      func() bool // nil = always run
	legacyScheduling bool        // rolling compatibility for replicated combined nodes
	processor        Processor
	logger           *zap.Logger

	activeMu sync.Mutex
	active   map[string]struct{} // task IDs currently being claimed or processed locally

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
	retires  []chan struct{} // one per worker still eligible to request work
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
		nodeID:           nodeID,
		groups:           make(map[string]*tenantGroup),
		active:           make(map[string]struct{}),
		queue:            q,
		fsm:              fsm,
		raft:             raft,
		processor:        proc,
		legacyScheduling: true,
		logger:           logger,
		ctx:              ctx,
		cancel:           cancel,
	}
}

// SetClaimer sets a streaming claim client.  When set, workers use this
// instead of raft.Apply so followers' workers can claim via the leader.
func (p *Pool) SetClaimer(c Claimer) { p.claimer = c }

// SetCompleter configures leader-forwarded result commits.
func (p *Pool) SetCompleter(c Completer) { p.completer = c }

// SetWorkerGuard gates worker processing. Nodes use it to ensure the Raft
// leader remains control-plane only and never requests or executes work.
func (p *Pool) SetWorkerGuard(fn func() bool) { p.workerGuard = fn }

// DisableLegacyScheduling makes this a strictly stateless Worker pool. It
// never reads a local Queue/FSM and waits for Leader-owned assignments.
func (p *Pool) DisableLegacyScheduling() { p.legacyScheduling = false }

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

// Drain retires every worker without canceling an already-started Processor.
// It is used by stateless Worker shutdown so normal rollouts commit results
// instead of leaving them for claim-lease recovery.
func (p *Pool) Drain(timeout time.Duration) error {
	p.Reconcile(map[string]int{})
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		p.cancel()
		return nil
	case <-time.After(timeout):
		p.cancel()
		return fmt.Errorf("worker pool drain timed out after %s", timeout)
	}
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

func (p *Pool) spawnWorkers(grp *tenantGroup, count int) {
	for i := 0; i < count; i++ {
		retire := make(chan struct{})
		grp.retires = append(grp.retires, retire)
		grp.current++

		p.wg.Add(1)
		go p.workerLoop(p.ctx, retire, grp.tenantID)
	}
}

func (p *Pool) killWorkers(grp *tenantGroup, count int) {
	// Retire the newest workers. Retirement stops the next assignment request
	// but deliberately does not cancel an already claimed business task; only
	// Pool.Shutdown/leadership shutdown cancels the shared hard-stop context.
	for i := 0; i < count && len(grp.retires) > 0; i++ {
		retire := grp.retires[len(grp.retires)-1]
		grp.retires = grp.retires[:len(grp.retires)-1]
		close(retire)
		grp.current--
	}
}

// workerLoop is the main goroutine for a single worker.
func (p *Pool) workerLoop(ctx context.Context, retire <-chan struct{}, tenantID string) {
	defer func() {
		p.wg.Done()
	}()

	logger := p.logger.With(zap.String("tenant", tenantID), zap.String("node", p.nodeID))
	logger.Debug("worker started")

	for {
		select {
		case <-ctx.Done():
			logger.Debug("worker exiting")
			return
		case <-retire:
			logger.Debug("worker retired")
			return
		default:
		}
		if p.workerGuard != nil && !p.workerGuard() {
			select {
			case <-ctx.Done():
				return
			case <-retire:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		var task *types.TaskRecord
		steal := false
		reserved := false
		leaderAssigned := false
		legacyScheduling := true

		// Production path: the worker reports one idle slot, and only the Raft
		// leader chooses and claims the concrete task.
		if assigner, ok := p.claimer.(TaskAssigner); ok {
			assigned, supported, err := assigner.Assign(ctx, tenantID)
			if supported {
				legacyScheduling = false
				if err != nil {
					logger.Debug("leader assignment unavailable", zap.Error(err))
					select {
					case <-ctx.Done():
						return
					case <-time.After(100 * time.Millisecond):
					}
					continue
				}
				task = assigned
				leaderAssigned = task != nil
			} else if !p.legacyScheduling {
				legacyScheduling = false
			}
		}

		if legacyScheduling {
			// Compatibility path used only while rolling against an older leader.
			// 1. Try local queue first.
			task = p.dequeueLocal(tenantID)

			// 2. Prefer another tenant's queue on this same node.
			if task == nil {
				task = p.dequeueLocalOtherTenant(tenantID)
				steal = task != nil
			}

			// 3. Only the leader performs the normal global recovery scan.
			if task == nil {
				task = p.findRecoveryTask(tenantID)
				reserved = task != nil
			}

			// 4. Legacy cross-node work steal.
			if task == nil {
				task = p.findStealTask(tenantID)
				steal = task != nil
				reserved = task != nil
			}
		}

		if task == nil {
			// Nothing to do — sleep and retry.
			select {
			case <-ctx.Done():
				return
			case <-retire:
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		// Leadership/allocation may have changed while Assign was blocked. Never
		// begin business execution after this worker has been cancelled or gated.
		if ctx.Err() != nil || (p.workerGuard != nil && !p.workerGuard()) {
			return
		}
		if !reserved && !p.reserveTask(task.TaskID) {
			continue
		}
		if !leaderAssigned {
			// Legacy clients still claim a worker-selected task during rollout.
			if err := p.claimTask(task, steal); err != nil {
				p.releaseTask(task.TaskID)
				logger.Warn("failed to claim task", zap.String("task_id", task.TaskID), zap.Error(err))
				select {
				case <-ctx.Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
				continue
			}
		}

		// 5. Process the task (context-aware).
		result, err := p.processor.Process(ctx, task.TaskID, task.TenantID, json.RawMessage(task.Payload))
		if err != nil && ctx.Err() != nil {
			// A rollout interrupted execution. Leave the claim unfinished so the
			// leader lease scanner can return it to pending for retry.
			p.releaseTask(task.TaskID)
			logger.Warn("task interrupted; waiting for lease recovery",
				zap.String("task_id", task.TaskID), zap.Error(err))
			return
		}

		// 6. Publish result via Raft.
		var completeErr error
		if err != nil {
			completeErr = p.completeTask(task.TaskID, task.TenantID, "", err.Error(), true)
			logger.Warn("task failed",
				zap.String("task_id", task.TaskID),
				zap.Error(err),
			)
		} else {
			completeErr = p.completeTask(task.TaskID, task.TenantID, result, "", false)
			logger.Debug("task completed", zap.String("task_id", task.TaskID))
		}
		p.releaseTask(task.TaskID)
		if completeErr != nil {
			logger.Error("task result was not committed; lease recovery will retry the task",
				zap.String("task_id", task.TaskID), zap.Error(completeErr))
		}
		// A scale-down that raced with this assignment is graceful: commit the
		// final state above, then retire without requesting another task.
		select {
		case <-retire:
			logger.Debug("worker retired after completing in-flight task")
			return
		default:
		}
	}
}

// dequeueLocal tries to dequeue a task from the local queue.
func (p *Pool) dequeueLocal(tenantID string) *types.TaskRecord {
	if p.queue == nil {
		return nil
	}
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

// dequeueLocalOtherTenant scans node-local queues in deterministic tenant
// order. A worker first gets a chance to consume its assigned tenant, then
// reuses the same node's idle capacity for another tenant's local backlog.
func (p *Pool) dequeueLocalOtherTenant(tenantID string) *types.TaskRecord {
	if p.fsm == nil {
		return nil
	}
	tenants := p.fsm.GetAllTenants()
	ids := make([]string, 0, len(tenants))
	for id := range tenants {
		if id != tenantID {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		if task := p.dequeueLocal(id); task != nil {
			return task
		}
	}
	return nil
}

// findRecoveryTask scans the FSM for tasks that were re-queued after a node
// failure and tries to claim one.
func (p *Pool) findRecoveryTask(tenantID string) *types.TaskRecord {
	if p.raft == nil || !p.raft.IsLeader() {
		return nil
	}
	tasks := p.fsm.FindPendingTasks(tenantID)
	for _, task := range tasks {
		if p.reserveTask(task.TaskID) {
			return task
		}
	}
	return nil
}

func (p *Pool) findStealTask(tenantID string) *types.TaskRecord {
	tasks := p.fsm.FindStealablePendingTasks(tenantID, time.Now().UTC().Add(-workStealThreshold))
	for _, task := range tasks {
		if p.reserveTask(task.TaskID) {
			return task
		}
	}
	return nil
}

// claimTask claims a task.  Uses streaming claim client if available
// (works from any node); falls back to direct raft.Apply (leader only).
func (p *Pool) claimTask(task *types.TaskRecord, steal bool) error {
	if p.claimer != nil {
		var ok bool
		var err error
		if steal {
			if sc, supported := p.claimer.(StealableClaimer); supported {
				ok, err = sc.ClaimSteal(task.TaskID, task.TenantID, task.Payload)
			} else {
				ok, err = p.claimer.Claim(task.TaskID, task.TenantID, task.Payload)
			}
		} else {
			ok, err = p.claimer.Claim(task.TaskID, task.TenantID, task.Payload)
		}
		if err != nil {
			p.logger.Warn("claim stream failed, trying raft", zap.Error(err))
		} else if ok {
			return nil
		} else {
			return fmt.Errorf("claim rejected")
		}
	}
	data := raftpkg.ClaimTaskData{
		TaskID:   task.TaskID,
		TenantID: task.TenantID,
		NodeID:   p.nodeID,
		Payload:  task.Payload,
		Steal:    steal,
	}
	cmd := raftpkg.MustMarshalCommand(raftpkg.OpClaimTask, data)
	return p.raft.Apply(cmd, 5000).Error()
}

// completeTask publishes the final result (done or failed) to Raft.
func (p *Pool) completeTask(taskID, tenantID, result, errStr string, failed bool) error {
	if p.completer != nil {
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if lastErr = p.completer.Complete(taskID, tenantID, result, errStr, failed); lastErr == nil {
				return nil
			}
			if attempt < 2 {
				select {
				case <-p.ctx.Done():
					attempt = 2
				case <-time.After(time.Duration(attempt+1) * 300 * time.Millisecond):
				}
			}
		}
		p.logger.Warn("result stream failed, trying raft", zap.Error(lastErr))
	}
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
	return p.raft.Apply(cmd, 5000).Error()
}

func (p *Pool) reserveTask(taskID string) bool {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if _, exists := p.active[taskID]; exists {
		return false
	}
	p.active[taskID] = struct{}{}
	return true
}

func (p *Pool) releaseTask(taskID string) {
	p.activeMu.Lock()
	delete(p.active, taskID)
	p.activeMu.Unlock()
}
