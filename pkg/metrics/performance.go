package metrics

import (
	"encoding/json"
	"sync"
	"time"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

// Performance stores process-local control-plane observations. These values
// are deliberately not part of the Raft FSM: feeding diagnostics back into
// consensus would add the very writes that the diagnostics are measuring.
type Performance struct {
	mu        sync.Mutex
	startedAt time.Time
	raft      map[string]*operationAggregate
	scheduler schedulerAggregate
}

type operationAggregate struct {
	Count       uint64
	Items       uint64
	Errors      uint64
	TotalMicros int64
	MaxMicros   int64
	LastMicros  int64

	windowCount       uint64
	windowItems       uint64
	windowErrors      uint64
	windowTotalMicros int64
	windowMaxMicros   int64
}

type schedulerAggregate struct {
	SelectionCount          uint64
	PendingScanned          uint64
	TasksSelected           uint64
	LoadAwareRequests       uint64
	LoadThrottledRequests   uint64
	LoadUnavailableRequests uint64
	StaleLoadRequests       uint64
	TotalSelectMicros       int64
	MaxSelectMicros         int64
	LastSelectMicros        int64
	AssignmentQueueDepth    int64
	CompletionQueueDepth    int64
	WorkerLoads             map[string]WorkerLoadSnapshot

	windowSelectionCount          uint64
	windowPendingScanned          uint64
	windowTasksSelected           uint64
	windowLoadAwareRequests       uint64
	windowLoadThrottledRequests   uint64
	windowLoadUnavailableRequests uint64
	windowStaleLoadRequests       uint64
	windowTotalSelectMicros       int64
	windowMaxSelectMicros         int64
}

type RaftOperationSnapshot struct {
	Applies       uint64 `json:"applies"`
	Items         uint64 `json:"items"`
	Errors        uint64 `json:"errors"`
	AverageMicros int64  `json:"average_us"`
	MaxMicros     int64  `json:"max_us"`
	LastMicros    int64  `json:"last_us"`
	AverageBatch  int64  `json:"average_batch"`
}

type SchedulerSnapshot struct {
	Selections              uint64                        `json:"selections"`
	PendingScanned          uint64                        `json:"pending_scanned"`
	TasksSelected           uint64                        `json:"tasks_selected"`
	LoadAwareRequests       uint64                        `json:"load_aware_requests"`
	LoadThrottledRequests   uint64                        `json:"load_throttled_requests"`
	LoadUnavailableRequests uint64                        `json:"load_unavailable_requests"`
	StaleLoadRequests       uint64                        `json:"stale_load_requests"`
	AverageSelectMicros     int64                         `json:"average_select_us"`
	MaxSelectMicros         int64                         `json:"max_select_us"`
	LastSelectMicros        int64                         `json:"last_select_us"`
	AssignmentQueueDepth    int64                         `json:"assignment_queue_depth"`
	CompletionQueueDepth    int64                         `json:"completion_queue_depth"`
	MaxWorkerCPUMillis      int64                         `json:"max_worker_cpu_millis"`
	WorkerLoads             map[string]WorkerLoadSnapshot `json:"worker_loads"`
}

// WorkerLoadSnapshot is a recent Leader-local observation. It is intentionally
// absent from the Raft FSM and from per-node historical series.
type WorkerLoadSnapshot struct {
	CPUUtilizationMillis int       `json:"cpu_utilization_millis"`
	RunningTasks         int       `json:"running_tasks"`
	Capacity             int       `json:"capacity"`
	ObservedAt           time.Time `json:"observed_at"`
}

type PerformanceSnapshot struct {
	StartedAt time.Time                        `json:"started_at"`
	Raft      map[string]RaftOperationSnapshot `json:"raft"`
	Scheduler SchedulerSnapshot                `json:"scheduler"`
}

// PerformanceDiagnostics combines the current process-local snapshot with
// bounded 174-point historical series. The endpoint is served by the current
// leader (followers proxy to it), so NodeID identifies the observation source.
type PerformanceDiagnostics struct {
	NodeID      string              `json:"node_id"`
	CollectedAt time.Time           `json:"collected_at"`
	Current     PerformanceSnapshot `json:"current"`
	History     map[string]VarData  `json:"history"`
}

type operationWindow struct {
	Applies     uint64
	Items       uint64
	Errors      uint64
	TotalMicros int64
	MaxMicros   int64
}

type schedulerWindow struct {
	Selections              uint64
	PendingScanned          uint64
	TasksSelected           uint64
	LoadAwareRequests       uint64
	LoadThrottledRequests   uint64
	LoadUnavailableRequests uint64
	StaleLoadRequests       uint64
	TotalSelectMicros       int64
	MaxSelectMicros         int64
	AssignmentQueueDepth    int64
	CompletionQueueDepth    int64
	MaxWorkerCPUMillis      int64
	ReportingWorkers        int64
}

type performanceWindow struct {
	Raft      map[string]operationWindow
	Scheduler schedulerWindow
}

func NewPerformance() *Performance {
	return &Performance{
		startedAt: time.Now().UTC(),
		raft:      make(map[string]*operationAggregate),
		scheduler: schedulerAggregate{WorkerLoads: make(map[string]WorkerLoadSnapshot)},
	}
}

// ObserveRaftApply records the time from Raft.Apply until the ApplyFuture is
// resolved. Batch item count is derived from the replicated command itself so
// every producer is measured consistently.
func (p *Performance) ObserveRaftApply(command []byte, duration time.Duration, applyErr error) {
	op, items := commandShape(command)
	micros := duration.Microseconds()
	if micros < 0 {
		micros = 0
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	aggregate := p.raft[op]
	if aggregate == nil {
		aggregate = &operationAggregate{}
		p.raft[op] = aggregate
	}
	aggregate.Count++
	aggregate.Items += uint64(items)
	aggregate.TotalMicros += micros
	aggregate.LastMicros = micros
	if micros > aggregate.MaxMicros {
		aggregate.MaxMicros = micros
	}
	if applyErr != nil {
		aggregate.Errors++
	}

	aggregate.windowCount++
	aggregate.windowItems += uint64(items)
	aggregate.windowTotalMicros += micros
	if micros > aggregate.windowMaxMicros {
		aggregate.windowMaxMicros = micros
	}
	if applyErr != nil {
		aggregate.windowErrors++
	}
}

// ObservePendingSelection measures the leader-only scheduling work performed
// before the ClaimBatch Raft Apply. scanned is the copied/sorted pending
// snapshot size; selected is the number of concrete task-to-node assignments.
func (p *Performance) ObservePendingSelection(scanned, selected int, duration time.Duration) {
	micros := duration.Microseconds()
	if micros < 0 {
		micros = 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := &p.scheduler
	s.SelectionCount++
	s.PendingScanned += uint64(max(scanned, 0))
	s.TasksSelected += uint64(max(selected, 0))
	s.TotalSelectMicros += micros
	s.LastSelectMicros = micros
	if micros > s.MaxSelectMicros {
		s.MaxSelectMicros = micros
	}
	s.windowSelectionCount++
	s.windowPendingScanned += uint64(max(scanned, 0))
	s.windowTasksSelected += uint64(max(selected, 0))
	s.windowTotalSelectMicros += micros
	if micros > s.windowMaxSelectMicros {
		s.windowMaxSelectMicros = micros
	}
}

func (p *Performance) SetDispatcherQueueDepths(assignment, completion int) {
	p.mu.Lock()
	p.scheduler.AssignmentQueueDepth = int64(max(assignment, 0))
	p.scheduler.CompletionQueueDepth = int64(max(completion, 0))
	p.mu.Unlock()
}

func (p *Performance) ObserveWorkerLoad(
	nodeID string,
	cpuMillis, runningTasks, capacity int,
	observedAt time.Time,
) {
	if nodeID == "" {
		return
	}
	p.mu.Lock()
	p.scheduler.WorkerLoads[nodeID] = WorkerLoadSnapshot{
		CPUUtilizationMillis: max(cpuMillis, 0),
		RunningTasks:         max(runningTasks, 0),
		Capacity:             max(capacity, 0),
		ObservedAt:           observedAt.UTC(),
	}
	p.mu.Unlock()
}

func (p *Performance) ObserveLoadAdmission(loadAware, throttled, unavailable, stale int) {
	p.mu.Lock()
	s := &p.scheduler
	s.LoadAwareRequests += uint64(max(loadAware, 0))
	s.LoadThrottledRequests += uint64(max(throttled, 0))
	s.LoadUnavailableRequests += uint64(max(unavailable, 0))
	s.StaleLoadRequests += uint64(max(stale, 0))
	s.windowLoadAwareRequests += uint64(max(loadAware, 0))
	s.windowLoadThrottledRequests += uint64(max(throttled, 0))
	s.windowLoadUnavailableRequests += uint64(max(unavailable, 0))
	s.windowStaleLoadRequests += uint64(max(stale, 0))
	p.mu.Unlock()
}

func (p *Performance) Snapshot() PerformanceSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.snapshotLocked()
}

func (p *Performance) sample() performanceWindow {
	p.mu.Lock()
	defer p.mu.Unlock()
	window := performanceWindow{Raft: make(map[string]operationWindow, len(p.raft))}
	for op, aggregate := range p.raft {
		window.Raft[op] = operationWindow{
			Applies: aggregate.windowCount, Items: aggregate.windowItems,
			Errors: aggregate.windowErrors, TotalMicros: aggregate.windowTotalMicros,
			MaxMicros: aggregate.windowMaxMicros,
		}
		aggregate.windowCount = 0
		aggregate.windowItems = 0
		aggregate.windowErrors = 0
		aggregate.windowTotalMicros = 0
		aggregate.windowMaxMicros = 0
	}
	s := &p.scheduler
	workerLoads, maxCPU := p.freshWorkerLoadsLocked(time.Now().UTC())
	window.Scheduler = schedulerWindow{
		Selections: s.windowSelectionCount, PendingScanned: s.windowPendingScanned,
		TasksSelected: s.windowTasksSelected, TotalSelectMicros: s.windowTotalSelectMicros,
		LoadAwareRequests:       s.windowLoadAwareRequests,
		LoadThrottledRequests:   s.windowLoadThrottledRequests,
		LoadUnavailableRequests: s.windowLoadUnavailableRequests,
		StaleLoadRequests:       s.windowStaleLoadRequests,
		MaxSelectMicros:         s.windowMaxSelectMicros,
		AssignmentQueueDepth:    s.AssignmentQueueDepth,
		CompletionQueueDepth:    s.CompletionQueueDepth,
		MaxWorkerCPUMillis:      int64(maxCPU),
		ReportingWorkers:        int64(len(workerLoads)),
	}
	s.windowSelectionCount = 0
	s.windowPendingScanned = 0
	s.windowTasksSelected = 0
	s.windowLoadAwareRequests = 0
	s.windowLoadThrottledRequests = 0
	s.windowLoadUnavailableRequests = 0
	s.windowStaleLoadRequests = 0
	s.windowTotalSelectMicros = 0
	s.windowMaxSelectMicros = 0
	return window
}

func (p *Performance) snapshotLocked() PerformanceSnapshot {
	snapshot := PerformanceSnapshot{
		StartedAt: p.startedAt,
		Raft:      make(map[string]RaftOperationSnapshot, len(p.raft)),
	}
	for op, aggregate := range p.raft {
		snapshot.Raft[op] = RaftOperationSnapshot{
			Applies: aggregate.Count, Items: aggregate.Items, Errors: aggregate.Errors,
			AverageMicros: divideInt64(aggregate.TotalMicros, aggregate.Count),
			MaxMicros:     aggregate.MaxMicros, LastMicros: aggregate.LastMicros,
			AverageBatch: divideUint64(aggregate.Items, aggregate.Count),
		}
	}
	s := p.scheduler
	workerLoads, maxCPU := p.freshWorkerLoadsLocked(time.Now().UTC())
	snapshot.Scheduler = SchedulerSnapshot{
		Selections: s.SelectionCount, PendingScanned: s.PendingScanned,
		TasksSelected:           s.TasksSelected,
		LoadAwareRequests:       s.LoadAwareRequests,
		LoadThrottledRequests:   s.LoadThrottledRequests,
		LoadUnavailableRequests: s.LoadUnavailableRequests,
		StaleLoadRequests:       s.StaleLoadRequests,
		AverageSelectMicros:     divideInt64(s.TotalSelectMicros, s.SelectionCount),
		MaxSelectMicros:         s.MaxSelectMicros, LastSelectMicros: s.LastSelectMicros,
		AssignmentQueueDepth: s.AssignmentQueueDepth,
		CompletionQueueDepth: s.CompletionQueueDepth,
		MaxWorkerCPUMillis:   int64(maxCPU),
		WorkerLoads:          workerLoads,
	}
	return snapshot
}

const workerLoadRetention = 5 * time.Second

func (p *Performance) freshWorkerLoadsLocked(now time.Time) (map[string]WorkerLoadSnapshot, int) {
	out := make(map[string]WorkerLoadSnapshot, len(p.scheduler.WorkerLoads))
	maxCPU := 0
	for nodeID, load := range p.scheduler.WorkerLoads {
		if load.ObservedAt.IsZero() || now.Sub(load.ObservedAt) > workerLoadRetention {
			delete(p.scheduler.WorkerLoads, nodeID)
			continue
		}
		out[nodeID] = load
		if load.CPUUtilizationMillis > maxCPU {
			maxCPU = load.CPUUtilizationMillis
		}
	}
	return out, maxCPU
}

func divideInt64(total int64, count uint64) int64 {
	if count == 0 {
		return 0
	}
	return total / int64(count)
}

func divideUint64(total, count uint64) int64 {
	if count == 0 {
		return 0
	}
	return int64(total / count)
}

func commandShape(command []byte) (string, int) {
	var envelope raftpkg.Command
	if err := json.Unmarshal(command, &envelope); err != nil || envelope.Op == "" {
		return "unknown", 0
	}
	switch envelope.Op {
	case raftpkg.OpCreateTaskBatch:
		var data struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data.Tasks)
	case raftpkg.OpClaimBatch:
		var data struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data.Tasks)
	case raftpkg.OpCompleteBatch:
		var data struct {
			Tasks []json.RawMessage `json:"tasks"`
		}
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data.Tasks)
	case raftpkg.OpRequeueTasks:
		var data raftpkg.RequeueTasksData
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data.TaskIDs)
	case raftpkg.OpUpdateAllocation:
		var data map[string]json.RawMessage
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data)
	default:
		return envelope.Op, 1
	}
}
