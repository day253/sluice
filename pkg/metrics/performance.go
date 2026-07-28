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
	SelectionCount                   uint64
	PendingScanned                   uint64
	TasksSelected                    uint64
	LoadAwareRequests                uint64
	LoadThrottledRequests            uint64
	LoadUnavailableRequests          uint64
	StaleLoadRequests                uint64
	TotalSelectMicros                int64
	MaxSelectMicros                  int64
	LastSelectMicros                 int64
	SubmissionBatches                uint64
	SubmissionRequests               uint64
	SubmissionTasks                  uint64
	TotalSubmissionWaitUS            int64
	MaxSubmissionWaitUS              int64
	LastSubmissionWaitUS             int64
	SubmissionQueueDepth             int64
	SubmissionBackpressureWaits      uint64
	SubmissionBackpressureRejections uint64
	TotalSubmissionBackpressureUS    int64
	MaxSubmissionBackpressureUS      int64
	LastSubmissionBackpressureUS     int64
	SubmissionApplyInFlight          int64
	SubmissionApplyWaiting           int64
	SubmissionApplyLimit             int64
	AssignmentQueueDepth             int64
	CompletionQueueDepth             int64
	AllocationPlanChecks             uint64
	AllocationPlanApplies            uint64
	AllocationPlanNoops              uint64
	WorkerLoads                      map[string]WorkerLoadSnapshot
	WorkerTelemetry                  map[string]WorkerLoadSnapshot

	windowSelectionCount                   uint64
	windowPendingScanned                   uint64
	windowTasksSelected                    uint64
	windowLoadAwareRequests                uint64
	windowLoadThrottledRequests            uint64
	windowLoadUnavailableRequests          uint64
	windowStaleLoadRequests                uint64
	windowAllocationPlanChecks             uint64
	windowAllocationPlanApplies            uint64
	windowAllocationPlanNoops              uint64
	windowTotalSelectMicros                int64
	windowMaxSelectMicros                  int64
	windowSubmissionBatches                uint64
	windowSubmissionRequests               uint64
	windowSubmissionTasks                  uint64
	windowTotalSubmissionWaitUS            int64
	windowMaxSubmissionWaitUS              int64
	windowSubmissionBackpressureWaits      uint64
	windowSubmissionBackpressureRejections uint64
	windowTotalSubmissionBackpressureUS    int64
	windowMaxSubmissionBackpressureUS      int64
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
	Selections                       uint64                        `json:"selections"`
	PendingScanned                   uint64                        `json:"pending_scanned"`
	TasksSelected                    uint64                        `json:"tasks_selected"`
	LoadAwareRequests                uint64                        `json:"load_aware_requests"`
	LoadThrottledRequests            uint64                        `json:"load_throttled_requests"`
	LoadUnavailableRequests          uint64                        `json:"load_unavailable_requests"`
	StaleLoadRequests                uint64                        `json:"stale_load_requests"`
	AverageSelectMicros              int64                         `json:"average_select_us"`
	MaxSelectMicros                  int64                         `json:"max_select_us"`
	LastSelectMicros                 int64                         `json:"last_select_us"`
	SubmissionBatches                uint64                        `json:"submission_batches"`
	SubmissionRequests               uint64                        `json:"submission_requests"`
	SubmissionTasks                  uint64                        `json:"submission_tasks"`
	AverageSubmissionBatch           int64                         `json:"average_submission_batch"`
	AverageSubmissionReqs            int64                         `json:"average_submission_requests"`
	AverageSubmissionWaitUS          int64                         `json:"average_submission_queue_us"`
	MaxSubmissionWaitUS              int64                         `json:"max_submission_queue_us"`
	LastSubmissionWaitUS             int64                         `json:"last_submission_queue_us"`
	SubmissionQueueDepth             int64                         `json:"submission_queue_depth"`
	SubmissionBackpressureWaits      uint64                        `json:"submission_backpressure_waits"`
	SubmissionBackpressureRejections uint64                        `json:"submission_backpressure_rejections"`
	AverageSubmissionBackpressureUS  int64                         `json:"average_submission_backpressure_us"`
	MaxSubmissionBackpressureUS      int64                         `json:"max_submission_backpressure_us"`
	LastSubmissionBackpressureUS     int64                         `json:"last_submission_backpressure_us"`
	SubmissionApplyInFlight          int64                         `json:"submission_apply_inflight"`
	SubmissionApplyWaiting           int64                         `json:"submission_apply_waiting"`
	SubmissionApplyLimit             int64                         `json:"submission_apply_limit"`
	AssignmentQueueDepth             int64                         `json:"assignment_queue_depth"`
	CompletionQueueDepth             int64                         `json:"completion_queue_depth"`
	AllocationPlanChecks             uint64                        `json:"allocation_plan_checks"`
	AllocationPlanApplies            uint64                        `json:"allocation_plan_applies"`
	AllocationPlanNoops              uint64                        `json:"allocation_plan_noops"`
	MaxWorkerCPUMillis               int64                         `json:"max_worker_cpu_millis"`
	WorkerLoads                      map[string]WorkerLoadSnapshot `json:"worker_loads"`
	WorkerTelemetry                  map[string]WorkerLoadSnapshot `json:"worker_telemetry"`
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
	Selections                       uint64
	PendingScanned                   uint64
	TasksSelected                    uint64
	LoadAwareRequests                uint64
	LoadThrottledRequests            uint64
	LoadUnavailableRequests          uint64
	StaleLoadRequests                uint64
	AllocationPlanChecks             uint64
	AllocationPlanApplies            uint64
	AllocationPlanNoops              uint64
	TotalSelectMicros                int64
	MaxSelectMicros                  int64
	SubmissionBatches                uint64
	SubmissionRequests               uint64
	SubmissionTasks                  uint64
	TotalSubmissionWaitUS            int64
	MaxSubmissionWaitUS              int64
	SubmissionQueueDepth             int64
	SubmissionBackpressureWaits      uint64
	SubmissionBackpressureRejections uint64
	TotalSubmissionBackpressureUS    int64
	MaxSubmissionBackpressureUS      int64
	SubmissionApplyInFlight          int64
	SubmissionApplyWaiting           int64
	SubmissionApplyLimit             int64
	AssignmentQueueDepth             int64
	CompletionQueueDepth             int64
	MaxWorkerCPUMillis               int64
	ReportingWorkers                 int64
}

type performanceWindow struct {
	Raft      map[string]operationWindow
	Scheduler schedulerWindow
}

func NewPerformance() *Performance {
	return &Performance{
		startedAt: time.Now().UTC(),
		raft:      make(map[string]*operationAggregate),
		scheduler: schedulerAggregate{
			WorkerLoads:     make(map[string]WorkerLoadSnapshot),
			WorkerTelemetry: make(map[string]WorkerLoadSnapshot),
		},
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

func (p *Performance) ObserveSubmissionBatch(requests, tasks int, queueWait time.Duration) {
	waitMicros := max(queueWait.Microseconds(), 0)
	p.mu.Lock()
	s := &p.scheduler
	s.SubmissionBatches++
	s.SubmissionRequests += uint64(max(requests, 0))
	s.SubmissionTasks += uint64(max(tasks, 0))
	s.TotalSubmissionWaitUS += waitMicros
	s.LastSubmissionWaitUS = waitMicros
	if waitMicros > s.MaxSubmissionWaitUS {
		s.MaxSubmissionWaitUS = waitMicros
	}
	s.windowSubmissionBatches++
	s.windowSubmissionRequests += uint64(max(requests, 0))
	s.windowSubmissionTasks += uint64(max(tasks, 0))
	s.windowTotalSubmissionWaitUS += waitMicros
	if waitMicros > s.windowMaxSubmissionWaitUS {
		s.windowMaxSubmissionWaitUS = waitMicros
	}
	p.mu.Unlock()
}

func (p *Performance) SetSubmissionQueueDepth(depth int) {
	p.mu.Lock()
	p.scheduler.SubmissionQueueDepth = int64(max(depth, 0))
	p.mu.Unlock()
}

func (p *Performance) ObserveSubmissionBackpressure(wait time.Duration, rejected bool) {
	waitMicros := max(wait.Microseconds(), 0)
	p.mu.Lock()
	s := &p.scheduler
	if rejected {
		s.SubmissionBackpressureRejections++
		s.windowSubmissionBackpressureRejections++
		p.mu.Unlock()
		return
	}
	s.SubmissionBackpressureWaits++
	s.TotalSubmissionBackpressureUS += waitMicros
	s.LastSubmissionBackpressureUS = waitMicros
	if waitMicros > s.MaxSubmissionBackpressureUS {
		s.MaxSubmissionBackpressureUS = waitMicros
	}
	s.windowSubmissionBackpressureWaits++
	s.windowTotalSubmissionBackpressureUS += waitMicros
	if waitMicros > s.windowMaxSubmissionBackpressureUS {
		s.windowMaxSubmissionBackpressureUS = waitMicros
	}
	p.mu.Unlock()
}

func (p *Performance) SetSubmissionApplyPressure(inFlight, waiting, limit int) {
	p.mu.Lock()
	p.scheduler.SubmissionApplyInFlight = int64(max(inFlight, 0))
	p.scheduler.SubmissionApplyWaiting = int64(max(waiting, 0))
	p.scheduler.SubmissionApplyLimit = int64(max(limit, 0))
	p.mu.Unlock()
}

func (p *Performance) ObserveAllocationPlan(applied, unchanged bool) {
	p.mu.Lock()
	s := &p.scheduler
	s.AllocationPlanChecks++
	s.windowAllocationPlanChecks++
	if applied {
		s.AllocationPlanApplies++
		s.windowAllocationPlanApplies++
	}
	if unchanged {
		s.AllocationPlanNoops++
		s.windowAllocationPlanNoops++
	}
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

// ObserveWorkerTelemetry stores ingress-time Worker feedback for read-only
// diagnostics. Unlike WorkerLoads, this mirror is not conditioned on the
// dispatcher freshness window and is never consumed by autoscaling.
func (p *Performance) ObserveWorkerTelemetry(
	nodeID string,
	cpuMillis, runningTasks, capacity int,
	observedAt time.Time,
) {
	if nodeID == "" {
		return
	}
	p.mu.Lock()
	p.scheduler.WorkerTelemetry[nodeID] = WorkerLoadSnapshot{
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
		LoadAwareRequests:                s.windowLoadAwareRequests,
		LoadThrottledRequests:            s.windowLoadThrottledRequests,
		LoadUnavailableRequests:          s.windowLoadUnavailableRequests,
		StaleLoadRequests:                s.windowStaleLoadRequests,
		AllocationPlanChecks:             s.windowAllocationPlanChecks,
		AllocationPlanApplies:            s.windowAllocationPlanApplies,
		AllocationPlanNoops:              s.windowAllocationPlanNoops,
		MaxSelectMicros:                  s.windowMaxSelectMicros,
		SubmissionBatches:                s.windowSubmissionBatches,
		SubmissionRequests:               s.windowSubmissionRequests,
		SubmissionTasks:                  s.windowSubmissionTasks,
		TotalSubmissionWaitUS:            s.windowTotalSubmissionWaitUS,
		MaxSubmissionWaitUS:              s.windowMaxSubmissionWaitUS,
		SubmissionQueueDepth:             s.SubmissionQueueDepth,
		SubmissionBackpressureWaits:      s.windowSubmissionBackpressureWaits,
		SubmissionBackpressureRejections: s.windowSubmissionBackpressureRejections,
		TotalSubmissionBackpressureUS:    s.windowTotalSubmissionBackpressureUS,
		MaxSubmissionBackpressureUS:      s.windowMaxSubmissionBackpressureUS,
		SubmissionApplyInFlight:          s.SubmissionApplyInFlight,
		SubmissionApplyWaiting:           s.SubmissionApplyWaiting,
		SubmissionApplyLimit:             s.SubmissionApplyLimit,
		AssignmentQueueDepth:             s.AssignmentQueueDepth,
		CompletionQueueDepth:             s.CompletionQueueDepth,
		MaxWorkerCPUMillis:               int64(maxCPU),
		ReportingWorkers:                 int64(len(workerLoads)),
	}
	s.windowSelectionCount = 0
	s.windowPendingScanned = 0
	s.windowTasksSelected = 0
	s.windowLoadAwareRequests = 0
	s.windowLoadThrottledRequests = 0
	s.windowLoadUnavailableRequests = 0
	s.windowStaleLoadRequests = 0
	s.windowAllocationPlanChecks = 0
	s.windowAllocationPlanApplies = 0
	s.windowAllocationPlanNoops = 0
	s.windowTotalSelectMicros = 0
	s.windowMaxSelectMicros = 0
	s.windowSubmissionBatches = 0
	s.windowSubmissionRequests = 0
	s.windowSubmissionTasks = 0
	s.windowTotalSubmissionWaitUS = 0
	s.windowMaxSubmissionWaitUS = 0
	s.windowSubmissionBackpressureWaits = 0
	s.windowSubmissionBackpressureRejections = 0
	s.windowTotalSubmissionBackpressureUS = 0
	s.windowMaxSubmissionBackpressureUS = 0
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
	workerTelemetry := p.freshWorkerTelemetryLocked(time.Now().UTC())
	snapshot.Scheduler = SchedulerSnapshot{
		Selections: s.SelectionCount, PendingScanned: s.PendingScanned,
		TasksSelected:           s.TasksSelected,
		LoadAwareRequests:       s.LoadAwareRequests,
		LoadThrottledRequests:   s.LoadThrottledRequests,
		LoadUnavailableRequests: s.LoadUnavailableRequests,
		StaleLoadRequests:       s.StaleLoadRequests,
		AverageSelectMicros:     divideInt64(s.TotalSelectMicros, s.SelectionCount),
		MaxSelectMicros:         s.MaxSelectMicros, LastSelectMicros: s.LastSelectMicros,
		SubmissionBatches:                s.SubmissionBatches,
		SubmissionRequests:               s.SubmissionRequests,
		SubmissionTasks:                  s.SubmissionTasks,
		AverageSubmissionBatch:           divideUint64(s.SubmissionTasks, s.SubmissionBatches),
		AverageSubmissionReqs:            divideUint64(s.SubmissionRequests, s.SubmissionBatches),
		AverageSubmissionWaitUS:          divideInt64(s.TotalSubmissionWaitUS, s.SubmissionBatches),
		MaxSubmissionWaitUS:              s.MaxSubmissionWaitUS,
		LastSubmissionWaitUS:             s.LastSubmissionWaitUS,
		SubmissionQueueDepth:             s.SubmissionQueueDepth,
		SubmissionBackpressureWaits:      s.SubmissionBackpressureWaits,
		SubmissionBackpressureRejections: s.SubmissionBackpressureRejections,
		AverageSubmissionBackpressureUS:  divideInt64(s.TotalSubmissionBackpressureUS, s.SubmissionBackpressureWaits),
		MaxSubmissionBackpressureUS:      s.MaxSubmissionBackpressureUS,
		LastSubmissionBackpressureUS:     s.LastSubmissionBackpressureUS,
		SubmissionApplyInFlight:          s.SubmissionApplyInFlight,
		SubmissionApplyWaiting:           s.SubmissionApplyWaiting,
		SubmissionApplyLimit:             s.SubmissionApplyLimit,
		AssignmentQueueDepth:             s.AssignmentQueueDepth,
		CompletionQueueDepth:             s.CompletionQueueDepth,
		AllocationPlanChecks:             s.AllocationPlanChecks,
		AllocationPlanApplies:            s.AllocationPlanApplies,
		AllocationPlanNoops:              s.AllocationPlanNoops,
		MaxWorkerCPUMillis:               int64(maxCPU),
		WorkerLoads:                      workerLoads,
		WorkerTelemetry:                  workerTelemetry,
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

func (p *Performance) freshWorkerTelemetryLocked(now time.Time) map[string]WorkerLoadSnapshot {
	out := make(map[string]WorkerLoadSnapshot, len(p.scheduler.WorkerTelemetry))
	for nodeID, load := range p.scheduler.WorkerTelemetry {
		if load.ObservedAt.IsZero() || now.Sub(load.ObservedAt) > workerLoadRetention {
			delete(p.scheduler.WorkerTelemetry, nodeID)
			continue
		}
		out[nodeID] = load
	}
	return out
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
	case raftpkg.OpSetWorkerCapacities:
		var data raftpkg.SetWorkerCapacitiesData
		_ = json.Unmarshal(envelope.Data, &data)
		return envelope.Op, len(data.NodeIDs)
	default:
		return envelope.Op, 1
	}
}
