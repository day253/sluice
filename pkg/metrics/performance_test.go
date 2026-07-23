package metrics

import (
	"errors"
	"testing"
	"time"

	raftpkg "github.com/day253/sluice/pkg/raft"
)

func TestPerformanceRecordsRaftBatchAndSchedulerWindows(t *testing.T) {
	performance := NewPerformance()
	claim := raftpkg.MustMarshalCommand(raftpkg.OpClaimBatch, raftpkg.ClaimBatchData{
		Tasks: []raftpkg.ClaimTaskData{{TaskID: "1"}, {TaskID: "2"}, {TaskID: "3"}},
	})
	performance.ObserveRaftApply(claim, 12*time.Millisecond, nil)
	performance.ObserveRaftApply(claim, 18*time.Millisecond, errors.New("commit failed"))
	performance.ObservePendingSelection(20_000, 128, 4*time.Millisecond)
	performance.SetDispatcherQueueDepths(7, 9)
	performance.ObserveWorkerLoad("worker-a", 720, 5, 10, time.Now())
	performance.ObserveWorkerLoad("worker-b", 910, 8, 10, time.Now())
	performance.ObserveLoadAdmission(12, 3, 2, 1)

	snapshot := performance.Snapshot()
	operation := snapshot.Raft[raftpkg.OpClaimBatch]
	if operation.Applies != 2 || operation.Items != 6 || operation.Errors != 1 {
		t.Fatalf("claim snapshot = %+v", operation)
	}
	if operation.AverageMicros != 15_000 || operation.MaxMicros != 18_000 ||
		operation.LastMicros != 18_000 || operation.AverageBatch != 3 {
		t.Fatalf("claim latency/batch snapshot = %+v", operation)
	}
	if got := snapshot.Scheduler; got.Selections != 1 || got.PendingScanned != 20_000 ||
		got.TasksSelected != 128 || got.AverageSelectMicros != 4_000 ||
		got.AssignmentQueueDepth != 7 || got.CompletionQueueDepth != 9 ||
		got.LoadAwareRequests != 12 || got.LoadThrottledRequests != 3 ||
		got.LoadUnavailableRequests != 2 || got.StaleLoadRequests != 1 ||
		got.MaxWorkerCPUMillis != 910 || len(got.WorkerLoads) != 2 {
		t.Fatalf("scheduler snapshot = %+v", got)
	}

	window := performance.sample()
	if got := window.Raft[raftpkg.OpClaimBatch]; got.Applies != 2 || got.Items != 6 ||
		got.Errors != 1 || got.TotalMicros != 30_000 || got.MaxMicros != 18_000 {
		t.Fatalf("claim window = %+v", got)
	}
	if got := performance.sample(); got.Raft[raftpkg.OpClaimBatch].Applies != 0 ||
		got.Scheduler.Selections != 0 || got.Scheduler.AssignmentQueueDepth != 7 ||
		got.Scheduler.LoadAwareRequests != 0 ||
		got.Scheduler.MaxWorkerCPUMillis != 910 ||
		got.Scheduler.ReportingWorkers != 2 {
		t.Fatalf("second window must reset events but retain gauges: %+v", got)
	}
	if got := performance.Snapshot().Raft[raftpkg.OpClaimBatch].Applies; got != 2 {
		t.Fatalf("sampling reset cumulative applies: %d", got)
	}
}

func TestCommandShapeCountsReplicatedItems(t *testing.T) {
	tests := []struct {
		name    string
		command []byte
		op      string
		items   int
	}{
		{
			name: "create batch",
			command: raftpkg.MustMarshalCommand(raftpkg.OpCreateTaskBatch, raftpkg.CreateTaskBatchData{
				Tasks: []raftpkg.CreateTaskData{{TaskID: "1"}, {TaskID: "2"}},
			}),
			op: raftpkg.OpCreateTaskBatch, items: 2,
		},
		{
			name: "complete batch",
			command: raftpkg.MustMarshalCommand(raftpkg.OpCompleteBatch, raftpkg.CompleteBatchData{
				Tasks: []raftpkg.CompleteTaskData{{TaskID: "1"}, {TaskID: "2"}, {TaskID: "3"}},
			}),
			op: raftpkg.OpCompleteBatch, items: 3,
		},
		{name: "single", command: raftpkg.MustMarshalCommand(raftpkg.OpUpsertTenant, map[string]any{"id": "t"}), op: raftpkg.OpUpsertTenant, items: 1},
		{name: "invalid", command: []byte("not-json"), op: "unknown", items: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			op, items := commandShape(test.command)
			if op != test.op || items != test.items {
				t.Fatalf("command shape = %s/%d, want %s/%d", op, items, test.op, test.items)
			}
		})
	}
}
