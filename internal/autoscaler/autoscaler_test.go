package autoscaler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPolicyScalesUpFromBacklogWithoutWaitingForCPU(t *testing.T) {
	config := DefaultConfig()
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		Backlog: 20_000, PendingTasks: 20_000, TaskBreakdownValid: true,
		AllocatedWorkers: 500, WorkerCapacity: 500,
	}

	first := policy.Recommend(5, signals, start, state)
	if first.BacklogDesired != 50 || first.Desired != 15 {
		t.Fatalf("first recommendation = %+v, want backlog desired 50 and bounded scale to 15", first)
	}
	state.RecordApplied(start)

	rateLimited := policy.Recommend(15, signals, start.Add(4*time.Second), state)
	if rateLimited.Desired != 15 || rateLimited.Reason != "scale-up rate limited" {
		t.Fatalf("rate-limited recommendation = %+v", rateLimited)
	}
	next := policy.Recommend(15, signals, start.Add(5*time.Second), state)
	if next.Desired != 30 {
		t.Fatalf("second scale recommendation = %+v, want 30", next)
	}
}

func TestPolicyUsesActualExecutionSlotUtilizationForSmallBacklog(t *testing.T) {
	config := DefaultConfig()
	recommendation := (Policy{Config: config}).Recommend(
		5,
		Signals{
			Backlog: 1, PendingTasks: 1, TaskBreakdownValid: true,
			ExecutionSignalsValid: true, ExecutingTasks: 500,
			WorkerCapacity: 500, WorkerInstances: 5, ReportingWorkers: 5,
		},
		time.Unix(1000, 0),
		&State{},
	)
	if recommendation.BacklogDesired != 1 || recommendation.UtilizationDesired != 8 ||
		recommendation.Desired != 8 {
		t.Fatalf("recommendation = %+v, want utilization-driven scale from 5 to 8", recommendation)
	}
}

func TestPolicyUsesActualHeterogeneousWorkerInstanceCount(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	recommendation := (Policy{Config: config}).Recommend(
		2,
		Signals{
			Backlog: 1, PendingTasks: 1, TaskBreakdownValid: true,
			AllocatedWorkers: 300, WorkerCapacity: 300, WorkerInstances: 2,
			ExecutionSignalsValid: true, ExecutingTasks: 300, ReportingWorkers: 2,
		},
		time.Unix(1000, 0),
		&State{},
	)
	if recommendation.UtilizationDesired != 3 || recommendation.Desired != 3 {
		t.Fatalf(
			"heterogeneous recommendation = %+v, want actual 2-instance utilization target 3",
			recommendation,
		)
	}
}

func TestPolicyRequiresSustainedSpareCapacityBeforeBoundedScaleDown(t *testing.T) {
	config := DefaultConfig()
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		TaskBreakdownValid: true, ExecutionSignalsValid: true,
		RateCountersValid: true, ObservedAt: start,
		TelemetrySource: "leader-0", TelemetryStartedAt: start.Add(-time.Minute),
	}

	if got := policy.Recommend(20, signals, start, state); got.Desired != 20 {
		t.Fatalf("initial low-load recommendation = %+v, want rate baseline", got)
	}
	signals.ObservedAt = start.Add(time.Second)
	if got := policy.Recommend(20, signals, start.Add(time.Second), state); got.Desired != 20 {
		t.Fatalf("second low-load recommendation = %+v, want stabilization", got)
	}
	if got := policy.Recommend(20, signals, start.Add(300*time.Second), state); got.Desired != 20 {
		t.Fatalf("early scale-down recommendation = %+v, want 20", got)
	}
	first := policy.Recommend(20, signals, start.Add(301*time.Second), state)
	if first.Desired != 15 {
		t.Fatalf("first scale-down recommendation = %+v, want 15", first)
	}
	state.RecordApplied(start.Add(301 * time.Second))
	if got := policy.Recommend(15, signals, start.Add(360*time.Second), state); got.Desired != 15 {
		t.Fatalf("rate-limited scale-down recommendation = %+v, want 15", got)
	}
	if got := policy.Recommend(15, signals, start.Add(361*time.Second), state); got.Desired != 12 {
		t.Fatalf("second scale-down recommendation = %+v, want 12", got)
	}
}

func TestPolicyScalesIdleRolloutToMinimumAndBackUpForNewBacklog(t *testing.T) {
	config := DefaultConfig()
	config.ScaleDownStabilization = 0
	config.ScaleDownPercent = 50
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		TaskBreakdownValid: true, ExecutionSignalsValid: true,
		RateCountersValid: true, ObservedAt: start,
		TelemetrySource: "leader-0", TelemetryStartedAt: start.Add(-time.Minute),
		ReportingWorkers: 50, WorkerInstances: 50, WorkerCapacity: 5_000,
	}

	if got := policy.Recommend(50, signals, start, state); got.Desired != 50 ||
		!got.ScaleDownBlocked {
		t.Fatalf("initial idle recommendation = %+v, want rate-baseline hold at 50", got)
	}
	signals.ObservedAt = start.Add(time.Second)
	first := policy.Recommend(50, signals, start.Add(time.Second), state)
	if first.Desired != 25 || first.Reason != "sustained spare execution capacity" {
		t.Fatalf("first idle scale-down = %+v, want 50 -> 25", first)
	}
	state.RecordApplied(start.Add(time.Second))

	current := int32(25)
	for step, want := range []int32{13, 7, 5} {
		now := start.Add(time.Duration(step+1)*time.Minute + time.Second)
		signals.ObservedAt = now
		got := policy.Recommend(current, signals, now, state)
		if got.Desired != want {
			t.Fatalf("idle scale-down step %d = %+v, want %d", step, got, want)
		}
		state.RecordApplied(now)
		current = want
	}
	signals.PendingTasks = 40_000
	signals.Backlog = 40_000
	signals.ObservedAt = start.Add(4*time.Minute + 2*time.Second)
	scaleUp := policy.Recommend(current, signals, signals.ObservedAt, state)
	if scaleUp.RawDesired != 100 || scaleUp.Desired != 15 ||
		scaleUp.Reason != "scale-up: queue depth" {
		t.Fatalf("new backlog recommendation = %+v, want bounded 5 -> 15 scale-up", scaleUp)
	}
}

func TestHTTPReaderCombinesBacklogAndOnlyLiveWorkerCapacity(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/autoscaling", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"observed_at":"2026-07-24T00:00:00Z",
			"unfinished_tasks":22,"pending_tasks":17,"running_tasks":5,
			"oldest_pending_age_ms":2500,"task_breakdown_valid":true,
			"allocated_workers":85,"worker_capacity":100,"worker_instances":1,
			"execution_signals_valid":true,"reporting_workers":1,
			"executing_tasks":5,"average_worker_cpu_millis":420,
			"max_worker_cpu_millis":420,"rate_counters_valid":true,
			"telemetry_source":"leader-0","telemetry_started_at":"2026-07-23T23:00:00Z",
			"submitted_tasks_total":120,"completed_tasks_total":98
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	signals, err := (HTTPReader{Client: server.Client()}).Read(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if signals.Backlog != 22 || signals.PendingTasks != 17 || signals.RunningTasks != 5 ||
		signals.OldestPendingAge != 2500*time.Millisecond ||
		signals.AllocatedWorkers != 85 || signals.WorkerCapacity != 100 ||
		signals.WorkerInstances != 1 || !signals.ExecutionSignalsValid ||
		signals.ExecutingTasks != 5 || signals.AverageWorkerCPUMillis != 420 ||
		!signals.RateCountersValid || signals.SubmittedTasksTotal != 120 ||
		signals.CompletedTasksTotal != 98 {
		t.Fatalf("signals = %+v", signals)
	}
}

func TestHTTPReaderDefaultClientIgnoresEnvironmentProxy(t *testing.T) {
	var proxyRequests atomic.Int64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyRequests.Add(1)
		http.Error(w, "cluster-internal request reached external proxy", http.StatusBadGateway)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/autoscaling", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			`{"observed_at":"2026-07-24T00:00:00Z","unfinished_tasks":3,` +
				`"pending_tasks":3,"task_breakdown_valid":true,` +
				`"allocated_workers":4,"worker_capacity":10,` +
				`"worker_instances":1}`,
		))
	})
	target := httptest.NewServer(mux)
	defer target.Close()

	direct := newDirectHTTPClient()
	transport, ok := direct.Transport.(*http.Transport)
	if !ok || transport.Proxy != nil {
		t.Fatalf("cluster-internal transport proxy = %v, want nil", transport)
	}
	signals, err := (HTTPReader{}).Read(context.Background(), target.URL)
	if err != nil {
		t.Fatal(err)
	}
	if signals.Backlog != 3 || signals.PendingTasks != 3 ||
		signals.AllocatedWorkers != 4 || signals.WorkerCapacity != 10 ||
		signals.WorkerInstances != 1 {
		t.Fatalf("signals = %+v", signals)
	}
	if got := proxyRequests.Load(); got != 0 {
		t.Fatalf("external proxy received %d cluster-internal requests, want 0", got)
	}
}

func TestHTTPReaderDoesNotRecombineConsistentServerSnapshot(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/autoscaling", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{
			"observed_at":"2026-07-24T00:00:00Z","unfinished_tasks":1,
			"pending_tasks":1,"task_breakdown_valid":true,"allocated_workers":50,
			"worker_capacity":100,"worker_instances":1
		}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	signals, err := (HTTPReader{Client: server.Client()}).Read(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if signals.Backlog != 1 || signals.PendingTasks != 1 ||
		signals.AllocatedWorkers != 50 || signals.WorkerCapacity != 100 ||
		signals.WorkerInstances != 1 {
		t.Fatalf("consistent signals = %+v", signals)
	}
}

func TestPolicyTakesMaxOfCPUQueueDrainAndArrivalCandidates(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.MaxReplicas = 100
	config.TargetBacklogPerPod = 10_000
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	base := Signals{
		ObservedAt: start, Backlog: 600, PendingTasks: 600,
		TaskBreakdownValid: true,
		WorkerCapacity:     500, WorkerInstances: 5,
		ExecutionSignalsValid: true, ReportingWorkers: 5,
		ExecutingTasks: 350, AverageWorkerCPUMillis: 900,
		RateCountersValid: true, TelemetrySource: "leader-0",
		TelemetryStartedAt:  start.Add(-time.Minute),
		SubmittedTasksTotal: 1_000, CompletedTasksTotal: 500,
	}
	first := policy.Recommend(5, base, start, state)
	if first.CPUDesired != 7 || first.RatesValid {
		t.Fatalf("first recommendation = %+v, want CPU desired 7 and rate baseline", first)
	}

	nextSignals := base
	nextSignals.ObservedAt = start.Add(10 * time.Second)
	nextSignals.SubmittedTasksTotal += 1_000
	nextSignals.CompletedTasksTotal += 500
	next := policy.Recommend(5, nextSignals, start.Add(10*time.Second), state)
	if next.DrainDesired != 2 || next.ArrivalDesired != 13 ||
		next.RawDesired != 13 || next.Desired != 13 ||
		next.DominantSignal != "arrival rate" || !next.RateProjection {
		t.Fatalf("multi-signal recommendation = %+v, want arrival-driven 13", next)
	}
}

func TestPolicyDoesNotProjectColdRateFromUnsaturatedShortBurst(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.MaxReplicas = 100
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		ObservedAt: start, Backlog: 16_937, PendingTasks: 16_405,
		RunningTasks: 532, TaskBreakdownValid: true,
		WorkerCapacity: 5_000, WorkerInstances: 50,
		ExecutionSignalsValid: true, ReportingWorkers: 50,
		ExecutingTasks: 289, AverageWorkerCPUMillis: 42,
		RateCountersValid: true, TelemetrySource: "leader-0",
		TelemetryStartedAt: start.Add(-time.Minute),
	}
	_ = policy.Recommend(50, signals, start, state)
	signals.ObservedAt = start.Add(5 * time.Second)
	signals.SubmittedTasksTotal = 10_000
	signals.CompletedTasksTotal = 1_532

	got := policy.Recommend(50, signals, signals.ObservedAt, state)
	if !got.RatesValid || got.RateProjection ||
		got.RatePressure < 5.7 || got.RatePressure > 5.9 ||
		got.DrainDesired != 0 || got.ArrivalDesired != 0 ||
		got.QueueDesired != 42 || got.Desired != 50 {
		t.Fatalf("cold short-burst rate was projected across idle Pods: %+v", got)
	}
}

func TestPolicyResetsRatesAcrossLeaderTelemetryEpoch(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		ObservedAt: start, TaskBreakdownValid: true,
		ExecutionSignalsValid: true, RateCountersValid: true,
		WorkerInstances: 2, WorkerCapacity: 200,
		TelemetrySource: "leader-a", TelemetryStartedAt: start.Add(-time.Minute),
	}
	_ = policy.Recommend(2, signals, start, state)
	signals.ObservedAt = start.Add(10 * time.Second)
	signals.SubmittedTasksTotal = 100
	signals.CompletedTasksTotal = 50
	if got := policy.Recommend(2, signals, signals.ObservedAt, state); !got.RatesValid {
		t.Fatalf("same-epoch rates are invalid: %+v", got)
	}
	signals.ObservedAt = start.Add(20 * time.Second)
	signals.TelemetrySource = "leader-b"
	signals.TelemetryStartedAt = start.Add(15 * time.Second)
	signals.SubmittedTasksTotal = 1
	signals.CompletedTasksTotal = 0
	got := policy.Recommend(2, signals, signals.ObservedAt, state)
	if got.RatesValid || got.ArrivalDesired != 0 || got.DrainDesired != 0 {
		t.Fatalf("leader epoch did not reset rate candidates: %+v", got)
	}
}

func TestPolicyAllowsQueueScaleUpButBlocksScaleDownWithMissingSoftMetrics(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	up := policy.Recommend(
		2,
		Signals{Backlog: 10_000, PendingTasks: 10_000, TaskBreakdownValid: true},
		time.Unix(1000, 0),
		&State{},
	)
	if up.Desired <= 2 {
		t.Fatalf("missing execution metrics blocked safe queue scale-up: %+v", up)
	}
	down := policy.Recommend(
		10,
		Signals{TaskBreakdownValid: true},
		time.Unix(1000, 0),
		&State{},
	)
	if down.Desired != 10 || !down.ScaleDownBlocked {
		t.Fatalf("missing execution metrics allowed scale-down: %+v", down)
	}
}

func TestPolicyRequiresTelemetryCoverageOnlyForScaleDown(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	config.ScaleDownStabilization = 0
	policy := Policy{Config: config}
	now := time.Unix(1000, 0)
	signals := Signals{
		TaskBreakdownValid: true, RateCountersValid: true,
		ExecutionSignalsValid: true, WorkerInstances: 10,
		WorkerCapacity: 1_000, ReportingWorkers: 7,
		ObservedAt: now, TelemetrySource: "leader-0",
		TelemetryStartedAt: now.Add(-time.Minute),
	}
	state := &State{}
	_ = policy.Recommend(10, signals, now, state)
	signals.ObservedAt = now.Add(time.Second)
	partial := policy.Recommend(10, signals, signals.ObservedAt, state)
	if partial.Desired != 10 || !partial.ScaleDownBlocked ||
		partial.TelemetryCoverage != 70 {
		t.Fatalf("partial telemetry allowed scale-down: %+v", partial)
	}

	signals.ReportingWorkers = 8
	signals.ObservedAt = now.Add(2 * time.Second)
	complete := policy.Recommend(10, signals, signals.ObservedAt, state)
	if complete.Desired != 8 || complete.ScaleDownBlocked ||
		complete.TelemetryCoverage != 80 {
		t.Fatalf("complete telemetry did not allow bounded scale-down: %+v", complete)
	}
}

func TestPolicyDoesNotExtrapolateOneCPUReporterAcrossMissingWorkers(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	now := time.Unix(1000, 0)
	signals := Signals{
		TaskBreakdownValid: true, RateCountersValid: true,
		ExecutionSignalsValid: true, WorkerInstances: 10,
		WorkerCapacity: 1_000, ReportingWorkers: 1,
		AverageWorkerCPUMillis: 900, ObservedAt: now,
		TelemetrySource: "leader-0", TelemetryStartedAt: now.Add(-time.Minute),
	}
	_ = policy.Recommend(10, signals, now, state)
	signals.ObservedAt = now.Add(time.Second)
	recommendation := policy.Recommend(
		10,
		signals,
		signals.ObservedAt,
		state,
	)
	if recommendation.CPUDesired != 2 || recommendation.Desired != 10 ||
		!recommendation.ScaleDownBlocked || recommendation.RateProjection ||
		recommendation.RatePressure != 0 {
		t.Fatalf("partial CPU telemetry was extrapolated to all Workers: %+v", recommendation)
	}
}

func TestPolicyToleranceUsesRelativeChangeWithoutRoundingUp(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	recommendation := (Policy{Config: config}).Recommend(
		5,
		Signals{
			Backlog: 2_400, PendingTasks: 2_400, TaskBreakdownValid: true,
		},
		time.Unix(1000, 0),
		&State{},
	)
	if recommendation.QueueDesired != 6 || recommendation.Desired != 6 {
		t.Fatalf("20%% change was incorrectly hidden by 10%% tolerance: %+v", recommendation)
	}
}

func TestPolicyDominantSignalSurvivesMaxReplicaClamp(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.MaxReplicas = 100
	config.TolerancePercent = 0
	recommendation := (Policy{Config: config}).Recommend(
		50,
		Signals{
			Backlog: 100_000, PendingTasks: 100_000, TaskBreakdownValid: true,
		},
		time.Unix(1000, 0),
		&State{},
	)
	if recommendation.QueueDesired != 250 || recommendation.RawDesired != 100 ||
		recommendation.DominantSignal != "queue depth" {
		t.Fatalf("max clamp hid the dominant pressure signal: %+v", recommendation)
	}
}

func TestPolicyDoesNotChaseRateCandidateWhilePodsAreStarting(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TargetBacklogPerPod = 10_000
	config.TolerancePercent = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		ObservedAt: start, Backlog: 600, PendingTasks: 600,
		TaskBreakdownValid: true, WorkerInstances: 5, WorkerCapacity: 500,
		ExecutionSignalsValid: true, ReportingWorkers: 5,
		RateCountersValid: true, TelemetrySource: "leader-0",
		TelemetryStartedAt:  start.Add(-time.Minute),
		SubmittedTasksTotal: 1_000, CompletedTasksTotal: 500,
	}
	_ = policy.Recommend(10, signals, start, state)
	signals.ObservedAt = start.Add(10 * time.Second)
	signals.SubmittedTasksTotal += 1_000
	signals.CompletedTasksTotal += 500
	recommendation := policy.Recommend(10, signals, signals.ObservedAt, state)
	if !recommendation.RatesValid || recommendation.ArrivalDesired != 0 ||
		recommendation.DrainDesired != 0 || recommendation.Desired != 10 {
		t.Fatalf("rate candidate chased Pods already starting: %+v", recommendation)
	}
}

func TestPolicyLeaderEpochResetBlocksScaleDownUntilRatesRecover(t *testing.T) {
	config := DefaultConfig()
	config.MinReplicas = 1
	config.TolerancePercent = 0
	config.ScaleDownStabilization = 0
	policy := Policy{Config: config}
	state := &State{}
	start := time.Unix(1000, 0)
	signals := Signals{
		ObservedAt: start, TaskBreakdownValid: true,
		ExecutionSignalsValid: true, WorkerInstances: 10,
		ReportingWorkers: 10, WorkerCapacity: 1_000,
		RateCountersValid: true, TelemetrySource: "leader-a",
		TelemetryStartedAt: start.Add(-time.Minute),
	}
	if got := policy.Recommend(10, signals, start, state); !got.ScaleDownBlocked {
		t.Fatalf("first rate sample did not block scale-down: %+v", got)
	}
	signals.ObservedAt = start.Add(time.Second)
	if got := policy.Recommend(10, signals, signals.ObservedAt, state); got.Desired != 8 {
		t.Fatalf("same-epoch rates did not allow bounded scale-down: %+v", got)
	}
	signals.ObservedAt = start.Add(2 * time.Second)
	signals.TelemetrySource = "leader-b"
	signals.TelemetryStartedAt = signals.ObservedAt
	if got := policy.Recommend(10, signals, signals.ObservedAt, state); !got.ScaleDownBlocked ||
		got.Desired != 10 {
		t.Fatalf("new Leader epoch did not block scale-down: %+v", got)
	}
}

type signalReaderFunc func(context.Context, string) (Signals, error)

func (f signalReaderFunc) Read(ctx context.Context, baseURL string) (Signals, error) {
	return f(ctx, baseURL)
}

func TestRunnerRetainsReplicasOnSignalErrorAndPatchesOnlyWorkerTarget(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	control := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(3))},
	}
	worker := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice-worker", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(5))},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(control, worker).Build()
	now := time.Unix(1000, 0)
	runner := &Runner{
		Client: k8sClient, Namespace: "default", StatefulSet: "sluice-worker",
		SluiceURL: "http://sluice", Policy: Policy{Config: DefaultConfig()},
		Now: func() time.Time { return now },
		Reader: signalReaderFunc(func(context.Context, string) (Signals, error) {
			return Signals{}, errors.New("temporary API failure")
		}),
	}
	if err := runner.ReconcileOnce(context.Background()); err == nil {
		t.Fatal("signal failure was not reported")
	}
	assertReplicas(t, k8sClient, "sluice-worker", 5)
	assertReplicas(t, k8sClient, "sluice", 3)

	runner.Reader = signalReaderFunc(func(context.Context, string) (Signals, error) {
		return Signals{Backlog: 20_000, AllocatedWorkers: 500, WorkerCapacity: 500}, nil
	})
	if err := runner.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertReplicas(t, k8sClient, "sluice-worker", 15)
	assertReplicas(t, k8sClient, "sluice", 3)
}

func TestRunnerEnforcesMinimumBeforeReadingUnavailableSignals(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	control := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(3))},
	}
	worker := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice-worker", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(1))},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(control, worker).Build()
	readCalled := false
	runner := &Runner{
		Client: k8sClient, Namespace: "default", StatefulSet: "sluice-worker",
		SluiceURL: "http://unavailable", Policy: Policy{Config: DefaultConfig()},
		Reader: signalReaderFunc(func(context.Context, string) (Signals, error) {
			readCalled = true
			return Signals{}, errors.New("unavailable")
		}),
	}
	if err := runner.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if readCalled {
		t.Fatal("signal reader was called before restoring the configured minimum")
	}
	assertReplicas(t, k8sClient, "sluice-worker", DefaultConfig().MinReplicas)
	assertReplicas(t, k8sClient, "sluice", 3)
}

func TestRunnerResolvesRealServiceClusterIPInsteadOfClusterDNS(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	worker := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice-worker", Namespace: "default"},
		Spec:       appsv1.StatefulSetSpec{Replicas: ptr(int32(5))},
	}
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "sluice", Namespace: "default"},
		Spec:       corev1.ServiceSpec{ClusterIP: "10.152.183.17"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(worker, service).Build()
	var gotURL string
	runner := &Runner{
		Client: k8sClient, Namespace: "default", StatefulSet: "sluice-worker",
		SluiceURL: "http://sluice:9090", SluiceService: "sluice", SluicePort: 9090,
		Policy: Policy{Config: DefaultConfig()},
		Reader: signalReaderFunc(func(_ context.Context, baseURL string) (Signals, error) {
			gotURL = baseURL
			return Signals{}, nil
		}),
	}
	if err := runner.ReconcileOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotURL != "http://10.152.183.17:9090" {
		t.Fatalf("signal URL = %q, want real Service ClusterIP", gotURL)
	}
}

func assertReplicas(t *testing.T, k8sClient client.Client, name string, want int32) {
	t.Helper()
	var statefulSet appsv1.StatefulSet
	if err := k8sClient.Get(
		context.Background(),
		types.NamespacedName{Name: name, Namespace: "default"},
		&statefulSet,
	); err != nil {
		t.Fatal(err)
	}
	if statefulSet.Spec.Replicas == nil || *statefulSet.Spec.Replicas != want {
		t.Fatalf("%s replicas = %v, want %d", name, statefulSet.Spec.Replicas, want)
	}
}
