// Package autoscaler scales only the stateless Worker execution plane from
// Sluice workload signals. It never changes control/Raft membership.
package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// Signals is a process-local snapshot read from the Sluice API. It is neither
// persisted in Raft nor included in FSM snapshots.
type Signals struct {
	ObservedAt         time.Time
	Backlog            int64
	PendingTasks       int64
	RunningTasks       int64
	OldestPendingAge   time.Duration
	TaskBreakdownValid bool
	AllocatedWorkers   int64
	WorkerCapacity     int64
	WorkerInstances    int64

	ExecutionSignalsValid  bool
	ReportingWorkers       int64
	ExecutingTasks         int64
	AverageWorkerCPUMillis int64
	MaxWorkerCPUMillis     int64

	RateCountersValid   bool
	TelemetrySource     string
	TelemetryStartedAt  time.Time
	SubmittedTasksTotal int64
	CompletedTasksTotal int64
}

// Config defines workload-aware horizontal scaling bounds.
type Config struct {
	MinReplicas                 int32
	MaxReplicas                 int32
	WorkersPerPod               int32
	TargetBacklogPerPod         int64
	TargetWorkerUtilization     int32
	TargetCPUUtilization        int32
	TargetQueueDrainTime        time.Duration
	TargetThroughputUtilization int32
	TolerancePercent            int32
	MinTelemetryCoveragePercent int32
	ScaleUpPercent              int32
	ScaleUpPods                 int32
	ScaleUpPeriod               time.Duration
	ScaleDownPercent            int32
	ScaleDownPeriod             time.Duration
	ScaleDownStabilization      time.Duration
}

// DefaultConfig is intentionally conservative on scale-down and responsive on
// scale-up. Per-Pod Processor concurrency remains a static deployment bound.
func DefaultConfig() Config {
	return Config{
		MinReplicas: 5, MaxReplicas: 100, WorkersPerPod: 100,
		TargetBacklogPerPod: 400, TargetWorkerUtilization: 70,
		TargetCPUUtilization: 70, TargetQueueDrainTime: 30 * time.Second,
		TargetThroughputUtilization: 80, TolerancePercent: 10,
		MinTelemetryCoveragePercent: 80,
		ScaleUpPercent:              100, ScaleUpPods: 10, ScaleUpPeriod: 5 * time.Second,
		ScaleDownPercent: 25, ScaleDownPeriod: time.Minute,
		ScaleDownStabilization: 5 * time.Minute,
	}
}

// Validate rejects configurations that could remove all execution capacity or
// make the controller divide by zero.
func (c Config) Validate() error {
	if c.MinReplicas < 1 || c.MaxReplicas < c.MinReplicas {
		return fmt.Errorf("autoscaling requires 1 <= minReplicas <= maxReplicas")
	}
	if c.WorkersPerPod < 1 || c.TargetBacklogPerPod < 1 {
		return fmt.Errorf("workersPerPod and targetBacklogPerPod must be positive")
	}
	if c.TargetWorkerUtilization < 1 || c.TargetWorkerUtilization > 100 ||
		c.TargetCPUUtilization < 1 || c.TargetCPUUtilization > 100 ||
		c.TargetThroughputUtilization < 1 || c.TargetThroughputUtilization > 100 {
		return fmt.Errorf("utilization targets must be between 1 and 100")
	}
	if c.TargetQueueDrainTime <= 0 {
		return fmt.Errorf("targetQueueDrainTime must be positive")
	}
	if c.TolerancePercent < 0 || c.TolerancePercent > 100 {
		return fmt.Errorf("tolerancePercent must be between 0 and 100")
	}
	if c.MinTelemetryCoveragePercent < 1 || c.MinTelemetryCoveragePercent > 100 {
		return fmt.Errorf("minTelemetryCoveragePercent must be between 1 and 100")
	}
	if c.ScaleUpPercent < 1 || c.ScaleUpPods < 1 || c.ScaleUpPeriod <= 0 {
		return fmt.Errorf("scale-up bounds must be positive")
	}
	if c.ScaleDownPercent < 1 || c.ScaleDownPercent > 100 ||
		c.ScaleDownPeriod <= 0 || c.ScaleDownStabilization < 0 {
		return fmt.Errorf("scale-down bounds are invalid")
	}
	return nil
}

// Recommendation explains one bounded scaling decision.
type Recommendation struct {
	Current            int32
	Desired            int32
	RawDesired         int32
	BacklogDesired     int32
	QueueDesired       int32
	UtilizationDesired int32
	CPUDesired         int32
	DrainDesired       int32
	ArrivalDesired     int32
	UtilizationPercent float64
	CPUPercent         float64
	ArrivalRate        float64
	CompletionRate     float64
	RatesValid         bool
	TelemetryCoverage  float64
	ScaleDownBlocked   bool
	DominantSignal     string
	Reason             string
}

// State is the non-durable stabilization memory for one scale target.
type State struct {
	BelowSince time.Time
	LastScale  time.Time

	telemetryEpoch string
	lastObservedAt time.Time
	lastSubmitted  int64
	lastCompleted  int64
	arrivalRate    float64
	completionRate float64
	ratesValid     bool
}

// StateStore keeps bounded process-local stabilization state per scale target.
type StateStore struct {
	mu     sync.Mutex
	states map[string]*State
}

func (s *StateStore) Recommend(
	key string, policy Policy, current int32, signals Signals, now time.Time,
) Recommendation {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]*State)
	}
	state := s.states[key]
	if state == nil {
		state = &State{}
		s.states[key] = state
	}
	return policy.Recommend(current, signals, now, state)
}

func (s *StateStore) RecordApplied(key string, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[string]*State)
	}
	state := s.states[key]
	if state == nil {
		state = &State{}
		s.states[key] = state
	}
	state.RecordApplied(now)
}

// Policy converts current workload signals into a rate-limited replica count.
type Policy struct {
	Config Config
}

// Recommend never scales control nodes and never changes per-Pod concurrency.
// On bad signals the caller must retain the current replica count.
func (p Policy) Recommend(current int32, signals Signals, now time.Time, state *State) Recommendation {
	config := p.Config
	current = clamp(current, config.MinReplicas, config.MaxReplicas)
	state.observeRates(signals)

	pending := signals.Backlog
	if signals.TaskBreakdownValid {
		pending = signals.PendingTasks
	}
	queueDesired := int32(0)
	if pending > 0 {
		queueDesired = ceilDiv(pending, config.TargetBacklogPerPod)
	}

	utilization := float64(0)
	utilizationDesired := int32(0)
	registeredPods := signals.WorkerInstances
	if registeredPods == 0 && signals.WorkerCapacity > 0 {
		registeredPods = int64(ceilDiv(signals.WorkerCapacity, int64(config.WorkersPerPod)))
	}
	if signals.ExecutionSignalsValid && signals.WorkerCapacity > 0 && registeredPods > 0 {
		utilization = float64(signals.ExecutingTasks) / float64(signals.WorkerCapacity) * 100
		utilizationDesired = int32(math.Ceil(
			float64(registeredPods) * utilization / float64(config.TargetWorkerUtilization),
		))
	}

	cpuPercent := float64(0)
	cpuDesired := int32(0)
	if signals.ExecutionSignalsValid && signals.ReportingWorkers > 0 {
		cpuPercent = float64(signals.AverageWorkerCPUMillis) / 10
		cpuDesired = int32(math.Ceil(
			float64(signals.ReportingWorkers) * cpuPercent /
				float64(config.TargetCPUUtilization),
		))
	}

	drainDesired, arrivalDesired := int32(0), int32(0)
	// Rate projection uses only Pods that produced the measured completion
	// rate. While StatefulSet desired is still above registered Pods, those
	// already-starting Pods satisfy the current rate recommendation; wait for
	// them before projecting another rate-driven increase.
	if state.ratesValid && registeredPods >= int64(current) &&
		registeredPods > 0 && state.completionRate > 0 {
		perPodCompletionRate := state.completionRate / float64(registeredPods)
		if pending > 0 {
			drainDesired = ceilFloat(
				float64(pending) /
					(config.TargetQueueDrainTime.Seconds() * perPodCompletionRate),
			)
		}
		if state.arrivalRate > 0 {
			arrivalDesired = ceilFloat(
				state.arrivalRate /
					(perPodCompletionRate * float64(config.TargetThroughputUtilization) / 100),
			)
		}
	}

	raw := max32(
		config.MinReplicas, queueDesired, utilizationDesired,
		cpuDesired, drainDesired, arrivalDesired,
	)
	raw = clamp(raw, config.MinReplicas, config.MaxReplicas)
	desired := raw
	reason := "within target"
	telemetryCoverage := float64(0)
	telemetryCoverageComplete := false
	if registeredPods == 0 {
		// No execution instances is a coherent complete snapshot. The minimum
		// replica bound still prevents removal of all execution capacity.
		telemetryCoverageComplete = signals.ExecutionSignalsValid
	} else {
		telemetryCoverage = float64(signals.ReportingWorkers) / float64(registeredPods) * 100
		telemetryCoverageComplete = signals.ExecutionSignalsValid &&
			signals.ReportingWorkers <= registeredPods &&
			telemetryCoverage >= float64(config.MinTelemetryCoveragePercent)
	}
	dominant := dominantSignal(
		raw, queueDesired, utilizationDesired, cpuDesired, drainDesired, arrivalDesired,
	)

	relativeChangePercent := float64(abs32(raw-current)) / float64(current) * 100
	if raw != current && relativeChangePercent <= float64(config.TolerancePercent) {
		desired = current
		reason = "within scaling tolerance"
		state.BelowSince = time.Time{}
	}

	switch {
	case desired > current:
		state.BelowSince = time.Time{}
		if !state.LastScale.IsZero() && now.Sub(state.LastScale) < config.ScaleUpPeriod {
			desired = current
			reason = "scale-up rate limited"
			break
		}
		increase := max32(config.ScaleUpPods, int32(math.Ceil(float64(current)*float64(config.ScaleUpPercent)/100)))
		desired = min32(raw, current+increase)
		reason = "scale-up: " + dominant
	case desired < current:
		if !signals.TaskBreakdownValid || !telemetryCoverageComplete ||
			!state.ratesValid {
			desired = current
			reason = "scale-down blocked by incomplete signals"
			state.BelowSince = time.Time{}
			break
		}
		if state.BelowSince.IsZero() {
			state.BelowSince = now
		}
		if now.Sub(state.BelowSince) < config.ScaleDownStabilization {
			desired = current
			reason = "scale-down stabilization"
			break
		}
		if !state.LastScale.IsZero() && now.Sub(state.LastScale) < config.ScaleDownPeriod {
			desired = current
			reason = "scale-down rate limited"
			break
		}
		reduction := max32(1, int32(math.Floor(float64(current)*float64(config.ScaleDownPercent)/100)))
		desired = max32(raw, current-reduction)
		reason = "sustained spare execution capacity"
	default:
		state.BelowSince = time.Time{}
	}

	return Recommendation{
		Current: current, Desired: desired, RawDesired: raw,
		BacklogDesired: queueDesired, QueueDesired: queueDesired,
		UtilizationDesired: utilizationDesired, CPUDesired: cpuDesired,
		DrainDesired: drainDesired, ArrivalDesired: arrivalDesired,
		UtilizationPercent: utilization, CPUPercent: cpuPercent,
		ArrivalRate: state.arrivalRate, CompletionRate: state.completionRate,
		RatesValid: state.ratesValid, TelemetryCoverage: telemetryCoverage,
		ScaleDownBlocked: raw < current &&
			(!signals.TaskBreakdownValid || !telemetryCoverageComplete ||
				!state.ratesValid),
		DominantSignal: dominant, Reason: reason,
	}
}

const rateEWMAAlpha = 0.5

func (s *State) observeRates(signals Signals) {
	if !signals.RateCountersValid || signals.ObservedAt.IsZero() {
		s.ratesValid = false
		return
	}
	epoch := signals.TelemetrySource + "/" + signals.TelemetryStartedAt.UTC().Format(time.RFC3339Nano)
	if epoch == "/" || epoch != s.telemetryEpoch ||
		signals.SubmittedTasksTotal < s.lastSubmitted ||
		signals.CompletedTasksTotal < s.lastCompleted {
		s.telemetryEpoch = epoch
		s.lastObservedAt = signals.ObservedAt
		s.lastSubmitted = signals.SubmittedTasksTotal
		s.lastCompleted = signals.CompletedTasksTotal
		s.arrivalRate = 0
		s.completionRate = 0
		s.ratesValid = false
		return
	}
	elapsed := signals.ObservedAt.Sub(s.lastObservedAt).Seconds()
	if elapsed <= 0 {
		return
	}
	arrival := float64(signals.SubmittedTasksTotal-s.lastSubmitted) / elapsed
	completion := float64(signals.CompletedTasksTotal-s.lastCompleted) / elapsed
	if !s.ratesValid {
		s.arrivalRate, s.completionRate = arrival, completion
		s.ratesValid = true
	} else {
		s.arrivalRate = rateEWMAAlpha*arrival + (1-rateEWMAAlpha)*s.arrivalRate
		s.completionRate = rateEWMAAlpha*completion + (1-rateEWMAAlpha)*s.completionRate
	}
	s.lastObservedAt = signals.ObservedAt
	s.lastSubmitted = signals.SubmittedTasksTotal
	s.lastCompleted = signals.CompletedTasksTotal
}

func dominantSignal(
	raw, queue, utilization, cpu, drain, arrival int32,
) string {
	dominantName := "minimum replica floor"
	dominantValue := int32(0)
	for _, candidate := range []struct {
		name  string
		value int32
	}{
		{"queue depth", queue},
		{"running-slot utilization", utilization},
		{"CPU utilization", cpu},
		{"queue drain SLO", drain},
		{"arrival rate", arrival},
	} {
		if candidate.value > dominantValue {
			dominantName, dominantValue = candidate.name, candidate.value
		}
	}
	if dominantValue == 0 || raw > dominantValue {
		return "minimum replica floor"
	}
	return dominantName
}

// RecordApplied starts the next rate-limit interval only after the Kubernetes
// scale write succeeds.
func (s *State) RecordApplied(now time.Time) {
	s.LastScale = now
}

// SignalReader reads current state from Sluice without mutating it.
type SignalReader interface {
	Read(context.Context, string) (Signals, error)
}

// HTTPReader reads the production admin API exposed by a control Service.
type HTTPReader struct {
	Client *http.Client
}

func (r HTTPReader) Read(ctx context.Context, baseURL string) (Signals, error) {
	httpClient := r.Client
	if httpClient == nil {
		httpClient = newDirectHTTPClient()
	}
	baseURL = strings.TrimRight(baseURL, "/")

	var snapshot struct {
		ObservedAt             time.Time `json:"observed_at"`
		UnfinishedTasks        int64     `json:"unfinished_tasks"`
		PendingTasks           int64     `json:"pending_tasks"`
		RunningTasks           int64     `json:"running_tasks"`
		OldestPendingAgeMillis int64     `json:"oldest_pending_age_ms"`
		TaskBreakdownValid     bool      `json:"task_breakdown_valid"`
		AllocatedWorkers       int64     `json:"allocated_workers"`
		WorkerCapacity         int64     `json:"worker_capacity"`
		WorkerInstances        int64     `json:"worker_instances"`
		ExecutionSignalsValid  bool      `json:"execution_signals_valid"`
		ReportingWorkers       int64     `json:"reporting_workers"`
		ExecutingTasks         int64     `json:"executing_tasks"`
		AverageWorkerCPUMillis int64     `json:"average_worker_cpu_millis"`
		MaxWorkerCPUMillis     int64     `json:"max_worker_cpu_millis"`
		RateCountersValid      bool      `json:"rate_counters_valid"`
		TelemetrySource        string    `json:"telemetry_source"`
		TelemetryStartedAt     time.Time `json:"telemetry_started_at"`
		SubmittedTasksTotal    int64     `json:"submitted_tasks_total"`
		CompletedTasksTotal    int64     `json:"completed_tasks_total"`
	}
	if err := readJSON(
		ctx, httpClient, baseURL+"/api/v1/admin/autoscaling", &snapshot,
	); err != nil {
		return Signals{}, fmt.Errorf("read autoscaling snapshot: %w", err)
	}
	return Signals{
		ObservedAt:             snapshot.ObservedAt,
		Backlog:                snapshot.UnfinishedTasks,
		PendingTasks:           snapshot.PendingTasks,
		RunningTasks:           snapshot.RunningTasks,
		OldestPendingAge:       time.Duration(snapshot.OldestPendingAgeMillis) * time.Millisecond,
		TaskBreakdownValid:     snapshot.TaskBreakdownValid,
		AllocatedWorkers:       snapshot.AllocatedWorkers,
		WorkerCapacity:         snapshot.WorkerCapacity,
		WorkerInstances:        snapshot.WorkerInstances,
		ExecutionSignalsValid:  snapshot.ExecutionSignalsValid,
		ReportingWorkers:       snapshot.ReportingWorkers,
		ExecutingTasks:         snapshot.ExecutingTasks,
		AverageWorkerCPUMillis: snapshot.AverageWorkerCPUMillis,
		MaxWorkerCPUMillis:     snapshot.MaxWorkerCPUMillis,
		RateCountersValid:      snapshot.RateCountersValid,
		TelemetrySource:        snapshot.TelemetrySource,
		TelemetryStartedAt:     snapshot.TelemetryStartedAt,
		SubmittedTasksTotal:    snapshot.SubmittedTasksTotal,
		CompletedTasksTotal:    snapshot.CompletedTasksTotal,
	}, nil
}

// newDirectHTTPClient deliberately ignores HTTP_PROXY/HTTPS_PROXY. The Sluice
// URL is a cluster-internal control Service; sending current workload mirrors
// through a host or corporate proxy can both leak control-plane state and make
// scaling unavailable when synthetic DNS is returned for the Service name.
func newDirectHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &http.Client{Timeout: 3 * time.Second, Transport: transport}
}

func readJSON(ctx context.Context, httpClient *http.Client, url string, target any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s returned %s", url, response.Status)
	}
	if err := json.NewDecoder(response.Body).Decode(target); err != nil {
		return err
	}
	return nil
}

// Runner is a controller-runtime leader-elected Runnable for a direct Helm
// release. Only this process owns the Worker StatefulSet replica field.
type Runner struct {
	Client        client.Client
	Namespace     string
	StatefulSet   string
	SluiceURL     string
	SluiceService string
	SluicePort    int32
	Interval      time.Duration
	Policy        Policy
	Reader        SignalReader
	Now           func() time.Time

	mu    sync.Mutex
	state State
}

func (r *Runner) NeedLeaderElection() bool { return true }

func (r *Runner) Start(ctx context.Context) error {
	interval := r.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if err := r.ReconcileOnce(ctx); err != nil {
		log.FromContext(ctx).Error(err, "initial workload autoscaling check failed")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := r.ReconcileOnce(ctx); err != nil {
				log.FromContext(ctx).Error(err, "workload autoscaling check failed")
			}
		}
	}
}

// ReconcileOnce enforces the static replica bounds without depending on signal
// availability. Once current is within bounds, every read or Kubernetes error
// retains the current replica count.
func (r *Runner) ReconcileOnce(ctx context.Context) error {
	var target appsv1.StatefulSet
	key := types.NamespacedName{Namespace: r.Namespace, Name: r.StatefulSet}
	if err := r.Client.Get(ctx, key, &target); err != nil {
		return fmt.Errorf("get Worker StatefulSet: %w", err)
	}
	current := r.Policy.Config.MinReplicas
	if target.Spec.Replicas != nil {
		current = *target.Spec.Replicas
	}
	bounded := clamp(current, r.Policy.Config.MinReplicas, r.Policy.Config.MaxReplicas)
	if bounded != current {
		before := target.DeepCopy()
		target.Spec.Replicas = ptr(bounded)
		if err := r.Client.Patch(ctx, &target, client.MergeFrom(before)); err != nil {
			return fmt.Errorf("enforce Worker replica bounds: %w", err)
		}
		log.FromContext(ctx).Info("workload autoscaler enforced Worker replica bounds",
			"from", current, "to", bounded,
			"minReplicas", r.Policy.Config.MinReplicas,
			"maxReplicas", r.Policy.Config.MaxReplicas)
		return nil
	}
	reader := r.Reader
	if reader == nil {
		reader = HTTPReader{}
	}
	sluiceURL, err := r.resolveSluiceURL(ctx)
	if err != nil {
		return fmt.Errorf("resolve Sluice control Service; retaining %d replicas: %w", current, err)
	}
	signals, err := reader.Read(ctx, sluiceURL)
	if err != nil {
		return fmt.Errorf("read workload signals; retaining %d replicas: %w", current, err)
	}
	now := time.Now()
	if r.Now != nil {
		now = r.Now()
	}
	r.mu.Lock()
	recommendation := r.Policy.Recommend(current, signals, now, &r.state)
	if recommendation.Desired == current {
		r.mu.Unlock()
		if signals.Backlog > 0 {
			log.FromContext(ctx).Info("workload autoscaler observed pressure",
				"unfinished", signals.Backlog, "pending", signals.PendingTasks,
				"running", signals.RunningTasks,
				"oldestPendingAge", signals.OldestPendingAge,
				"executingTasks", signals.ExecutingTasks,
				"averageWorkerCPUPercent", float64(signals.AverageWorkerCPUMillis)/10,
				"allocatedWorkers", signals.AllocatedWorkers,
				"workerCapacity", signals.WorkerCapacity,
				"workerInstances", signals.WorkerInstances, "replicas", current,
				"reportingWorkers", signals.ReportingWorkers,
				"queueDesired", recommendation.QueueDesired,
				"runningDesired", recommendation.UtilizationDesired,
				"cpuDesired", recommendation.CPUDesired,
				"drainDesired", recommendation.DrainDesired,
				"arrivalDesired", recommendation.ArrivalDesired,
				"arrivalRate", recommendation.ArrivalRate,
				"completionRate", recommendation.CompletionRate,
				"telemetryCoveragePercent", recommendation.TelemetryCoverage,
				"scaleDownBlocked", recommendation.ScaleDownBlocked,
				"rawDesired", recommendation.RawDesired,
				"dominantSignal", recommendation.DominantSignal,
				"reason", recommendation.Reason)
		}
		return nil
	}
	before := target.DeepCopy()
	target.Spec.Replicas = ptr(recommendation.Desired)
	if err := r.Client.Patch(ctx, &target, client.MergeFrom(before)); err != nil {
		r.mu.Unlock()
		return fmt.Errorf("patch Worker replicas: %w", err)
	}
	r.state.RecordApplied(now)
	r.mu.Unlock()
	log.FromContext(ctx).Info("workload autoscaler changed Worker replicas",
		"from", current, "to", recommendation.Desired, "rawDesired", recommendation.RawDesired,
		"unfinished", signals.Backlog, "pending", signals.PendingTasks,
		"running", signals.RunningTasks, "oldestPendingAge", signals.OldestPendingAge,
		"executingTasks", signals.ExecutingTasks,
		"allocatedWorkers", signals.AllocatedWorkers,
		"workerCapacity", signals.WorkerCapacity, "workerInstances", signals.WorkerInstances,
		"reportingWorkers", signals.ReportingWorkers,
		"runningUtilizationPercent", recommendation.UtilizationPercent,
		"cpuPercent", recommendation.CPUPercent,
		"queueDesired", recommendation.QueueDesired,
		"runningDesired", recommendation.UtilizationDesired,
		"cpuDesired", recommendation.CPUDesired,
		"drainDesired", recommendation.DrainDesired,
		"arrivalDesired", recommendation.ArrivalDesired,
		"arrivalRate", recommendation.ArrivalRate,
		"completionRate", recommendation.CompletionRate,
		"telemetryCoveragePercent", recommendation.TelemetryCoverage,
		"scaleDownBlocked", recommendation.ScaleDownBlocked,
		"dominantSignal", recommendation.DominantSignal,
		"reason", recommendation.Reason)
	return nil
}

func (r *Runner) resolveSluiceURL(ctx context.Context) (string, error) {
	if r.SluiceService == "" {
		if r.SluiceURL == "" {
			return "", fmt.Errorf("SluiceURL or SluiceService is required")
		}
		return r.SluiceURL, nil
	}
	var service corev1.Service
	key := types.NamespacedName{Namespace: r.Namespace, Name: r.SluiceService}
	if err := r.Client.Get(ctx, key, &service); err != nil {
		return "", fmt.Errorf("get Service %s: %w", key, err)
	}
	if net.ParseIP(service.Spec.ClusterIP) == nil {
		return "", fmt.Errorf("Service %s has no routable ClusterIP", key)
	}
	port := r.SluicePort
	if port == 0 {
		port = 9090
	}
	return "http://" + net.JoinHostPort(service.Spec.ClusterIP, strconv.Itoa(int(port))), nil
}

func ceilDiv(value, divisor int64) int32 {
	if value <= 0 {
		return 0
	}
	return int32((value + divisor - 1) / divisor)
}

func ceilFloat(value float64) int32 {
	if value <= 0 {
		return 0
	}
	if value >= float64(math.MaxInt32) {
		return math.MaxInt32
	}
	return int32(math.Ceil(value))
}

func abs32(value int32) int32 {
	if value < 0 {
		return -value
	}
	return value
}

func clamp(value, low, high int32) int32 { return min32(high, max32(low, value)) }
func min32(values ...int32) int32 {
	result := values[0]
	for _, value := range values[1:] {
		if value < result {
			result = value
		}
	}
	return result
}
func max32(values ...int32) int32 {
	result := values[0]
	for _, value := range values[1:] {
		if value > result {
			result = value
		}
	}
	return result
}
func ptr[T any](value T) *T { return &value }
