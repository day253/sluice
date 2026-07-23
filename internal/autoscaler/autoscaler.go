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
	Backlog          int64
	AllocatedWorkers int64
	WorkerCapacity   int64
	WorkerInstances  int64
}

// Config defines workload-aware horizontal scaling bounds.
type Config struct {
	MinReplicas             int32
	MaxReplicas             int32
	WorkersPerPod           int32
	TargetBacklogPerPod     int64
	TargetWorkerUtilization int32
	ScaleUpPercent          int32
	ScaleUpPods             int32
	ScaleUpPeriod           time.Duration
	ScaleDownPercent        int32
	ScaleDownPeriod         time.Duration
	ScaleDownStabilization  time.Duration
}

// DefaultConfig is intentionally conservative on scale-down and responsive on
// scale-up. Per-Pod Processor concurrency remains a static deployment bound.
func DefaultConfig() Config {
	return Config{
		MinReplicas: 5, MaxReplicas: 100, WorkersPerPod: 100,
		TargetBacklogPerPod: 400, TargetWorkerUtilization: 70,
		ScaleUpPercent: 100, ScaleUpPods: 10, ScaleUpPeriod: 5 * time.Second,
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
	if c.TargetWorkerUtilization < 1 || c.TargetWorkerUtilization > 100 {
		return fmt.Errorf("targetWorkerUtilization must be between 1 and 100")
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
	UtilizationDesired int32
	UtilizationPercent float64
	Reason             string
}

// State is the non-durable stabilization memory for one scale target.
type State struct {
	BelowSince time.Time
	LastScale  time.Time
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
	backlogDesired := int32(0)
	if signals.Backlog > 0 {
		backlogDesired = ceilDiv(signals.Backlog, config.TargetBacklogPerPod)
	}

	utilization := float64(0)
	utilizationDesired := int32(0)
	if signals.Backlog > 0 && signals.WorkerCapacity > 0 {
		utilization = float64(signals.AllocatedWorkers) / float64(signals.WorkerCapacity) * 100
		registeredPods := signals.WorkerInstances
		// Keep compatibility with externally supplied signal readers while the
		// production reader always reports the exact heterogeneous instance
		// count.
		if registeredPods == 0 {
			registeredPods = int64(ceilDiv(
				signals.WorkerCapacity,
				int64(config.WorkersPerPod),
			))
		}
		utilizationDesired = int32(math.Ceil(
			float64(registeredPods) * utilization / float64(config.TargetWorkerUtilization),
		))
	}

	raw := max32(config.MinReplicas, backlogDesired, utilizationDesired)
	raw = clamp(raw, config.MinReplicas, config.MaxReplicas)
	desired := raw
	reason := "within target"

	switch {
	case raw > current:
		state.BelowSince = time.Time{}
		if !state.LastScale.IsZero() && now.Sub(state.LastScale) < config.ScaleUpPeriod {
			desired = current
			reason = "scale-up rate limited"
			break
		}
		increase := max32(config.ScaleUpPods, int32(math.Ceil(float64(current)*float64(config.ScaleUpPercent)/100)))
		desired = min32(raw, current+increase)
		reason = "backlog or Worker allocation utilization above target"
	case raw < current:
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
		BacklogDesired: backlogDesired, UtilizationDesired: utilizationDesired,
		UtilizationPercent: utilization, Reason: reason,
	}
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

	var tenants map[string]struct {
		Inflight int64 `json:"inflight"`
	}
	if err := readJSON(ctx, httpClient, baseURL+"/api/v1/admin/tenants", &tenants); err != nil {
		return Signals{}, fmt.Errorf("read tenants: %w", err)
	}
	var nodes struct {
		Nodes []struct {
			NodeID       string `json:"node_id"`
			Role         string `json:"role"`
			Status       string `json:"status"`
			TotalWorkers int64  `json:"total_workers"`
		} `json:"nodes"`
	}
	if err := readJSON(ctx, httpClient, baseURL+"/api/v1/admin/nodes", &nodes); err != nil {
		return Signals{}, fmt.Errorf("read nodes: %w", err)
	}
	var allocations struct {
		Nodes []struct {
			NodeID  string           `json:"node_id"`
			Tenants map[string]int64 `json:"tenants"`
		} `json:"nodes"`
	}
	if err := readJSON(ctx, httpClient, baseURL+"/api/v1/admin/allocations", &allocations); err != nil {
		return Signals{}, fmt.Errorf("read allocations: %w", err)
	}

	var signals Signals
	liveWorkers := make(map[string]struct{}, len(nodes.Nodes))
	for _, tenant := range tenants {
		signals.Backlog += tenant.Inflight
	}
	for _, node := range nodes.Nodes {
		if node.Role == "worker" && node.Status == "up" {
			liveWorkers[node.NodeID] = struct{}{}
			signals.WorkerCapacity += node.TotalWorkers
			signals.WorkerInstances++
		}
	}
	for _, allocation := range allocations.Nodes {
		if _, live := liveWorkers[allocation.NodeID]; !live {
			continue
		}
		for _, workers := range allocation.Tenants {
			signals.AllocatedWorkers += workers
		}
	}
	return signals, nil
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
				"backlog", signals.Backlog, "allocatedWorkers", signals.AllocatedWorkers,
				"workerCapacity", signals.WorkerCapacity,
				"workerInstances", signals.WorkerInstances, "replicas", current,
				"rawDesired", recommendation.RawDesired, "reason", recommendation.Reason)
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
		"backlog", signals.Backlog, "allocatedWorkers", signals.AllocatedWorkers,
		"workerCapacity", signals.WorkerCapacity, "workerInstances", signals.WorkerInstances,
		"utilizationPercent", recommendation.UtilizationPercent,
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
