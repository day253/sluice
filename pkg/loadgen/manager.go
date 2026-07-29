package loadgen

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ManagerConfig struct {
	PrepareConcurrency int
	BatchSize          int
	PollInterval       time.Duration
	WaveInterval       time.Duration
	DrainDeadline      time.Duration
	ZeroConfirmations  int
	RetryDelay         time.Duration
	Now                func() time.Time
}

type Manager struct {
	client ClusterClient
	config ManagerConfig

	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.RWMutex
	run    *Run
	done   map[string]chan struct{}
}

func NewManager(client ClusterClient, config ManagerConfig) *Manager {
	if config.PrepareConcurrency <= 0 {
		config.PrepareConcurrency = 12
	}
	if config.BatchSize <= 0 || config.BatchSize > DefaultBatchSize {
		config.BatchSize = DefaultBatchSize
	}
	if config.PollInterval <= 0 {
		config.PollInterval = time.Second
	}
	if config.WaveInterval <= 0 {
		config.WaveInterval = time.Second
	}
	if config.DrainDeadline <= 0 {
		config.DrainDeadline = 15 * time.Minute
	}
	if config.ZeroConfirmations <= 0 {
		config.ZeroConfirmations = 2
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = 250 * time.Millisecond
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		client: client, config: config, ctx: ctx, cancel: cancel,
		done: make(map[string]chan struct{}),
	}
}

func (m *Manager) Close() {
	m.cancel()
}

func (m *Manager) Start(request StartRequest) (Run, error) {
	normalized, err := normalizeStartRequest(request)
	if err != nil {
		return Run{}, err
	}

	m.mu.Lock()
	if m.run != nil && !terminalStatus(m.run.Status) {
		m.mu.Unlock()
		return Run{}, ErrRunActive
	}
	now := m.config.Now()
	id := fmt.Sprintf("load-%x", now.UnixNano())
	initialConcurrency := newSubmissionController(normalized.request.Options.SubmissionMode).current
	run := &Run{
		ID: id, Name: normalized.request.Name, Recipe: normalized.request.Recipe,
		Operation: normalized.request.Operation, Status: "preparing",
		StartedAt: now, TenantCount: len(normalized.specs),
		TotalTasks:               normalized.totalTasks,
		SubmissionMode:           normalized.request.Options.SubmissionMode,
		SubmissionConcurrency:    initialConcurrency,
		MaxSubmissionConcurrency: initialConcurrency,
		Options:                  normalized.request.Options,
		TenantSpecs:              append([]TenantSpec(nil), normalized.specs...),
		Message:                  "Load Generator Pod is validating the target tenant pool…",
	}
	m.run = run
	done := make(chan struct{})
	m.done[id] = done
	snapshot := cloneRun(run)
	m.mu.Unlock()

	go func() {
		defer close(done)
		m.execute(id, normalized)
	}()
	return snapshot, nil
}

func (m *Manager) Current() (Run, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.run == nil {
		return Run{}, false
	}
	return cloneRun(m.run), true
}

func (m *Manager) Stop(id string) (Run, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.run == nil || m.run.ID != id {
		return Run{}, ErrRunNotFound
	}
	if !terminalStatus(m.run.Status) {
		m.run.StopRequested = true
		m.run.Message = "Stopping future batches; already committed tasks will continue."
	}
	return cloneRun(m.run), nil
}

func (m *Manager) Wait(id string, timeout time.Duration) (Run, error) {
	m.mu.RLock()
	done := m.done[id]
	m.mu.RUnlock()
	if done == nil {
		return Run{}, ErrRunNotFound
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		run, ok := m.Current()
		if !ok || run.ID != id {
			return Run{}, ErrRunNotFound
		}
		return run, nil
	case <-timer.C:
		return Run{}, fmt.Errorf("load generation run %s did not finish within %s", id, timeout)
	}
}

func (m *Manager) execute(id string, normalized normalizedRequest) {
	if err := m.validateTargets(normalized); err != nil {
		m.finish(id, "failed", err.Error())
		return
	}
	if normalized.generatedPool {
		if err := m.prepareTenants(id, normalized.specs); err != nil {
			status := "failed"
			if m.stopRequested(id) {
				status = "stopped"
			}
			m.finish(id, status, err.Error())
			return
		}
	} else {
		m.update(id, func(run *Run) {
			run.Prepared = len(normalized.specs)
			run.Message = fmt.Sprintf(
				"Load Generator Pod validated %d existing tenants.",
				len(normalized.specs),
			)
		})
	}
	if normalized.request.Operation == "tenants" {
		m.finish(id, "completed", fmt.Sprintf(
			"Created or updated %d reusable tenants from the Load Generator Pod.",
			len(normalized.specs),
		))
		return
	}

	m.update(id, func(run *Run) {
		run.Status = "submitting"
		run.Message = fmt.Sprintf(
			"Load Generator Pod is submitting %d tasks in round-robin order…",
			run.TotalTasks,
		)
	})
	jobs := buildRoundRobinJobs(normalized.specs)
	waveCount := 1
	if normalized.request.Options.Delivery == "waves" {
		waveCount = normalized.request.Options.Waves
	}
	waves := splitWaves(jobs, waveCount)
	controller := newSubmissionController(normalized.request.Options.SubmissionMode)
	for waveIndex, wave := range waves {
		if m.stopRequested(id) || m.ctx.Err() != nil {
			break
		}
		batches := make([][]job, 0, (len(wave)+m.config.BatchSize-1)/m.config.BatchSize)
		for start := 0; start < len(wave); start += m.config.BatchSize {
			end := min(len(wave), start+m.config.BatchSize)
			batches = append(batches, wave[start:end])
		}
		m.submitBatches(id, normalized.request.Recipe, waveIndex, len(waves), batches, controller)
		if waveIndex < len(waves)-1 && !m.stopRequested(id) {
			if !m.sleep(m.config.WaveInterval) {
				break
			}
		}
	}

	now := m.config.Now()
	m.update(id, func(run *Run) {
		run.Status = "draining"
		run.SubmittedAt = &now
		if run.StopRequested {
			run.Message = "Future submissions stopped; draining committed tasks…"
		} else {
			run.Message = "All submission batches settled; monitoring unfinished tasks…"
		}
	})
	if err := m.monitorDrain(id, normalized.specs); err != nil {
		m.finish(id, "failed", err.Error())
		return
	}
	run, _ := m.Current()
	switch {
	case run.StopRequested:
		m.finish(id, "stopped", fmt.Sprintf(
			"Stopped after %d accepted tasks; committed work drained.", run.Submitted,
		))
	case run.Failed > 0:
		m.finish(id, "failed", fmt.Sprintf(
			"%d task submissions failed; %d accepted tasks drained.",
			run.Failed, run.Submitted,
		))
	default:
		m.finish(id, "completed", fmt.Sprintf(
			"All %d tasks submitted by the Load Generator Pod drained.", run.Submitted,
		))
	}
}

func (m *Manager) validateTargets(normalized normalizedRequest) error {
	tenants, err := m.client.ListTenants(m.ctx)
	if err != nil {
		return fmt.Errorf("read tenant pool: %w", err)
	}
	if normalized.generatedPool {
		for id, tenant := range tenants {
			if strings.HasPrefix(id, tenantPrefix) && tenant.Inflight > 0 {
				return fmt.Errorf(
					"Load Lab tenant pool still has unfinished tasks; %s has %d",
					id, tenant.Inflight,
				)
			}
		}
		return nil
	}
	for _, spec := range normalized.specs {
		if _, ok := tenants[spec.ID]; !ok {
			return fmt.Errorf("tenant %s does not exist", spec.ID)
		}
	}
	return nil
}

func (m *Manager) prepareTenants(id string, specs []TenantSpec) error {
	type result struct {
		err error
	}
	results := make(chan result, len(specs))
	sem := make(chan struct{}, m.config.PrepareConcurrency)
	started := 0
	for _, spec := range specs {
		if m.stopRequested(id) || m.ctx.Err() != nil {
			break
		}
		started++
		sem <- struct{}{}
		go func(spec TenantSpec) {
			defer func() { <-sem }()
			err := m.client.UpsertTenant(m.ctx, spec)
			results <- result{err: err}
		}(spec)
	}
	for index := 0; index < started; index++ {
		item := <-results
		if item.err != nil {
			return fmt.Errorf("prepare tenant: %w", item.err)
		}
		m.update(id, func(run *Run) {
			run.Prepared++
			run.Message = fmt.Sprintf(
				"Load Generator Pod prepared %d/%d tenants…",
				run.Prepared, run.TenantCount,
			)
		})
	}
	if started != len(specs) {
		return fmt.Errorf("stopped before tenant preparation completed")
	}
	return nil
}

type batchResult struct {
	size          int
	accepted      int
	err           error
	statusCode    int
	latency       time.Duration
	backpressured bool
}

func (m *Manager) submitBatches(
	id, recipe string,
	waveIndex, waveCount int,
	batches [][]job,
	controller *submissionController,
) {
	results := make(chan batchResult, len(batches))
	next := 0
	active := 0
	for (next < len(batches) && !m.stopRequested(id)) || active > 0 {
		for next < len(batches) && active < controller.current && !m.stopRequested(id) {
			batch := batches[next]
			next++
			active++
			go func(batch []job) {
				started := time.Now()
				accepted, statusCode, err, backpressured := m.submitBatch(id, recipe, batch)
				results <- batchResult{
					size: len(batch), accepted: accepted, err: err,
					statusCode: statusCode, latency: time.Since(started),
					backpressured: backpressured,
				}
			}(batch)
		}
		if active == 0 {
			break
		}
		result := <-results
		active--
		controller.observe(result.err != nil, result.backpressured, result.latency)
		m.update(id, func(run *Run) {
			run.Submitted += result.accepted
			run.Failed += result.size - result.accepted
			run.SubmissionConcurrency = controller.current
			run.MaxSubmissionConcurrency = controller.maxObserved
			run.SubmissionBackoffs = controller.backoffs
			run.Message = fmt.Sprintf(
				"Submitting wave %d/%d from the Load Generator Pod · %d/%d accepted…",
				waveIndex+1, waveCount, run.Submitted, run.TotalTasks,
			)
		})
	}
}

func (m *Manager) submitBatch(
	id, recipe string, batch []job,
) (accepted, statusCode int, finalErr error, backpressured bool) {
	tasks := make([]Task, len(batch))
	for index, item := range batch {
		tasks[index] = Task{
			TenantID: item.tenantID,
			Payload: map[string]any{
				"source": "load-generator", "run_id": id,
				"recipe": recipe, "index": item.index,
			},
			IdempotencyKey: fmt.Sprintf("%s:%s:%d", id, item.tenantID, item.index),
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		accepted, statusCode, finalErr = m.client.SubmitBatch(m.ctx, tasks)
		if finalErr == nil {
			return accepted, statusCode, nil, backpressured
		}
		if statusCode == 429 || statusCode == 503 {
			backpressured = true
		}
		if attempt == 0 && !m.stopRequested(id) {
			jitter := time.Duration(rand.Intn(250)) * time.Millisecond
			if !m.sleep(m.config.RetryDelay + jitter) {
				break
			}
		}
	}
	return 0, statusCode, finalErr, backpressured
}

func (m *Manager) monitorDrain(id string, specs []TenantSpec) error {
	targets := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		targets[spec.ID] = struct{}{}
	}
	deadline := time.Now().Add(m.config.DrainDeadline)
	zeroSamples := 0
	for time.Now().Before(deadline) {
		tenants, err := m.client.ListTenants(m.ctx)
		if err != nil {
			return fmt.Errorf("monitor tenant backlog: %w", err)
		}
		remaining := 0
		for tenantID := range targets {
			remaining += tenants[tenantID].Inflight
		}
		m.update(id, func(run *Run) {
			run.Remaining = remaining
			run.PeakBacklog = max(run.PeakBacklog, remaining)
			if remaining > 0 {
				run.Message = fmt.Sprintf(
					"Load Generator Pod is draining %d unfinished tasks…", remaining,
				)
			} else {
				run.Message = "Confirming the backlog is durably empty…"
			}
		})
		if remaining == 0 {
			zeroSamples++
			if zeroSamples >= m.config.ZeroConfirmations {
				return nil
			}
		} else {
			zeroSamples = 0
		}
		if !m.sleep(m.config.PollInterval) {
			return fmt.Errorf("load generator stopped while monitoring committed tasks")
		}
	}
	run, _ := m.Current()
	return fmt.Errorf("drain deadline exceeded with %d unfinished tasks", run.Remaining)
}

func (m *Manager) update(id string, change func(*Run)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.run == nil || m.run.ID != id {
		return
	}
	change(m.run)
}

func (m *Manager) finish(id, status, message string) {
	now := m.config.Now()
	m.update(id, func(run *Run) {
		run.Status = status
		run.Message = message
		run.EndedAt = &now
	})
}

func (m *Manager) stopRequested(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.run == nil || m.run.ID != id || m.run.StopRequested
}

func (m *Manager) sleep(duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-m.ctx.Done():
		return false
	}
}

func cloneRun(input *Run) Run {
	result := *input
	result.TenantSpecs = append([]TenantSpec(nil), input.TenantSpecs...)
	return result
}

func terminalStatus(status string) bool {
	return status == "completed" || status == "failed" || status == "stopped"
}

type submissionController struct {
	fixed       bool
	current     int
	maxObserved int
	backoffs    int
	successful  int
}

func newSubmissionController(mode string) *submissionController {
	if mode != "auto" {
		value, _ := strconv.Atoi(mode)
		return &submissionController{fixed: true, current: value, maxObserved: value}
	}
	return &submissionController{current: 8, maxObserved: 8}
}

func (c *submissionController) observe(failed, backpressured bool, latency time.Duration) {
	if c.fixed {
		return
	}
	if failed || backpressured || latency >= time.Second {
		c.current = max(4, c.current/2)
		c.backoffs++
		c.successful = 0
		return
	}
	c.successful++
	if c.successful >= c.current {
		c.current = min(16, c.current+2)
		c.maxObserved = max(c.maxObserved, c.current)
		c.successful = 0
	}
}
