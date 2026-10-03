package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/day253/sluice/internal/testutil"
)

type fakeClusterClient struct {
	mu                      sync.Mutex
	tenants                 map[string]TenantSnapshot
	keys                    map[string]bool
	submitted               int
	submitRequests          int
	active                  int
	maxActive               int
	firstEnded              bool
	startedBeforeFirstEnded int
	submitDelay             time.Duration
	failFirst               bool
	gate                    *testutil.OverlapGate
}

func newFakeClusterClient() *fakeClusterClient {
	return &fakeClusterClient{
		tenants: make(map[string]TenantSnapshot),
		keys:    make(map[string]bool),
	}
}

func (f *fakeClusterClient) ListTenants(context.Context) (map[string]TenantSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	result := make(map[string]TenantSnapshot, len(f.tenants))
	for id, tenant := range f.tenants {
		result[id] = tenant
	}
	return result, nil
}

func (f *fakeClusterClient) UpsertTenant(_ context.Context, spec TenantSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenants[spec.ID] = TenantSnapshot{
		ID: spec.ID, Name: spec.Name, MaxWorkers: spec.MaxWorkers,
	}
	return nil
}

func (f *fakeClusterClient) SubmitBatch(
	_ context.Context, tasks []Task,
) (int, int, error) {
	f.mu.Lock()
	f.submitRequests++
	request := f.submitRequests
	f.active++
	f.maxActive = max(f.maxActive, f.active)
	if !f.firstEnded {
		f.startedBeforeFirstEnded++
	}
	delay := f.submitDelay
	shouldFail := f.failFirst && request == 1
	gate := f.gate
	f.mu.Unlock()

	// The gate holds the early batches so a correct rolling ingress reaches the
	// configured concurrency causally instead of depending on how fast this
	// machine happens to schedule its goroutines.
	if gate != nil {
		gate.Arrive(request - 1)
	}
	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.active--
	if request == 1 {
		f.firstEnded = true
	}
	if shouldFail {
		return 0, http.StatusServiceUnavailable, fmt.Errorf("temporary overload")
	}
	for _, task := range tasks {
		if f.keys[task.IdempotencyKey] {
			return 0, http.StatusConflict, fmt.Errorf("duplicate idempotency key")
		}
		f.keys[task.IdempotencyKey] = true
		f.submitted++
	}
	return len(tasks), http.StatusAccepted, nil
}

func TestManagerOwnsTenantCreationBatchingAndDrainOutsideBrowser(t *testing.T) {
	client := newFakeClusterClient()
	client.submitDelay = 10 * time.Millisecond
	// The gate deadline stays well below the 2s Wait below so a wave-barrier
	// regression reports the missing overlap instead of a wait timeout.
	client.gate = testutil.NewOverlapGate(4, 8, 500*time.Millisecond)
	manager := NewManager(client, ManagerConfig{
		PrepareConcurrency: 4,
		BatchSize:          2,
		PollInterval:       time.Millisecond,
		WaveInterval:       time.Millisecond,
		DrainDeadline:      time.Second,
		ZeroConfirmations:  1,
		RetryDelay:         time.Millisecond,
	})
	defer manager.Close()

	run, err := manager.Start(StartRequest{
		Name:      "server-side",
		Recipe:    "regression",
		Operation: "load",
		Options: Options{
			TenantCount:    4,
			TasksPerTenant: 4,
			Quota:          3,
			SubmissionMode: "4",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err = manager.Wait(run.ID, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if run.Status != "completed" || run.Submitted != 16 || run.Failed != 0 ||
		len(client.tenants) != 4 || client.submitted != 16 ||
		len(client.keys) != 16 || client.submitRequests != 8 ||
		client.maxActive != 4 || client.startedBeforeFirstEnded != 8 {
		t.Fatalf(
			"run=%+v tenants=%d tasks=%d keys=%d requests=%d max-active=%d before-first-ended=%d",
			run, len(client.tenants), client.submitted, len(client.keys),
			client.submitRequests, client.maxActive, client.startedBeforeFirstEnded,
		)
	}
	if overlapMiss, totalMiss := client.gate.Missed(); overlapMiss || totalMiss {
		t.Fatalf(
			"rolling ingress did not hold 4 batches in flight: overlap-miss=%v total-miss=%v",
			overlapMiss, totalMiss,
		)
	}
}

// TestManagerRollingIngressHoldsConfiguredBatchesInFlight is the SUBMIT-007
// regression: the ingress must overlap `submissionConcurrency` batches and keep
// refilling as they settle. The fake client's gate makes both properties causal
// — the early batches cannot finish before the configured concurrency is
// actually in flight, and the first batch cannot finish before every batch has
// started — so a serial or wave-barrier implementation fails on any machine,
// while the real implementation never waits.
func TestManagerRollingIngressHoldsConfiguredBatchesInFlight(t *testing.T) {
	const (
		concurrency = 16
		batches     = 20
	)
	client := newFakeClusterClient()
	client.submitDelay = 2 * time.Millisecond
	client.gate = testutil.NewOverlapGate(concurrency, batches, 5*time.Second)
	manager := NewManager(client, ManagerConfig{
		PrepareConcurrency: 4,
		BatchSize:          1000,
		PollInterval:       time.Millisecond,
		WaveInterval:       time.Millisecond,
		DrainDeadline:      5 * time.Second,
		ZeroConfirmations:  1,
		RetryDelay:         time.Millisecond,
	})
	defer manager.Close()

	run, err := manager.Start(StartRequest{
		Name:      "rolling-ingress",
		Recipe:    "regression",
		Operation: "load",
		Options: Options{
			TenantCount:    4,
			TasksPerTenant: 5000,
			Quota:          1,
			SubmissionMode: "16",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err = manager.Wait(run.ID, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	overlapMiss, totalMiss := client.gate.Missed()
	client.mu.Lock()
	defer client.mu.Unlock()
	if overlapMiss || totalMiss {
		t.Fatalf(
			"rolling ingress did not keep %d batches in flight: overlap-miss=%v total-miss=%v max-active=%d before-first-ended=%d",
			concurrency, overlapMiss, totalMiss, client.maxActive, client.startedBeforeFirstEnded,
		)
	}
	if run.Status != "completed" || run.Submitted != 20_000 || run.Failed != 0 ||
		client.submitRequests != batches || client.maxActive != concurrency ||
		client.startedBeforeFirstEnded != batches {
		t.Fatalf(
			"run=%+v requests=%d max-active=%d before-first-ended=%d, want %d batches at concurrency %d",
			run, client.submitRequests, client.maxActive, client.startedBeforeFirstEnded,
			batches, concurrency,
		)
	}
}

func TestManagerRejectsConcurrentRunAndRetriesBackpressureIdempotently(t *testing.T) {
	client := newFakeClusterClient()
	client.submitDelay = 5 * time.Millisecond
	client.failFirst = true
	manager := NewManager(client, ManagerConfig{
		BatchSize:         2,
		PollInterval:      time.Millisecond,
		DrainDeadline:     time.Second,
		ZeroConfirmations: 1,
		RetryDelay:        time.Millisecond,
	})
	defer manager.Close()

	run, err := manager.Start(StartRequest{
		Operation: "load",
		Options: Options{
			TenantCount:    1,
			TasksPerTenant: 4,
			SubmissionMode: "auto",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(StartRequest{
		Operation: "tenants",
		Options:   Options{TenantCount: 1},
	}); err != ErrRunActive {
		t.Fatalf("concurrent Start error = %v, want %v", err, ErrRunActive)
	}
	run, err = manager.Wait(run.ID, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if run.Status != "completed" || run.Submitted != 4 ||
		run.SubmissionBackoffs == 0 || client.submitted != 4 ||
		len(client.keys) != 4 || client.submitRequests != 3 {
		t.Fatalf("backpressure run=%+v client=%+v", run, client)
	}
}

func TestManagerDoesNotOverwriteBusyGeneratedTenantPool(t *testing.T) {
	client := newFakeClusterClient()
	client.tenants["load-lab-007"] = TenantSnapshot{
		ID: "load-lab-007", Inflight: 2,
	}
	manager := NewManager(client, ManagerConfig{})
	defer manager.Close()
	run, err := manager.Start(StartRequest{
		Operation: "tenants",
		Options:   Options{TenantCount: 10},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err = manager.Wait(run.ID, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != "failed" ||
		run.Message != "Load Lab tenant pool still has unfinished tasks; load-lab-007 has 2" {
		t.Fatalf("busy pool run = %+v", run)
	}
}
