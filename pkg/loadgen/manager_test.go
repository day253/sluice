package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeClusterClient struct {
	mu             sync.Mutex
	tenants        map[string]TenantSnapshot
	keys           map[string]bool
	submitted      int
	submitRequests int
	active         int
	maxActive      int
	submitDelay    time.Duration
	failFirst      bool
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
	delay := f.submitDelay
	shouldFail := f.failFirst && request == 1
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.active--
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
		client.maxActive != 4 {
		t.Fatalf(
			"run=%+v tenants=%d tasks=%d keys=%d requests=%d max-active=%d",
			run, len(client.tenants), client.submitted, len(client.keys),
			client.submitRequests, client.maxActive,
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
