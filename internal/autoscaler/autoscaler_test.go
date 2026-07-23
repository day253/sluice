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
	signals := Signals{Backlog: 20_000, AllocatedWorkers: 500, WorkerCapacity: 500}

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

func TestPolicyUsesAllocatedWorkerUtilizationForSmallBacklog(t *testing.T) {
	config := DefaultConfig()
	recommendation := (Policy{Config: config}).Recommend(
		5,
		Signals{Backlog: 1, AllocatedWorkers: 500, WorkerCapacity: 500},
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
	recommendation := (Policy{Config: config}).Recommend(
		2,
		Signals{
			Backlog: 1, AllocatedWorkers: 300, WorkerCapacity: 300,
			WorkerInstances: 2,
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
	signals := Signals{}

	if got := policy.Recommend(20, signals, start, state); got.Desired != 20 {
		t.Fatalf("initial low-load recommendation = %+v, want stabilization", got)
	}
	if got := policy.Recommend(20, signals, start.Add(299*time.Second), state); got.Desired != 20 {
		t.Fatalf("early scale-down recommendation = %+v, want 20", got)
	}
	first := policy.Recommend(20, signals, start.Add(300*time.Second), state)
	if first.Desired != 15 {
		t.Fatalf("first scale-down recommendation = %+v, want 15", first)
	}
	state.RecordApplied(start.Add(300 * time.Second))
	if got := policy.Recommend(15, signals, start.Add(359*time.Second), state); got.Desired != 15 {
		t.Fatalf("rate-limited scale-down recommendation = %+v, want 15", got)
	}
	if got := policy.Recommend(15, signals, start.Add(360*time.Second), state); got.Desired != 12 {
		t.Fatalf("second scale-down recommendation = %+v, want 12", got)
	}
}

func TestHTTPReaderCombinesBacklogAndOnlyLiveWorkerCapacity(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/tenants", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"a":{"inflight":17},"b":{"inflight":5}}`))
	})
	mux.HandleFunc("/api/v1/admin/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[` +
			`{"node_id":"control-0","role":"control","status":"up","total_workers":0},` +
			`{"node_id":"worker-0","role":"worker","status":"up","total_workers":100},` +
			`{"node_id":"worker-1","role":"worker","status":"down","total_workers":100}]}`))
	})
	mux.HandleFunc("/api/v1/admin/allocations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[` +
			`{"node_id":"worker-0","tenants":{"a":60,"b":25}},` +
			`{"node_id":"worker-1","tenants":{"a":100}}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	signals, err := (HTTPReader{Client: server.Client()}).Read(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if signals != (Signals{
		Backlog: 22, AllocatedWorkers: 85, WorkerCapacity: 100, WorkerInstances: 1,
	}) {
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
	mux.HandleFunc("/api/v1/admin/tenants", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"direct":{"inflight":3}}`))
	})
	mux.HandleFunc("/api/v1/admin/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(
			`{"nodes":[{"node_id":"worker-0","role":"worker","status":"up","total_workers":10}]}`,
		))
	})
	mux.HandleFunc("/api/v1/admin/allocations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[{"node_id":"worker-0","tenants":{"direct":4}}]}`))
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
	if signals != (Signals{
		Backlog: 3, AllocatedWorkers: 4, WorkerCapacity: 10, WorkerInstances: 1,
	}) {
		t.Fatalf("signals = %+v", signals)
	}
	if got := proxyRequests.Load(); got != 0 {
		t.Fatalf("external proxy received %d cluster-internal requests, want 0", got)
	}
}

func TestHTTPReaderExcludesStaleAllocationFromRollingDownWorker(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/admin/tenants", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"rolling":{"inflight":1}}`))
	})
	mux.HandleFunc("/api/v1/admin/nodes", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[` +
			`{"node_id":"worker-live","role":"worker","status":"up","total_workers":100},` +
			`{"node_id":"worker-old","role":"worker","status":"down","total_workers":100}]}`))
	})
	mux.HandleFunc("/api/v1/admin/allocations", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"nodes":[` +
			`{"node_id":"worker-live","tenants":{"rolling":50}},` +
			`{"node_id":"worker-old","tenants":{"rolling":100}}]}`))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	signals, err := (HTTPReader{Client: server.Client()}).Read(context.Background(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if signals != (Signals{
		Backlog: 1, AllocatedWorkers: 50, WorkerCapacity: 100, WorkerInstances: 1,
	}) {
		t.Fatalf("rolling signals = %+v, want only live Worker allocation", signals)
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
