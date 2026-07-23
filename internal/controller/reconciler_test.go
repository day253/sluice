package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	sluicev1 "github.com/day253/sluice/api/v1"
	workloadautoscaler "github.com/day253/sluice/internal/autoscaler"
)

func newClusterReconciler(t *testing.T, cluster *sluicev1.SluiceCluster) (*SluiceClusterReconciler, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core": corev1.AddToScheme, "apps": appsv1.AddToScheme,
		"autoscaling": autoscalingv2.AddToScheme, "sluice": sluicev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("add %s scheme: %v", name, err)
		}
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&sluicev1.SluiceCluster{}).
		WithObjects(cluster).Build()
	return &SluiceClusterReconciler{Client: fakeClient, Scheme: scheme}, fakeClient
}

func reconcileCluster(t *testing.T, reconciler *SluiceClusterReconciler, name types.NamespacedName) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: name}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestClusterReconcilerAutoscalingTargetsOnlyStatelessWorkers(t *testing.T) {
	cluster := &sluicev1.SluiceCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: sluicev1.SchemeGroupVersion.String(), Kind: "SluiceCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: sluicev1.SluiceClusterSpec{
			Replicas: 3, WorkerReplicas: 4, WorkersPerNode: 80,
			Autoscaling: &sluicev1.WorkerAutoscalingSpec{Enabled: true, MinReplicas: 2, MaxReplicas: 20},
		},
	}
	reconciler, k8sClient := newClusterReconciler(t, cluster)
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	reconcileCluster(t, reconciler, key)

	var control appsv1.StatefulSet
	if err := k8sClient.Get(context.Background(), key, &control); err != nil {
		t.Fatal(err)
	}
	if control.Spec.Replicas == nil || *control.Spec.Replicas != 3 {
		t.Fatalf("control replicas = %v, want fixed 3", control.Spec.Replicas)
	}
	if control.Spec.Template.Labels["app.kubernetes.io/component"] != "control" || len(control.Spec.VolumeClaimTemplates) != 1 {
		t.Fatalf("control StatefulSet lost role/storage boundary: %+v", control.Spec)
	}
	for ordinal := 0; ordinal < 3; ordinal++ {
		var podService corev1.Service
		serviceKey := types.NamespacedName{Name: fmt.Sprintf("sample-%d-raft", ordinal), Namespace: "default"}
		if err := k8sClient.Get(context.Background(), serviceKey, &podService); err != nil {
			t.Fatal(err)
		}
		if got := podService.Spec.Selector["statefulset.kubernetes.io/pod-name"]; got != fmt.Sprintf("sample-%d", ordinal) {
			t.Fatalf("control Pod Service %s selector = %q", serviceKey.Name, got)
		}
	}

	workerKey := types.NamespacedName{Name: "sample-worker", Namespace: "default"}
	var worker appsv1.StatefulSet
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if worker.Spec.Replicas == nil || *worker.Spec.Replicas != 2 {
		t.Fatalf("initial autoscaled Worker replicas = %v, want min 2", worker.Spec.Replicas)
	}
	if worker.Spec.PodManagementPolicy != appsv1.ParallelPodManagement || len(worker.Spec.VolumeClaimTemplates) != 0 {
		t.Fatalf("Worker is not stateless/parallel: %+v", worker.Spec)
	}
	if got := worker.Spec.Template.Spec.Containers[0].Env[1]; got.Name != "CONTROLLER_HOST" || got.Value != "sample" {
		t.Fatalf("initial Worker controller endpoint = %+v, want service-name fallback", got)
	}
	if got := worker.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu().String(); got != "100m" {
		t.Fatalf("Worker CPU request = %s, HPA utilization requires a request", got)
	}

	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := k8sClient.Get(context.Background(), workerKey, &hpa); err != nil {
		t.Fatal(err)
	}
	if hpa.Spec.ScaleTargetRef.Kind != "StatefulSet" || hpa.Spec.ScaleTargetRef.Name != "sample-worker" {
		t.Fatalf("HPA target = %+v, want only sample-worker StatefulSet", hpa.Spec.ScaleTargetRef)
	}
	if hpa.Spec.MinReplicas == nil || *hpa.Spec.MinReplicas != 2 || hpa.Spec.MaxReplicas != 20 {
		t.Fatalf("HPA bounds = %v/%d", hpa.Spec.MinReplicas, hpa.Spec.MaxReplicas)
	}
	if len(hpa.Spec.Metrics) != 1 || hpa.Spec.Metrics[0].Resource == nil ||
		hpa.Spec.Metrics[0].Resource.Name != corev1.ResourceCPU {
		t.Fatalf("default HPA metric = %+v, want CPU utilization", hpa.Spec.Metrics)
	}
	if hpa.Spec.Behavior == nil || hpa.Spec.Behavior.ScaleDown == nil ||
		hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds == nil ||
		*hpa.Spec.Behavior.ScaleDown.StabilizationWindowSeconds != 300 {
		t.Fatalf("HPA scale-down behavior = %+v, want 300s stabilization", hpa.Spec.Behavior)
	}

	for name, required := range map[string]string{
		"sample-entrypoint":        "--role=control",
		"sample-worker-entrypoint": "--role=worker",
	} {
		var config corev1.ConfigMap
		if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: name, Namespace: "default"}, &config); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(config.Data["entrypoint.sh"], required) {
			t.Fatalf("%s does not contain %q", name, required)
		}
	}

	// Production Kubernetes allocates ClusterIP on Service creation. Once the
	// address is observed, the Worker template must use it directly instead of
	// depending on cluster DNS (CTRL-003 target clusters may return fake IPs).
	var apiService corev1.Service
	if err := k8sClient.Get(context.Background(), key, &apiService); err != nil {
		t.Fatal(err)
	}
	apiService.Spec.ClusterIP = "10.152.183.42"
	apiService.Spec.ClusterIPs = []string{"10.152.183.42"}
	if err := k8sClient.Update(context.Background(), &apiService); err != nil {
		t.Fatal(err)
	}
	var controlService corev1.Service
	controlServiceKey := types.NamespacedName{Name: "sample-0-raft", Namespace: "default"}
	if err := k8sClient.Get(context.Background(), controlServiceKey, &controlService); err != nil {
		t.Fatal(err)
	}
	controlService.Spec.ClusterIP = "10.152.183.43"
	controlService.Spec.ClusterIPs = []string{"10.152.183.43"}
	if err := k8sClient.Update(context.Background(), &controlService); err != nil {
		t.Fatal(err)
	}
	reconcileCluster(t, reconciler, key)
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if got := worker.Spec.Template.Spec.Containers[0].Env[1]; got.Name != "CONTROLLER_HOST" || got.Value != "10.152.183.42" {
		t.Fatalf("Worker controller endpoint = %+v, want API Service ClusterIP", got)
	}
	var controlEntrypoint corev1.ConfigMap
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "sample-entrypoint", Namespace: "default"}, &controlEntrypoint); err != nil {
		t.Fatal(err)
	}
	if script := controlEntrypoint.Data["entrypoint.sh"]; !strings.Contains(script, `0) SELF_HOST="10.152.183.43"`) {
		t.Fatalf("control entrypoint does not use stable Pod Service ClusterIP: %s", script)
	}
}

func TestClusterReconcilerDoesNotFightHPAAndRestoresStaticReplicasWhenDisabled(t *testing.T) {
	cluster := &sluicev1.SluiceCluster{
		TypeMeta:   metav1.TypeMeta{APIVersion: sluicev1.SchemeGroupVersion.String(), Kind: "SluiceCluster"},
		ObjectMeta: metav1.ObjectMeta{Name: "sample", Namespace: "default", UID: types.UID("cluster-uid")},
		Spec: sluicev1.SluiceClusterSpec{Replicas: 3, WorkerReplicas: 4,
			Autoscaling: &sluicev1.WorkerAutoscalingSpec{Enabled: true, MinReplicas: 2, MaxReplicas: 20}},
	}
	reconciler, k8sClient := newClusterReconciler(t, cluster)
	clusterKey := types.NamespacedName{Name: "sample", Namespace: "default"}
	workerKey := types.NamespacedName{Name: "sample-worker", Namespace: "default"}
	reconcileCluster(t, reconciler, clusterKey)

	var worker appsv1.StatefulSet
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	worker.Spec.Replicas = ptr(int32(9)) // emulate the HPA scale subresource
	if err := k8sClient.Update(context.Background(), &worker); err != nil {
		t.Fatal(err)
	}
	reconcileCluster(t, reconciler, clusterKey)
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if got := *worker.Spec.Replicas; got != 9 {
		t.Fatalf("operator overwrote HPA replicas with %d, want 9", got)
	}

	var updated sluicev1.SluiceCluster
	if err := k8sClient.Get(context.Background(), clusterKey, &updated); err != nil {
		t.Fatal(err)
	}
	updated.Spec.Autoscaling = nil
	updated.Spec.WorkerReplicas = 7
	if err := k8sClient.Update(context.Background(), &updated); err != nil {
		t.Fatal(err)
	}
	reconcileCluster(t, reconciler, clusterKey)
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if got := *worker.Spec.Replicas; got != 7 {
		t.Fatalf("static Worker replicas after disabling HPA = %d, want 7", got)
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := k8sClient.Get(context.Background(), workerKey, &hpa); !apierrors.IsNotFound(err) {
		t.Fatalf("disabled HPA still exists: %v", err)
	}
}

type controllerSignalReaderFunc func(context.Context, string) (workloadautoscaler.Signals, error)

func (f controllerSignalReaderFunc) Read(
	ctx context.Context, baseURL string,
) (workloadautoscaler.Signals, error) {
	return f(ctx, baseURL)
}

func TestClusterReconcilerWorkloadModeScalesWorkersAndNeverControlOrHPA(t *testing.T) {
	cluster := &sluicev1.SluiceCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: sluicev1.SchemeGroupVersion.String(),
			Kind:       "SluiceCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "sample", Namespace: "default", UID: types.UID("cluster-uid"),
		},
		Spec: sluicev1.SluiceClusterSpec{
			Replicas: 3, WorkerReplicas: 4, WorkersPerNode: 100,
			Autoscaling: &sluicev1.WorkerAutoscalingSpec{
				Enabled: true, Mode: "workload", MinReplicas: 2, MaxReplicas: 20,
				Workload: &sluicev1.WorkloadAutoscalingSpec{
					PollIntervalSeconds: 5, TargetBacklogPerPod: 100,
					TargetWorkerUtilization: 70, ScaleUpPercent: 100,
					ScaleUpPods: 10, ScaleDownPercent: 25,
					ScaleDownStabilizationSeconds: ptr(int32(300)),
				},
			},
		},
	}
	reconciler, k8sClient := newClusterReconciler(t, cluster)
	now := time.Unix(1000, 0)
	reconciler.Now = func() time.Time { return now }
	reconciler.SignalReader = controllerSignalReaderFunc(
		func(_ context.Context, baseURL string) (workloadautoscaler.Signals, error) {
			if baseURL != "http://sample:9090" {
				t.Fatalf("signal base URL = %q", baseURL)
			}
			return workloadautoscaler.Signals{
				Backlog: 2_000, AllocatedWorkers: 200, WorkerCapacity: 200,
			}, nil
		},
	)
	key := types.NamespacedName{Name: "sample", Namespace: "default"}
	reconcileCluster(t, reconciler, key)

	var worker appsv1.StatefulSet
	workerKey := types.NamespacedName{Name: "sample-worker", Namespace: "default"}
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if worker.Spec.Replicas == nil || *worker.Spec.Replicas != 12 {
		t.Fatalf("workload-scaled Worker replicas = %v, want bounded scale from 2 to 12", worker.Spec.Replicas)
	}
	var control appsv1.StatefulSet
	if err := k8sClient.Get(context.Background(), key, &control); err != nil {
		t.Fatal(err)
	}
	if control.Spec.Replicas == nil || *control.Spec.Replicas != 3 {
		t.Fatalf("control replicas = %v, want fixed 3", control.Spec.Replicas)
	}
	var hpa autoscalingv2.HorizontalPodAutoscaler
	if err := k8sClient.Get(context.Background(), workerKey, &hpa); !apierrors.IsNotFound(err) {
		t.Fatalf("workload mode unexpectedly created HPA: %v", err)
	}
	var current sluicev1.SluiceCluster
	if err := k8sClient.Get(context.Background(), key, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.DesiredWorkerReplicas != 12 {
		t.Fatalf("desired Worker status = %d, want 12", current.Status.DesiredWorkerReplicas)
	}

	// An unavailable read is not interpreted as zero load and cannot remove
	// execution capacity.
	reconciler.SignalReader = controllerSignalReaderFunc(
		func(context.Context, string) (workloadautoscaler.Signals, error) {
			return workloadautoscaler.Signals{}, errors.New("control API unavailable")
		},
	)
	reconcileCluster(t, reconciler, key)
	if err := k8sClient.Get(context.Background(), workerKey, &worker); err != nil {
		t.Fatal(err)
	}
	if worker.Spec.Replicas == nil || *worker.Spec.Replicas != 12 {
		t.Fatalf("Worker replicas after signal failure = %v, want 12", worker.Spec.Replicas)
	}
}

func TestNormalizeClusterSpecRejectsAutoscalingControlPlaneOrInvalidBounds(t *testing.T) {
	for name, spec := range map[string]sluicev1.SluiceClusterSpec{
		"even control quorum": {Replicas: 4},
		"invalid HPA bounds": {Replicas: 3, Autoscaling: &sluicev1.WorkerAutoscalingSpec{
			Enabled: true, MinReplicas: 10, MaxReplicas: 5,
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := normalizeClusterSpec(spec); err == nil {
				t.Fatal("invalid cluster spec was accepted")
			}
		})
	}
}

func TestNormalizeClusterSpecDefaultsAndHonorsWorkloadScaleDownWindow(t *testing.T) {
	base := sluicev1.SluiceClusterSpec{
		Replicas: 3, WorkersPerNode: 100,
		Autoscaling: &sluicev1.WorkerAutoscalingSpec{
			Enabled: true, Mode: "workload", MinReplicas: 2, MaxReplicas: 20,
		},
	}
	defaulted, err := normalizeClusterSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	if got := defaulted.workloadAutoscaling.ScaleDownStabilization; got != 5*time.Minute {
		t.Fatalf("default scale-down stabilization = %s, want 5m", got)
	}
	base.Autoscaling.Workload = &sluicev1.WorkloadAutoscalingSpec{
		ScaleDownStabilizationSeconds: ptr(int32(0)),
	}
	explicitZero, err := normalizeClusterSpec(base)
	if err != nil {
		t.Fatal(err)
	}
	if got := explicitZero.workloadAutoscaling.ScaleDownStabilization; got != 0 {
		t.Fatalf("explicit scale-down stabilization = %s, want 0", got)
	}
}
