// Package controller contains the SluiceCluster and Tenant reconcilers.
package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sluicev1 "github.com/day253/sluice/api/v1"
	workloadautoscaler "github.com/day253/sluice/internal/autoscaler"
)

// SluiceClusterReconciler reconciles a SluiceCluster object.
type SluiceClusterReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	SignalReader     workloadautoscaler.SignalReader
	Now              func() time.Time
	AutoscalerStates workloadautoscaler.StateStore
}

// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=sluiceclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=sluiceclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=autoscaling,resources=horizontalpodautoscalers,verbs=get;list;watch;create;update;patch;delete

func (r *SluiceClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var cluster sluicev1.SluiceCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	spec, err := normalizeClusterSpec(cluster.Spec)
	if err != nil {
		return ctrl.Result{}, err
	}
	labels := map[string]string{
		"app.kubernetes.io/name":     "sluice",
		"app.kubernetes.io/instance": cluster.Name,
	}
	controlLabels := cloneLabels(labels)
	controlLabels["app.kubernetes.io/component"] = "control"
	workerLabels := map[string]string{
		"app.kubernetes.io/name":      "sluice-worker",
		"app.kubernetes.io/instance":  cluster.Name,
		"app.kubernetes.io/component": "worker",
	}

	headlessSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: cluster.Name + "-headless", Namespace: cluster.Namespace,
	}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, headlessSvc, func() error {
		headlessSvc.Labels = controlLabels
		headlessSvc.Spec = corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{Name: "api", Port: 9090, TargetPort: intstr.FromInt32(9090)},
				{Name: "raft", Port: 7000, TargetPort: intstr.FromInt32(7000)},
			},
			Selector: controlLabels,
		}
		return controllerutil.SetControllerReference(&cluster, headlessSvc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("headless svc: %w", err)
	}

	apiSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, apiSvc, func() error {
		apiSvc.Labels = controlLabels
		clusterIP, clusterIPs := apiSvc.Spec.ClusterIP, apiSvc.Spec.ClusterIPs
		apiSvc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "api", Port: 9090, TargetPort: intstr.FromInt32(9090)},
			},
			Selector: controlLabels,
		}
		apiSvc.Spec.ClusterIP, apiSvc.Spec.ClusterIPs = clusterIP, clusterIPs
		return controllerutil.SetControllerReference(&cluster, apiSvc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("api svc: %w", err)
	}

	workerSvc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{
		Name: cluster.Name + "-worker-headless", Namespace: cluster.Namespace,
	}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, workerSvc, func() error {
		workerSvc.Labels = workerLabels
		workerSvc.Spec = corev1.ServiceSpec{
			ClusterIP: "None", PublishNotReadyAddresses: true,
			Ports:    []corev1.ServicePort{{Name: "health", Port: 9090, TargetPort: intstr.FromInt32(9090)}},
			Selector: workerLabels,
		}
		return controllerutil.SetControllerReference(&cluster, workerSvc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("worker headless svc: %w", err)
	}
	controlHosts := make([]string, spec.controlReplicas)
	for ordinal := int32(0); ordinal < spec.controlReplicas; ordinal++ {
		serviceName := fmt.Sprintf("%s-%d-raft", cluster.Name, ordinal)
		podService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: cluster.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, podService, func() error {
			podLabels := cloneLabels(controlLabels)
			podLabels["statefulset.kubernetes.io/pod-name"] = fmt.Sprintf("%s-%d", cluster.Name, ordinal)
			clusterIP, clusterIPs := podService.Spec.ClusterIP, podService.Spec.ClusterIPs
			podService.Labels = controlLabels
			podService.Spec = corev1.ServiceSpec{
				Type: corev1.ServiceTypeClusterIP, PublishNotReadyAddresses: true,
				Ports: []corev1.ServicePort{
					{Name: "api", Port: 9090, TargetPort: intstr.FromInt32(9090)},
					{Name: "raft", Port: 7000, TargetPort: intstr.FromInt32(7000)},
				},
				Selector: podLabels,
			}
			podService.Spec.ClusterIP, podService.Spec.ClusterIPs = clusterIP, clusterIPs
			return controllerutil.SetControllerReference(&cluster, podService, r.Scheme)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("control Pod service %s: %w", serviceName, err)
		}
		controlHosts[ordinal] = podService.Spec.ClusterIP
		if controlHosts[ordinal] == "" {
			controlHosts[ordinal] = serviceName
		}
	}

	controllerHost := apiSvc.Spec.ClusterIP
	if controllerHost == "" {
		// A real API server fills ClusterIP in the Create response. The fallback
		// keeps fake clients and clusters without allocation admission usable
		// until the next reconcile observes the assigned address.
		controllerHost = cluster.Name
	}
	for name, script := range map[string]string{
		cluster.Name + "-entrypoint":        controlEntrypointScript(spec, controlHosts),
		cluster.Name + "-worker-entrypoint": workerEntrypointScript(spec),
	} {
		cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
			cm.Labels = labels
			cm.Data = map[string]string{"entrypoint.sh": script}
			return controllerutil.SetControllerReference(&cluster, cm, r.Scheme)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("configmap %s: %w", name, err)
		}
	}

	persistenceSize := "1Gi"
	if cluster.Spec.Persistence != nil && cluster.Spec.Persistence.Size != "" {
		persistenceSize = cluster.Spec.Persistence.Size
	}

	controlSTS := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name, Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, controlSTS, func() error {
		controlSTS.Labels = controlLabels
		controlSTS.Spec = appsv1.StatefulSetSpec{
			ServiceName: cluster.Name + "-headless", Replicas: &spec.controlReplicas,
			PodManagementPolicy: appsv1.OrderedReadyPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: controlLabels}, Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: ptr(int64(30)),
				Containers: []corev1.Container{{
					Name: "sluice", Image: spec.image, Command: []string{"/bin/sh", "/entrypoint.sh"},
					Env:          []corev1.EnvVar{{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}}},
					Ports:        []corev1.ContainerPort{{Name: "api", ContainerPort: 9090}, {Name: "raft", ContainerPort: 7000}},
					VolumeMounts: []corev1.VolumeMount{{Name: "entrypoint", MountPath: "/entrypoint.sh", SubPath: "entrypoint.sh", ReadOnly: true}, {Name: "data", MountPath: "/data"}},
					Resources:    spec.resources, ReadinessProbe: healthProbe(10), LivenessProbe: healthProbe(20),
				}},
				Volumes: []corev1.Volume{{Name: "entrypoint", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-entrypoint"}, DefaultMode: ptr(int32(0755)),
				}}}},
			}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}, Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(persistenceSize)}},
			}}},
		}
		return controllerutil.SetControllerReference(&cluster, controlSTS, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("control statefulset: %w", err)
	}

	workerSTS := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-worker", Namespace: cluster.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, workerSTS, func() error {
		workerReplicas := spec.workerReplicas
		if spec.autoscaling.Enabled {
			if workerSTS.ResourceVersion == "" || workerSTS.Spec.Replicas == nil {
				workerReplicas = spec.autoscaling.MinReplicas
			} else {
				workerReplicas = *workerSTS.Spec.Replicas
			}
		}
		workerSTS.Labels = workerLabels
		workerSTS.Spec = appsv1.StatefulSetSpec{
			ServiceName: cluster.Name + "-worker-headless", Replicas: &workerReplicas,
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector:            &metav1.LabelSelector{MatchLabels: workerLabels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: workerLabels}, Spec: corev1.PodSpec{
				TerminationGracePeriodSeconds: ptr(int64(30)),
				Containers: []corev1.Container{{
					Name: "worker", Image: spec.image, Command: []string{"/bin/sh", "/entrypoint.sh"},
					Env: []corev1.EnvVar{
						{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
						{Name: "CONTROLLER_HOST", Value: controllerHost},
					},
					Ports:        []corev1.ContainerPort{{Name: "health", ContainerPort: 9090}},
					VolumeMounts: []corev1.VolumeMount{{Name: "entrypoint", MountPath: "/entrypoint.sh", SubPath: "entrypoint.sh", ReadOnly: true}},
					Resources:    spec.resources, ReadinessProbe: healthProbe(2), LivenessProbe: healthProbe(20),
				}},
				Volumes: []corev1.Volume{{Name: "entrypoint", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-worker-entrypoint"}, DefaultMode: ptr(int32(0755)),
				}}}},
			}},
		}
		return controllerutil.SetControllerReference(&cluster, workerSTS, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("worker statefulset: %w", err)
	}

	hpa := &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{
		Name: cluster.Name + "-worker", Namespace: cluster.Namespace,
	}}
	if spec.autoscaling.Enabled && spec.autoscalingMode == "hpa" {
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, hpa, func() error {
			hpa.Labels = workerLabels
			hpa.Spec = autoscalingv2.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: cluster.Name + "-worker"},
				MinReplicas:    &spec.autoscaling.MinReplicas, MaxReplicas: spec.autoscaling.MaxReplicas,
				Metrics: spec.autoscaling.Metrics, Behavior: spec.autoscaling.Behavior,
			}
			return controllerutil.SetControllerReference(&cluster, hpa, r.Scheme)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("worker hpa: %w", err)
		}
	} else if err := r.Get(ctx, client.ObjectKey{Name: cluster.Name + "-worker", Namespace: cluster.Namespace}, hpa); err == nil {
		if err := r.Delete(ctx, hpa); err != nil && !errors.IsNotFound(err) {
			return ctrl.Result{}, fmt.Errorf("delete disabled worker hpa: %w", err)
		}
	} else if !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("get disabled worker hpa: %w", err)
	}

	if spec.autoscaling.Enabled && spec.autoscalingMode == "workload" {
		reader := r.SignalReader
		if reader == nil {
			reader = workloadautoscaler.HTTPReader{}
		}
		signals, readErr := reader.Read(ctx, "http://"+controllerHost+":9090")
		if readErr != nil {
			// Missing or stale workload signals must never cause scale-down.
			log.FromContext(ctx).Error(readErr, "read workload autoscaling signals; retaining Worker replicas")
		} else {
			current := spec.autoscaling.MinReplicas
			if workerSTS.Spec.Replicas != nil {
				current = *workerSTS.Spec.Replicas
			}
			now := time.Now()
			if r.Now != nil {
				now = r.Now()
			}
			stateKey := cluster.Namespace + "/" + cluster.Name
			recommendation := r.AutoscalerStates.Recommend(
				stateKey, workloadautoscaler.Policy{Config: spec.workloadAutoscaling},
				current, signals, now,
			)
			if recommendation.Desired != current {
				before := workerSTS.DeepCopy()
				workerSTS.Spec.Replicas = ptr(recommendation.Desired)
				if err := r.Patch(ctx, workerSTS, client.MergeFrom(before)); err != nil {
					return ctrl.Result{}, fmt.Errorf("scale Worker StatefulSet from workload: %w", err)
				}
				r.AutoscalerStates.RecordApplied(stateKey, now)
				log.FromContext(ctx).Info("scaled CRD Worker StatefulSet from workload",
					"from", current, "to", recommendation.Desired,
					"backlog", signals.Backlog, "allocatedWorkers", signals.AllocatedWorkers,
					"workerCapacity", signals.WorkerCapacity, "reason", recommendation.Reason)
			}
		}
	}

	cluster.Status.ReadyReplicas = controlSTS.Status.ReadyReplicas
	cluster.Status.ControlReadyReplicas = controlSTS.Status.ReadyReplicas
	cluster.Status.WorkerReadyReplicas = workerSTS.Status.ReadyReplicas
	cluster.Status.DesiredWorkerReplicas = spec.workerReplicas
	if spec.autoscaling.Enabled && spec.autoscalingMode == "hpa" {
		cluster.Status.DesiredWorkerReplicas = hpa.Status.DesiredReplicas
		if cluster.Status.DesiredWorkerReplicas == 0 {
			cluster.Status.DesiredWorkerReplicas = spec.autoscaling.MinReplicas
		}
	} else if spec.autoscaling.Enabled && workerSTS.Spec.Replicas != nil {
		cluster.Status.DesiredWorkerReplicas = *workerSTS.Spec.Replicas
	}
	cluster.Status.Leader = ""
	if err := r.Status().Update(ctx, &cluster); err != nil {
		log.FromContext(ctx).Error(err, "failed to update status")
	}

	requeueAfter := 30 * time.Second
	if spec.autoscaling.Enabled && spec.autoscalingMode == "workload" {
		requeueAfter = spec.workloadPollInterval
	}
	return ctrl.Result{RequeueAfter: requeueAfter}, nil
}

func (r *SluiceClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sluicev1.SluiceCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&autoscalingv2.HorizontalPodAutoscaler{}).
		Owns(&corev1.Service{}).
		Complete(r)
}

// ---- Tenant reconciler ----

type TenantReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=tenants,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=tenants/status,verbs=get;update;patch

func (r *TenantReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var tenant sluicev1.Tenant
	if err := r.Get(ctx, req.NamespacedName, &tenant); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Find the target cluster.
	clusterName := tenant.Spec.ClusterRef
	if clusterName == "" {
		// Default to first SluiceCluster in namespace.
		var list sluicev1.SluiceClusterList
		if err := r.List(ctx, &list, client.InNamespace(tenant.Namespace)); err == nil && len(list.Items) > 0 {
			clusterName = list.Items[0].Name
		}
	}

	// This is where the operator would call the sluice gRPC/HTTP API
	// to upsert the tenant. For now, phase reflects spec.
	tenant.Status.Phase = "Active"
	tenant.Status.AllocatedWorkers = tenant.Spec.MaxWorkers

	if err := r.Status().Update(ctx, &tenant); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 60 * time.Second}, nil
}

func (r *TenantReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sluicev1.Tenant{}).
		Complete(r)
}

// ---- helpers ----

func ptr[T any](v T) *T { return &v }

type normalizedSpec struct {
	controlReplicas      int32
	workerReplicas       int32
	workersPerNode       int32
	image                string
	logLevel             string
	resources            corev1.ResourceRequirements
	autoscaling          sluicev1.WorkerAutoscalingSpec
	autoscalingMode      string
	workloadAutoscaling  workloadautoscaler.Config
	workloadPollInterval time.Duration
}

func normalizeClusterSpec(in sluicev1.SluiceClusterSpec) (normalizedSpec, error) {
	out := normalizedSpec{
		controlReplicas: in.Replicas,
		workerReplicas:  in.WorkerReplicas,
		workersPerNode:  in.WorkersPerNode,
		image:           in.Image,
		logLevel:        in.LogLevel,
	}
	if out.controlReplicas == 0 {
		out.controlReplicas = 3
	}
	if out.controlReplicas < 1 || out.controlReplicas > 11 || out.controlReplicas%2 == 0 {
		return normalizedSpec{}, fmt.Errorf("replicas must be an odd control count between 1 and 11")
	}
	if out.workerReplicas == 0 {
		out.workerReplicas = out.controlReplicas
	}
	if out.workerReplicas < 1 {
		return normalizedSpec{}, fmt.Errorf("workerReplicas must be positive")
	}
	if out.workersPerNode == 0 {
		out.workersPerNode = 100
	}
	if out.image == "" {
		out.image = "ghcr.io/day253/sluice:latest"
	}
	if out.logLevel == "" {
		out.logLevel = "info"
	}
	resources, err := normalizeResources(in.Resources)
	if err != nil {
		return normalizedSpec{}, err
	}
	out.resources = resources
	if in.Autoscaling != nil {
		in.Autoscaling.DeepCopyInto(&out.autoscaling)
	}
	if out.autoscaling.Enabled {
		if out.autoscaling.MinReplicas == 0 {
			out.autoscaling.MinReplicas = 1
		}
		if out.autoscaling.MaxReplicas == 0 {
			out.autoscaling.MaxReplicas = 100
		}
		if out.autoscaling.MinReplicas < 1 || out.autoscaling.MaxReplicas < out.autoscaling.MinReplicas {
			return normalizedSpec{}, fmt.Errorf("autoscaling requires 1 <= minReplicas <= maxReplicas")
		}
		out.autoscalingMode = strings.ToLower(out.autoscaling.Mode)
		if out.autoscalingMode == "" {
			out.autoscalingMode = "hpa"
		}
		if out.autoscalingMode != "hpa" && out.autoscalingMode != "workload" {
			return normalizedSpec{}, fmt.Errorf("autoscaling mode must be hpa or workload")
		}
		if out.autoscalingMode == "hpa" && len(out.autoscaling.Metrics) == 0 {
			target := int32(70)
			out.autoscaling.Metrics = []autoscalingv2.MetricSpec{{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name:   corev1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{Type: autoscalingv2.UtilizationMetricType, AverageUtilization: &target},
				},
			}}
		}
		if out.autoscalingMode == "hpa" && out.autoscaling.Behavior == nil {
			out.autoscaling.Behavior = defaultWorkerHPABehavior()
		}
		if out.autoscalingMode == "workload" {
			workload := workloadautoscaler.DefaultConfig()
			workload.MinReplicas, workload.MaxReplicas = out.autoscaling.MinReplicas, out.autoscaling.MaxReplicas
			workload.WorkersPerPod = out.workersPerNode
			pollSeconds := int32(5)
			if configured := out.autoscaling.Workload; configured != nil {
				if configured.PollIntervalSeconds > 0 {
					pollSeconds = configured.PollIntervalSeconds
				}
				if configured.TargetBacklogPerPod > 0 {
					workload.TargetBacklogPerPod = configured.TargetBacklogPerPod
				}
				if configured.TargetWorkerUtilization > 0 {
					workload.TargetWorkerUtilization = configured.TargetWorkerUtilization
				}
				if configured.TargetCPUUtilization > 0 {
					workload.TargetCPUUtilization = configured.TargetCPUUtilization
				}
				if configured.TargetQueueDrainSeconds > 0 {
					workload.TargetQueueDrainTime =
						time.Duration(configured.TargetQueueDrainSeconds) * time.Second
				}
				if configured.TargetThroughputUtilization > 0 {
					workload.TargetThroughputUtilization =
						configured.TargetThroughputUtilization
				}
				if configured.TolerancePercent != nil {
					workload.TolerancePercent = *configured.TolerancePercent
				}
				if configured.MinTelemetryCoveragePercent > 0 {
					workload.MinTelemetryCoveragePercent =
						configured.MinTelemetryCoveragePercent
				}
				if configured.ScaleUpPercent > 0 {
					workload.ScaleUpPercent = configured.ScaleUpPercent
				}
				if configured.ScaleUpPods > 0 {
					workload.ScaleUpPods = configured.ScaleUpPods
				}
				if configured.ScaleDownPercent > 0 {
					workload.ScaleDownPercent = configured.ScaleDownPercent
				}
				if configured.ScaleDownStabilizationSeconds != nil {
					workload.ScaleDownStabilization = time.Duration(*configured.ScaleDownStabilizationSeconds) * time.Second
				}
			}
			workload.ScaleUpPeriod = time.Duration(pollSeconds) * time.Second
			if err := workload.Validate(); err != nil {
				return normalizedSpec{}, err
			}
			out.workloadAutoscaling = workload
			out.workloadPollInterval = time.Duration(pollSeconds) * time.Second
		}
	}
	return out, nil
}

func normalizeResources(in *sluicev1.ResourceSpec) (corev1.ResourceRequirements, error) {
	out := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100m"), corev1.ResourceMemory: resource.MustParse("128Mi")},
		Limits:   corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("500m"), corev1.ResourceMemory: resource.MustParse("512Mi")},
	}
	if in == nil {
		return out, nil
	}
	for name, value := range in.Requests {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid resource request %s=%q: %w", name, value, err)
		}
		out.Requests[corev1.ResourceName(name)] = quantity
	}
	for name, value := range in.Limits {
		quantity, err := resource.ParseQuantity(value)
		if err != nil {
			return corev1.ResourceRequirements{}, fmt.Errorf("invalid resource limit %s=%q: %w", name, value, err)
		}
		out.Limits[corev1.ResourceName(name)] = quantity
	}
	return out, nil
}

func defaultWorkerHPABehavior() *autoscalingv2.HorizontalPodAutoscalerBehavior {
	maxPolicy := autoscalingv2.MaxChangePolicySelect
	return &autoscalingv2.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: ptr(int32(0)), SelectPolicy: &maxPolicy,
			Policies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 60},
				{Type: autoscalingv2.PodsScalingPolicy, Value: 10, PeriodSeconds: 60},
			},
		},
		ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: ptr(int32(300)), SelectPolicy: &maxPolicy,
			Policies: []autoscalingv2.HPAScalingPolicy{{Type: autoscalingv2.PercentScalingPolicy, Value: 25, PeriodSeconds: 60}},
		},
	}
}

func cloneLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func healthProbe(initialDelay int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/api/v1/health", Port: intstr.FromInt32(9090)}},
		InitialDelaySeconds: initialDelay, PeriodSeconds: 5,
	}
}

func controlEntrypointScript(spec normalizedSpec, controlHosts []string) string {
	cases := make([]string, 0, len(controlHosts))
	for ordinal, host := range controlHosts {
		cases = append(cases, fmt.Sprintf("  %d) SELF_HOST=%q ;;", ordinal, host))
	}
	bootstrapHost := controlHosts[0]
	return fmt.Sprintf(`#!/bin/sh
set -e
ORDINAL=$(echo "${POD_NAME##*-}" | grep -o '[0-9]*$' || echo "0")
case "${ORDINAL}" in
%s
  *) echo "unknown control ordinal ${ORDINAL}" >&2; exit 1 ;;
esac
RAFT_ADVERTISE="${SELF_HOST}:7000"
if [ "$ORDINAL" = "0" ]; then
  exec sluice --role=control --id="${POD_NAME}" --api=0.0.0.0:9090 --raft=0.0.0.0:7000 --raft-advertise="${RAFT_ADVERTISE}" --data=/data --bootstrap --workers=0 --raft-voters=%d --raft-members=%d --log-level=%s
else
  if [ -f /data/raft/raft-stable.db ]; then
    exec sluice --role=control --id="${POD_NAME}" --api=0.0.0.0:9090 --raft=0.0.0.0:7000 --raft-advertise="${RAFT_ADVERTISE}" --data=/data --workers=0 --raft-voters=%d --raft-members=%d --log-level=%s
  fi
  JOIN="%s:9090"
  for i in $(seq 1 30); do
    wget -qO- "http://${JOIN}/api/v1/health" 2>/dev/null && break
    sleep 2
  done
  exec sluice --role=control --id="${POD_NAME}" --api=0.0.0.0:9090 --raft=0.0.0.0:7000 --raft-advertise="${RAFT_ADVERTISE}" --data=/data --join="${JOIN}" --workers=0 --raft-voters=%d --raft-members=%d --log-level=%s
fi

`, strings.Join(cases, "\n"), spec.controlReplicas, spec.controlReplicas, spec.logLevel,
		spec.controlReplicas, spec.controlReplicas, spec.logLevel, bootstrapHost,
		spec.controlReplicas, spec.controlReplicas, spec.logLevel)
}

func workerEntrypointScript(spec normalizedSpec) string {
	return fmt.Sprintf(`#!/bin/sh
set -e
exec sluice --role=worker --id="${POD_NAME}" --api=0.0.0.0:9090 --controller="${CONTROLLER_HOST}:9090" --workers=%d --log-level=%s
`, spec.workersPerNode, spec.logLevel)
}
