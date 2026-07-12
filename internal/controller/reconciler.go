// Package controller contains the SluiceCluster and Tenant reconcilers.
package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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
)

// SluiceClusterReconciler reconciles a SluiceCluster object.
type SluiceClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=sluiceclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sluice.day253.github.com,resources=sluiceclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=services;configmaps,verbs=get;list;watch;create;update;patch;delete

func (r *SluiceClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	var cluster sluicev1.SluiceCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Defaults.
	if cluster.Spec.Image == "" {
		cluster.Spec.Image = "ghcr.io/day253/sluice:latest"
	}
	if cluster.Spec.WorkersPerNode == 0 {
		cluster.Spec.WorkersPerNode = 100
	}
	if cluster.Spec.LogLevel == "" {
		cluster.Spec.LogLevel = "info"
	}
	replicas := cluster.Spec.Replicas
	if replicas == 0 {
		replicas = 3
	}

	labels := map[string]string{
		"app.kubernetes.io/name":     "sluice",
		"app.kubernetes.io/instance": cluster.Name,
	}

	// ---- Headless Service ----
	headlessSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-headless",
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, headlessSvc, func() error {
		headlessSvc.Labels = labels
		headlessSvc.Spec = corev1.ServiceSpec{
			ClusterIP:                "None",
			PublishNotReadyAddresses: true,
			Ports: []corev1.ServicePort{
				{Name: "api", Port: 9090, TargetPort: intstr.FromInt32(9090)},
				{Name: "raft", Port: 7000, TargetPort: intstr.FromInt32(7000)},
			},
			Selector: labels,
		}
		return controllerutil.SetControllerReference(&cluster, headlessSvc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("headless svc: %w", err)
	}

	// ---- ClusterIP Service ----
	apiSvc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, apiSvc, func() error {
		apiSvc.Labels = labels
		apiSvc.Spec = corev1.ServiceSpec{
			Type: corev1.ServiceTypeClusterIP,
			Ports: []corev1.ServicePort{
				{Name: "api", Port: 9090, TargetPort: intstr.FromInt32(9090)},
			},
			Selector: labels,
		}
		return controllerutil.SetControllerReference(&cluster, apiSvc, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("api svc: %w", err)
	}

	// ---- ConfigMap (entrypoint) ----
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name + "-entrypoint",
			Namespace: cluster.Namespace,
		},
	}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, cm, func() error {
		cm.Labels = labels
		cm.Data = map[string]string{
			"entrypoint.sh": entrypointScript(cluster),
		}
		return controllerutil.SetControllerReference(&cluster, cm, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("configmap: %w", err)
	}

	// ---- StatefulSet ----
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
		},
	}
	persistenceSize := "1Gi"
	if cluster.Spec.Persistence != nil && cluster.Spec.Persistence.Size != "" {
		persistenceSize = cluster.Spec.Persistence.Size
	}

	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		sts.Labels = labels
		sts.Spec = appsv1.StatefulSetSpec{
			ServiceName: cluster.Name + "-headless",
			Replicas:    &replicas,
			Selector:    &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: ptr(int64(30)),
					Containers: []corev1.Container{{
						Name:    "sluice",
						Image:   cluster.Spec.Image,
						Command: []string{"/bin/sh", "/entrypoint.sh"},
						Env: []corev1.EnvVar{{
							Name: "POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
							},
						}},
						Ports: []corev1.ContainerPort{
							{Name: "api", ContainerPort: 9090},
							{Name: "raft", ContainerPort: 7000},
						},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "entrypoint", MountPath: "/entrypoint.sh", SubPath: "entrypoint.sh", ReadOnly: true},
							{Name: "data", MountPath: "/data"},
						},
						ReadinessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/api/v1/health", Port: intstr.FromInt32(9090)}},
							InitialDelaySeconds: 10,
							PeriodSeconds:       5,
						},
						LivenessProbe: &corev1.Probe{
							ProbeHandler:        corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/api/v1/health", Port: intstr.FromInt32(9090)}},
							InitialDelaySeconds: 20,
							PeriodSeconds:       10,
						},
					}},
					Volumes: []corev1.Volume{{
						Name: "entrypoint",
						VolumeSource: corev1.VolumeSource{
							ConfigMap: &corev1.ConfigMapVolumeSource{
								LocalObjectReference: corev1.LocalObjectReference{Name: cluster.Name + "-entrypoint"},
								DefaultMode:          ptr(int32(0755)),
							},
						},
					}},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{
				ObjectMeta: metav1.ObjectMeta{Name: "data"},
				Spec: corev1.PersistentVolumeClaimSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(persistenceSize)}},
				},
			}},
		}
		return controllerutil.SetControllerReference(&cluster, sts, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, fmt.Errorf("statefulset: %w", err)
	}

	// Update status.
	cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas

	// Try to query the API for leader info.
	leader := ""
	apiURL := fmt.Sprintf("http://%s.%s.svc:9090/api/v1/health", cluster.Name, cluster.Namespace)
	// Best-effort health check; ignore errors.
	// In production, use an HTTP client with timeout.
	_ = apiURL
	cluster.Status.Leader = leader

	if err := r.Status().Update(ctx, &cluster); err != nil {
		log.Error(err, "failed to update status")
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *SluiceClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sluicev1.SluiceCluster{}).
		Owns(&appsv1.StatefulSet{}).
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

func entrypointScript(c sluicev1.SluiceCluster) string {
	w := c.Spec.WorkersPerNode
	if w == 0 {
		w = 100
	}
	l := c.Spec.LogLevel
	if l == "" {
		l = "info"
	}
	return fmt.Sprintf(`#!/bin/sh
set -e
ORDINAL=$(echo "${POD_NAME##*-}" | grep -o '[0-9]*$' || echo "0")
HEADLESS="%s-headless"
if [ "$ORDINAL" = "0" ]; then
  exec sluice --id="${POD_NAME}" --api=0.0.0.0:9090 --raft=0.0.0.0:7000 --data=/data --bootstrap --workers=%d --log-level=%s
else
  JOIN="%s-0.${HEADLESS}:9090"
  for i in $(seq 1 30); do
    wget -qO- "http://${JOIN}/api/v1/health" 2>/dev/null && break
    sleep 2
  done
  exec sluice --id="${POD_NAME}" --api=0.0.0.0:9090 --raft=0.0.0.0:7000 --data=/data --join="${JOIN}" --workers=%d --log-level=%s
fi
`, c.Name, w, l, c.Name, w, l)
}
