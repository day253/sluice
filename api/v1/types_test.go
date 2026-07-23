package v1

import (
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
)

func TestSluiceClusterDeepCopyOwnsAutoscalingMetricsAndBehavior(t *testing.T) {
	target := int32(70)
	window := int32(300)
	tolerance := int32(10)
	original := &SluiceCluster{Spec: SluiceClusterSpec{Autoscaling: &WorkerAutoscalingSpec{
		Enabled: true, MinReplicas: 2, MaxReplicas: 20,
		Workload: &WorkloadAutoscalingSpec{
			TargetBacklogPerPod: 400, TargetWorkerUtilization: 70,
			TolerancePercent: &tolerance,
			ScaleDownStabilizationSeconds: func() *int32 {
				value := int32(300)
				return &value
			}(),
		},
		Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{Target: autoscalingv2.MetricTarget{AverageUtilization: &target}}}},
		Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: &window,
		}},
	}}}
	copy := original.DeepCopy()
	*copy.Spec.Autoscaling.Metrics[0].Resource.Target.AverageUtilization = 90
	*copy.Spec.Autoscaling.Behavior.ScaleDown.StabilizationWindowSeconds = 60
	copy.Spec.Autoscaling.Workload.TargetBacklogPerPod = 10
	*copy.Spec.Autoscaling.Workload.TolerancePercent = 0
	*copy.Spec.Autoscaling.Workload.ScaleDownStabilizationSeconds = 0
	if got := *original.Spec.Autoscaling.Metrics[0].Resource.Target.AverageUtilization; got != 70 {
		t.Fatalf("metric target was aliased: %d", got)
	}
	if got := *original.Spec.Autoscaling.Behavior.ScaleDown.StabilizationWindowSeconds; got != 300 {
		t.Fatalf("behavior was aliased: %d", got)
	}
	if got := original.Spec.Autoscaling.Workload.TargetBacklogPerPod; got != 400 {
		t.Fatalf("workload policy was aliased: %d", got)
	}
	if got := *original.Spec.Autoscaling.Workload.TolerancePercent; got != 10 {
		t.Fatalf("workload tolerance was aliased: %d", got)
	}
	if got := *original.Spec.Autoscaling.Workload.ScaleDownStabilizationSeconds; got != 300 {
		t.Fatalf("workload scale-down stabilization was aliased: %d", got)
	}
}
