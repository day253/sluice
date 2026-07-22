package v1

import (
	"testing"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
)

func TestSluiceClusterDeepCopyOwnsAutoscalingMetricsAndBehavior(t *testing.T) {
	target := int32(70)
	window := int32(300)
	original := &SluiceCluster{Spec: SluiceClusterSpec{Autoscaling: &WorkerAutoscalingSpec{
		Enabled: true, MinReplicas: 2, MaxReplicas: 20,
		Metrics: []autoscalingv2.MetricSpec{{Type: autoscalingv2.ResourceMetricSourceType,
			Resource: &autoscalingv2.ResourceMetricSource{Target: autoscalingv2.MetricTarget{AverageUtilization: &target}}}},
		Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{ScaleDown: &autoscalingv2.HPAScalingRules{
			StabilizationWindowSeconds: &window,
		}},
	}}}
	copy := original.DeepCopy()
	*copy.Spec.Autoscaling.Metrics[0].Resource.Target.AverageUtilization = 90
	*copy.Spec.Autoscaling.Behavior.ScaleDown.StabilizationWindowSeconds = 60
	if got := *original.Spec.Autoscaling.Metrics[0].Resource.Target.AverageUtilization; got != 70 {
		t.Fatalf("metric target was aliased: %d", got)
	}
	if got := *original.Spec.Autoscaling.Behavior.ScaleDown.StabilizationWindowSeconds; got != 300 {
		t.Fatalf("behavior was aliased: %d", got)
	}
}
