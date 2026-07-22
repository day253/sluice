// Package v1 contains the SluiceCluster and Tenant CRD types.
package v1

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=sc

type SluiceCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              SluiceClusterSpec   `json:"spec,omitempty"`
	Status            SluiceClusterStatus `json:"status,omitempty"`
}

type SluiceClusterSpec struct {
	// Replicas is the fixed control/Raft replica count. It is deliberately not
	// an HPA target; changing quorum membership is a protocol operation.
	Replicas int32 `json:"replicas"`
	// WorkerReplicas is the static execution-plane size and the initial size
	// when autoscaling is enabled.
	WorkerReplicas int32                  `json:"workerReplicas,omitempty"`
	Image          string                 `json:"image,omitempty"`
	WorkersPerNode int32                  `json:"workersPerNode,omitempty"`
	LogLevel       string                 `json:"logLevel,omitempty"`
	Persistence    *PersistenceSpec       `json:"persistence,omitempty"`
	Resources      *ResourceSpec          `json:"resources,omitempty"`
	Autoscaling    *WorkerAutoscalingSpec `json:"autoscaling,omitempty"`
}

// WorkerAutoscalingSpec configures an autoscaling/v2 HPA for only the
// stateless Worker StatefulSet. Metrics and behavior use native Kubernetes
// types so resource, custom Pods/Object, and external backlog metrics all keep
// their standard HPA semantics.
type WorkerAutoscalingSpec struct {
	Enabled     bool                                           `json:"enabled,omitempty"`
	MinReplicas int32                                          `json:"minReplicas,omitempty"`
	MaxReplicas int32                                          `json:"maxReplicas,omitempty"`
	Metrics     []autoscalingv2.MetricSpec                     `json:"metrics,omitempty"`
	Behavior    *autoscalingv2.HorizontalPodAutoscalerBehavior `json:"behavior,omitempty"`
}

type PersistenceSpec struct {
	Size string `json:"size,omitempty"`
}

type ResourceSpec struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type SluiceClusterStatus struct {
	// ReadyReplicas is retained as the legacy control-ready field.
	ReadyReplicas         int32      `json:"readyReplicas,omitempty"`
	ControlReadyReplicas  int32      `json:"controlReadyReplicas,omitempty"`
	WorkerReadyReplicas   int32      `json:"workerReadyReplicas,omitempty"`
	DesiredWorkerReplicas int32      `json:"desiredWorkerReplicas,omitempty"`
	Leader                string     `json:"leader,omitempty"`
	Nodes                 []NodeInfo `json:"nodes,omitempty"`
}

type NodeInfo struct {
	Name   string `json:"name,omitempty"`
	Status string `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

type SluiceClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []SluiceCluster `json:"items"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=tnt

type Tenant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              TenantSpec   `json:"spec,omitempty"`
	Status            TenantStatus `json:"status,omitempty"`
}

type TenantSpec struct {
	MaxWorkers int32  `json:"maxWorkers"`
	ClusterRef string `json:"clusterRef,omitempty"`
}

type TenantStatus struct {
	Phase            string `json:"phase,omitempty"`
	Inflight         int32  `json:"inflight,omitempty"`
	AllocatedWorkers int32  `json:"allocatedWorkers,omitempty"`
}

// +kubebuilder:object:root=true

type TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Tenant `json:"items"`
}
