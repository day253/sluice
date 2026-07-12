// Package v1 contains the SluiceCluster and Tenant CRD types.
package v1

import (
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
	Replicas       int32               `json:"replicas"`
	Image          string              `json:"image,omitempty"`
	WorkersPerNode int32               `json:"workersPerNode,omitempty"`
	LogLevel       string              `json:"logLevel,omitempty"`
	Persistence    *PersistenceSpec    `json:"persistence,omitempty"`
	Resources      *ResourceSpec       `json:"resources,omitempty"`
}

type PersistenceSpec struct {
	Size string `json:"size,omitempty"`
}

type ResourceSpec struct {
	Requests map[string]string `json:"requests,omitempty"`
	Limits   map[string]string `json:"limits,omitempty"`
}

type SluiceClusterStatus struct {
	ReadyReplicas int32       `json:"readyReplicas,omitempty"`
	Leader        string      `json:"leader,omitempty"`
	Nodes         []NodeInfo  `json:"nodes,omitempty"`
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
	Phase             string `json:"phase,omitempty"`
	Inflight          int32  `json:"inflight,omitempty"`
	AllocatedWorkers  int32  `json:"allocatedWorkers,omitempty"`
}

// +kubebuilder:object:root=true

type TenantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Tenant `json:"items"`
}
