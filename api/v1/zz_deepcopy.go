// Code generated manually.  Provides DeepCopy* for CRD types.
package v1

import (
	runtime "k8s.io/apimachinery/pkg/runtime"
)

// ---- SluiceCluster ----

func (in *SluiceCluster) DeepCopyInto(out *SluiceCluster) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	in.Spec.DeepCopyInto(&out.Spec)
	in.Status.DeepCopyInto(&out.Status)
}

func (in *SluiceCluster) DeepCopy() *SluiceCluster {
	if in == nil {
		return nil
	}
	out := new(SluiceCluster)
	in.DeepCopyInto(out)
	return out
}

func (in *SluiceClusterSpec) DeepCopyInto(out *SluiceClusterSpec) {
	*out = *in
	if in.Persistence != nil {
		clone := *in.Persistence
		out.Persistence = &clone
	}
	if in.Resources != nil {
		clone := ResourceSpec{}
		if in.Resources.Requests != nil {
			clone.Requests = make(map[string]string, len(in.Resources.Requests))
			for k, v := range in.Resources.Requests { clone.Requests[k] = v }
		}
		if in.Resources.Limits != nil {
			clone.Limits = make(map[string]string, len(in.Resources.Limits))
			for k, v := range in.Resources.Limits { clone.Limits[k] = v }
		}
		out.Resources = &clone
	}
}

func (in *SluiceClusterStatus) DeepCopyInto(out *SluiceClusterStatus) {
	*out = *in
	if in.Nodes != nil {
		in, out := &in.Nodes, &out.Nodes
		*out = make([]NodeInfo, len(*in))
		copy(*out, *in)
	}
}

// ---- SluiceClusterList ----

func (in *SluiceClusterList) DeepCopyInto(out *SluiceClusterList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]SluiceCluster, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *SluiceClusterList) DeepCopy() *SluiceClusterList {
	if in == nil {
		return nil
	}
	out := new(SluiceClusterList)
	in.DeepCopyInto(out)
	return out
}

// ---- Tenant ----

func (in *Tenant) DeepCopyInto(out *Tenant) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	out.Status = in.Status
}

func (in *Tenant) DeepCopy() *Tenant {
	if in == nil {
		return nil
	}
	out := new(Tenant)
	in.DeepCopyInto(out)
	return out
}

// ---- TenantList ----

func (in *TenantList) DeepCopyInto(out *TenantList) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ListMeta.DeepCopyInto(&out.ListMeta)
	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = make([]Tenant, len(*in))
		for i := range *in {
			(*in)[i].DeepCopyInto(&(*out)[i])
		}
	}
}

func (in *TenantList) DeepCopy() *TenantList {
	if in == nil {
		return nil
	}
	out := new(TenantList)
	in.DeepCopyInto(out)
	return out
}

// ---- runtime.Object ----

func (in *SluiceCluster) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil { return c }
	return nil
}
func (in *SluiceClusterList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil { return c }
	return nil
}
func (in *Tenant) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil { return c }
	return nil
}
func (in *TenantList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil { return c }
	return nil
}

// Ensure runtime.Object interface.
var _ runtime.Object = &SluiceCluster{}
var _ runtime.Object = &SluiceClusterList{}
var _ runtime.Object = &Tenant{}
var _ runtime.Object = &TenantList{}
