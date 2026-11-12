/*
Copyright 2026 WhizUs.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package nodes classifies Kubernetes nodes by which mechanism owns their
// lifecycle, and drains them.
package nodes

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

// Labels, annotations and taints that identify how a node is managed. These are
// set by SKS and by Karpenter, not by this operator.
const (
	// SKSNodepoolIDLabel carries the owning SKS nodepool UUID. SKS sets it on
	// every node it creates, which makes the pool lookup a label read rather
	// than a walk from providerID through the instance and its instance-pool.
	SKSNodepoolIDLabel = "node.exoscale.net/nodepool-id"

	// KarpenterNodePoolLabel names the Karpenter NodePool that owns the node.
	KarpenterNodePoolLabel = "karpenter.sh/nodepool"

	// KarpenterDoNotDisruptAnnotation opts a node out of disruption.
	KarpenterDoNotDisruptAnnotation = "karpenter.sh/do-not-disrupt"

	// KarpenterDisruptedTaint is applied by Karpenter to a node it is already
	// terminating.
	KarpenterDisruptedTaint = "karpenter.sh/disrupted"

	// ProviderIDPrefix precedes the Exoscale compute instance UUID.
	ProviderIDPrefix = "exoscale://"

	// TrueValue is the opt-out annotation value.
	TrueValue = "true"

	// RegionLabel is set on SKS-managed nodes; ZoneLabel on Karpenter nodes.
	RegionLabel = "topology.kubernetes.io/region"
	ZoneLabel   = "topology.kubernetes.io/zone"
)

// NodeClaim identifiers, used to find the object to delete for a Karpenter node.
const (
	NodeClaimAPIVersion = "karpenter.sh/v1"
	NodeClaimKind       = "NodeClaim"
	NodeClaimGroup      = "karpenter.sh"
	NodeClaimVersion    = "v1"
)

// Info is everything the controllers need to know about a node.
type Info struct {
	Name string
	// PoolType is empty when the node is managed by neither mechanism.
	PoolType sksv1alpha1.PoolType
	// SKSNodepoolID is set for SKSManaged nodes.
	SKSNodepoolID string
	// KarpenterNodePool and NodeClaimName are set for KarpenterManaged nodes.
	// NodeClaimName may be empty if the node carries no owner reference, in
	// which case the claim has to be located by providerID.
	KarpenterNodePool string
	NodeClaimName     string

	InstanceID     string
	KubeletVersion string
	Zone           string

	Ready         bool
	Unschedulable bool
	// Terminating means the Node object is already being deleted, so something
	// else is mid-way through removing it.
	Terminating bool
}

// Managed reports whether this operator recognises the node's owner.
func (i Info) Managed() bool { return i.PoolType != "" }

// Classify reads a node's ownership from its labels, owner references and
// provider ID. It never errors: an unrecognised node yields an Info with an
// empty PoolType and is simply left alone.
func Classify(node *corev1.Node) Info {
	info := Info{
		Name:           node.Name,
		KubeletVersion: node.Status.NodeInfo.KubeletVersion,
		Unschedulable:  node.Spec.Unschedulable,
		Terminating:    node.DeletionTimestamp != nil,
		Ready:          IsReady(node),
		Zone:           zoneOf(node),
	}

	if id, err := InstanceIDFromProviderID(node.Spec.ProviderID); err == nil {
		info.InstanceID = id
	}

	// Karpenter is checked first: its nodes never carry the SKS nodepool label,
	// but checking the more specific owner first keeps the classification stable
	// if that ever changes.
	if pool, ok := node.Labels[KarpenterNodePoolLabel]; ok && pool != "" {
		info.PoolType = sksv1alpha1.PoolTypeKarpenterManaged
		info.KarpenterNodePool = pool
		info.NodeClaimName = nodeClaimNameFromOwners(node)
		return info
	}

	if poolID, ok := node.Labels[SKSNodepoolIDLabel]; ok && poolID != "" {
		info.PoolType = sksv1alpha1.PoolTypeSKSManaged
		info.SKSNodepoolID = poolID
		return info
	}

	return info
}

// InstanceIDFromProviderID extracts the Exoscale compute instance UUID.
func InstanceIDFromProviderID(providerID string) (string, error) {
	if providerID == "" {
		return "", fmt.Errorf("node has no providerID")
	}
	if !strings.HasPrefix(providerID, ProviderIDPrefix) {
		return "", fmt.Errorf("providerID %q is not an Exoscale instance", providerID)
	}
	id := strings.TrimPrefix(providerID, ProviderIDPrefix)
	if id == "" {
		return "", fmt.Errorf("providerID %q has no instance ID", providerID)
	}
	return id, nil
}

// IsReady reports whether the node's Ready condition is True.
func IsReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// SkipReason explains why a node must not be replaced, or "" when it may be.
// Nodes another controller is already retiring are left alone: taking over
// mid-termination would double-disrupt the workload.
func SkipReason(node *corev1.Node) string {
	if node.DeletionTimestamp != nil {
		return "node is already terminating"
	}
	if node.Annotations[sksv1alpha1.SkipAnnotation] == TrueValue {
		return fmt.Sprintf("node carries %s=true", sksv1alpha1.SkipAnnotation)
	}
	if _, ok := node.Annotations[KarpenterDoNotDisruptAnnotation]; ok {
		return fmt.Sprintf("node carries %s", KarpenterDoNotDisruptAnnotation)
	}
	for _, t := range node.Spec.Taints {
		if t.Key == KarpenterDisruptedTaint {
			return "Karpenter is already disrupting this node"
		}
	}
	return ""
}

func zoneOf(node *corev1.Node) string {
	if z, ok := node.Labels[ZoneLabel]; ok && z != "" {
		return z
	}
	return node.Labels[RegionLabel]
}

func nodeClaimNameFromOwners(node *corev1.Node) string {
	for _, ref := range node.OwnerReferences {
		if ref.Kind == NodeClaimKind && ref.APIVersion == NodeClaimAPIVersion {
			return ref.Name
		}
	}
	return ""
}
