/*
Copyright 2026.

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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// PoolType identifies which mechanism owns a node's lifecycle.
// +kubebuilder:validation:Enum=SKSManaged;KarpenterManaged
type PoolType string

const (
	// PoolTypeSKSManaged is a node belonging to an SKS nodepool, identified by
	// the node.exoscale.net/nodepool-id label.
	PoolTypeSKSManaged PoolType = "SKSManaged"
	// PoolTypeKarpenterManaged is a node owned by a Karpenter NodeClaim.
	PoolTypeKarpenterManaged PoolType = "KarpenterManaged"
)

// ReplacementPhase is the position of a replacement in its state machine.
// +kubebuilder:validation:Enum=Pending;ScalingUp;WaitingForNode;Cordoning;Draining;Evicting;Deleting;Settling;Completed;Failed
type ReplacementPhase string

const (
	// PhasePending re-validates preconditions before anything is mutated.
	PhasePending ReplacementPhase = "Pending"
	// PhaseScalingUp grows the SKS nodepool by one.
	PhaseScalingUp ReplacementPhase = "ScalingUp"
	// PhaseWaitingForNode waits for the surged node to register and go Ready.
	PhaseWaitingForNode ReplacementPhase = "WaitingForNode"
	// PhaseCordoning marks the outdated node unschedulable.
	PhaseCordoning ReplacementPhase = "Cordoning"
	// PhaseDraining evicts the outdated node's pods.
	PhaseDraining ReplacementPhase = "Draining"
	// PhaseEvicting removes the instance from the SKS nodepool.
	PhaseEvicting ReplacementPhase = "Evicting"
	// PhaseDeleting deletes the Karpenter NodeClaim.
	PhaseDeleting ReplacementPhase = "Deleting"
	// PhaseSettling waits for the pods displaced by this replacement to become
	// schedulable again. It is deliberately non-terminal: it holds the
	// rotation's slot against maxUnavailable and defers completedAt -- and with
	// it the cooldown -- so the next rotation cannot start while this one's
	// workloads are still coming up.
	PhaseSettling ReplacementPhase = "Settling"
	// PhaseCompleted is terminal and successful.
	PhaseCompleted ReplacementPhase = "Completed"
	// PhaseFailed is terminal and unsuccessful; no further action is taken.
	PhaseFailed ReplacementPhase = "Failed"
)

// IsTerminal reports whether no further reconciliation will change the phase.
func (p ReplacementPhase) IsTerminal() bool {
	return p == PhaseCompleted || p == PhaseFailed
}

// Condition types set on a NodeReplacement.
const (
	// ConditionSucceeded is True once the outdated node is gone, False when the
	// replacement failed terminally.
	ConditionSucceeded = "Succeeded"
)

// NodeReplacementSpec defines the desired state of NodeReplacement.
// Every field is set by the controller when the object is created and is
// immutable afterwards, so a restart resumes against the same target.
type NodeReplacementSpec struct {
	// nodeName is the outdated node to replace.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="nodeName is immutable"
	NodeName string `json:"nodeName"`

	// policyName is the NodeUpgradePolicy that requested this replacement.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="policyName is immutable"
	PolicyName string `json:"policyName"`

	// poolType is how the node's lifecycle is managed.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="poolType is immutable"
	PoolType PoolType `json:"poolType"`

	// instanceID is the Exoscale compute instance UUID, taken from the node's
	// providerID.
	// +optional
	InstanceID string `json:"instanceID,omitempty"`

	// sksNodepoolID is the owning SKS nodepool UUID, for SKSManaged nodes.
	// +optional
	SKSNodepoolID string `json:"sksNodepoolID,omitempty"`

	// karpenterNodePool is the owning Karpenter NodePool, for KarpenterManaged
	// nodes. The SKS nodepool has no name on the Node object, so it is resolved
	// from the API into status.poolName instead.
	// +optional
	KarpenterNodePool string `json:"karpenterNodePool,omitempty"`

	// nodeClaimName is the owning Karpenter NodeClaim, for KarpenterManaged nodes.
	// +optional
	NodeClaimName string `json:"nodeClaimName,omitempty"`

	// targetVersion is the version the replacement node is expected to reach.
	// +kubebuilder:validation:Required
	TargetVersion string `json:"targetVersion"`

	// reason explains why this node was selected.
	// +optional
	Reason string `json:"reason,omitempty"`
}

// NodeReplacementStatus defines the observed state of NodeReplacement.
type NodeReplacementStatus struct {
	// conditions represent the current state of the replacement.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// phase is the position in the replacement state machine.
	// +optional
	Phase ReplacementPhase `json:"phase,omitempty"`

	// message is a human-readable explanation of the current phase.
	// +optional
	Message string `json:"message,omitempty"`

	// poolName is the resolved name of the pool the node belongs to.
	// +optional
	PoolName string `json:"poolName,omitempty"`

	// poolSizeBefore is the nodepool size recorded before surging, and the size
	// the pool is restored to if the replacement is rolled back.
	// +optional
	PoolSizeBefore *int64 `json:"poolSizeBefore,omitempty"`

	// replacementNodeName is the node that appeared in place of the old one.
	// +optional
	ReplacementNodeName string `json:"replacementNodeName,omitempty"`

	// replacementNodeReadyAt is when the replacement node was first seen Ready.
	// The wait for its DaemonSet pods is measured from here rather than from the
	// phase start, so a slow-to-appear node does not eat that budget.
	// +optional
	ReplacementNodeReadyAt *metav1.Time `json:"replacementNodeReadyAt,omitempty"`

	// replacementNodeVersion is the kubelet version the replacement came up with.
	// A value below spec.targetVersion means the target is not actually
	// obtainable and halts the rollout.
	// +optional
	ReplacementNodeVersion string `json:"replacementNodeVersion,omitempty"`

	// actionIssued records that the destructive call for the current phase (the
	// SKS eviction, or the NodeClaim deletion) already went through, so a
	// requeue waits for the node to disappear instead of issuing it twice.
	// +optional
	ActionIssued bool `json:"actionIssued,omitempty"`

	// cordoned records whether this controller cordoned the node, so a rollback
	// only uncordons what it made unschedulable.
	// +optional
	Cordoned bool `json:"cordoned,omitempty"`

	// startedAt is when the replacement began.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// phaseStartedAt is when the current phase began, used for phase timeouts.
	// +optional
	PhaseStartedAt *metav1.Time `json:"phaseStartedAt,omitempty"`

	// completedAt is when the replacement reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=nrp
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeName`
// +kubebuilder:printcolumn:name="Pool",type=string,JSONPath=`.status.poolName`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.poolType`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.spec.targetVersion`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NodeReplacement is the Schema for the nodereplacements API.
// One object tracks one node rotation. The SKS flow spans several minutes of
// asynchronous cloud operations, so the phase lives here rather than in memory:
// the controller resumes mid-flight after a restart, including when it drained
// its own node.
type NodeReplacement struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of NodeReplacement
	// +required
	Spec NodeReplacementSpec `json:"spec"`

	// status defines the observed state of NodeReplacement
	// +optional
	Status NodeReplacementStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// NodeReplacementList contains a list of NodeReplacement.
type NodeReplacementList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeReplacement `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(SchemeGroupVersion, &NodeReplacement{}, &NodeReplacementList{})
		return nil
	})
}
