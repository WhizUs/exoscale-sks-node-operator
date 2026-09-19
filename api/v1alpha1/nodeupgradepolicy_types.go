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

// Annotation keys understood by the operator.
const (
	// SkipAnnotation excludes a Node from replacement while set to "true".
	SkipAnnotation = "sks.whizus.com/skip"
)

// Condition types set on a NodeUpgradePolicy.
const (
	ConditionReady       = "Ready"
	ConditionProgressing = "Progressing"
	ConditionWindowOpen  = "WindowOpen"
	ConditionDegraded    = "Degraded"
)

// Condition reasons.
const (
	ReasonTargetUnreachable = "TargetUnreachable"
	ReasonInWindow          = "InWindow"
	ReasonOutOfWindow       = "OutOfWindow"
	ReasonPaused            = "Paused"
	ReasonUpToDate          = "UpToDate"
	ReasonReplacing         = "Replacing"
	ReasonExoscaleError     = "ExoscaleError"
	ReasonDryRun            = "DryRun"
	ReasonCoolingDown       = "CoolingDown"
)

// VersionSource selects how the target Kubernetes version is resolved.
// +kubebuilder:validation:Enum=Auto;ControlPlane;Pinned
type VersionSource string

const (
	// VersionSourceAuto resolves the highest of the control-plane version, the
	// newest version Exoscale offers, and the highest kubelet already observed,
	// restricted to the control plane's own major.minor.
	VersionSourceAuto VersionSource = "Auto"
	// VersionSourceControlPlane targets exactly the control-plane version, so
	// only nodes strictly below it are replaced.
	VersionSourceControlPlane VersionSource = "ControlPlane"
	// VersionSourcePinned targets VersionPolicy.PinnedVersion verbatim.
	VersionSourcePinned VersionSource = "Pinned"
)

// ExoscaleSpec locates the SKS cluster. Both fields are discovered from the
// Node objects when left empty.
type ExoscaleSpec struct {
	// zone is the Exoscale zone, e.g. "ch-gva-2". Defaults to the zone read from
	// the node labels, or the EXOSCALE_ZONE environment variable.
	// +optional
	Zone string `json:"zone,omitempty"`

	// clusterID is the SKS cluster UUID. Defaults to the cluster that owns a
	// nodepool referenced by one of this cluster's nodes.
	// +optional
	ClusterID string `json:"clusterID,omitempty"`
}

// MaintenanceWindow is a recurring period during which replacements may start.
// A replacement already in flight always runs to completion, even past the end
// of the window: leaving a node half-drained or a pool surged is worse than
// overrunning.
type MaintenanceWindow struct {
	// schedule is a cron expression marking the START of the window.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:example="0 2 * * *"
	Schedule string `json:"schedule"`

	// duration is how long the window stays open after each start.
	// +kubebuilder:validation:Required
	// +kubebuilder:example="1h"
	Duration metav1.Duration `json:"duration"`

	// timeZone is an IANA time zone name used to interpret schedule.
	// +kubebuilder:default="UTC"
	// +kubebuilder:example="Europe/Vienna"
	// +optional
	TimeZone string `json:"timeZone,omitempty"`
}

// VersionPolicy controls which Kubernetes version nodes are held to.
type VersionPolicy struct {
	// source selects how the target version is resolved.
	// +kubebuilder:default=Auto
	// +optional
	Source VersionSource `json:"source,omitempty"`

	// pinnedVersion is the target version when source is Pinned.
	// +kubebuilder:validation:Pattern=`^v?[0-9]+\.[0-9]+\.[0-9]+$`
	// +optional
	PinnedVersion string `json:"pinnedVersion,omitempty"`
}

// RolloutSpec bounds how fast and how far replacements go.
type RolloutSpec struct {
	// maxUnavailable is the number of replacements in flight cluster-wide.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxUnavailable int32 `json:"maxUnavailable,omitempty"`

	// maxUnavailablePerPool is the number of replacements in flight per nodepool.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxUnavailablePerPool int32 `json:"maxUnavailablePerPool,omitempty"`

	// cooldown is the quiet period after each successful replacement.
	// +kubebuilder:default="5m"
	// +optional
	Cooldown metav1.Duration `json:"cooldown,omitempty"`

	// nodeReadyTimeout bounds the wait for a surged node to become Ready.
	// +kubebuilder:default="15m"
	// +optional
	NodeReadyTimeout metav1.Duration `json:"nodeReadyTimeout,omitempty"`

	// settleTimeout bounds the two waits for workloads rather than nodes: the
	// replacement node's DaemonSet pods becoming Ready before anything is
	// drained onto it, and the pods displaced by the drain becoming schedulable
	// again before the replacement is considered complete.
	//
	// A node reports Ready with no workload images pulled, so treating Ready as
	// "this node can carry load" drains onto a node that cannot yet serve, and
	// releases the next rotation while the previous one is still cold.
	//
	// Neither wait ever fails a replacement: on expiry it warns and proceeds.
	// Set to 0 to disable both and gate on node readiness alone.
	// +kubebuilder:default="10m"
	// +optional
	SettleTimeout *metav1.Duration `json:"settleTimeout,omitempty"`

	// replacementTimeout bounds a single replacement end to end.
	// +kubebuilder:default="45m"
	// +optional
	ReplacementTimeout metav1.Duration `json:"replacementTimeout,omitempty"`
}

// DrainSpec controls how pods are moved off a node before it is destroyed.
type DrainSpec struct {
	// enabled drains the node before replacing it. When false the node is only
	// cordoned. Note that for Karpenter-managed nodes the control plane drains
	// the node itself on NodeClaim deletion regardless of this setting.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`

	// timeout bounds the drain.
	// +kubebuilder:default="10m"
	// +optional
	Timeout metav1.Duration `json:"timeout,omitempty"`

	// gracePeriodSeconds overrides each pod's termination grace period.
	// -1 honours the pod's own setting.
	// +kubebuilder:default=-1
	// +kubebuilder:validation:Minimum=-1
	// +optional
	GracePeriodSeconds int32 `json:"gracePeriodSeconds,omitempty"`

	// ignoreDaemonSets continues even though DaemonSet pods cannot be evicted.
	// +kubebuilder:default=true
	// +optional
	IgnoreDaemonSets *bool `json:"ignoreDaemonSets,omitempty"`

	// deleteEmptyDirData allows evicting pods backed by an emptyDir volume.
	// +kubebuilder:default=true
	// +optional
	DeleteEmptyDirData *bool `json:"deleteEmptyDirData,omitempty"`

	// force allows evicting pods that are not managed by a controller.
	// +kubebuilder:default=false
	// +optional
	Force *bool `json:"force,omitempty"`

	// disableEviction deletes pods outright instead of using the Eviction API,
	// which ignores PodDisruptionBudgets. Escape hatch only.
	// +kubebuilder:default=false
	// +optional
	DisableEviction *bool `json:"disableEviction,omitempty"`
}

// SKSStrategy configures replacement of SKS-managed nodes.
//
// An SKS-managed node is always replaced by growing its nodepool by one and
// waiting for the new node to be Ready before the outdated one is drained and
// evicted, so capacity never dips. Eviction shrinks the pool again, leaving it
// at the size it started at.
type SKSStrategy struct {
	// enabled turns on replacement of SKS-managed nodes.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// KarpenterStrategy configures replacement of Karpenter-managed nodes.
type KarpenterStrategy struct {
	// enabled turns on replacement of Karpenter-managed nodes by deleting their
	// NodeClaim. This is an involuntary disruption and therefore bypasses
	// Karpenter's own disruption budgets; this policy's window and concurrency
	// limits are the only gate.
	// +kubebuilder:default=true
	// +optional
	Enabled *bool `json:"enabled,omitempty"`
}

// StrategiesSpec groups the per-pool-type strategies.
type StrategiesSpec struct {
	// sksManaged configures nodes belonging to an SKS nodepool.
	// +optional
	SKSManaged SKSStrategy `json:"sksManaged,omitempty"`

	// karpenterManaged configures nodes owned by a Karpenter NodeClaim.
	// +optional
	KarpenterManaged KarpenterStrategy `json:"karpenterManaged,omitempty"`
}

// NodeUpgradePolicySpec defines the desired state of NodeUpgradePolicy.
type NodeUpgradePolicySpec struct {
	// paused stops new replacements from being created. Replacements already in
	// flight still run to completion.
	// +kubebuilder:default=false
	// +optional
	Paused bool `json:"paused,omitempty"`

	// dryRun evaluates everything and reports it through status and events, but
	// never cordons, drains, scales or evicts.
	// +kubebuilder:default=false
	// +optional
	DryRun bool `json:"dryRun,omitempty"`

	// exoscale locates the SKS cluster.
	// +optional
	Exoscale ExoscaleSpec `json:"exoscale,omitempty"`

	// maintenanceWindows lists the periods during which replacements may start.
	// An empty list means replacements never start.
	// +optional
	MaintenanceWindows []MaintenanceWindow `json:"maintenanceWindows,omitempty"`

	// nodeSelector limits the policy to nodes carrying these labels. Empty
	// selects every node.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// versionPolicy controls which version nodes are held to.
	// +optional
	VersionPolicy VersionPolicy `json:"versionPolicy,omitempty"`

	// rollout bounds how fast and how far replacements go.
	// +optional
	Rollout RolloutSpec `json:"rollout,omitempty"`

	// drain controls how pods are moved off a node.
	// +optional
	Drain DrainSpec `json:"drain,omitempty"`

	// strategies configures the per-pool-type replacement mechanisms.
	// +optional
	Strategies StrategiesSpec `json:"strategies,omitempty"`

	// historyLimit is how many finished NodeReplacement objects to retain.
	// +kubebuilder:default=10
	// +kubebuilder:validation:Minimum=0
	// +optional
	HistoryLimit int32 `json:"historyLimit,omitempty"`
}

// NodeUpgradePolicyStatus defines the observed state of NodeUpgradePolicy.
type NodeUpgradePolicyStatus struct {
	// conditions represent the current state of the policy.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the .metadata.generation this status was computed from.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// controlPlaneVersion is the Kubernetes version reported by the SKS cluster.
	// +optional
	ControlPlaneVersion string `json:"controlPlaneVersion,omitempty"`

	// targetVersion is the version nodes are currently held to.
	// +optional
	TargetVersion string `json:"targetVersion,omitempty"`

	// targetVersionSource explains where targetVersion came from.
	// +optional
	TargetVersionSource string `json:"targetVersionSource,omitempty"`

	// outdatedNodes is how many managed nodes run below targetVersion.
	// +optional
	OutdatedNodes int32 `json:"outdatedNodes"`

	// totalNodes is how many nodes this policy manages.
	// +optional
	TotalNodes int32 `json:"totalNodes"`

	// activeReplacements names the NodeReplacement objects currently in flight.
	// +optional
	ActiveReplacements []string `json:"activeReplacements,omitempty"`

	// nextWindowStart is when the next maintenance window opens.
	// +optional
	NextWindowStart *metav1.Time `json:"nextWindowStart,omitempty"`

	// lastCompletedAt is when the most recent replacement finished.
	// +optional
	LastCompletedAt *metav1.Time `json:"lastCompletedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:resource:scope=Cluster,shortName=nup
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.targetVersion`
// +kubebuilder:printcolumn:name="Outdated",type=integer,JSONPath=`.status.outdatedNodes`
// +kubebuilder:printcolumn:name="Total",type=integer,JSONPath=`.status.totalNodes`
// +kubebuilder:printcolumn:name="Window",type=string,JSONPath=`.status.conditions[?(@.type=="WindowOpen")].status`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// NodeUpgradePolicy is the Schema for the nodeupgradepolicies API.
type NodeUpgradePolicy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of NodeUpgradePolicy
	// +required
	Spec NodeUpgradePolicySpec `json:"spec"`

	// status defines the observed state of NodeUpgradePolicy
	// +optional
	Status NodeUpgradePolicyStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// NodeUpgradePolicyList contains a list of NodeUpgradePolicy.
type NodeUpgradePolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NodeUpgradePolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(scheme *runtime.Scheme) error {
		scheme.AddKnownTypes(SchemeGroupVersion, &NodeUpgradePolicy{}, &NodeUpgradePolicyList{})
		return nil
	})
}
