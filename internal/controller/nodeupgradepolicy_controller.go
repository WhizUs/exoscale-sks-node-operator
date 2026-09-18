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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
	"github.com/whizus/exoscale-sks-node-operator/internal/exoscale"
	"github.com/whizus/exoscale-sks-node-operator/internal/metrics"
	"github.com/whizus/exoscale-sks-node-operator/internal/nodes"
	"github.com/whizus/exoscale-sks-node-operator/internal/version"
	"github.com/whizus/exoscale-sks-node-operator/internal/window"
)

// PolicyNameLabel ties a NodeReplacement back to the policy that created it.
const PolicyNameLabel = "sks.whizus.com/policy"

// NodeNameLabel records which node a NodeReplacement targets.
const NodeNameLabel = "sks.whizus.com/node"

// idleRequeue re-checks a policy periodically even when nothing changed, so a
// newly published Kubernetes patch is noticed without waiting for a Node event.
const idleRequeue = 10 * time.Minute

// NodeUpgradePolicyReconciler selects outdated nodes and schedules their
// replacement. It never touches a node itself; it only creates NodeReplacement
// objects, which the replacement controller executes.
type NodeUpgradePolicyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	Exoscale exoscale.Provider

	// SelfNodeName is the node this operator runs on, from the downward API.
	// Its replacement is ordered last so the operator does not evict itself
	// while other work is still pending.
	SelfNodeName string

	// Now is overridable in tests.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodeupgradepolicies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodeupgradepolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodeupgradepolicies/finalizers,verbs=update
// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodereplacements,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=core,resources=nodes,verbs=get;list;watch;patch;update
// +kubebuilder:rbac:groups=core,resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=events.k8s.io,resources=events,verbs=create;patch

// Reconcile evaluates one policy.
func (r *NodeUpgradePolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var policy sksv1alpha1.NodeUpgradePolicy
	if err := r.Get(ctx, req.NamespacedName, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			metrics.ForgetPolicy(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	st := &policyState{policy: &policy, now: r.now()}

	result, err := r.evaluate(ctx, st)
	if err != nil {
		st.setDegraded(sksv1alpha1.ReasonExoscaleError, err.Error())
		r.Recorder.Eventf(&policy, nil, corev1.EventTypeWarning,
			sksv1alpha1.ReasonExoscaleError, "Reconciling", "%s", err.Error())
		if statusErr := r.updateStatus(ctx, st); statusErr != nil {
			logger.Error(statusErr, "updating status after failure")
		}
		return ctrl.Result{}, err
	}

	if err := r.updateStatus(ctx, st); err != nil {
		return ctrl.Result{}, err
	}
	return result, nil
}

// policyState accumulates everything one reconcile pass computed, so status is
// written once at the end regardless of which branch was taken.
type policyState struct {
	policy *sksv1alpha1.NodeUpgradePolicy
	now    time.Time

	status     sksv1alpha1.NodeUpgradePolicyStatus
	conditions []metav1.Condition
}

func (s *policyState) setCondition(condType, status, reason, msg string) {
	cond := metav1.Condition{
		Type:               condType,
		Reason:             reason,
		Message:            truncate(msg, 32*1024),
		ObservedGeneration: s.policy.Generation,
	}
	if status == "True" {
		cond.Status = metav1.ConditionTrue
	} else {
		cond.Status = metav1.ConditionFalse
	}
	s.conditions = append(s.conditions, cond)
}

func (s *policyState) setDegraded(reason, msg string) {
	s.setCondition(sksv1alpha1.ConditionDegraded, "True", reason, msg)
	s.setCondition(sksv1alpha1.ConditionReady, "False", reason, msg)
}

func (r *NodeUpgradePolicyReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// evaluate does the real work and returns the requeue decision.
func (r *NodeUpgradePolicyReconciler) evaluate(
	ctx context.Context,
	st *policyState,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	policy := st.policy

	windows, err := window.ParseAll(policy.Spec.MaintenanceWindows)
	if err != nil {
		// A malformed window is a spec bug: report it and wait for an edit
		// rather than retrying a schedule that will never parse.
		st.setDegraded("InvalidMaintenanceWindow", err.Error())
		r.Recorder.Eventf(policy, nil, corev1.EventTypeWarning,
			"InvalidMaintenanceWindow", "Validating", "%s", err.Error())
		return ctrl.Result{}, nil
	}

	managed, err := r.managedNodes(ctx, policy)
	if err != nil {
		return ctrl.Result{}, err
	}
	st.status.TotalNodes = int32(len(managed))

	open := windows.IsOpen(st.now)
	st.setCondition(sksv1alpha1.ConditionWindowOpen, boolStatus(open),
		windowReason(open), windowMessage(windows, st.now, open))
	if next, ok := windows.NextStart(st.now); ok {
		t := metav1.NewTime(next)
		st.status.NextWindowStart = &t
	}
	requeue := requeueDelay(windows, st.now)

	// With nothing selected there is nothing to decide, and no reason to reach
	// for the Exoscale API — which could not even be addressed, since the zone
	// and cluster are discovered from the nodes themselves.
	if len(managed) == 0 {
		st.setCondition(sksv1alpha1.ConditionProgressing, "False", "Idle", "no nodes selected")
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonUpToDate,
			"policy selects no nodes")
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	exoClient, err := r.exoscaleClient(ctx, policy, managed)
	if err != nil {
		return ctrl.Result{}, err
	}

	cluster, err := exoClient.GetCluster(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}
	available, err := exoClient.ListVersions(ctx)
	if err != nil {
		return ctrl.Result{}, err
	}

	target, err := version.Resolve(version.TargetInput{
		ControlPlane:    cluster.Version,
		Available:       available,
		ObservedKubelet: readyKubeletVersions(managed),
		Source:          policy.Spec.VersionPolicy.SourceOrDefault(),
		Pinned:          policy.Spec.VersionPolicy.PinnedVersion,
	})
	if err != nil {
		st.setDegraded("InvalidVersionPolicy", err.Error())
		r.Recorder.Eventf(policy, nil, corev1.EventTypeWarning,
			"InvalidVersionPolicy", "Validating", "%s", err.Error())
		return ctrl.Result{}, nil
	}

	st.status.ControlPlaneVersion = cluster.Version
	st.status.TargetVersion = target.Version.String()
	st.status.TargetVersionSource = target.Source

	replacements, err := r.ownedReplacements(ctx, policy)
	if err != nil {
		return ctrl.Result{}, err
	}
	active := activeReplacements(replacements)
	st.status.ActiveReplacements = namesOf(active)
	st.status.LastCompletedAt = lastCompletion(replacements)

	outdated := outdatedCandidates(managed, target.Version)
	st.status.OutdatedNodes = int32(len(outdated))

	r.publishMetrics(policy.Name, st, windows, cluster, target, len(active))

	if err := r.pruneHistory(ctx, policy, replacements); err != nil {
		logger.Error(err, "pruning replacement history")
	}

	// A target that a fresh node does not actually reach is not obtainable.
	// Halting here bounds the damage to a single wasted rotation per target
	// rather than rotating the whole fleet for nothing.
	if node, ver := unreachableTarget(replacements, target.Version); node != "" {
		msg := fmt.Sprintf(
			"replacement for %s came up at %s, below target %s; halting until the target changes",
			node, ver, target.Version)
		st.setDegraded(sksv1alpha1.ReasonTargetUnreachable, msg)
		r.Recorder.Eventf(policy, nil, corev1.EventTypeWarning,
			sksv1alpha1.ReasonTargetUnreachable, "Evaluating", "%s", msg)
		return ctrl.Result{RequeueAfter: idleRequeue}, nil
	}

	st.setCondition(sksv1alpha1.ConditionProgressing, boolStatus(len(active) > 0),
		progressingReason(len(active)),
		fmt.Sprintf("%d replacement(s) in flight, %d node(s) outdated", len(active), len(outdated)))

	switch {
	case policy.Spec.Paused:
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonPaused,
			"policy is paused; no new replacements will be started")
		return ctrl.Result{RequeueAfter: requeue}, nil

	case len(outdated) == 0:
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonUpToDate,
			fmt.Sprintf("all %d managed node(s) are at %s", len(managed), target.Version))
		return ctrl.Result{RequeueAfter: requeue}, nil

	case !open:
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonOutOfWindow,
			fmt.Sprintf("%d node(s) outdated; waiting for the maintenance window", len(outdated)))
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	// One replacement per cooldown, so the cluster gets a quiet period to settle
	// before the next node is taken. Without this, rotations run back to back as
	// fast as each node disappears.
	if left := r.cooldownRemaining(st); left > 0 {
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonCoolingDown,
			fmt.Sprintf("%d node(s) outdated; cooling down for another %s after the last replacement",
				len(outdated), left.Round(time.Second)))
		if left < requeue {
			requeue = left
		}
		return ctrl.Result{RequeueAfter: requeue}, nil
	}

	created, err := r.startReplacements(ctx, st, target, outdated, active)
	if err != nil {
		return ctrl.Result{}, err
	}

	if policy.Spec.DryRun {
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonDryRun,
			fmt.Sprintf("%d node(s) outdated; dryRun is enabled, so nothing will be changed",
				len(outdated)))
	} else {
		st.setCondition(sksv1alpha1.ConditionReady, "True", sksv1alpha1.ReasonReplacing,
			fmt.Sprintf("%d node(s) outdated; %d replacement(s) in flight, %d started this pass",
				len(outdated), len(active), created))
	}

	return ctrl.Result{RequeueAfter: requeue}, nil
}

// managedNode pairs a Node with its classification.
type managedNode struct {
	node *corev1.Node
	info nodes.Info
}

func (r *NodeUpgradePolicyReconciler) managedNodes(
	ctx context.Context,
	policy *sksv1alpha1.NodeUpgradePolicy,
) ([]managedNode, error) {
	var list corev1.NodeList
	opts := []client.ListOption{}
	if len(policy.Spec.NodeSelector) > 0 {
		opts = append(opts, client.MatchingLabels(policy.Spec.NodeSelector))
	}
	if err := r.List(ctx, &list, opts...); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	out := make([]managedNode, 0, len(list.Items))
	for i := range list.Items {
		node := &list.Items[i]
		info := nodes.Classify(node)
		if !info.Managed() || !strategyEnabled(policy, info.PoolType) {
			continue
		}
		out = append(out, managedNode{node: node, info: info})
	}
	return out, nil
}

func strategyEnabled(policy *sksv1alpha1.NodeUpgradePolicy, poolType sksv1alpha1.PoolType) bool {
	switch poolType {
	case sksv1alpha1.PoolTypeSKSManaged:
		return policy.Spec.Strategies.SKSManaged.IsEnabled()
	case sksv1alpha1.PoolTypeKarpenterManaged:
		return policy.Spec.Strategies.KarpenterManaged.IsEnabled()
	default:
		return false
	}
}

// exoscaleClient builds the API client, discovering the cluster from any
// SKS-managed node when the policy does not name one.
func (r *NodeUpgradePolicyReconciler) exoscaleClient(
	ctx context.Context,
	policy *sksv1alpha1.NodeUpgradePolicy,
	managed []managedNode,
) (exoscale.Client, error) {
	opts := exoscale.Options{
		Zone:      policy.Spec.Exoscale.Zone,
		ClusterID: policy.Spec.Exoscale.ClusterID,
	}
	for _, m := range managed {
		if opts.Zone == "" && m.info.Zone != "" {
			opts.Zone = m.info.Zone
		}
		if opts.NodepoolIDHint == "" && m.info.SKSNodepoolID != "" {
			opts.NodepoolIDHint = m.info.SKSNodepoolID
		}
	}
	if opts.Zone == "" {
		opts.Zone = os.Getenv("EXOSCALE_ZONE")
	}
	if opts.Zone == "" {
		return nil, fmt.Errorf(
			"cannot determine the Exoscale zone: set spec.exoscale.zone")
	}
	return r.Exoscale.Get(ctx, opts)
}

func readyKubeletVersions(managed []managedNode) []string {
	out := make([]string, 0, len(managed))
	for _, m := range managed {
		if m.info.Ready {
			out = append(out, m.info.KubeletVersion)
		}
	}
	return out
}

func outdatedCandidates(managed []managedNode, target version.Version) []managedNode {
	out := make([]managedNode, 0, len(managed))
	for _, m := range managed {
		if version.IsOutdated(m.info.KubeletVersion, target) {
			out = append(out, m)
		}
	}
	return out
}

func (r *NodeUpgradePolicyReconciler) ownedReplacements(
	ctx context.Context,
	policy *sksv1alpha1.NodeUpgradePolicy,
) ([]sksv1alpha1.NodeReplacement, error) {
	var list sksv1alpha1.NodeReplacementList
	if err := r.List(ctx, &list, client.MatchingLabels{PolicyNameLabel: policy.Name}); err != nil {
		return nil, fmt.Errorf("list node replacements: %w", err)
	}
	return list.Items, nil
}

func activeReplacements(all []sksv1alpha1.NodeReplacement) []sksv1alpha1.NodeReplacement {
	out := make([]sksv1alpha1.NodeReplacement, 0, len(all))
	for _, nr := range all {
		if !nr.Status.Phase.IsTerminal() {
			out = append(out, nr)
		}
	}
	return out
}

// unreachableTarget reports the first completed replacement whose new node came
// up below the target it was created for.
func unreachableTarget(all []sksv1alpha1.NodeReplacement, target version.Version) (string, string) {
	for _, nr := range all {
		if nr.Status.Phase != sksv1alpha1.PhaseCompleted {
			continue
		}
		if nr.Spec.TargetVersion != target.String() || nr.Status.ReplacementNodeVersion == "" {
			continue
		}
		if version.IsOutdated(nr.Status.ReplacementNodeVersion, target) {
			return nr.Status.ReplacementNodeName, nr.Status.ReplacementNodeVersion
		}
	}
	return "", ""
}

// startReplacements creates NodeReplacements for as many candidates as the
// budgets allow, and returns how many it created.
func (r *NodeUpgradePolicyReconciler) startReplacements(
	ctx context.Context,
	st *policyState,
	target version.Target,
	outdated []managedNode,
	active []sksv1alpha1.NodeReplacement,
) (int, error) {
	logger := log.FromContext(ctx)
	policy := st.policy

	budget := int(policy.Spec.Rollout.MaxUnavailableOrDefault()) - len(active)
	if budget <= 0 {
		return 0, nil
	}

	perPool := poolCounts(active)
	sortCandidates(outdated, r.SelfNodeName)

	created := 0
	for _, cand := range outdated {
		if budget <= 0 {
			break
		}
		if reason := nodes.SkipReason(cand.node); reason != "" {
			logger.V(1).Info("skipping node", "node", cand.node.Name, "reason", reason)
			continue
		}
		if reason := r.poolBudgetReason(policy, cand, perPool); reason != "" {
			logger.V(1).Info("deferring node", "node", cand.node.Name, "reason", reason)
			continue
		}

		if policy.Spec.DryRun {
			// Dry run stops here deliberately: reporting what would happen is
			// safe, whereas creating the object would start real work.
			r.Recorder.Eventf(policy, nil, corev1.EventTypeNormal, "DryRunWouldReplace", "Scheduling",
				"would replace %s (kubelet %s, target %s)",
				cand.node.Name, cand.info.KubeletVersion, target.Version)
			budget--
			perPool[poolKey(cand.info)]++
			continue
		}

		nr := buildReplacement(policy, cand, target)
		if err := ctrl.SetControllerReference(policy, nr, r.Scheme); err != nil {
			return created, fmt.Errorf("set owner on %s: %w", nr.Name, err)
		}
		if err := r.Create(ctx, nr); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// A terminal replacement for this node and target already
				// exists; leave it as the record and move on.
				continue
			}
			return created, fmt.Errorf("create node replacement %s: %w", nr.Name, err)
		}

		r.Recorder.Eventf(policy, nil, corev1.EventTypeNormal, "ReplacementScheduled", "Scheduling",
			"scheduled replacement of %s (kubelet %s, target %s)",
			cand.node.Name, cand.info.KubeletVersion, target.Version)
		logger.Info("scheduled replacement",
			"node", cand.node.Name, "target", target.Version, "replacement", nr.Name)

		created++
		budget--
		perPool[poolKey(cand.info)]++
	}
	return created, nil
}

// poolBudgetReason reports why a candidate must wait, or "" if it may proceed.
// Capacity itself needs no floor: SKS nodes are surged before anything is
// removed, and Karpenter reprovisions on the pods left pending.
func (r *NodeUpgradePolicyReconciler) poolBudgetReason(
	policy *sksv1alpha1.NodeUpgradePolicy,
	cand managedNode,
	perPool map[string]int,
) string {
	key := poolKey(cand.info)
	limit := int(policy.Spec.Rollout.MaxUnavailablePerPoolOrDefault())
	if perPool[key] >= limit {
		return fmt.Sprintf("pool %s already has %d replacement(s) in flight", key, perPool[key])
	}
	return ""
}

func poolCounts(active []sksv1alpha1.NodeReplacement) map[string]int {
	out := map[string]int{}
	for _, nr := range active {
		switch nr.Spec.PoolType {
		case sksv1alpha1.PoolTypeSKSManaged:
			out["sks/"+nr.Spec.SKSNodepoolID]++
		case sksv1alpha1.PoolTypeKarpenterManaged:
			out["karpenter/"+nr.Spec.KarpenterNodePool]++
		}
	}
	return out
}

func poolKey(info nodes.Info) string {
	if info.PoolType == sksv1alpha1.PoolTypeKarpenterManaged {
		return "karpenter/" + info.KarpenterNodePool
	}
	return "sks/" + info.SKSNodepoolID
}

// sortCandidates orders replacements: furthest behind first, then oldest, with
// the operator's own node always last so it keeps running while it has work to
// hand out.
func sortCandidates(cands []managedNode, selfNode string) {
	parsed := make(map[string]version.Version, len(cands))
	for _, c := range cands {
		if v, err := version.Parse(c.info.KubeletVersion); err == nil {
			parsed[c.node.Name] = v
		}
	}

	slices.SortStableFunc(cands, func(a, b managedNode) int {

		aSelf, bSelf := a.node.Name == selfNode, b.node.Name == selfNode
		if aSelf != bSelf {
			if bSelf {
				return -1
			}
			return 1
		}
		if cmp := parsed[a.node.Name].Compare(parsed[b.node.Name]); cmp != 0 {
			return cmp
		}
		if !a.node.CreationTimestamp.Equal(&b.node.CreationTimestamp) {
			if a.node.CreationTimestamp.Before(&b.node.CreationTimestamp) {
				return -1
			}
			return 1
		}
		return strings.Compare(a.node.Name, b.node.Name)
	})
}

func buildReplacement(
	policy *sksv1alpha1.NodeUpgradePolicy,
	cand managedNode,
	target version.Target,
) *sksv1alpha1.NodeReplacement {
	spec := sksv1alpha1.NodeReplacementSpec{
		NodeName:      cand.node.Name,
		PolicyName:    policy.Name,
		PoolType:      cand.info.PoolType,
		InstanceID:    cand.info.InstanceID,
		TargetVersion: target.Version.String(),
		Reason: fmt.Sprintf("kubelet %s is below target %s (%s)",
			cand.info.KubeletVersion, target.Version, target.Source),
	}
	switch cand.info.PoolType {
	case sksv1alpha1.PoolTypeSKSManaged:
		spec.SKSNodepoolID = cand.info.SKSNodepoolID
	case sksv1alpha1.PoolTypeKarpenterManaged:
		spec.NodeClaimName = cand.info.NodeClaimName
		spec.KarpenterNodePool = cand.info.KarpenterNodePool
	}

	return &sksv1alpha1.NodeReplacement{
		ObjectMeta: metav1.ObjectMeta{
			Name: replacementName(cand.node.Name, target.Version.String()),
			Labels: map[string]string{
				PolicyNameLabel: policy.Name,
				NodeNameLabel:   cand.node.Name,
			},
		},
		Spec: spec,
	}
}

// replacementName is deterministic, so re-reconciling the same node and target
// collides with the existing object instead of starting a second rotation.
func replacementName(nodeName, target string) string {
	sum := sha256.Sum256([]byte(nodeName + "|" + target))
	suffix := hex.EncodeToString(sum[:])[:8]

	const maxNamePrefix = 200
	if len(nodeName) > maxNamePrefix {
		nodeName = nodeName[:maxNamePrefix]
	}
	return fmt.Sprintf("%s-%s", nodeName, suffix)
}

// pruneHistory trims completed replacements past the retention limit. Failed
// objects are kept: each one is the record that stops the same node being
// retried automatically, and deleting it would restart a rotation that already
// went wrong.
func (r *NodeUpgradePolicyReconciler) pruneHistory(
	ctx context.Context,
	policy *sksv1alpha1.NodeUpgradePolicy,
	all []sksv1alpha1.NodeReplacement,
) error {
	limit := int(policy.Spec.HistoryLimit)
	if limit <= 0 {
		limit = 10
	}

	completed := make([]sksv1alpha1.NodeReplacement, 0, len(all))
	for _, nr := range all {
		if nr.Status.Phase == sksv1alpha1.PhaseCompleted {
			completed = append(completed, nr)
		}
	}
	if len(completed) <= limit {
		return nil
	}

	slices.SortFunc(completed, func(a, b sksv1alpha1.NodeReplacement) int {
		return b.CreationTimestamp.Compare(a.CreationTimestamp.Time)
	})
	for i := limit; i < len(completed); i++ {
		victim := completed[i]
		if err := r.Delete(ctx, &victim); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete %s: %w", victim.Name, err)
		}
	}
	return nil
}

func (r *NodeUpgradePolicyReconciler) publishMetrics(
	name string,
	st *policyState,
	windows window.Set,
	cluster *exoscale.Cluster,
	target version.Target,
	active int,
) {
	metrics.ManagedNodes.WithLabelValues(name).Set(float64(st.status.TotalNodes))
	metrics.OutdatedNodes.WithLabelValues(name).Set(float64(st.status.OutdatedNodes))
	metrics.ActiveReplacements.WithLabelValues(name).Set(float64(active))
	metrics.InMaintenanceWindow.WithLabelValues(name).Set(metrics.Bool(windows.IsOpen(st.now)))
	metrics.RecordTargetVersion(name, target.Version.String(), cluster.Version, target.Source)
}

func (r *NodeUpgradePolicyReconciler) updateStatus(ctx context.Context, st *policyState) error {
	policy := st.policy
	policy.Status.ObservedGeneration = policy.Generation
	policy.Status.ControlPlaneVersion = st.status.ControlPlaneVersion
	policy.Status.TargetVersion = st.status.TargetVersion
	policy.Status.TargetVersionSource = st.status.TargetVersionSource
	policy.Status.OutdatedNodes = st.status.OutdatedNodes
	policy.Status.TotalNodes = st.status.TotalNodes
	policy.Status.ActiveReplacements = st.status.ActiveReplacements
	policy.Status.NextWindowStart = st.status.NextWindowStart
	policy.Status.LastCompletedAt = st.status.LastCompletedAt

	applied := map[string]bool{}
	for _, cond := range st.conditions {
		meta.SetStatusCondition(&policy.Status.Conditions, cond)
		applied[cond.Type] = true
	}
	// Degraded is sticky otherwise: clear it whenever a pass completed without
	// setting it.
	if !applied[sksv1alpha1.ConditionDegraded] {
		meta.SetStatusCondition(&policy.Status.Conditions, metav1.Condition{
			Type:               sksv1alpha1.ConditionDegraded,
			Status:             metav1.ConditionFalse,
			Reason:             "NoErrors",
			Message:            "no errors observed",
			ObservedGeneration: policy.Generation,
		})
	}

	if err := r.Status().Update(ctx, policy); err != nil {
		if apierrors.IsConflict(err) {
			// Another writer won; the next reconcile recomputes from scratch.
			return nil
		}
		return fmt.Errorf("update policy status: %w", err)
	}
	return nil
}

// SetupWithManager wires the controller, watching Nodes so version drift is
// noticed as soon as a kubelet re-registers.
func (r *NodeUpgradePolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&sksv1alpha1.NodeUpgradePolicy{}).
		Owns(&sksv1alpha1.NodeReplacement{}).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.policiesForNode)).
		Named("nodeupgradepolicy").
		Complete(r)
}

// policiesForNode enqueues every policy whose selector matches a changed node.
func (r *NodeUpgradePolicyReconciler) policiesForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	node, ok := obj.(*corev1.Node)
	if !ok {
		return nil
	}

	var list sksv1alpha1.NodeUpgradePolicyList
	if err := r.List(ctx, &list); err != nil {
		log.FromContext(ctx).Error(err, "listing policies for node event", "node", node.Name)
		return nil
	}

	out := make([]reconcile.Request, 0, len(list.Items))
	for i := range list.Items {
		policy := &list.Items[i]
		if !selectorMatches(policy.Spec.NodeSelector, node.Labels) {
			continue
		}
		out = append(out, reconcile.Request{NamespacedName: types.NamespacedName{Name: policy.Name}})
	}
	return out
}

func selectorMatches(selector, labels map[string]string) bool {
	for k, v := range selector {
		if labels[k] != v {
			return false
		}
	}
	return true
}

func namesOf(items []sksv1alpha1.NodeReplacement) []string {
	if len(items) == 0 {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, nr := range items {
		out = append(out, nr.Name)
	}
	slices.Sort(out)
	return out
}

// lastCompletion is the most recent moment a replacement reached a terminal
// phase. Derived from the owned objects rather than tracked separately, so it
// survives a restart and cannot drift out of step with them.
func lastCompletion(all []sksv1alpha1.NodeReplacement) *metav1.Time {
	var newest *metav1.Time
	for i := range all {
		done := all[i].Status.CompletedAt
		if done == nil {
			continue
		}
		if newest == nil || done.After(newest.Time) {
			newest = done
		}
	}
	return newest
}

// cooldownRemaining reports how long is left of the quiet period, or zero when
// a replacement may start.
func (r *NodeUpgradePolicyReconciler) cooldownRemaining(st *policyState) time.Duration {
	if st.status.LastCompletedAt == nil {
		return 0
	}
	cooldown := st.policy.Spec.Rollout.CooldownOrDefault()
	elapsed := st.now.Sub(st.status.LastCompletedAt.Time)
	if elapsed >= cooldown {
		return 0
	}
	return cooldown - elapsed
}

func requeueDelay(windows window.Set, now time.Time) time.Duration {
	d, ok := windows.RequeueAfter(now)
	if !ok || d > idleRequeue {
		return idleRequeue
	}
	return d
}

func boolStatus(b bool) string {
	if b {
		return "True"
	}
	return "False"
}

func windowReason(open bool) string {
	if open {
		return sksv1alpha1.ReasonInWindow
	}
	return sksv1alpha1.ReasonOutOfWindow
}

func windowMessage(windows window.Set, now time.Time, open bool) string {
	if len(windows) == 0 {
		return "no maintenance windows configured; no replacements will be started"
	}
	if open {
		if end, ok := windows.CurrentEnd(now); ok {
			return fmt.Sprintf("maintenance window is open until %s", end.UTC().Format(time.RFC3339))
		}
		return "maintenance window is open"
	}
	if next, ok := windows.NextStart(now); ok {
		return fmt.Sprintf("next maintenance window opens at %s", next.UTC().Format(time.RFC3339))
	}
	return "no upcoming maintenance window"
}

func progressingReason(active int) string {
	if active > 0 {
		return sksv1alpha1.ReasonReplacing
	}
	return "Idle"
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max]
}
