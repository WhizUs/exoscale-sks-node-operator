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
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
	"github.com/whizus/exoscale-sks-node-operator/internal/exoscale"
	"github.com/whizus/exoscale-sks-node-operator/internal/metrics"
	"github.com/whizus/exoscale-sks-node-operator/internal/nodes"
	"github.com/whizus/exoscale-sks-node-operator/internal/version"
)

const (
	// pollInterval paces the waits on asynchronous cloud and kubelet state.
	pollInterval = 15 * time.Second

	// waitBudget bounds any phase that is merely waiting on external state and
	// has no timeout of its own, so a wedged phase cannot run until the overall
	// replacement timeout.
	waitBudget = 20 * time.Minute
)

// errRetry marks a transient condition: the phase is unchanged and the object
// is requeued rather than failed.
var errRetry = errors.New("retry")

// errUnknownSurgedNode means a rollback could not tell which node the surge
// added, so it deliberately removed nothing.
var errUnknownSurgedNode = errors.New("surged node could not be identified")

// NodeReplacementReconciler executes one node rotation.
//
// Every phase is idempotent and the phase itself lives in the object's status,
// so a restart mid-rotation resumes where it left off — which matters because
// the operator may be draining the node it is running on.
type NodeReplacementReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder events.EventRecorder
	Exoscale exoscale.Provider
	Drainer  *nodes.Drainer

	// MaxConcurrentReconciles bounds parallel rotations. Draining blocks its
	// worker for up to the drain timeout, so this must exceed the highest
	// maxUnavailable in use.
	MaxConcurrentReconciles int

	// Now is overridable in tests.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodereplacements,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodereplacements/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=sks.whizus.com,resources=nodereplacements/finalizers,verbs=update
// +kubebuilder:rbac:groups=karpenter.sh,resources=nodeclaims,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups=core,resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups=apps,resources=daemonsets;replicasets;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=core,resources=replicationcontrollers,verbs=get;list;watch

// Reconcile advances one replacement by at most one phase.
func (r *NodeReplacementReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var nr sksv1alpha1.NodeReplacement
	if err := r.Get(ctx, req.NamespacedName, &nr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if nr.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}

	var policy sksv1alpha1.NodeUpgradePolicy
	if err := r.Get(ctx, types.NamespacedName{Name: nr.Spec.PolicyName}, &policy); err != nil {
		if apierrors.IsNotFound(err) {
			// The owner is gone; garbage collection will remove this object.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	before := nr.Status.DeepCopy()
	result, err := r.advance(ctx, &nr, &policy)

	if statusErr := r.persist(ctx, &nr, before); statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	return result, err
}

// advance runs the handler for the current phase.
func (r *NodeReplacementReconciler) advance(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("node", nr.Spec.NodeName, "phase", nr.Status.Phase)

	if nr.Status.StartedAt == nil {
		now := r.timestamp()
		nr.Status.StartedAt = &now
		nr.Status.PhaseStartedAt = &now
		nr.Status.Phase = sksv1alpha1.PhasePending
	}

	if r.expired(nr, policy) {
		r.fail(nr, "ReplacementTimeout", fmt.Sprintf(
			"replacement exceeded %s", policy.Spec.Rollout.ReplacementTimeoutOrDefault()))
		return r.finishFailed(ctx, nr, policy)
	}

	handler := r.handlerFor(nr.Status.Phase)
	if handler == nil {
		r.fail(nr, "UnknownPhase", fmt.Sprintf("no handler for phase %q", nr.Status.Phase))
		return ctrl.Result{}, nil
	}

	next, err := handler(ctx, nr, policy)
	switch {
	case errors.Is(err, errRetry):
		// Waiting is paced by time alone. Counting attempts here was a bug: each
		// wait wrote status, the write re-triggered this controller through its
		// own watch, and the counter burned through in seconds — long before the
		// real timeout could apply.
		if elapsed := r.phaseElapsed(nr); elapsed > waitBudget {
			r.fail(nr, "PhaseTimeout", fmt.Sprintf(
				"phase %s did not settle within %s: %s", nr.Status.Phase, waitBudget, nr.Status.Message))
			return r.finishFailed(ctx, nr, policy)
		}
		logger.V(1).Info("waiting", "message", nr.Status.Message)
		return ctrl.Result{RequeueAfter: pollInterval}, nil

	case err != nil:
		// Returning the error lets controller-runtime apply its backoff. The
		// message is recorded, but only written when it actually changed, so a
		// repeating error does not re-trigger this controller through its own
		// watch and defeat that backoff.
		nr.Status.Message = err.Error()
		if elapsed := r.phaseElapsed(nr); elapsed > waitBudget {
			r.fail(nr, "PhaseFailed", fmt.Sprintf("phase %s kept failing for %s: %v",
				nr.Status.Phase, waitBudget, err))
			return r.finishFailed(ctx, nr, policy)
		}
		return ctrl.Result{}, err

	case next == sksv1alpha1.PhaseFailed:
		logger.Info("phase transition", "from", nr.Status.Phase, "to", next)
		return r.finishFailed(ctx, nr, policy)

	case next != nr.Status.Phase:
		logger.Info("phase transition", "from", nr.Status.Phase, "to", next)
		r.transition(nr, next)
		if next.IsTerminal() {
			r.recordTerminal(nr, policy)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{Requeue: true}, nil

	default:
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
}

type phaseHandler func(
	context.Context,
	*sksv1alpha1.NodeReplacement,
	*sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error)

func (r *NodeReplacementReconciler) handlerFor(phase sksv1alpha1.ReplacementPhase) phaseHandler {
	switch phase {
	case sksv1alpha1.PhasePending:
		return r.handlePending
	case sksv1alpha1.PhaseScalingUp:
		return r.handleScalingUp
	case sksv1alpha1.PhaseWaitingForNode:
		return r.handleWaitingForNode
	case sksv1alpha1.PhaseCordoning:
		return r.handleCordoning
	case sksv1alpha1.PhaseDraining:
		return r.handleDraining
	case sksv1alpha1.PhaseEvicting:
		return r.handleEvicting
	case sksv1alpha1.PhaseDeleting:
		return r.handleDeleting
	default:
		return nil
	}
}

// handlePending re-validates every precondition before anything is mutated.
func (r *NodeReplacementReconciler) handlePending(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil {
		return nr.Status.Phase, err
	}
	if node == nil {
		nr.Status.Message = "node no longer exists; nothing to replace"
		return sksv1alpha1.PhaseCompleted, nil
	}
	if node.DeletionTimestamp != nil {
		nr.Status.Message = "node is already being removed by another controller"
		return sksv1alpha1.PhaseCompleted, nil
	}
	if node.Annotations[sksv1alpha1.SkipAnnotation] == nodes.TrueValue {
		return nr.Status.Phase, fmt.Errorf("node opted out via %s", sksv1alpha1.SkipAnnotation)
	}

	if nr.Spec.PoolType == sksv1alpha1.PoolTypeKarpenterManaged {
		nr.Status.PoolName = nr.Spec.KarpenterNodePool
		return sksv1alpha1.PhaseCordoning, nil
	}

	pool, err := r.nodepool(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}
	if !pool.IsRunning() {
		nr.Status.Message = fmt.Sprintf("nodepool %s is %q, waiting for running", pool.Name, pool.State)
		return nr.Status.Phase, errRetry
	}

	return sksv1alpha1.PhaseScalingUp, nil
}

// handleScalingUp grows the pool by one and records the size to restore.
func (r *NodeReplacementReconciler) handleScalingUp(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	pool, err := r.nodepool(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}
	if !pool.IsRunning() {
		nr.Status.Message = fmt.Sprintf("nodepool %s is %q, waiting for running", pool.Name, pool.State)
		return nr.Status.Phase, errRetry
	}

	// The size is read immediately before scaling because ScaleSKSNodepool takes
	// an absolute size and the cluster-autoscaler owns the same field.
	if nr.Status.PoolSizeBefore == nil {
		size := pool.Size
		nr.Status.PoolSizeBefore = &size
	}
	target := *nr.Status.PoolSizeBefore + 1
	if pool.Size >= target {
		nr.Status.Message = fmt.Sprintf("nodepool %s already at size %d", pool.Name, pool.Size)
		return sksv1alpha1.PhaseWaitingForNode, nil
	}

	exoClient, err := r.client(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}
	if err := exoClient.ScaleNodepool(ctx, nr.Spec.SKSNodepoolID, target); err != nil {
		return nr.Status.Phase, err
	}

	r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "NodepoolScaledUp", "Scaling",
		"scaled nodepool %s from %d to %d", pool.Name, *nr.Status.PoolSizeBefore, target)
	return sksv1alpha1.PhaseWaitingForNode, nil
}

// handleWaitingForNode waits for the surged capacity to arrive, then checks that
// the new node actually reached the target.
func (r *NodeReplacementReconciler) handleWaitingForNode(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	if r.phaseElapsed(nr) > policy.Spec.Rollout.NodeReadyTimeoutOrDefault() {
		r.fail(nr, "NodeReadyTimeout", fmt.Sprintf(
			"no replacement node became Ready within %s",
			policy.Spec.Rollout.NodeReadyTimeoutOrDefault()))
		return sksv1alpha1.PhaseFailed, nil
	}

	pool, err := r.nodepool(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}

	ready, newest, err := r.poolReadyNodes(ctx, nr)
	if err != nil {
		return nr.Status.Phase, err
	}

	want := int64(1)
	if nr.Status.PoolSizeBefore != nil {
		want = *nr.Status.PoolSizeBefore + 1
	}
	if int64(ready) < want || newest == nil {
		nr.Status.Message = fmt.Sprintf(
			"nodepool %s has %d/%d Ready node(s)", pool.Name, ready, want)
		return nr.Status.Phase, errRetry
	}

	info := nodes.Classify(newest)
	nr.Status.ReplacementNodeName = newest.Name
	nr.Status.ReplacementNodeVersion = info.KubeletVersion

	// The surge doubles as the probe for whether the target is obtainable at
	// all. If the fresh node came up below it, the old node is left untouched
	// and the surge is undone, so the cost of an unobtainable target is one
	// node created and destroyed rather than the whole fleet rotated.
	targetVersion, err := version.Parse(nr.Spec.TargetVersion)
	if err != nil {
		return nr.Status.Phase, err
	}
	if version.IsOutdated(info.KubeletVersion, targetVersion) {
		msg := fmt.Sprintf(
			"replacement node %s came up at %s, below target %s; leaving %s in place",
			newest.Name, info.KubeletVersion, nr.Spec.TargetVersion, nr.Spec.NodeName)
		r.Recorder.Eventf(nr, nil, corev1.EventTypeWarning,
			sksv1alpha1.ReasonTargetUnreachable, "Verifying", "%s", msg)
		r.fail(nr, sksv1alpha1.ReasonTargetUnreachable, msg)
		return sksv1alpha1.PhaseFailed, nil
	}

	r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "ReplacementNodeReady", "Waiting",
		"node %s is Ready at %s", newest.Name, info.KubeletVersion)
	return sksv1alpha1.PhaseCordoning, nil
}

// handleCordoning marks the outdated node unschedulable so nothing new lands on
// it while it is being emptied.
func (r *NodeReplacementReconciler) handleCordoning(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	_ *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil {
		return nr.Status.Phase, err
	}
	if node == nil {
		return sksv1alpha1.PhaseCompleted, nil
	}

	if !node.Spec.Unschedulable {
		if err := r.Drainer.Cordon(ctx, node); err != nil {
			return nr.Status.Phase, err
		}
		r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "NodeCordoned", "Cordoning",
			"cordoned %s", node.Name)
	}
	nr.Status.Cordoned = true
	return sksv1alpha1.PhaseDraining, nil
}

// handleDraining evicts the node's pods, honouring PodDisruptionBudgets.
func (r *NodeReplacementReconciler) handleDraining(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	next := sksv1alpha1.PhaseEvicting
	if nr.Spec.PoolType == sksv1alpha1.PoolTypeKarpenterManaged {
		next = sksv1alpha1.PhaseDeleting
	}

	if !policy.Spec.Drain.IsEnabled() {
		nr.Status.Message = "draining disabled by policy; node only cordoned"
		return next, nil
	}

	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil {
		return nr.Status.Phase, err
	}
	if node == nil {
		return sksv1alpha1.PhaseCompleted, nil
	}

	// Bounded so a PodDisruptionBudget that can never be satisfied cannot pin
	// this worker, and so shutdown is not blocked by an in-flight drain.
	drainCtx, cancel := context.WithTimeout(ctx, policy.Spec.Drain.DrainTimeout())
	defer cancel()

	if err := r.Drainer.Drain(drainCtx, node, policy.Spec.Drain); err != nil {
		return nr.Status.Phase, fmt.Errorf("drain %s: %w", node.Name, err)
	}

	r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "NodeDrained", "Draining",
		"drained %s", node.Name)
	return next, nil
}

// handleEvicting removes the instance from its SKS nodepool. Eviction also
// shrinks the pool by one, which restores the size the surge grew.
func (r *NodeReplacementReconciler) handleEvicting(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil {
		return nr.Status.Phase, err
	}
	if node == nil {
		nr.Status.Message = completionMessage(nr)
		return sksv1alpha1.PhaseCompleted, nil
	}

	if nr.Status.ActionIssued {
		nr.Status.Message = fmt.Sprintf("waiting for node %s to be removed", nr.Spec.NodeName)
		return nr.Status.Phase, errRetry
	}

	pool, err := r.nodepool(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}
	if !pool.IsRunning() {
		nr.Status.Message = fmt.Sprintf("nodepool %s is %q, waiting for running", pool.Name, pool.State)
		return nr.Status.Phase, errRetry
	}
	if nr.Spec.InstanceID == "" {
		return nr.Status.Phase, fmt.Errorf("node %s has no Exoscale instance ID", nr.Spec.NodeName)
	}

	exoClient, err := r.client(ctx, nr, policy)
	if err != nil {
		return nr.Status.Phase, err
	}
	if err := exoClient.EvictNodepoolMember(ctx, nr.Spec.SKSNodepoolID, nr.Spec.InstanceID); err != nil {
		return nr.Status.Phase, err
	}

	nr.Status.ActionIssued = true
	r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "InstanceEvicted", "Evicting",
		"evicted instance %s from nodepool %s", nr.Spec.InstanceID, pool.Name)

	nr.Status.Message = fmt.Sprintf("waiting for node %s to be removed", nr.Spec.NodeName)
	return nr.Status.Phase, errRetry
}

// handleDeleting removes the Karpenter NodeClaim. Karpenter then terminates the
// instance and reprovisions on demand.
func (r *NodeReplacementReconciler) handleDeleting(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	_ *sksv1alpha1.NodeUpgradePolicy,
) (sksv1alpha1.ReplacementPhase, error) {
	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil {
		return nr.Status.Phase, err
	}
	if node == nil {
		nr.Status.Message = completionMessage(nr)
		return sksv1alpha1.PhaseCompleted, nil
	}

	if nr.Status.ActionIssued {
		nr.Status.Message = fmt.Sprintf("waiting for node %s to be removed", nr.Spec.NodeName)
		return nr.Status.Phase, errRetry
	}

	claimName := nr.Spec.NodeClaimName
	if claimName == "" {
		claimName, err = r.findNodeClaim(ctx, nr.Spec.InstanceID)
		if err != nil {
			return nr.Status.Phase, err
		}
	}
	if claimName == "" {
		return nr.Status.Phase, fmt.Errorf("no NodeClaim found for node %s", nr.Spec.NodeName)
	}

	claim := newNodeClaim()
	claim.SetName(claimName)
	if err := r.Delete(ctx, claim); err != nil && !apierrors.IsNotFound(err) {
		return nr.Status.Phase, fmt.Errorf("delete nodeclaim %s: %w", claimName, err)
	}

	nr.Status.ActionIssued = true
	r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "NodeClaimDeleted", "Deleting",
		"deleted NodeClaim %s; Karpenter will terminate and reprovision", claimName)

	nr.Status.Message = fmt.Sprintf("waiting for node %s to be removed", nr.Spec.NodeName)
	return nr.Status.Phase, errRetry
}

// rollback undoes whatever a failed replacement had already changed, so a
// failure never leaves a surged pool or a cordoned node behind.
func (r *NodeReplacementReconciler) rollback(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) {
	logger := log.FromContext(ctx)

	// The instance was already destroyed, so there is nothing to undo and the
	// pool is back at its original size by way of the eviction itself.
	if !nr.Status.ActionIssued && nr.Status.PoolSizeBefore != nil {
		switch err := r.undoSurge(ctx, nr, policy); {
		case errors.Is(err, errUnknownSurgedNode):
			logger.Info("leaving the nodepool one node larger; the surged node could not be identified",
				"nodepool", nr.Spec.SKSNodepoolID)
			r.Recorder.Eventf(nr, nil, corev1.EventTypeWarning, "SurgeNotReclaimed", "RollingBack",
				"nodepool %s is left one node above %d: the node added by this replacement could not "+
					"be identified, and removing an unidentified one risks destroying a node that is "+
					"still carrying workload. The cluster-autoscaler will reclaim it.",
				nr.Spec.SKSNodepoolID, *nr.Status.PoolSizeBefore)
		case err != nil:
			logger.Error(err, "undoing surge", "nodepool", nr.Spec.SKSNodepoolID)
			r.Recorder.Eventf(nr, nil, corev1.EventTypeWarning, "RollbackFailed", "RollingBack",
				"could not restore nodepool %s to size %d: %v",
				nr.Status.PoolName, *nr.Status.PoolSizeBefore, err)
		}
	}

	if nr.Status.Cordoned {
		if err := r.uncordon(ctx, nr); err != nil {
			logger.Error(err, "uncordoning node", "node", nr.Spec.NodeName)
			r.Recorder.Eventf(nr, nil, corev1.EventTypeWarning, "RollbackFailed", "RollingBack",
				"could not uncordon %s: %v", nr.Spec.NodeName, err)
		} else {
			nr.Status.Cordoned = false
			r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "NodeUncordoned", "RollingBack",
				"uncordoned %s after a failed replacement", nr.Spec.NodeName)
		}
	}

}

// finishFailed marks the replacement failed, undoes whatever it had already
// changed, and records the outcome. Every failure path goes through here, so a
// failed replacement can never leave a surged pool or a cordoned node behind.
func (r *NodeReplacementReconciler) finishFailed(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (ctrl.Result, error) {
	r.transition(nr, sksv1alpha1.PhaseFailed)
	r.rollback(ctx, nr, policy)
	r.recordTerminal(nr, policy)
	return ctrl.Result{}, nil
}

func (r *NodeReplacementReconciler) undoSurge(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) error {
	exoClient, err := r.client(ctx, nr, policy)
	if err != nil {
		return err
	}
	pool, err := exoClient.GetNodepool(ctx, nr.Spec.SKSNodepoolID)
	if err != nil {
		return err
	}
	if pool.Size <= *nr.Status.PoolSizeBefore {
		return nil
	}

	// Only ever remove a node this replacement can positively identify as the
	// one it added. Scaling the pool down instead would let Exoscale choose the
	// victim, and it can pick a node that is still carrying workload and was
	// never drained — which is strictly worse than leaving the pool one node
	// larger for the cluster-autoscaler to reclaim gracefully.
	name := nr.Status.ReplacementNodeName
	if name == "" {
		return errUnknownSurgedNode
	}
	node, err := r.getNode(ctx, name)
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}
	info := nodes.Classify(node)
	if info.InstanceID == "" {
		return errUnknownSurgedNode
	}

	// It is a healthy node the scheduler may already have placed pods on, so it
	// is drained like any other before being destroyed.
	if err := r.Drainer.Cordon(ctx, node); err != nil {
		return err
	}
	if policy.Spec.Drain.IsEnabled() {
		drainCtx, cancel := context.WithTimeout(ctx, policy.Spec.Drain.DrainTimeout())
		defer cancel()
		if err := r.Drainer.Drain(drainCtx, node, policy.Spec.Drain); err != nil {
			return err
		}
	}
	return exoClient.EvictNodepoolMember(ctx, nr.Spec.SKSNodepoolID, info.InstanceID)
}

func (r *NodeReplacementReconciler) uncordon(ctx context.Context, nr *sksv1alpha1.NodeReplacement) error {
	node, err := r.getNode(ctx, nr.Spec.NodeName)
	if err != nil || node == nil {
		return err
	}
	return r.Drainer.Uncordon(ctx, node)
}

// poolReadyNodes counts Ready nodes in the replacement's pool and returns the
// newest one that is not the node being replaced.
func (r *NodeReplacementReconciler) poolReadyNodes(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
) (int, *corev1.Node, error) {
	var list corev1.NodeList
	if err := r.List(ctx, &list, client.MatchingLabels{
		nodes.SKSNodepoolIDLabel: nr.Spec.SKSNodepoolID,
	}); err != nil {
		return 0, nil, fmt.Errorf("list nodepool nodes: %w", err)
	}

	ready := 0
	var newest *corev1.Node
	for i := range list.Items {
		node := &list.Items[i]
		if !nodes.IsReady(node) || node.DeletionTimestamp != nil {
			continue
		}
		ready++
		if node.Name == nr.Spec.NodeName {
			continue
		}
		if newest == nil || node.CreationTimestamp.After(newest.CreationTimestamp.Time) {
			newest = node
		}
	}
	return ready, newest, nil
}

func (r *NodeReplacementReconciler) findNodeClaim(ctx context.Context, instanceID string) (string, error) {
	if instanceID == "" {
		return "", nil
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group: nodes.NodeClaimGroup, Version: nodes.NodeClaimVersion, Kind: nodes.NodeClaimKind + "List",
	})
	if err := r.List(ctx, list); err != nil {
		return "", fmt.Errorf("list nodeclaims: %w", err)
	}

	want := nodes.ProviderIDPrefix + instanceID
	for i := range list.Items {
		providerID, _, _ := unstructured.NestedString(list.Items[i].Object, "status", "providerID")
		if providerID == want {
			return list.Items[i].GetName(), nil
		}
	}
	return "", nil
}

func newNodeClaim() *unstructured.Unstructured {
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(schema.GroupVersionKind{
		Group: nodes.NodeClaimGroup, Version: nodes.NodeClaimVersion, Kind: nodes.NodeClaimKind,
	})
	return claim
}

func (r *NodeReplacementReconciler) getNode(ctx context.Context, name string) (*corev1.Node, error) {
	var node corev1.Node
	if err := r.Get(ctx, types.NamespacedName{Name: name}, &node); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get node %s: %w", name, err)
	}
	return &node, nil
}

func (r *NodeReplacementReconciler) client(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (exoscale.Client, error) {
	return r.Exoscale.Get(ctx, exoscale.Options{
		Zone:           policy.Spec.Exoscale.Zone,
		ClusterID:      policy.Spec.Exoscale.ClusterID,
		NodepoolIDHint: nr.Spec.SKSNodepoolID,
	})
}

func (r *NodeReplacementReconciler) nodepool(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) (*exoscale.Nodepool, error) {
	exoClient, err := r.client(ctx, nr, policy)
	if err != nil {
		return nil, err
	}
	pool, err := exoClient.GetNodepool(ctx, nr.Spec.SKSNodepoolID)
	if err != nil {
		return nil, err
	}
	metrics.RecordNodepool(pool.Name, pool.State, pool.Size)
	nr.Status.PoolName = pool.Name
	return pool, nil
}

// completionMessage states the outcome, rather than leaving behind whatever the
// preceding phase was waiting on.
func completionMessage(nr *sksv1alpha1.NodeReplacement) string {
	if nr.Status.ReplacementNodeName != "" {
		return fmt.Sprintf("replaced %s with %s running %s",
			nr.Spec.NodeName, nr.Status.ReplacementNodeName, nr.Status.ReplacementNodeVersion)
	}
	return fmt.Sprintf("%s was removed; its replacement is provisioned on demand", nr.Spec.NodeName)
}

func (r *NodeReplacementReconciler) transition(nr *sksv1alpha1.NodeReplacement, next sksv1alpha1.ReplacementPhase) {
	now := r.timestamp()
	nr.Status.Phase = next
	nr.Status.PhaseStartedAt = &now
	nr.Status.ActionIssued = false

	if next.IsTerminal() {
		nr.Status.CompletedAt = &now
	}
	// Only success is written here; failures already carry a specific reason
	// recorded by fail(), which must not be flattened to a generic one.
	if next == sksv1alpha1.PhaseCompleted {
		meta.SetStatusCondition(&nr.Status.Conditions, metav1.Condition{
			Type:    sksv1alpha1.ConditionSucceeded,
			Status:  metav1.ConditionTrue,
			Reason:  "Replaced",
			Message: nr.Status.Message,
		})
	}
}

// fail records why a replacement is being abandoned. It deliberately leaves the
// phase alone: finishFailed owns the transition, and setting the phase here
// would make the transition look like a no-op and skip the rollback.
func (r *NodeReplacementReconciler) fail(nr *sksv1alpha1.NodeReplacement, reason, msg string) {
	nr.Status.Message = msg
	meta.SetStatusCondition(&nr.Status.Conditions, metav1.Condition{
		Type:    sksv1alpha1.ConditionSucceeded,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: truncate(msg, 32*1024),
	})
	r.Recorder.Eventf(nr, nil, corev1.EventTypeWarning, reason, "Failing", "%s", msg)
}

func (r *NodeReplacementReconciler) recordTerminal(
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) {
	result := "succeeded"
	if nr.Status.Phase == sksv1alpha1.PhaseFailed {
		result = "failed"
	}
	poolType := string(nr.Spec.PoolType)

	metrics.ReplacementsTotal.WithLabelValues(policy.Name, poolType, result).Inc()
	if nr.Status.StartedAt != nil && nr.Status.CompletedAt != nil {
		elapsed := nr.Status.CompletedAt.Sub(nr.Status.StartedAt.Time)
		metrics.ReplacementDuration.WithLabelValues(policy.Name, poolType, result).Observe(elapsed.Seconds())
	}

	if result == "succeeded" {
		r.Recorder.Eventf(nr, nil, corev1.EventTypeNormal, "ReplacementCompleted", "Completing",
			"replaced %s with %s", nr.Spec.NodeName, nr.Status.ReplacementNodeName)
	}
}

func (r *NodeReplacementReconciler) persist(
	ctx context.Context,
	nr *sksv1alpha1.NodeReplacement,
	before *sksv1alpha1.NodeReplacementStatus,
) error {
	if equalStatus(before, &nr.Status) {
		return nil
	}
	if err := r.Status().Update(ctx, nr); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("update replacement status: %w", err)
	}
	return nil
}

func equalStatus(a, b *sksv1alpha1.NodeReplacementStatus) bool {
	return a.Phase == b.Phase &&
		a.Message == b.Message &&
		a.PoolName == b.PoolName &&
		a.ActionIssued == b.ActionIssued &&
		a.Cordoned == b.Cordoned &&
		a.ReplacementNodeName == b.ReplacementNodeName &&
		a.ReplacementNodeVersion == b.ReplacementNodeVersion &&
		equalInt64Ptr(a.PoolSizeBefore, b.PoolSizeBefore) &&
		a.StartedAt.Equal(b.StartedAt) &&
		a.CompletedAt.Equal(b.CompletedAt) &&
		len(a.Conditions) == len(b.Conditions)
}

func equalInt64Ptr(a, b *int64) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

func (r *NodeReplacementReconciler) expired(
	nr *sksv1alpha1.NodeReplacement,
	policy *sksv1alpha1.NodeUpgradePolicy,
) bool {
	if nr.Status.StartedAt == nil {
		return false
	}
	return r.now().Sub(nr.Status.StartedAt.Time) > policy.Spec.Rollout.ReplacementTimeoutOrDefault()
}

func (r *NodeReplacementReconciler) phaseElapsed(nr *sksv1alpha1.NodeReplacement) time.Duration {
	if nr.Status.PhaseStartedAt == nil {
		return 0
	}
	return r.now().Sub(nr.Status.PhaseStartedAt.Time)
}

func (r *NodeReplacementReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *NodeReplacementReconciler) timestamp() metav1.Time {
	return metav1.NewTime(r.now())
}

// SetupWithManager wires the controller.
func (r *NodeReplacementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	concurrency := r.MaxConcurrentReconciles
	if concurrency <= 0 {
		concurrency = 4
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&sksv1alpha1.NodeReplacement{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrency}).
		Named("nodereplacement").
		Complete(r)
}
