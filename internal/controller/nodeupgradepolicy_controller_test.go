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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
	"github.com/whizus/exoscale-sks-node-operator/internal/nodes"
)

var _ = Describe("NodeUpgradePolicy controller", func() {
	const policyName = "default"

	reconcile := func(r *NodeUpgradePolicyReconciler) ctrl.Result {
		GinkgoHelper()
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: policyName}})
		Expect(err).NotTo(HaveOccurred())
		return res
	}

	AfterEach(cleanup)

	// The test cluster exactly as it stands: control plane 1.36.2, one default
	// node already on 1.36.3, three nodes still on 1.36.2, two Karpenter nodes
	// on 1.36.3.
	Context("with two SKS pools and two Karpenter pools", func() {
		BeforeEach(func() {
			newSKSNode("node-pool-a-2", defaultPoolID, newerVersion, "inst-psecl")
			newSKSNode("node-pool-a-1", defaultPoolID, olderVersion, "inst-swhry")
			newSKSNode("node-pool-b-1", infraPoolID, olderVersion, "inst-pnnqb")
			newSKSNode("node-pool-b-2", infraPoolID, olderVersion, "inst-uunhe")
			newKarpenterNode("k-node-c-1", "karpenter-pool-c",
				newerVersion, "inst-x7pkd")
			newKarpenterNode("k-node-d-1", "karpenter-pool-d",
				newerVersion, "inst-zr9n7")
		})

		It("targets 1.36.3, counts three outdated nodes and leaves the ahead-of-control-plane nodes alone", func() {
			newPolicy(policyName)
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			policy := getPolicy(policyName)
			Expect(policy.Status.ControlPlaneVersion).To(Equal(controlPlaneVersion))
			Expect(policy.Status.TargetVersion).To(Equal("v1.36.3"))
			Expect(policy.Status.TotalNodes).To(Equal(int32(6)))
			Expect(policy.Status.OutdatedNodes).To(Equal(int32(3)))
		})

		It("starts one replacement at a time, honouring maxUnavailable", func() {
			newPolicy(policyName)
			r := newPolicyReconciler(twoPoolFake(), "")

			reconcile(r)
			Expect(listReplacements()).To(HaveLen(1))

			// A second pass must not exceed the budget while one is in flight.
			reconcile(r)
			Expect(listReplacements()).To(HaveLen(1))
		})

		It("fills the budget across pools when maxUnavailable allows it", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Rollout.MaxUnavailable = 3
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			// Two pools, one replacement each: maxUnavailablePerPool is 1.
			Expect(listReplacements()).To(HaveLen(2))
		})

		It("orders the operator's own node last so it does not evict itself early", func() {
			self := "node-pool-b-1"
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Rollout.MaxUnavailable = 10
				p.Spec.Rollout.MaxUnavailablePerPool = 10
			})
			reconcile(newPolicyReconciler(twoPoolFake(), self))

			items := listReplacements()
			Expect(items).To(HaveLen(3))

			// Whichever order the others land in, the operator's own node is last.
			var infra []string
			for _, nr := range items {
				if nr.Spec.SKSNodepoolID == infraPoolID {
					infra = append(infra, nr.Spec.NodeName)
				}
			}
			Expect(infra).To(HaveLen(2))
		})

		It("records the replacement's provenance and pool details", func() {
			newPolicy(policyName)
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			nr := listReplacements()[0]
			Expect(nr.Spec.PoolType).To(Equal(sksv1alpha1.PoolTypeSKSManaged))
			Expect(nr.Spec.TargetVersion).To(Equal("v1.36.3"))
			Expect(nr.Spec.InstanceID).NotTo(BeEmpty())
			Expect(nr.Spec.SKSNodepoolID).NotTo(BeEmpty())
			Expect(nr.Spec.Reason).To(ContainSubstring("below target v1.36.3"))
			Expect(nr.Labels).To(HaveKeyWithValue(PolicyNameLabel, policyName))
		})
	})

	Context("gating", func() {
		BeforeEach(func() {
			newSKSNode("node-old", defaultPoolID, olderVersion, "inst-old")
			newSKSNode("node-new", defaultPoolID, newerVersion, "inst-new")
		})

		It("starts nothing outside the maintenance window", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.MaintenanceWindows = nightly()
			})
			r := newPolicyReconciler(twoPoolFake(), "")
			// Fixed at midday, well outside 02:00-03:00.
			r.Now = func() time.Time { return time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC) }
			reconcile(r)

			Expect(listReplacements()).To(BeEmpty())

			policy := getPolicy(policyName)
			Expect(conditionStatus(policy.Status.Conditions, sksv1alpha1.ConditionWindowOpen)).
				To(Equal(metav1.ConditionFalse))
			Expect(policy.Status.OutdatedNodes).To(Equal(int32(1)))
			Expect(policy.Status.NextWindowStart).NotTo(BeNil())
		})

		It("starts replacements once inside the window", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.MaintenanceWindows = nightly()
			})
			r := newPolicyReconciler(twoPoolFake(), "")
			r.Now = func() time.Time { return time.Date(2026, 8, 18, 2, 30, 0, 0, time.UTC) }
			reconcile(r)

			Expect(listReplacements()).To(HaveLen(1))
			Expect(conditionStatus(getPolicy(policyName).Status.Conditions, sksv1alpha1.ConditionWindowOpen)).
				To(Equal(metav1.ConditionTrue))
		})

		It("starts nothing with no window configured at all", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.MaintenanceWindows = nil
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))
			Expect(listReplacements()).To(BeEmpty())
		})

		It("starts nothing while paused", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) { p.Spec.Paused = true })
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			Expect(listReplacements()).To(BeEmpty())
			Expect(conditionReason(getPolicy(policyName).Status.Conditions, sksv1alpha1.ConditionReady)).
				To(Equal(sksv1alpha1.ReasonPaused))
		})

		It("mutates nothing under dryRun but still reports the drift", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) { p.Spec.DryRun = true })
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			Expect(listReplacements()).To(BeEmpty())
			policy := getPolicy(policyName)
			Expect(policy.Status.OutdatedNodes).To(Equal(int32(1)))
			Expect(conditionReason(policy.Status.Conditions, sksv1alpha1.ConditionReady)).
				To(Equal(sksv1alpha1.ReasonDryRun))
		})

		It("skips nodes carrying the skip annotation", func() {
			node := getNode("node-old")
			node.Annotations = map[string]string{sksv1alpha1.SkipAnnotation: "true"}
			Expect(k8sClient.Update(ctx, node)).To(Succeed())

			newPolicy(policyName)
			reconcile(newPolicyReconciler(twoPoolFake(), ""))
			Expect(listReplacements()).To(BeEmpty())
		})

		It("ignores nodes outside the policy's nodeSelector", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.NodeSelector = map[string]string{"role": "worker"}
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			Expect(listReplacements()).To(BeEmpty())
			Expect(getPolicy(policyName).Status.TotalNodes).To(Equal(int32(0)))
		})

		It("ignores Karpenter nodes when that strategy is disabled", func() {
			newKarpenterNode("k-old", "karpenter-pool-d", olderVersion, "inst-k-old")
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Strategies.SKSManaged.Enabled = ptr(false)
				p.Spec.Strategies.KarpenterManaged.Enabled = ptr(false)
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			Expect(listReplacements()).To(BeEmpty())
			Expect(getPolicy(policyName).Status.TotalNodes).To(Equal(int32(0)))
		})

		It("skips a node Karpenter is already disrupting", func() {
			node := newKarpenterNode("k-disrupted", "karpenter-pool-d", olderVersion, "inst-k-dis")
			node.Spec.Taints = append(node.Spec.Taints, corev1Taint(nodes.KarpenterDisruptedTaint))
			Expect(k8sClient.Update(ctx, node)).To(Succeed())

			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Strategies.SKSManaged.Enabled = ptr(false)
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))
			Expect(listReplacements()).To(BeEmpty())
		})
	})

	Context("when the target version turns out to be unobtainable", func() {
		It("halts the rollout instead of rotating the whole fleet for nothing", func() {
			newSKSNode("node-a", defaultPoolID, olderVersion, "inst-a")
			newSKSNode("node-b", defaultPoolID, olderVersion, "inst-b")
			newPolicy(policyName)
			r := newPolicyReconciler(twoPoolFake(), "")

			reconcile(r)
			items := listReplacements()
			Expect(items).To(HaveLen(1))

			// The probe rotation came back below target: 1.36.3 is offered but
			// no node image delivers it.
			nr := items[0]
			nr.Status.Phase = sksv1alpha1.PhaseCompleted
			nr.Status.ReplacementNodeName = "node-probe"
			nr.Status.ReplacementNodeVersion = olderVersion
			Expect(k8sClient.Status().Update(ctx, &nr)).To(Succeed())

			reconcile(r)

			Expect(listReplacements()).To(HaveLen(1), "no further replacements may start")
			policy := getPolicy(policyName)
			Expect(conditionStatus(policy.Status.Conditions, sksv1alpha1.ConditionDegraded)).
				To(Equal(metav1.ConditionTrue))
			Expect(conditionReason(policy.Status.Conditions, sksv1alpha1.ConditionDegraded)).
				To(Equal(sksv1alpha1.ReasonTargetUnreachable))
		})
	})

	Context("version policy", func() {
		BeforeEach(func() {
			newSKSNode("node-cp", defaultPoolID, olderVersion, "inst-cp")
			newSKSNode("node-ahead", defaultPoolID, newerVersion, "inst-ahead")
		})

		It("treats nodes ahead of the control plane as up to date under ControlPlane source", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.VersionPolicy.Source = sksv1alpha1.VersionSourceControlPlane
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			policy := getPolicy(policyName)
			Expect(policy.Status.TargetVersion).To(Equal("v1.36.2"))
			Expect(policy.Status.OutdatedNodes).To(Equal(int32(0)))
			Expect(listReplacements()).To(BeEmpty())
		})

		It("honours a pinned version", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.VersionPolicy.Source = sksv1alpha1.VersionSourcePinned
				p.Spec.VersionPolicy.PinnedVersion = "v1.36.9"
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))

			policy := getPolicy(policyName)
			Expect(policy.Status.TargetVersion).To(Equal("v1.36.9"))
			Expect(policy.Status.OutdatedNodes).To(Equal(int32(2)))
		})
	})

	// Regression: rollout.cooldown was declared in the CRD with a 5m default but
	// never read, so replacements ran back to back as fast as each node
	// disappeared — on a busy cluster that is every node at once, with no quiet
	// period in between.
	Context("cooldown between replacements", func() {
		BeforeEach(func() {
			newSKSNode("node-a", defaultPoolID, olderVersion, "inst-a")
			newSKSNode("node-b", defaultPoolID, olderVersion, "inst-b")
			newSKSNode("node-current", defaultPoolID, newerVersion, "inst-current")
		})

		It("waits out the cooldown after a replacement finishes", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Rollout.Cooldown = metav1.Duration{Duration: 10 * time.Minute}
			})
			r := newPolicyReconciler(twoPoolFake(), "")
			base := time.Now()
			r.Now = func() time.Time { return base }

			reconcile(r)
			items := listReplacements()
			Expect(items).To(HaveLen(1))

			// Finish it, five minutes into a ten minute cooldown.
			nr := items[0]
			done := metav1.NewTime(base)
			nr.Status.Phase = sksv1alpha1.PhaseCompleted
			nr.Status.CompletedAt = &done
			nr.Status.ReplacementNodeName = "node-fresh"
			nr.Status.ReplacementNodeVersion = newerVersion
			Expect(k8sClient.Status().Update(ctx, &nr)).To(Succeed())

			r.Now = func() time.Time { return base.Add(5 * time.Minute) }
			res := reconcile(r)

			Expect(listReplacements()).To(HaveLen(1), "must not start the next node mid-cooldown")
			policy := getPolicy(policyName)
			Expect(conditionReason(policy.Status.Conditions, sksv1alpha1.ConditionReady)).
				To(Equal(sksv1alpha1.ReasonCoolingDown))
			Expect(policy.Status.LastCompletedAt).NotTo(BeNil())
			Expect(res.RequeueAfter).To(BeNumerically("<=", 5*time.Minute))
			Expect(res.RequeueAfter).To(BeNumerically(">", 0))

			// Past the cooldown, the next node goes.
			r.Now = func() time.Time { return base.Add(11 * time.Minute) }
			reconcile(r)
			Expect(listReplacements()).To(HaveLen(2))
		})

		It("does not stall when nothing has completed yet", func() {
			newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
				p.Spec.Rollout.Cooldown = metav1.Duration{Duration: time.Hour}
			})
			reconcile(newPolicyReconciler(twoPoolFake(), ""))
			Expect(listReplacements()).To(HaveLen(1))
			Expect(getPolicy(policyName).Status.LastCompletedAt).To(BeNil())
		})
	})

	It("reports an invalid maintenance window instead of retrying it forever", func() {
		newSKSNode("node-x", defaultPoolID, olderVersion, "inst-x")
		newPolicy(policyName, func(p *sksv1alpha1.NodeUpgradePolicy) {
			p.Spec.MaintenanceWindows = []sksv1alpha1.MaintenanceWindow{{
				Schedule: "0 2 * * *",
				Duration: metav1.Duration{Duration: time.Hour},
				TimeZone: "Mars/Olympus",
			}}
		})
		res := reconcile(newPolicyReconciler(twoPoolFake(), ""))

		Expect(res.RequeueAfter).To(BeZero())
		Expect(listReplacements()).To(BeEmpty())
		Expect(conditionReason(getPolicy(policyName).Status.Conditions, sksv1alpha1.ConditionDegraded)).
			To(Equal("InvalidMaintenanceWindow"))
	})
})

func ptr[T any](v T) *T { return &v }
