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
	"errors"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
	"github.com/whizus/exoscale-sks-node-operator/internal/exoscale"
	"github.com/whizus/exoscale-sks-node-operator/internal/nodes"
)

var _ = Describe("NodeReplacement controller", func() {
	const (
		policyName = "default"
		oldNode    = "node-pool-a-1"
		oldInstace = "bbbbbbbb-0000-0000-0000-000000000001"
	)

	var fake *exoscale.Fake

	newReplacementReconciler := func() *NodeReplacementReconciler {
		clientset, err := kubernetes.NewForConfig(cfg)
		Expect(err).NotTo(HaveOccurred())
		return &NodeReplacementReconciler{
			Client:   k8sClient,
			Scheme:   k8sClient.Scheme(),
			Recorder: events.NewFakeRecorder(256),
			Exoscale: &exoscale.StaticProvider{Client: fake},
			Drainer:  nodes.NewDrainer(clientset),
		}
	}

	// step drives one reconcile and returns the object's phase afterwards.
	step := func(r *NodeReplacementReconciler, name string) sksv1alpha1.ReplacementPhase {
		GinkgoHelper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})
		Expect(err).NotTo(HaveOccurred())
		return getReplacement(name).Status.Phase
	}

	// run drives the state machine until it settles or the step budget runs out.
	run := func(r *NodeReplacementReconciler, name string, steps int) sksv1alpha1.ReplacementPhase {
		GinkgoHelper()
		var phase sksv1alpha1.ReplacementPhase
		for range steps {
			phase = step(r, name)
			if phase.IsTerminal() {
				return phase
			}
		}
		return phase
	}

	newReplacement := func(mutate ...func(*sksv1alpha1.NodeReplacement)) string {
		nr := &sksv1alpha1.NodeReplacement{
			ObjectMeta: metav1.ObjectMeta{
				Name:   "replace-" + oldNode,
				Labels: map[string]string{PolicyNameLabel: policyName, NodeNameLabel: oldNode},
			},
			Spec: sksv1alpha1.NodeReplacementSpec{
				NodeName:          oldNode,
				PolicyName:        policyName,
				PoolType:          sksv1alpha1.PoolTypeSKSManaged,
				InstanceID:        oldInstace,
				SKSNodepoolID:     defaultPoolID,
				KarpenterNodePool: "",
				TargetVersion:     "v1.36.3",
			},
		}
		for _, m := range mutate {
			m(nr)
		}
		Expect(k8sClient.Create(ctx, nr)).To(Succeed())
		return nr.Name
	}

	BeforeEach(func() {
		fake = twoPoolFake()
		fake.Nodepools[defaultPoolID].Size = 1
	})
	AfterEach(cleanup)

	Context("SKS-managed node, Surge mode", func() {
		BeforeEach(func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)

			// Scaling up makes a fresh node appear, on the newest patch.
			fake.OnScale = func(poolID string, size int64) {
				if poolID != defaultPoolID || size < 2 {
					return
				}
				newSKSNode("node-pool-a-fresh", defaultPoolID, newerVersion, "inst-fresh")
			}
			// Eviction destroys the instance; the managed CCM removes the Node.
			fake.OnEvict = func(_, _ string) {
				node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: oldNode}}
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, node))).To(Succeed())
			}
		})

		It("surges, drains and evicts, leaving the pool at its original size", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			Expect(run(r, name, 20)).To(Equal(sksv1alpha1.PhaseCompleted))

			// Scaled up to 2, then eviction brought it back to 1.
			Expect(fake.CallLog()).To(Equal([]string{
				fmt.Sprintf("scale:%s:2", defaultPoolID),
				fmt.Sprintf("evict:%s:%s", defaultPoolID, oldInstace),
			}))
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(1)))

			nr := getReplacement(name)
			Expect(nr.Status.ReplacementNodeName).To(Equal("node-pool-a-fresh"))
			Expect(nr.Status.ReplacementNodeVersion).To(Equal(newerVersion))
			Expect(*nr.Status.PoolSizeBefore).To(Equal(int64(1)))
			Expect(conditionStatus(nr.Status.Conditions, sksv1alpha1.ConditionSucceeded)).
				To(Equal(metav1.ConditionTrue))

			// The outdated node is gone.
			err := k8sClient.Get(ctx, client.ObjectKey{Name: oldNode}, &corev1.Node{})
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})

		It("scales up before it cordons, so capacity never dips", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseScalingUp))
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse())

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(2)))
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse())

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseCordoning))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseDraining))
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeTrue())
		})

		It("only cordons when draining is disabled", func() {
			policy := getPolicy(policyName)
			policy.Spec.Drain.Enabled = ptr(false)
			Expect(k8sClient.Update(ctx, policy)).To(Succeed())

			name := newReplacement()
			Expect(run(newReplacementReconciler(), name, 20)).To(Equal(sksv1alpha1.PhaseCompleted))
			Expect(getReplacement(name).Status.Cordoned).To(BeTrue())
		})

		It("waits rather than acting while the nodepool is not running", func() {
			fake.Nodepools[defaultPoolID].State = "scaling"

			name := newReplacement()
			r := newReplacementReconciler()

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhasePending))
			Expect(fake.CallLog()).To(BeEmpty(), "must not scale a pool that is mid-operation")
			Expect(getReplacement(name).Status.Message).To(ContainSubstring("scaling"))
		})

	})

	Context("when the surged node comes up below target", func() {
		BeforeEach(func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)

			// The probe: Exoscale offers 1.36.3 but the image still delivers 1.36.2.
			fake.OnScale = func(poolID string, size int64) {
				if poolID != defaultPoolID || size < 2 {
					return
				}
				newSKSNode("node-pool-a-stale", defaultPoolID, olderVersion, "inst-stale")
			}
		})

		It("leaves the outdated node in place and undoes the surge", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			Expect(run(r, name, 20)).To(Equal(sksv1alpha1.PhaseFailed))

			nr := getReplacement(name)
			Expect(nr.Status.ReplacementNodeVersion).To(Equal(olderVersion))
			Expect(conditionReason(nr.Status.Conditions, sksv1alpha1.ConditionSucceeded)).
				To(Equal(sksv1alpha1.ReasonTargetUnreachable))

			// The old node was never cordoned, drained or evicted.
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse())
			Expect(fake.CallLog()).NotTo(ContainElement(
				fmt.Sprintf("evict:%s:%s", defaultPoolID, oldInstace)))

			// And the surge was undone, so the pool is back where it started.
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(1)))
		})
	})

	Context("when the surged node never becomes Ready", func() {
		BeforeEach(func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)
			// OnScale deliberately does nothing: no node ever appears.
		})

		It("fails on the timeout without touching the old node", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			base := time.Now()
			r.Now = func() time.Time { return base }

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseScalingUp))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(2)))

			// Push past the node-ready timeout.
			r.Now = func() time.Time { return base.Add(20 * time.Minute) }
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseFailed))

			Expect(conditionReason(getReplacement(name).Status.Conditions, sksv1alpha1.ConditionSucceeded)).
				To(Equal("NodeReadyTimeout"))
			// The node it was asked to replace is left exactly as it was found.
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse())
		})
	})

	Context("rollback after a drain failure", func() {
		It("uncordons the node it had cordoned", func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)
			fake.OnScale = func(poolID string, size int64) {
				if poolID == defaultPoolID && size >= 2 {
					newSKSNode("node-pool-a-fresh", defaultPoolID, newerVersion, "inst-fresh")
				}
			}

			name := newReplacement()
			r := newReplacementReconciler()

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseScalingUp))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseCordoning))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseDraining))

			// The eviction of the outdated node fails from here on.
			fake.Errors["EvictNodepoolMember"] = errors.New("exoscale is unavailable")
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseEvicting))
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeTrue())

			// Push past the wait budget so the failing eviction gives up.
			base := time.Now()
			r.Now = func() time.Time { return base.Add(waitBudget + time.Minute) }
			_, _ = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: name}})

			Expect(getReplacement(name).Status.Phase).To(Equal(sksv1alpha1.PhaseFailed))
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse(), "must be uncordoned again")
		})
	})

	Context("Karpenter-managed node", func() {
		const karpNode = "k-node-d-1"

		It("drains the node and deletes its NodeClaim", func() {
			newPolicy(policyName)
			newKarpenterNode(karpNode, "karpenter-pool-d", olderVersion, "inst-zr9n7")

			name := newReplacement(func(nr *sksv1alpha1.NodeReplacement) {
				nr.Name = "replace-" + karpNode
				nr.Spec.NodeName = karpNode
				nr.Spec.PoolType = sksv1alpha1.PoolTypeKarpenterManaged
				nr.Spec.InstanceID = "inst-zr9n7"
				nr.Spec.SKSNodepoolID = ""
				nr.Spec.KarpenterNodePool = "karpenter-pool-d"
				nr.Spec.NodeClaimName = karpNode + "-claim"
			})
			r := newReplacementReconciler()

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseCordoning))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseDraining))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseDeleting))

			// Issues the deletion, then waits for the node to disappear.
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseDeleting))
			Expect(nodeClaimExists(karpNode + "-claim")).To(BeFalse())
			Expect(getReplacement(name).Status.ActionIssued).To(BeTrue())

			// Karpenter's termination removes the Node; then it completes.
			Expect(k8sClient.Delete(ctx, &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: karpNode},
			})).To(Succeed())
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseCompleted))

			// No SKS API call was made for a Karpenter node.
			Expect(fake.CallLog()).To(BeEmpty())
		})
	})

	// Regression: waiting used to be bounded by an attempt counter. Each wait
	// wrote status, the write re-triggered this controller through its own
	// watch, and ten "attempts" burned in seconds — so a node that needed a
	// minute to boot was declared missing after twenty seconds.
	Context("while waiting for a surged node to appear", func() {
		BeforeEach(func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)
		})

		It("keeps waiting across many reconciles and only gives up on the timeout", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			base := time.Now()
			r.Now = func() time.Time { return base }

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseScalingUp))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))

			// Far more reconciles than the old attempt budget allowed.
			for range 30 {
				Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))
			}

			// The node turns up late, as a real one does.
			newSKSNode("node-pool-a-late", defaultPoolID, newerVersion, "inst-late")
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseCordoning))
			Expect(getReplacement(name).Status.ReplacementNodeName).
				To(Equal("node-pool-a-late"))
		})
	})

	// Regression: the rollback used to scale the pool down when it could not
	// identify the node it had added, which let Exoscale pick the victim. On the
	// test cluster it picked the outdated node and destroyed it undrained.
	Context("rolling back a surge it cannot attribute", func() {
		BeforeEach(func() {
			newPolicy(policyName)
			newSKSNode(oldNode, defaultPoolID, olderVersion, oldInstace)
		})

		It("leaves the pool one node larger rather than destroying an unidentified node", func() {
			name := newReplacement()
			r := newReplacementReconciler()

			base := time.Now()
			r.Now = func() time.Time { return base }

			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseScalingUp))
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseWaitingForNode))
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(2)))

			// No node ever registers, so the surged one cannot be named.
			r.Now = func() time.Time { return base.Add(20 * time.Minute) }
			Expect(step(r, name)).To(Equal(sksv1alpha1.PhaseFailed))

			Expect(fake.CallLog()).To(Equal([]string{fmt.Sprintf("scale:%s:2", defaultPoolID)}),
				"the surge is the only call: no blind scale-down, no eviction")
			Expect(fake.PoolSize(defaultPoolID)).To(Equal(int64(2)),
				"the surplus node is left for the cluster-autoscaler to reclaim")

			// And the node it was asked to replace is untouched.
			Expect(getNode(oldNode).Spec.Unschedulable).To(BeFalse())
		})
	})

	It("completes immediately when the node is already gone", func() {
		newPolicy(policyName)
		name := newReplacement()
		Expect(step(newReplacementReconciler(), name)).To(Equal(sksv1alpha1.PhaseCompleted))
		Expect(fake.CallLog()).To(BeEmpty())
	})
})

func getReplacement(name string) *sksv1alpha1.NodeReplacement {
	GinkgoHelper()
	var nr sksv1alpha1.NodeReplacement
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &nr)).To(Succeed())
	return &nr
}

func nodeClaimExists(name string) bool {
	GinkgoHelper()
	claim := &unstructured.Unstructured{}
	claim.SetGroupVersionKind(schema.GroupVersionKind{
		Group: nodes.NodeClaimGroup, Version: nodes.NodeClaimVersion, Kind: nodes.NodeClaimKind,
	})
	err := k8sClient.Get(ctx, client.ObjectKey{Name: name}, claim)
	if apierrors.IsNotFound(err) {
		return false
	}
	Expect(err).NotTo(HaveOccurred())
	return claim.GetDeletionTimestamp() == nil
}
