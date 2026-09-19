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

	. "github.com/onsi/gomega"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
	"github.com/whizus/exoscale-sks-node-operator/internal/exoscale"
	"github.com/whizus/exoscale-sks-node-operator/internal/nodes"
)

// Fixture identifiers, shaped like the real ones the operator meets.
const (
	defaultPoolID   = "aaaaaaaa-0000-0000-0000-000000000001"
	defaultPoolName = "sks-nodepool-a"
	infraPoolID     = "aaaaaaaa-0000-0000-0000-000000000002"
	infraPoolName   = "sks-nodepool-b"

	controlPlaneVersion = "1.36.2"
	newerVersion        = "v1.36.3"
	olderVersion        = "v1.36.2"
)

// alwaysOpen is a window that is open at every instant, so window handling can
// be tested separately from candidate selection.
func alwaysOpen() []sksv1alpha1.MaintenanceWindow {
	return []sksv1alpha1.MaintenanceWindow{{
		Schedule: "* * * * *",
		Duration: metav1.Duration{Duration: time.Hour},
		TimeZone: "UTC",
	}}
}

// nightly is the 02:00-03:00 window from the brief.
func nightly() []sksv1alpha1.MaintenanceWindow {
	return []sksv1alpha1.MaintenanceWindow{{
		Schedule: "0 2 * * *",
		Duration: metav1.Duration{Duration: time.Hour},
		TimeZone: "UTC",
	}}
}

// newSKSNode creates a Ready SKS-managed node and writes its status.
func newSKSNode(name, poolID, kubelet, instanceID string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				nodes.SKSNodepoolIDLabel: poolID,
				nodes.RegionLabel:        "ch-gva-2",
			},
		},
		Spec: corev1.NodeSpec{ProviderID: nodes.ProviderIDPrefix + instanceID},
	}
	return createNode(node, kubelet)
}

// newKarpenterNode creates a Ready Karpenter-managed node plus its NodeClaim.
func newKarpenterNode(name, pool, kubelet, instanceID string) *corev1.Node {
	claimName := name + "-claim"

	claim := newNodeClaim()
	claim.SetName(claimName)
	Expect(k8sClient.Create(ctx, claim)).To(Succeed())
	claim.Object["status"] = map[string]any{"providerID": nodes.ProviderIDPrefix + instanceID}
	Expect(k8sClient.Status().Update(ctx, claim)).To(Succeed())

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				nodes.KarpenterNodePoolLabel: pool,
				nodes.ZoneLabel:              "ch-gva-2",
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: nodes.NodeClaimAPIVersion,
				Kind:       nodes.NodeClaimKind,
				Name:       claimName,
				UID:        claim.GetUID(),
			}},
		},
		Spec: corev1.NodeSpec{ProviderID: nodes.ProviderIDPrefix + instanceID},
	}
	return createNode(node, kubelet)
}

func createNode(node *corev1.Node, kubelet string) *corev1.Node {
	Expect(k8sClient.Create(ctx, node)).To(Succeed())

	node.Status.NodeInfo.KubeletVersion = kubelet
	node.Status.Conditions = []corev1.NodeCondition{{
		Type:               corev1.NodeReady,
		Status:             corev1.ConditionTrue,
		LastHeartbeatTime:  metav1.Now(),
		LastTransitionTime: metav1.Now(),
	}}
	Expect(k8sClient.Status().Update(ctx, node)).To(Succeed())
	return node
}

func newPolicy(name string, mutate ...func(*sksv1alpha1.NodeUpgradePolicy)) *sksv1alpha1.NodeUpgradePolicy {
	policy := &sksv1alpha1.NodeUpgradePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: sksv1alpha1.NodeUpgradePolicySpec{
			MaintenanceWindows: alwaysOpen(),
			Rollout: sksv1alpha1.RolloutSpec{
				MaxUnavailable:        1,
				MaxUnavailablePerPool: 1,
			},
		},
	}
	for _, m := range mutate {
		m(policy)
	}
	Expect(k8sClient.Create(ctx, policy)).To(Succeed())
	return policy
}

// twoPoolFake has two SKS nodepools, with 1.36.3 available while the control
// plane is still on 1.36.2.
func twoPoolFake() *exoscale.Fake {
	f := exoscale.NewFake(
		exoscale.Cluster{
			ID:      "cluster-1",
			Name:    "test-cluster",
			Version: controlPlaneVersion,
			State:   "running",
		},
		exoscale.Nodepool{
			ID: defaultPoolID, Name: defaultPoolName, Size: 2,
			State: exoscale.NodepoolStateRunning, Version: controlPlaneVersion,
		},
		exoscale.Nodepool{
			ID: infraPoolID, Name: infraPoolName, Size: 2,
			State: exoscale.NodepoolStateRunning, Version: controlPlaneVersion,
		},
	)
	f.Versions = []string{"1.36.3", "1.36.2", "1.35.8"}
	return f
}

func newPolicyReconciler(fake *exoscale.Fake, selfNode string) *NodeUpgradePolicyReconciler {
	return &NodeUpgradePolicyReconciler{
		Client:       k8sClient,
		Scheme:       k8sClient.Scheme(),
		Recorder:     events.NewFakeRecorder(256),
		Exoscale:     &exoscale.StaticProvider{Client: fake},
		SelfNodeName: selfNode,
	}
}

func listReplacements() []sksv1alpha1.NodeReplacement {
	var list sksv1alpha1.NodeReplacementList
	Expect(k8sClient.List(ctx, &list)).To(Succeed())
	return list.Items
}

func getPolicy(name string) *sksv1alpha1.NodeUpgradePolicy {
	var policy sksv1alpha1.NodeUpgradePolicy
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &policy)).To(Succeed())
	return &policy
}

func getNode(name string) *corev1.Node {
	var node corev1.Node
	Expect(k8sClient.Get(ctx, client.ObjectKey{Name: name}, &node)).To(Succeed())
	return &node
}

func conditionStatus(conds []metav1.Condition, condType string) metav1.ConditionStatus {
	for _, c := range conds {
		if c.Type == condType {
			return c.Status
		}
	}
	return metav1.ConditionUnknown
}

func conditionReason(conds []metav1.Condition, condType string) string {
	for _, c := range conds {
		if c.Type == condType {
			return c.Reason
		}
	}
	return ""
}

// cleanup removes every object the specs create, so each spec starts clean.
func cleanup() {
	Expect(k8sClient.DeleteAllOf(ctx, &sksv1alpha1.NodeReplacement{})).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &sksv1alpha1.NodeUpgradePolicy{})).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &corev1.Node{})).To(Succeed())

	// envtest runs no kubelet, so a pod bound to a node would linger in
	// Terminating forever and every later spec's Settling phase would wait on
	// it. Force them away.
	Expect(k8sClient.DeleteAllOf(ctx, &corev1.Pod{},
		client.InNamespace(podNamespace),
		client.GracePeriodSeconds(0))).To(Succeed())
	Expect(k8sClient.DeleteAllOf(ctx, &appsv1.DaemonSet{},
		client.InNamespace(podNamespace))).To(Succeed())
	Eventually(func() int { return len(listPods()) }).Should(Equal(0))

	claims := &unstructured.UnstructuredList{}
	claims.SetGroupVersionKind(schema.GroupVersionKind{
		Group: nodes.NodeClaimGroup, Version: nodes.NodeClaimVersion, Kind: nodes.NodeClaimKind + "List",
	})
	Expect(k8sClient.List(ctx, claims)).To(Succeed())
	for i := range claims.Items {
		item := claims.Items[i]
		Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &item))).To(Succeed())
	}

	Eventually(func() int { return len(listReplacements()) }).Should(Equal(0))
}

// podNamespace holds every pod fixture, so cleanup can scope its deletes.
const podNamespace = "default"

func listPods() []corev1.Pod {
	var list corev1.PodList
	Expect(k8sClient.List(ctx, &list, client.InNamespace(podNamespace))).To(Succeed())
	return list.Items
}

// newDaemonSet creates a DaemonSet to own pod fixtures. Nothing reconciles it;
// it exists so its pods carry a real owner reference.
func newDaemonSet(name string) *appsv1.DaemonSet {
	selector := map[string]string{"app": name}
	ds := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: podNamespace},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: selector},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: selector},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
			},
		},
	}
	Expect(k8sClient.Create(ctx, ds)).To(Succeed())
	return ds
}

// newPod creates a pod. With no scheduler in envtest it stays Pending, which is
// exactly the cold-start state the settle waits are about.
func newPod(name string, mutate ...func(*corev1.Pod)) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: podNamespace},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c", Image: "busybox"}},
		},
	}
	for _, m := range mutate {
		m(pod)
	}
	Expect(k8sClient.Create(ctx, pod)).To(Succeed())
	return pod
}

// ownedBy binds a pod fixture to a DaemonSet and a node.
func ownedBy(ds *appsv1.DaemonSet, nodeName string) func(*corev1.Pod) {
	return func(pod *corev1.Pod) {
		pod.Spec.NodeName = nodeName
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "DaemonSet",
			Name:       ds.Name,
			UID:        ds.UID,
			Controller: ptr(true),
		}}
	}
}

// markPodPhase writes a pod's phase and Ready condition through the status
// subresource, standing in for the kubelet.
func markPodPhase(pod *corev1.Pod, phase corev1.PodPhase, isReady bool) {
	status := corev1.ConditionFalse
	if isReady {
		status = corev1.ConditionTrue
	}
	pod.Status.Phase = phase
	pod.Status.Conditions = []corev1.PodCondition{{
		Type:               corev1.PodReady,
		Status:             status,
		LastTransitionTime: metav1.Now(),
	}}
	Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())
}

// corev1Taint builds a NoSchedule taint with the given key.
func corev1Taint(key string) corev1.Taint {
	return corev1.Taint{Key: key, Effect: corev1.TaintEffectNoSchedule}
}
