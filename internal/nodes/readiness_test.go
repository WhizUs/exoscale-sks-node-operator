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

package nodes

import (
	"context"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type podOpt func(*corev1.Pod)

func onNode(name string) podOpt {
	return func(p *corev1.Pod) { p.Spec.NodeName = name }
}

func fromDaemonSet() podOpt {
	return func(p *corev1.Pod) {
		p.OwnerReferences = append(p.OwnerReferences, metav1.OwnerReference{
			Kind: "DaemonSet",
			Name: "agent",
		})
	}
}

func ready(v bool) podOpt {
	return func(p *corev1.Pod) {
		status := corev1.ConditionFalse
		if v {
			status = corev1.ConditionTrue
		}
		p.Status.Conditions = append(p.Status.Conditions, corev1.PodCondition{
			Type:   corev1.PodReady,
			Status: status,
		})
	}
}

func phase(ph corev1.PodPhase) podOpt {
	return func(p *corev1.Pod) { p.Status.Phase = ph }
}

func createdAt(t time.Time) podOpt {
	return func(p *corev1.Pod) { p.CreationTimestamp = metav1.NewTime(t) }
}

func deleting() podOpt {
	return func(p *corev1.Pod) {
		now := metav1.Now()
		p.DeletionTimestamp = &now
	}
}

func pod(name string, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kube-system"},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func TestDaemonSetPodsNotReady(t *testing.T) {
	const target = "node-a"

	objs := []*corev1.Pod{
		pod("cni-ready", onNode(target), fromDaemonSet(), ready(true)),
		pod("csi-starting", onNode(target), fromDaemonSet(), ready(false)),
		pod("ingress-starting", onNode(target), fromDaemonSet(), ready(false), phase(corev1.PodPending)),
		// A DaemonSet pod on a different node must not be counted.
		pod("cni-elsewhere", onNode("node-b"), fromDaemonSet(), ready(false)),
		// A plain workload pod is not a prerequisite for scheduling.
		pod("workload", onNode(target), ready(false)),
		// Terminal and terminating DaemonSet pods never become Ready.
		pod("agent-succeeded", onNode(target), fromDaemonSet(), ready(false), phase(corev1.PodSucceeded)),
		pod("agent-going-away", onNode(target), fromDaemonSet(), ready(false), deleting()),
	}

	c := fake.NewClientset()
	for _, p := range objs {
		if _, err := c.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed %s: %v", p.Name, err)
		}
	}

	got, err := DaemonSetPodsNotReady(context.Background(), c, target)
	if err != nil {
		t.Fatalf("DaemonSetPodsNotReady: %v", err)
	}

	want := []string{"kube-system/csi-starting", "kube-system/ingress-starting"}
	if !slices.Equal(got, want) {
		t.Errorf("DaemonSetPodsNotReady = %v, want %v", got, want)
	}
}

func TestDaemonSetPodsNotReadyAllReady(t *testing.T) {
	c := fake.NewClientset()
	p := pod("cni", onNode("node-a"), fromDaemonSet(), ready(true))
	if _, err := c.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	got, err := DaemonSetPodsNotReady(context.Background(), c, "node-a")
	if err != nil {
		t.Fatalf("DaemonSetPodsNotReady: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("DaemonSetPodsNotReady = %v, want none", got)
	}
}

// Clusters accumulate pods that have been unschedulable for months. An
// unscoped "no Pending pods" gate would wait on them forever, so only pods
// created at or after the rotation started may hold it.
func TestPendingPodsSinceIgnoresLongUnschedulablePods(t *testing.T) {
	since := time.Now().Add(-5 * time.Minute)

	objs := []*corev1.Pod{
		pod("stuck-for-months", phase(corev1.PodPending), createdAt(since.Add(-141*24*time.Hour))),
		pod("displaced", phase(corev1.PodPending), createdAt(since.Add(time.Minute))),
		pod("displaced-at-the-boundary", phase(corev1.PodPending), createdAt(since)),
		pod("already-running", phase(corev1.PodRunning), createdAt(since.Add(time.Minute))),
		pod("displaced-but-terminating", phase(corev1.PodPending), createdAt(since.Add(time.Minute)), deleting()),
	}

	c := fake.NewClientset()
	for _, p := range objs {
		if _, err := c.CoreV1().Pods(p.Namespace).Create(context.Background(), p, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seed %s: %v", p.Name, err)
		}
	}

	got, err := PendingPodsSince(context.Background(), c, since)
	if err != nil {
		t.Fatalf("PendingPodsSince: %v", err)
	}

	want := []string{"kube-system/displaced", "kube-system/displaced-at-the-boundary"}
	if !slices.Equal(got, want) {
		t.Errorf("PendingPodsSince = %v, want %v", got, want)
	}
}

func TestSummarizeElides(t *testing.T) {
	names := []string{"a", "b", "c", "d", "e", "f", "g"}
	if got, want := Summarize(names), "a, b, c, d, e and 2 more"; got != want {
		t.Errorf("Summarize = %q, want %q", got, want)
	}
	if got, want := Summarize([]string{"a", "b"}), "a, b"; got != want {
		t.Errorf("Summarize = %q, want %q", got, want)
	}
}
