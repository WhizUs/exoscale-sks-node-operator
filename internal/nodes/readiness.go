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

package nodes

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// maxReported caps how many pod names a wait message names, so status.message
// stays readable when a whole node's worth of pods is still starting.
const maxReported = 5

// DaemonSetPodsNotReady returns the DaemonSet-owned pods scheduled onto a node
// that are not yet Ready.
//
// A node reports Ready as soon as its kubelet registers, which is well before
// the CNI, CSI and ingress DaemonSets that every other pod depends on have
// pulled their images. Draining onto it at that point moves workloads onto a
// node that cannot run them yet.
func DaemonSetPodsNotReady(
	ctx context.Context,
	c kubernetes.Interface,
	nodeName string,
) ([]string, error) {
	pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return nil, fmt.Errorf("list pods on node %s: %w", nodeName, err)
	}

	var notReady []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		// The field selector already narrows this server-side; re-checking keeps
		// the filter honest against any client that does not honour it.
		if pod.Spec.NodeName != nodeName {
			continue
		}
		if !ownedByDaemonSet(pod) || isPodTerminal(pod) || pod.DeletionTimestamp != nil {
			continue
		}
		if !isPodReady(pod) {
			notReady = append(notReady, pod.Namespace+"/"+pod.Name)
		}
	}
	slices.Sort(notReady)
	return notReady, nil
}

// PendingPodsSince returns pods that are Pending and were created at or after
// since.
//
// The creation-time bound is what makes this usable: real clusters accumulate
// pods that have been unschedulable for months, and an unscoped "no Pending
// pods" gate would wait on them forever. Scoping to pods created by this
// rotation's own disruption keeps the wait tied to work this replacement
// actually caused.
//
// ContainerCreating is phase Pending, so image pulls on a cold node are
// counted. A pod that is Running but failing its readiness probe is not --
// listing every Running pod costs far more than the signal is worth.
func PendingPodsSince(
	ctx context.Context,
	c kubernetes.Interface,
	since time.Time,
) ([]string, error) {
	pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: "status.phase=" + string(corev1.PodPending),
	})
	if err != nil {
		return nil, fmt.Errorf("list pending pods: %w", err)
	}

	var pending []string
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase != corev1.PodPending || pod.DeletionTimestamp != nil {
			continue
		}
		if pod.CreationTimestamp.Time.Before(since) {
			continue
		}
		pending = append(pending, pod.Namespace+"/"+pod.Name)
	}
	slices.Sort(pending)
	return pending, nil
}

// Summarize renders pod names for a status message, elided past maxReported.
func Summarize(names []string) string {
	if len(names) <= maxReported {
		return strings.Join(names, ", ")
	}
	return fmt.Sprintf("%s and %d more",
		strings.Join(names[:maxReported], ", "), len(names)-maxReported)
}

func ownedByDaemonSet(pod *corev1.Pod) bool {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func isPodTerminal(pod *corev1.Pod) bool {
	return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed
}

func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}
