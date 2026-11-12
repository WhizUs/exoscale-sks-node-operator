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
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubectl/pkg/drain"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

// Drainer cordons and drains nodes. It delegates to kubectl's own drain helper,
// which already speaks the Eviction API — so PodDisruptionBudgets are honoured,
// and DaemonSet, mirror and static pods are filtered correctly rather than
// blocking the drain forever.
type Drainer struct {
	client kubernetes.Interface
}

// NewDrainer builds a Drainer over a client-go clientset.
func NewDrainer(client kubernetes.Interface) *Drainer {
	return &Drainer{client: client}
}

func (d *Drainer) helper(ctx context.Context, spec sksv1alpha1.DrainSpec) *drain.Helper {
	log := logf.FromContext(ctx)
	out := &logWriter{log: log}
	return &drain.Helper{
		Ctx:                 ctx,
		Client:              d.client,
		Force:               spec.IsForced(),
		GracePeriodSeconds:  int(spec.GracePeriodSeconds),
		IgnoreAllDaemonSets: spec.IgnoresDaemonSets(),
		DeleteEmptyDirData:  spec.DeletesEmptyDirData(),
		DisableEviction:     spec.EvictionDisabled(),
		Timeout:             spec.DrainTimeout(),
		Out:                 out,
		ErrOut:              out,
		OnPodDeletionOrEvictionFinished: func(pod *corev1.Pod, usingEviction bool, err error) {
			verb := "deleted"
			if usingEviction {
				verb = "evicted"
			}
			if err != nil {
				log.Error(err, "pod eviction failed", "pod", pod.Namespace+"/"+pod.Name)
				return
			}
			log.V(1).Info("pod "+verb, "pod", pod.Namespace+"/"+pod.Name)
		},
	}
}

// Cordon marks the node unschedulable. It is a no-op if already cordoned.
func (d *Drainer) Cordon(ctx context.Context, node *corev1.Node) error {
	if node.Spec.Unschedulable {
		return nil
	}
	h := d.helper(ctx, sksv1alpha1.DrainSpec{})
	if err := drain.RunCordonOrUncordon(h, node, true); err != nil {
		return fmt.Errorf("cordon node %s: %w", node.Name, err)
	}
	return nil
}

// Uncordon makes the node schedulable again, used to roll back a replacement
// that failed after cordoning.
func (d *Drainer) Uncordon(ctx context.Context, node *corev1.Node) error {
	if !node.Spec.Unschedulable {
		return nil
	}
	h := d.helper(ctx, sksv1alpha1.DrainSpec{})
	if err := drain.RunCordonOrUncordon(h, node, false); err != nil {
		return fmt.Errorf("uncordon node %s: %w", node.Name, err)
	}
	return nil
}

// Drain evicts the node's pods. The node must already be cordoned, otherwise
// evicted pods can be rescheduled straight back onto it.
func (d *Drainer) Drain(
	ctx context.Context,
	node *corev1.Node,
	spec sksv1alpha1.DrainSpec,
) error {
	h := d.helper(ctx, spec)
	if err := drain.RunNodeDrain(h, node.Name); err != nil {
		return fmt.Errorf("drain node %s: %w", node.Name, err)
	}
	return nil
}

// PendingPods reports how many pods drain would still have to move. Used to
// report progress without mutating anything, including under dry run.
func (d *Drainer) PendingPods(
	ctx context.Context,
	node *corev1.Node,
	spec sksv1alpha1.DrainSpec,
) (int, error) {
	h := d.helper(ctx, spec)
	list, errs := h.GetPodsForDeletion(node.Name)
	if len(errs) > 0 {
		return 0, fmt.Errorf("list pods for deletion on %s: %w", node.Name, errs[0])
	}
	return len(list.Pods()), nil
}

// logWriter adapts the drain helper's io.Writer output onto the controller log.
type logWriter struct {
	log logr.Logger
	buf bytes.Buffer
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.buf.Write(p)
	for {
		line, err := w.buf.ReadString('\n')
		if err != nil {
			// Keep the partial line for the next write.
			w.buf.Reset()
			w.buf.WriteString(line)
			break
		}
		if trimmed := strings.TrimSpace(line); trimmed != "" {
			w.log.V(1).Info(trimmed)
		}
	}
	return len(p), nil
}
