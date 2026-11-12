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

// Package metrics publishes the operator's Prometheus metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	namespace = "sks_node_operator"

	labelPolicy   = "policy"
	labelNodepool = "nodepool"
)

var (
	// OutdatedNodes is how many managed nodes run below the target version.
	OutdatedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "outdated_nodes",
		Help:      "Managed nodes whose kubelet is below the target version.",
	}, []string{labelPolicy})

	// ManagedNodes is how many nodes a policy manages.
	ManagedNodes = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "managed_nodes",
		Help:      "Nodes selected by the policy.",
	}, []string{labelPolicy})

	// TargetVersionInfo exposes the resolved target version as a label.
	TargetVersionInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "target_version_info",
		Help:      "Always 1; the target version and its provenance are labels.",
	}, []string{labelPolicy, "target_version", "control_plane_version", "source"})

	// InMaintenanceWindow is 1 while a policy's window is open.
	InMaintenanceWindow = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "in_maintenance_window",
		Help:      "1 while the policy's maintenance window is open, 0 otherwise.",
	}, []string{labelPolicy})

	// ActiveReplacements is how many replacements are in flight.
	ActiveReplacements = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "active_replacements",
		Help:      "Replacements currently in flight.",
	}, []string{labelPolicy})

	// ReplacementsTotal counts finished replacements by outcome.
	ReplacementsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "replacements_total",
		Help:      "Replacements that reached a terminal phase, by result.",
	}, []string{labelPolicy, "pool_type", "result"})

	// ReplacementDuration measures end-to-end replacement time.
	ReplacementDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "replacement_duration_seconds",
		Help:      "Time from creation to terminal phase of a NodeReplacement.",
		Buckets:   prometheus.ExponentialBuckets(30, 2, 8), // 30s .. ~64m
	}, []string{labelPolicy, "pool_type", "result"})

	// NodepoolState exposes each observed SKS nodepool's state as a label.
	// A pool stuck outside "running" cannot be scaled or evicted, so it has to
	// be alertable.
	NodepoolState = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "nodepool_state",
		Help:      "Always 1; the SKS nodepool's reported state is a label.",
	}, []string{labelNodepool, "state"})

	// NodepoolSize is the last observed size of an SKS nodepool.
	NodepoolSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "nodepool_size",
		Help:      "Last observed size of an SKS nodepool.",
	}, []string{labelNodepool})
)

func init() {
	metrics.Registry.MustRegister(
		OutdatedNodes,
		ManagedNodes,
		TargetVersionInfo,
		InMaintenanceWindow,
		ActiveReplacements,
		ReplacementsTotal,
		ReplacementDuration,
		NodepoolState,
		NodepoolSize,
	)
}

// RecordNodepool publishes a pool's size and state, clearing any previous state
// label so a pool never appears to be in two states at once.
func RecordNodepool(name, state string, size int64) {
	NodepoolState.DeletePartialMatch(prometheus.Labels{labelNodepool: name})
	NodepoolState.WithLabelValues(name, state).Set(1)
	NodepoolSize.WithLabelValues(name).Set(float64(size))
}

// RecordTargetVersion publishes the resolved target, replacing the previous one.
func RecordTargetVersion(policy, target, controlPlane, source string) {
	TargetVersionInfo.DeletePartialMatch(prometheus.Labels{labelPolicy: policy})
	TargetVersionInfo.WithLabelValues(policy, target, controlPlane, source).Set(1)
}

// ForgetPolicy drops every series belonging to a deleted policy.
func ForgetPolicy(policy string) {
	labels := prometheus.Labels{labelPolicy: policy}
	OutdatedNodes.DeletePartialMatch(labels)
	ManagedNodes.DeletePartialMatch(labels)
	TargetVersionInfo.DeletePartialMatch(labels)
	InMaintenanceWindow.DeletePartialMatch(labels)
	ActiveReplacements.DeletePartialMatch(labels)
}

// Bool converts a condition to a gauge value.
func Bool(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
