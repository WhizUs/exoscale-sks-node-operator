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

package v1alpha1

import "time"

// The CRD schema carries the real defaults, so anything read back from the API
// server is already populated. These accessors keep unit tests and any
// hand-built object behaving the same way.

func boolOr(v *bool, fallback bool) bool {
	if v == nil {
		return fallback
	}
	return *v
}

func durationOr(d time.Duration, fallback time.Duration) time.Duration {
	if d == 0 {
		return fallback
	}
	return d
}

// Enabled reports whether the node should be drained before replacement.
func (d DrainSpec) IsEnabled() bool { return boolOr(d.Enabled, true) }

// IgnoresDaemonSets reports whether undrainable DaemonSet pods are tolerated.
func (d DrainSpec) IgnoresDaemonSets() bool { return boolOr(d.IgnoreDaemonSets, true) }

// DeletesEmptyDirData reports whether pods with emptyDir volumes may be evicted.
func (d DrainSpec) DeletesEmptyDirData() bool { return boolOr(d.DeleteEmptyDirData, true) }

// IsForced reports whether pods without a controller may be evicted.
func (d DrainSpec) IsForced() bool { return boolOr(d.Force, false) }

// EvictionDisabled reports whether pods are deleted instead of evicted.
func (d DrainSpec) EvictionDisabled() bool { return boolOr(d.DisableEviction, false) }

// DrainTimeout returns the drain timeout, defaulted.
func (d DrainSpec) DrainTimeout() time.Duration {
	return durationOr(d.Timeout.Duration, 10*time.Minute)
}

// IsEnabled reports whether SKS-managed nodes are replaced.
func (s SKSStrategy) IsEnabled() bool { return boolOr(s.Enabled, true) }

// IsEnabled reports whether Karpenter-managed nodes are replaced.
func (k KarpenterStrategy) IsEnabled() bool { return boolOr(k.Enabled, true) }

// SourceOrDefault returns the version source, defaulted to Auto.
func (v VersionPolicy) SourceOrDefault() VersionSource {
	if v.Source == "" {
		return VersionSourceAuto
	}
	return v.Source
}

// MaxUnavailableOrDefault returns the cluster-wide concurrency limit.
func (r RolloutSpec) MaxUnavailableOrDefault() int32 {
	if r.MaxUnavailable <= 0 {
		return 1
	}
	return r.MaxUnavailable
}

// MaxUnavailablePerPoolOrDefault returns the per-pool concurrency limit.
func (r RolloutSpec) MaxUnavailablePerPoolOrDefault() int32 {
	if r.MaxUnavailablePerPool <= 0 {
		return 1
	}
	return r.MaxUnavailablePerPool
}

// CooldownOrDefault returns the post-replacement quiet period.
func (r RolloutSpec) CooldownOrDefault() time.Duration {
	return durationOr(r.Cooldown.Duration, 5*time.Minute)
}

// NodeReadyTimeoutOrDefault bounds the wait for a surged node.
func (r RolloutSpec) NodeReadyTimeoutOrDefault() time.Duration {
	return durationOr(r.NodeReadyTimeout.Duration, 15*time.Minute)
}

// SettleTimeoutOrDefault bounds the waits for workload readiness. A nil field
// means unset and takes the default; an explicit 0 disables those waits, so the
// two cases are deliberately distinguishable.
func (r RolloutSpec) SettleTimeoutOrDefault() time.Duration {
	if r.SettleTimeout == nil {
		return 10 * time.Minute
	}
	return r.SettleTimeout.Duration
}

// ReplacementTimeoutOrDefault bounds a replacement end to end.
func (r RolloutSpec) ReplacementTimeoutOrDefault() time.Duration {
	return durationOr(r.ReplacementTimeout.Duration, 45*time.Minute)
}

// TimeZoneOrDefault returns the window's IANA time zone, defaulted to UTC.
func (m MaintenanceWindow) TimeZoneOrDefault() string {
	if m.TimeZone == "" {
		return "UTC"
	}
	return m.TimeZone
}
