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

package version

import (
	"fmt"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

// Provenance names where a resolved target version came from.
const (
	FromPinned       = "Pinned"
	FromControlPlane = "ControlPlane"
	FromAvailable    = "AvailableSKSVersion"
	FromObserved     = "ObservedKubeletVersion"
)

// TargetInput is everything the resolver is allowed to consider.
type TargetInput struct {
	// ControlPlane is the version reported by the SKS cluster. Required.
	ControlPlane string
	// Available is the version list Exoscale offers, from ListSKSClusterVersions.
	Available []string
	// ObservedKubelet is the kubelet version of every Ready managed node.
	ObservedKubelet []string
	// Source selects the resolution strategy.
	Source sksv1alpha1.VersionSource
	// Pinned is the literal target when Source is Pinned.
	Pinned string
}

// Target is a resolved target version and its provenance.
type Target struct {
	Version Version
	// Source explains which input won, for status and for operators reading events.
	Source string
}

// Resolve computes the version that nodes are held to.
//
// Under VersionSourceAuto the target is the highest of the control-plane
// version, the newest version Exoscale offers, and the highest kubelet already
// running, restricted to the control plane's own major.minor.
//
// Two properties matter and are easy to lose:
//
//   - A node whose kubelet is *ahead* of the control plane is not outdated. SKS
//     boots new nodes on the newest patch of the minor, so being ahead is the
//     healthy steady state. Treating it as drift would replace the node, get
//     another node one patch ahead, and loop forever.
//
//   - Consulting Available is what lets a cluster where *every* node is outdated
//     make progress. Without it the target collapses to the control-plane
//     version, nothing qualifies as outdated, and the fleet silently never
//     updates.
//
// Restricting to the control plane's minor keeps a stray node from dragging the
// fleet across a minor boundary, where kubelet would lead the API server by more
// than the supported skew.
func Resolve(in TargetInput) (Target, error) {
	cp, err := Parse(in.ControlPlane)
	if err != nil {
		return Target{}, fmt.Errorf("control-plane version: %w", err)
	}

	switch in.Source {
	case sksv1alpha1.VersionSourcePinned:
		pinned, err := Parse(in.Pinned)
		if err != nil {
			return Target{}, fmt.Errorf("pinned version: %w", err)
		}
		return Target{Version: pinned, Source: FromPinned}, nil

	case sksv1alpha1.VersionSourceControlPlane:
		return Target{Version: cp, Source: FromControlPlane}, nil

	case sksv1alpha1.VersionSourceAuto, "":
		return resolveAuto(cp, in), nil

	default:
		return Target{}, fmt.Errorf("unknown version source %q", in.Source)
	}
}

func resolveAuto(cp Version, in TargetInput) Target {
	best := Target{Version: cp, Source: FromControlPlane}

	consider := func(candidates []string, provenance string) {
		for _, raw := range candidates {
			v, err := Parse(raw)
			// Unparseable entries are ignored rather than failing the whole
			// resolution: one odd node must not stall the rollout.
			if err != nil || !v.SameMinor(cp) {
				continue
			}
			if best.Version.LessThan(v) {
				best = Target{Version: v, Source: provenance}
			}
		}
	}

	consider(in.Available, FromAvailable)
	consider(in.ObservedKubelet, FromObserved)

	return best
}

// IsOutdated reports whether a kubelet version is strictly below target.
// An unparseable version is treated as up to date, so a node the operator does
// not understand is never destroyed.
func IsOutdated(kubelet string, target Version) bool {
	v, err := Parse(kubelet)
	if err != nil {
		return false
	}
	return v.LessThan(target)
}
