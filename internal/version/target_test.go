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

package version

import (
	"testing"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

func TestParse(t *testing.T) {
	tests := []struct {
		in      string
		want    Version
		wantErr bool
	}{
		{in: "v1.36.2", want: Version{1, 36, 2}},
		{in: "1.36.2", want: Version{1, 36, 2}},
		{in: "  v1.36.3  ", want: Version{1, 36, 3}},
		{in: "v1.36.3+exo.1", want: Version{1, 36, 3}},
		{in: "v1.36.3-rc.1", want: Version{1, 36, 3}},
		{in: "v1.36", wantErr: true},
		{in: "v1.36.x", wantErr: true},
		{in: "", wantErr: true},
	}
	for _, tc := range tests {
		got, err := Parse(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("Parse(%q) = %v, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("Parse(%q) unexpected error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Parse(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"v1.36.2", "v1.36.3", -1},
		{"v1.36.3", "v1.36.2", 1},
		{"v1.36.2", "v1.36.2", 0},
		{"v1.35.9", "v1.36.0", -1},
		{"v2.0.0", "v1.99.99", 1},
	}
	for _, tc := range tests {
		a, b := mustParse(t, tc.a), mustParse(t, tc.b)
		if got := a.Compare(b); got != tc.want {
			t.Errorf("%s.Compare(%s) = %d, want %d", tc.a, tc.b, got, tc.want)
		}
	}
}

// The test cluster's real shape: control plane on 1.36.2, one node already on
// 1.36.3, three nodes still on 1.36.2.
func TestResolveAuto_DevClusterShape(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane:    "1.36.2",
		Available:       []string{"1.36.3", "1.36.2", "1.35.8"},
		ObservedKubelet: []string{"v1.36.3", "v1.36.2", "v1.36.2", "v1.36.2"},
		Source:          sksv1alpha1.VersionSourceAuto,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 3}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}

	// The node ahead of the control plane must NOT be considered outdated;
	// replacing it would yield another node one patch ahead, forever.
	if IsOutdated("v1.36.3", got.Version) {
		t.Error("node at target version reported as outdated")
	}
	if !IsOutdated("v1.36.2", got.Version) {
		t.Error("node below target not reported as outdated")
	}
}

// Without consulting the versions Exoscale offers, a cluster where every node
// is outdated resolves target == control plane and never updates at all.
func TestResolveAuto_AllNodesOutdatedStillMakesProgress(t *testing.T) {
	in := TargetInput{
		ControlPlane:    "1.36.2",
		ObservedKubelet: []string{"v1.36.2", "v1.36.2"},
		Source:          sksv1alpha1.VersionSourceAuto,
	}

	stalled, err := Resolve(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if IsOutdated("v1.36.2", stalled.Version) {
		t.Fatal("precondition: without Available nothing should look outdated")
	}

	in.Available = []string{"1.36.3", "1.36.2"}
	got, err := Resolve(in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 3}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}
	if got.Source != FromAvailable {
		t.Errorf("source = %q, want %q", got.Source, FromAvailable)
	}
	if !IsOutdated("v1.36.2", got.Version) {
		t.Error("nodes should now be outdated so the rollout can start")
	}
}

// A newer minor must never become the target: kubelet may not lead the API
// server across a minor boundary.
func TestResolveAuto_ClampsToControlPlaneMinor(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane:    "1.36.2",
		Available:       []string{"1.37.0", "1.36.3"},
		ObservedKubelet: []string{"v1.38.0", "v1.36.2"},
		Source:          sksv1alpha1.VersionSourceAuto,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 3}); got.Version != want {
		t.Fatalf("target = %v, want %v (must not cross the minor boundary)", got.Version, want)
	}
}

func TestResolveAuto_IgnoresUnparseableCandidates(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane:    "1.36.2",
		Available:       []string{"not-a-version", "1.36.4"},
		ObservedKubelet: []string{"", "garbage"},
		Source:          sksv1alpha1.VersionSourceAuto,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 4}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}
}

func TestResolve_ControlPlaneSource(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane:    "1.36.2",
		Available:       []string{"1.36.9"},
		ObservedKubelet: []string{"v1.36.7"},
		Source:          sksv1alpha1.VersionSourceControlPlane,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 2}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}
	// Nodes ahead of the control plane stay untouched in this mode too.
	if IsOutdated("v1.36.7", got.Version) {
		t.Error("node ahead of control plane reported as outdated")
	}
}

func TestResolve_PinnedSource(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane: "1.36.2",
		Available:    []string{"1.36.9"},
		Source:       sksv1alpha1.VersionSourcePinned,
		Pinned:       "v1.36.5",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 5}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}

	if _, err := Resolve(TargetInput{
		ControlPlane: "1.36.2",
		Source:       sksv1alpha1.VersionSourcePinned,
		Pinned:       "nonsense",
	}); err == nil {
		t.Error("expected error for unparseable pinned version")
	}
}

func TestResolve_EmptySourceDefaultsToAuto(t *testing.T) {
	got, err := Resolve(TargetInput{
		ControlPlane: "1.36.2",
		Available:    []string{"1.36.3"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := (Version{1, 36, 3}); got.Version != want {
		t.Fatalf("target = %v, want %v", got.Version, want)
	}
}

func TestResolve_BadControlPlaneVersion(t *testing.T) {
	if _, err := Resolve(TargetInput{ControlPlane: ""}); err == nil {
		t.Error("expected error for empty control-plane version")
	}
}

// An unreadable kubelet version must never make a node a deletion candidate.
func TestIsOutdated_UnparseableIsNeverOutdated(t *testing.T) {
	target := Version{1, 36, 3}
	for _, in := range []string{"", "unknown", "v1.36"} {
		if IsOutdated(in, target) {
			t.Errorf("IsOutdated(%q) = true, want false", in)
		}
	}
}

func mustParse(t *testing.T, s string) Version {
	t.Helper()
	v, err := Parse(s)
	if err != nil {
		t.Fatalf("Parse(%q): %v", s, err)
	}
	return v
}
