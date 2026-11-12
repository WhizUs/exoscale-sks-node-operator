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

// Package version parses Kubernetes version strings and resolves the version
// that cluster nodes are held to.
package version

import (
	"fmt"
	"strconv"
	"strings"
)

// Version is a Kubernetes major.minor.patch version. Any pre-release or build
// suffix is parsed away: kubelet reports things like "v1.36.3+exo.1", and only
// the numeric triple is meaningful for deciding whether a node is outdated.
type Version struct {
	Major int
	Minor int
	Patch int
}

// Parse reads a Kubernetes version string, with or without a leading "v" and
// with or without a pre-release or build suffix.
func Parse(s string) (Version, error) {
	raw := strings.TrimSpace(s)
	if raw == "" {
		return Version{}, fmt.Errorf("empty version string")
	}
	raw = strings.TrimPrefix(raw, "v")

	// Drop any "-prerelease" or "+build" suffix.
	if i := strings.IndexAny(raw, "-+"); i >= 0 {
		raw = raw[:i]
	}

	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("malformed version %q: want major.minor.patch", s)
	}

	out := Version{}
	for i, dst := range []*int{&out.Major, &out.Minor, &out.Patch} {
		n, err := strconv.Atoi(parts[i])
		if err != nil {
			return Version{}, fmt.Errorf("malformed version %q: %w", s, err)
		}
		if n < 0 {
			return Version{}, fmt.Errorf("malformed version %q: negative component", s)
		}
		*dst = n
	}
	return out, nil
}

// String renders the version in Kubernetes' canonical "v" form.
func (v Version) String() string {
	return fmt.Sprintf("v%d.%d.%d", v.Major, v.Minor, v.Patch)
}

// Compare returns -1 if v sorts before o, 1 if after, 0 if equal.
func (v Version) Compare(o Version) int {
	switch {
	case v.Major != o.Major:
		return sign(v.Major - o.Major)
	case v.Minor != o.Minor:
		return sign(v.Minor - o.Minor)
	case v.Patch != o.Patch:
		return sign(v.Patch - o.Patch)
	default:
		return 0
	}
}

// LessThan reports whether v is strictly older than o.
func (v Version) LessThan(o Version) bool { return v.Compare(o) < 0 }

// SameMinor reports whether v and o share a major.minor line.
func (v Version) SameMinor(o Version) bool { return v.Major == o.Major && v.Minor == o.Minor }

func sign(n int) int {
	if n < 0 {
		return -1
	}
	return 1
}
