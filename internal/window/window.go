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

// Package window evaluates recurring maintenance windows expressed as a cron
// start plus a duration, in an explicit time zone.
package window

import (
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

// Window is one recurring period during which replacements may start.
type Window struct {
	schedule cron.Schedule
	duration time.Duration
}

// Set is a collection of windows, open whenever any member is open.
type Set []Window

// Parse compiles a single maintenance window.
func Parse(w sksv1alpha1.MaintenanceWindow) (Window, error) {
	if w.Duration.Duration <= 0 {
		return Window{}, fmt.Errorf("duration must be positive, got %s", w.Duration.Duration)
	}

	tz := w.TimeZoneOrDefault()
	if _, err := time.LoadLocation(tz); err != nil {
		return Window{}, fmt.Errorf("unknown time zone %q: %w", tz, err)
	}

	// robfig/cron reads the zone from a CRON_TZ prefix on the spec itself.
	spec := strings.TrimSpace(w.Schedule)
	if !hasTimeZonePrefix(spec) {
		spec = "CRON_TZ=" + tz + " " + spec
	}

	schedule, err := cron.ParseStandard(spec)
	if err != nil {
		return Window{}, fmt.Errorf("invalid schedule %q: %w", w.Schedule, err)
	}

	return Window{schedule: schedule, duration: w.Duration.Duration}, nil
}

// ParseAll compiles every window, reporting the first that does not compile.
func ParseAll(ws []sksv1alpha1.MaintenanceWindow) (Set, error) {
	out := make(Set, 0, len(ws))
	for i, w := range ws {
		parsed, err := Parse(w)
		if err != nil {
			return nil, fmt.Errorf("maintenanceWindows[%d]: %w", i, err)
		}
		out = append(out, parsed)
	}
	return out, nil
}

// start returns the most recent firing that could still cover t, and whether
// one exists. Only a firing in (t-duration, t] can have t inside its window, and
// Next reports the first firing strictly after the given instant, so a single
// call answers the question.
func (w Window) start(t time.Time) (time.Time, bool) {
	s := w.schedule.Next(t.Add(-w.duration))
	if s.After(t) {
		return time.Time{}, false
	}
	return s, true
}

// IsOpen reports whether t falls inside an occurrence of this window.
func (w Window) IsOpen(t time.Time) bool {
	_, ok := w.start(t)
	return ok
}

// End returns the instant this window's current occurrence closes.
func (w Window) End(t time.Time) (time.Time, bool) {
	s, ok := w.start(t)
	if !ok {
		return time.Time{}, false
	}
	return s.Add(w.duration), true
}

// NextStart returns the next time this window opens strictly after t.
func (w Window) NextStart(t time.Time) time.Time {
	return w.schedule.Next(t)
}

// IsOpen reports whether any window in the set is open at t. An empty set is
// never open: a policy with no maintenanceWindows never starts replacements.
func (s Set) IsOpen(t time.Time) bool {
	for _, w := range s {
		if w.IsOpen(t) {
			return true
		}
	}
	return false
}

// NextStart returns the earliest time any window opens strictly after t.
// The second result is false for an empty set.
func (s Set) NextStart(t time.Time) (time.Time, bool) {
	var best time.Time
	for _, w := range s {
		n := w.NextStart(t)
		if n.IsZero() {
			continue
		}
		if best.IsZero() || n.Before(best) {
			best = n
		}
	}
	return best, !best.IsZero()
}

// CurrentEnd returns the latest instant the currently open windows close.
// Overlapping windows extend each other, so the set stays open until the last
// of them ends.
func (s Set) CurrentEnd(t time.Time) (time.Time, bool) {
	var best time.Time
	for _, w := range s {
		end, ok := w.End(t)
		if !ok {
			continue
		}
		if best.IsZero() || end.After(best) {
			best = end
		}
	}
	return best, !best.IsZero()
}

// RequeueAfter returns how long to wait before re-evaluating the set: until the
// window closes when open, until it next opens when closed. The second result
// is false when no window will ever open.
func (s Set) RequeueAfter(t time.Time) (time.Duration, bool) {
	if end, ok := s.CurrentEnd(t); ok {
		return until(t, end), true
	}
	next, ok := s.NextStart(t)
	if !ok {
		return 0, false
	}
	return until(t, next), true
}

// until keeps requeue delays strictly positive so a boundary landing exactly on
// now cannot produce a zero-delay hot loop.
func until(from, to time.Time) time.Duration {
	d := to.Sub(from)
	if d < time.Second {
		return time.Second
	}
	return d
}

func hasTimeZonePrefix(spec string) bool {
	return strings.HasPrefix(spec, "CRON_TZ=") || strings.HasPrefix(spec, "TZ=")
}
