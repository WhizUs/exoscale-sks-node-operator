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

package window

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	sksv1alpha1 "github.com/whizus/exoscale-sks-node-operator/api/v1alpha1"
)

func win(schedule, tz string, d time.Duration) sksv1alpha1.MaintenanceWindow {
	return sksv1alpha1.MaintenanceWindow{
		Schedule: schedule,
		Duration: metav1.Duration{Duration: d},
		TimeZone: tz,
	}
}

func mustParse(t *testing.T, w sksv1alpha1.MaintenanceWindow) Window {
	t.Helper()
	got, err := Parse(w)
	if err != nil {
		t.Fatalf("Parse(%+v): %v", w, err)
	}
	return got
}

func at(t *testing.T, tz, layout, value string) time.Time {
	t.Helper()
	loc, err := time.LoadLocation(tz)
	if err != nil {
		t.Fatalf("LoadLocation(%q): %v", tz, err)
	}
	parsed, err := time.ParseInLocation(layout, value, loc)
	if err != nil {
		t.Fatalf("ParseInLocation(%q): %v", value, err)
	}
	return parsed
}

// The 02:00-03:00 window from the brief, in the cluster's own time zone.
func TestIsOpen_NightlyWindow(t *testing.T) {
	w := mustParse(t, win("0 2 * * *", "Europe/Vienna", time.Hour))

	tests := []struct {
		when string
		open bool
	}{
		{"2026-08-18 01:59:59", false},
		{"2026-08-18 02:00:00", true},
		{"2026-08-18 02:30:00", true},
		{"2026-08-18 02:59:59", true},
		{"2026-08-18 03:00:00", false}, // end is exclusive
		{"2026-08-18 12:00:00", false},
	}
	for _, tc := range tests {
		now := at(t, "Europe/Vienna", "2006-01-02 15:04:05", tc.when)
		if got := w.IsOpen(now); got != tc.open {
			t.Errorf("IsOpen(%s) = %v, want %v", tc.when, got, tc.open)
		}
	}
}

// A caller in a different zone must get the same answer.
func TestIsOpen_EvaluatedFromAnotherZone(t *testing.T) {
	w := mustParse(t, win("0 2 * * *", "Europe/Vienna", time.Hour))

	// 02:30 Vienna is 00:30 UTC in August (CEST, UTC+2).
	utc := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 00:30:00")
	if !w.IsOpen(utc) {
		t.Error("window should be open when evaluated from UTC")
	}

	utcOutside := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 02:30:00")
	if w.IsOpen(utcOutside) {
		t.Error("04:30 Vienna should be outside the window")
	}
}

func TestIsOpen_WindowSpanningMidnight(t *testing.T) {
	w := mustParse(t, win("0 23 * * *", "UTC", 3*time.Hour))

	for _, tc := range []struct {
		when string
		open bool
	}{
		{"2026-08-18 22:59:59", false},
		{"2026-08-18 23:30:00", true},
		{"2026-08-19 00:30:00", true},
		{"2026-08-19 01:59:59", true},
		{"2026-08-19 02:00:00", false},
	} {
		now := at(t, "UTC", "2006-01-02 15:04:05", tc.when)
		if got := w.IsOpen(now); got != tc.open {
			t.Errorf("IsOpen(%s) = %v, want %v", tc.when, got, tc.open)
		}
	}
}

// Vienna springs forward at 02:00 on 2026-03-29, so 02:00-03:00 local does not
// exist that night. The window must simply not fire rather than misbehave.
func TestIsOpen_DSTSpringForward(t *testing.T) {
	w := mustParse(t, win("0 2 * * *", "Europe/Vienna", time.Hour))

	skipped := at(t, "Europe/Vienna", "2006-01-02 15:04:05", "2026-03-29 03:30:00")
	if w.IsOpen(skipped) {
		t.Error("window must not be open at 03:30 on a spring-forward night")
	}

	// The following night is normal again.
	normal := at(t, "Europe/Vienna", "2006-01-02 15:04:05", "2026-03-30 02:30:00")
	if !w.IsOpen(normal) {
		t.Error("window should be open the next night")
	}
}

func TestNextStart(t *testing.T) {
	set, err := ParseAll([]sksv1alpha1.MaintenanceWindow{win("0 2 * * *", "UTC", time.Hour)})
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}

	now := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 12:00:00")
	next, ok := set.NextStart(now)
	if !ok {
		t.Fatal("expected a next start")
	}
	want := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-19 02:00:00")
	if !next.Equal(want) {
		t.Errorf("NextStart = %s, want %s", next, want)
	}
}

func TestSet_EmptyIsNeverOpen(t *testing.T) {
	var set Set
	if set.IsOpen(time.Now()) {
		t.Error("empty set must never be open")
	}
	if _, ok := set.NextStart(time.Now()); ok {
		t.Error("empty set must have no next start")
	}
	if _, ok := set.RequeueAfter(time.Now()); ok {
		t.Error("empty set must not schedule a requeue")
	}
}

func TestSet_OverlappingWindowsExtendEachOther(t *testing.T) {
	set, err := ParseAll([]sksv1alpha1.MaintenanceWindow{
		win("0 2 * * *", "UTC", time.Hour),
		win("30 2 * * *", "UTC", time.Hour),
	})
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}

	now := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 02:45:00")
	end, ok := set.CurrentEnd(now)
	if !ok {
		t.Fatal("expected the set to be open")
	}
	want := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 03:30:00")
	if !end.Equal(want) {
		t.Errorf("CurrentEnd = %s, want %s", end, want)
	}
}

func TestRequeueAfter(t *testing.T) {
	set, err := ParseAll([]sksv1alpha1.MaintenanceWindow{win("0 2 * * *", "UTC", time.Hour)})
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}

	// Inside the window: requeue when it closes.
	inside := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 02:15:00")
	d, ok := set.RequeueAfter(inside)
	if !ok || d != 45*time.Minute {
		t.Errorf("RequeueAfter(inside) = %s, %v; want 45m, true", d, ok)
	}

	// Outside: requeue when it opens.
	outside := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 01:00:00")
	d, ok = set.RequeueAfter(outside)
	if !ok || d != time.Hour {
		t.Errorf("RequeueAfter(outside) = %s, %v; want 1h, true", d, ok)
	}
}

// A boundary landing exactly on now must not produce a zero-delay requeue,
// which would spin the controller.
func TestRequeueAfter_NeverZero(t *testing.T) {
	set, err := ParseAll([]sksv1alpha1.MaintenanceWindow{win("0 2 * * *", "UTC", time.Hour)})
	if err != nil {
		t.Fatalf("ParseAll: %v", err)
	}
	boundary := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 02:00:00")
	d, ok := set.RequeueAfter(boundary)
	if !ok || d <= 0 {
		t.Errorf("RequeueAfter(boundary) = %s, %v; want > 0", d, ok)
	}
}

func TestParse_Errors(t *testing.T) {
	if _, err := Parse(win("not a cron", "UTC", time.Hour)); err == nil {
		t.Error("expected error for invalid schedule")
	}
	if _, err := Parse(win("0 2 * * *", "Mars/Olympus", time.Hour)); err == nil {
		t.Error("expected error for unknown time zone")
	}
	if _, err := Parse(win("0 2 * * *", "UTC", 0)); err == nil {
		t.Error("expected error for zero duration")
	}
	if _, err := ParseAll([]sksv1alpha1.MaintenanceWindow{win("bad", "UTC", time.Hour)}); err == nil {
		t.Error("expected ParseAll to surface the invalid member")
	}
}

func TestParse_DefaultsToUTCAndAcceptsDescriptors(t *testing.T) {
	w := mustParse(t, sksv1alpha1.MaintenanceWindow{
		Schedule: "@daily",
		Duration: metav1.Duration{Duration: time.Hour},
	})
	midnight := at(t, "UTC", "2006-01-02 15:04:05", "2026-08-18 00:30:00")
	if !w.IsOpen(midnight) {
		t.Error("@daily window should be open at 00:30 UTC")
	}
}
