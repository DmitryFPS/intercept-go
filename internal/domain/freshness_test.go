package domain

import (
	"testing"
	"time"
)

// Перехват ровно на границе 30 минут ещё разрешён к отправке, а более старый уже должен быть отброшен.
func TestFreshnessThirtyMinuteBoundary(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		age     time.Duration
		issue   string
		expired bool
	}{
		{name: "двадцать девять минут", age: 29 * time.Minute},
		{name: "ровно тридцать минут", age: 30 * time.Minute},
		{name: "тридцать одна минута", age: 31 * time.Minute, issue: FreshnessExpired, expired: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			source := now.Add(-test.age)
			_, expiresAt, issue := Freshness(source.Format(time.RFC3339Nano), now, now, 30*time.Minute)
			if issue != test.issue {
				t.Fatalf("причина: %q, ожидалось %q", issue, test.issue)
			}
			event := Event{ExpiresAt: expiresAt, FreshnessIssue: issue}
			if event.Expired(now) != test.expired {
				t.Fatalf("просрочено=%v, ожидалось %v", event.Expired(now), test.expired)
			}
		})
	}
}

// Перехват без времени, с ошибочным временем или временем из будущего нельзя считать пригодным к отправке.
func TestFreshnessRejectsMissingInvalidAndFutureTime(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		value string
		issue string
	}{
		{value: "", issue: FreshnessInvalidTime},
		{value: "не-время", issue: FreshnessInvalidTime},
		{value: now.Add(6 * time.Minute).Format(time.RFC3339Nano), issue: FreshnessFutureTime},
	} {
		_, _, issue := Freshness(test.value, now, now, 30*time.Minute)
		if issue != test.issue {
			t.Fatalf("для %q причина %q, ожидалось %q", test.value, issue, test.issue)
		}
	}
}
