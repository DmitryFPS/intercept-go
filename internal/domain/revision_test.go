package domain

import (
	"testing"
	"time"
)

func TestCompareSourceRevisionOrdersVersionsAndTransportReordering(t *testing.T) {
	t.Parallel()
	olderCreated := time.Date(2026, 10, 28, 10, 0, 0, 100, time.UTC)
	newerCreated := olderCreated.Add(time.Nanosecond)
	tests := []struct {
		name                               string
		incomingVersion, storedVersion     string
		incomingCreatedAt, storedCreatedAt time.Time
		want                               int
	}{
		{name: "new source second", incomingVersion: "2026-10-28 10:00:01", storedVersion: "2026-10-28 10:00:00", incomingCreatedAt: olderCreated, storedCreatedAt: newerCreated, want: 1},
		{name: "old source second received later", incomingVersion: "2026-10-28 10:00:00", storedVersion: "2026-10-28 10:00:01", incomingCreatedAt: newerCreated, storedCreatedAt: olderCreated, want: -1},
		{name: "same source second new observation", incomingVersion: "2026-10-28 10:00:00", storedVersion: "2026-10-28 10:00:00", incomingCreatedAt: newerCreated, storedCreatedAt: olderCreated, want: 1},
		{name: "transport reversal within same second", incomingVersion: "2026-10-28 10:00:00", storedVersion: "2026-10-28 10:00:00", incomingCreatedAt: olderCreated, storedCreatedAt: newerCreated, want: -1},
		{name: "same revision", incomingVersion: "2026-10-28T10:00:00Z", storedVersion: "2026-10-28 10:00:00", incomingCreatedAt: olderCreated, storedCreatedAt: olderCreated, want: 0},
		{name: "legacy state", incomingVersion: "2026-10-28 10:00:00", incomingCreatedAt: newerCreated, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := CompareSourceRevision(test.incomingVersion, test.incomingCreatedAt, test.storedVersion, test.storedCreatedAt); got != test.want {
				t.Fatalf("порядок версии %d, ожидалось %d", got, test.want)
			}
		})
	}
}

func TestValidateRevisionClockRejectsPoisoningButAllowsNaiveTimezone(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := Event{
		CreatedAt: now, SourceAt: now,
		Source: SourceRecord{UpdatedAt: "2026-10-28 15:00:00"},
	}
	if err := ValidateRevisionClock(event, now); err != nil {
		t.Fatalf("локальное время UTC+3 ошибочно отклонено: %v", err)
	}
	event.Source.UpdatedAt = "9999-10-28 15:00:00"
	if err := ValidateRevisionClock(event, now); err == nil {
		t.Fatal("далёкая будущая версия не отклонена")
	}
	event.Source.UpdatedAt = "не-время"
	if err := ValidateRevisionClock(event, now); err == nil {
		t.Fatal("неподдерживаемая версия не отклонена")
	}
	event.Source.UpdatedAt = "2026-10-28 15:00:00"
	event.CreatedAt = now.Add(6 * time.Minute)
	if err := ValidateRevisionClock(event, now); err == nil {
		t.Fatal("будущее время создания события не отклонено")
	}
}
