package domain

import (
	"strings"
	"time"
)

const (
	FreshnessInvalidTime = "missing_or_invalid_updated_at"
	FreshnessFutureTime  = "updated_at_in_future"
	FreshnessExpired     = "older_than_delivery_window"
)

var sourceTimeLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02",
}

// Freshness вычисляет единый срок доставки по времени изменения в EAVC.
// Время без часового пояса трактуется как UTC — так же, как в рабочей Python-версии.
func Freshness(updatedAt string, observedAt, now time.Time, maxAge time.Duration) (time.Time, time.Time, string) {
	sourceAt, ok := parseSourceTime(updatedAt)
	if !ok {
		return time.Time{}, now.UTC(), FreshnessInvalidTime
	}
	now = now.UTC()
	sourceAt = sourceAt.UTC()
	if sourceAt.After(now.Add(5 * time.Minute)) {
		return sourceAt, now, FreshnessFutureTime
	}
	oldest := sourceAt
	if !observedAt.IsZero() && observedAt.UTC().Before(oldest) {
		oldest = observedAt.UTC()
	}
	expiresAt := oldest.Add(maxAge)
	if now.After(expiresAt) {
		return sourceAt, expiresAt, FreshnessExpired
	}
	return sourceAt, expiresAt, ""
}

func parseSourceTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range sourceTimeLayouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}
