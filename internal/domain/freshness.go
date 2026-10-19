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

// Freshness вычисляет срок доставки для источников, которые уже сохраняют UTC.
func Freshness(updatedAt string, observedAt, now time.Time, maxAge time.Duration) (time.Time, time.Time, string) {
	return FreshnessInLocation(updatedAt, observedAt, now, maxAge, time.UTC)
}

// FreshnessInLocation трактует время без смещения в часовом поясе рабочего места.
// Строки с явным смещением продолжают разбираться без каких-либо преобразований.
// FreshnessInLocation вычисляет срок с учётом часового пояса старой EAVC.
func FreshnessInLocation(updatedAt string, observedAt, now time.Time, maxAge time.Duration, location *time.Location) (time.Time, time.Time, string) {
	if location == nil {
		location = time.UTC
	}
	sourceAt, ok := parseSourceTime(updatedAt, location)
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

func parseSourceTime(value string, location *time.Location) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range sourceTimeLayouts {
		var parsed time.Time
		var err error
		if strings.Contains(layout, "Z07:00") {
			parsed, err = time.Parse(layout, value)
		} else {
			parsed, err = time.ParseInLocation(layout, value, location)
		}
		if err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}
