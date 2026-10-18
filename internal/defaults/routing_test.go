package defaults

import (
	"testing"
	"time"

	"intercept/internal/domain"
)

func TestInitialRulesMatchApprovedExceptions(t *testing.T) {
	t.Parallel()
	rules, err := InitialRules()
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	tests := []struct {
		unit        string
		destination string
		thread      int64
	}{
		{`2 об 3 обр ОсН "Спартан" НГУ`, "shabashka", 592},
		{"1 мб 155 омбр", "aerodrom", 2},
		{"503 обмп 38 обрмп (отложенный режим)", "aerodrom", 24},
		{"503 обмп 38 обрмп (отложенный режим)", "word-main", 0},
		{`2об3 обр ОсН "Спартан" НГУ`, "shabashka", 0},
	}
	for _, test := range tests {
		event := domain.Event{
			ID: "event", ClientID: "client", CreatedAt: time.Now(),
			Source: domain.SourceRecord{ID: 1}, Position: domain.Position{UnitName: test.unit},
			Mode: domain.SourceModeNormal, ContentDigest: "digest",
		}
		plan, err := rules.Resolve(event)
		if err != nil {
			t.Fatalf("resolve %s: %v", test.unit, err)
		}
		found := false
		for _, delivery := range plan.Deliveries {
			if delivery.Destination.ID == test.destination && delivery.Decision.ThreadID == test.thread {
				found = true
			}
		}
		if !found {
			t.Fatalf("нет ожидаемого маршрута для %q: %#v", test.unit, plan.Deliveries)
		}
	}
}
