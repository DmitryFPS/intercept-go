package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestServerStoreAcceptIsAtomicAndIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	client := application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 12, SenderBotUsername: "textile_bot",
		ChatID: -1001, ThreadID: 10, KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatalf("client: %v", err)
	}
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatalf("rules: %v", err)
	}
	event := serverTestEvent()
	plan, _ := rules.Resolve(event)
	accepted, err := store.Accept(ctx, event, plan)
	if err != nil || !accepted {
		t.Fatalf("accept: %v %v", accepted, err)
	}
	accepted, err = store.Accept(ctx, event, plan)
	if err != nil || accepted {
		t.Fatalf("duplicate: %v %v", accepted, err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, time.Now().Add(time.Minute))
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs: %#v %v", jobs, err)
	}
	event.ContentDigest = "changed"
	if _, err := store.Accept(ctx, event, plan); err == nil {
		t.Fatal("один UUID с другим содержимым должен быть конфликтом")
	}
}

func serverTestEvent() domain.Event {
	now := time.Now().UTC()
	return domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 1, UUID: "source-1"},
		Position: domain.Position{UnitName: "1 мб 155 омбр"}, Mode: domain.SourceModeNormal,
		Content: "данные", ContentDigest: "digest",
	}
}

func serverTestRules() domain.RuleSet {
	return domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"a": {ID: "a", Name: "Аэродром", Kind: domain.DestinationTelegram, Enabled: true},
			"w": {ID: "w", Name: "39-ка", Kind: domain.DestinationWord, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("1 мб 155 омбр"): {
				"a": {Action: domain.RouteDeliver, ThreadID: 2},
				"w": {Action: domain.RouteDeliver},
			},
		},
		Unknown: map[string]domain.RouteDecision{"a": {Action: domain.RouteReview}},
	}
}
