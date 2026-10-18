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

func TestServerStoreKeepsRuleHistoryForRestore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first := serverTestRules()
	if err := store.PublishRules(ctx, first, "первый"); err != nil {
		t.Fatal(err)
	}
	second := serverTestRules()
	second.Version = 2
	second.Unknown["a"] = domain.RouteDecision{Action: domain.RouteSkip}
	if err := store.PublishRules(ctx, second, "второй"); err != nil {
		t.Fatal(err)
	}
	history, err := store.RuleHistory(ctx)
	if err != nil || len(history) != 2 || history[0].Version != 2 || !history[0].Active || history[1].Active {
		t.Fatalf("неожиданная история: %+v err=%v", history, err)
	}
	restored, err := store.RulesByVersion(ctx, 1)
	if err != nil || restored.Unknown["a"].Action != domain.RouteReview {
		t.Fatalf("не удалось прочитать старую версию: %+v err=%v", restored, err)
	}
	next, err := store.NextRuleVersion(ctx)
	if err != nil || next != 3 {
		t.Fatalf("следующая версия: %d err=%v", next, err)
	}
}

func TestServerStorePreservesOrderInsideTopicWithoutBlockingOtherTopics(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	client := application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 12,
		SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	destination := domain.Destination{ID: "telegram", Name: "Группа", Kind: domain.DestinationTelegram, Enabled: true}
	accept := func(id string, sourceAt time.Time, threadID int64) {
		event := serverTestEvent()
		event.ID = id
		event.Source.UUID = "source-" + id
		event.ContentDigest = "digest-" + id
		event.CreatedAt = sourceAt
		event.SourceAt = sourceAt
		event.ExpiresAt = now.Add(30 * time.Minute)
		plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: destination, Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: threadID},
		}}}
		if accepted, err := store.Accept(ctx, event, plan); err != nil || !accepted {
			t.Fatalf("принять %s: %v, %v", id, accepted, err)
		}
	}
	accept("event-1", now, 2)
	accept("event-2", now.Add(time.Second), 2)
	accept("event-3", now.Add(time.Second), 4)

	jobs, err := store.PendingDeliveries(ctx, 10, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].Event.ID != "event-1" || jobs[1].Event.ID != "event-3" {
		t.Fatalf("ожидались головы двух независимых топиков: %#v", jobs)
	}
	if err := store.MarkDeliveryRetry(ctx, "event-1", "telegram", now.Add(time.Minute), "нет сети"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, "event-3", "telegram", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, now.Add(3*time.Second))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("второе событие топика 2 не должно обгонять первое: %#v, %v", jobs, err)
	}
	if err := store.MarkDelivered(ctx, "event-1", "telegram", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, now.Add(time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "event-2" {
		t.Fatalf("после первого ожидалось второе событие топика: %#v, %v", jobs, err)
	}
}

func TestServerStoreMaintenanceKeepsPendingAndRecentData(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	client := application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 12,
		SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	accept := func(id string) {
		event := serverTestEvent()
		event.ID = id
		event.Source.UUID = "source-" + id
		event.ContentDigest = "digest-" + id
		plan, _ := rules.Resolve(event)
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("принять %s: accepted=%v err=%v", id, accepted, acceptErr)
		}
	}
	accept("old-done")
	accept("old-pending")
	accept("recent-done")
	for _, destinationID := range []string{"a", "w"} {
		if err := store.MarkDelivered(ctx, "old-done", destinationID, now.Add(-40*24*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkDelivered(ctx, "recent-done", destinationID, now); err != nil {
			t.Fatal(err)
		}
	}
	oldStamp := now.Add(-40 * 24 * time.Hour).Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx, "UPDATE received_events SET received_at=? WHERE event_id IN ('old-done','old-pending')", oldStamp); err != nil {
		t.Fatal(err)
	}
	result, err := store.PurgeOld(ctx, now.Add(-30*24*time.Hour), now.Add(-90*24*time.Hour), 100, 1000)
	if err != nil || result.Events != 1 {
		t.Fatalf("обслуживание: %+v err=%v", result, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Events != 2 || stats.Pending != 2 || stats.Delivered != 2 {
		t.Fatalf("неверные остатки сервера: %+v err=%v", stats, err)
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
