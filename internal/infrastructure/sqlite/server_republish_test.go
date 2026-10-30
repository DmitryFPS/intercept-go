package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestCurrentDeliveryJobForRepublishUsesLatestScopedRevision(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "point", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	makeEvent := func(id, blockUUID, text string, minutes int) domain.Event {
		return domain.Event{
			ID: id, ClientID: "point", CreatedAt: base.Add(time.Duration(minutes) * time.Minute),
			SourceAt: base, Source: domain.SourceRecord{UUID: "record-shared"},
			SourceBlockUUID: blockUUID, Content: text, ContentDigest: "digest-" + id,
			Blocks: []domain.ContentBlock{{
				UUID: blockUUID, Text: text,
				UpdatedAt: base.Add(time.Duration(minutes) * time.Minute).Format(time.RFC3339Nano),
			}},
		}
	}
	telegram := domain.Destination{ID: "telegram", Name: "Тема", Kind: domain.DestinationTelegram, Enabled: true}
	plan := func(threadID int64) domain.DeliveryPlan {
		return domain.DeliveryPlan{RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: telegram, Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: threadID},
		}}}
	}
	accept := func(event domain.Event, delivery domain.DeliveryPlan) {
		t.Helper()
		if accepted, err := store.Accept(ctx, event, delivery); err != nil || !accepted {
			t.Fatalf("принять %s: accepted=%v err=%v", event.ID, accepted, err)
		}
	}
	accept(makeEvent("old-a", "block-a", "12:00\n-Старый текст", 1), plan(11))
	accept(makeEvent("latest-a", "block-a", "12:00\n-Исправленный текст", 2), plan(12))
	accept(makeEvent("other-b", "block-b", "12:01\n-Отдельный блок", 3), plan(13))

	job, err := store.CurrentDeliveryJobForRepublish(ctx, "old-a", "telegram")
	if err != nil {
		t.Fatal(err)
	}
	if job.Event.ID != "latest-a" || job.Event.Content != "12:00\n-Исправленный текст" || job.Delivery.Decision.ThreadID != 12 {
		t.Fatalf("старая карточка не переключена на актуальный блок и тему: %+v", job)
	}
	if _, err := store.CurrentDeliveryJobForRepublish(ctx, "old-a", "missing"); err == nil {
		t.Fatal("нельзя использовать направление, которого не было у выбранной карточки")
	}
	if _, err := store.CurrentDeliveryJobForRepublish(ctx, "missing", "telegram"); err == nil {
		t.Fatal("несуществующая карточка не должна публиковаться")
	}

	// После ухода актуальной редакции из темы остаётся только задание cleanup.
	// Операторская команда должна его увидеть и отказать, не брать старый маршрут.
	accept(makeEvent("removed-a", "block-a", "12:00\n-Теперь другой маршрут", 4), domain.DeliveryPlan{RuleSetVersion: 1})
	job, err = store.CurrentDeliveryJobForRepublish(ctx, "old-a", "telegram")
	if err != nil {
		t.Fatal(err)
	}
	if job.Event.ID != "removed-a" || job.Delivery.RuleLevel != "cleanup" || job.Delivery.Decision.Action != domain.RouteSkip {
		t.Fatalf("старый Telegram-маршрут должен быть закрыт cleanup: %+v", job)
	}
}
