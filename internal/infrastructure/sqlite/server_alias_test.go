package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestUnknownAliasFallbackPinsActualGeneralAcrossEdits(t *testing.T) {
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
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	rules := serverTestRules()
	rules.Destinations["general"] = domain.Destination{ID: "general", Name: "Настроенный General", Kind: domain.DestinationTelegram, Enabled: true}
	rules.Unknown = map[string]domain.RouteDecision{"general": {Action: domain.RouteReview}}
	rules.UnknownAliases = map[string]string{"name:неизвестная команда": "name:1 мб 155 омбр"}
	base := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Second)
	makeEvent := func(id, position string, revision int) domain.Event {
		event := serverTestEvent()
		event.ID, event.Position.UnitName, event.ContentDigest = id, position, "digest-"+id
		event.Position.CatalogUUID = "catalog-1"
		event.CreatedAt = base.Add(time.Duration(revision) * time.Minute)
		event.SourceAt = event.CreatedAt
		event.ExpiresAt = event.CreatedAt.Add(30 * time.Minute)
		event.Source.UpdatedAt = event.CreatedAt.Format("2006-01-02 15:04:05")
		return event
	}
	first := makeEvent("alias-first", "НЕИЗВЕСТНАЯ  КОМАНДА", 1)
	plan, err := rules.Resolve(first)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, first, plan); err != nil || !accepted {
		t.Fatalf("первый приём: %v %v", accepted, err)
	}
	fallback := domain.PlannedDelivery{Destination: rules.Destinations["general"], Decision: rules.Unknown["general"], RuleLevel: "alias", SourcePositionKey: first.PositionKey()}
	fallback.SourceUnitNameKey = "name:" + domain.NormalizeUnitName(domain.RoutingUnitName(first.Position.UnitName))
	bad := application.DeliveryReceipt{Kind: "telegram", ChatID: -1002, ThreadID: 7, MessageIDs: []int64{900}}
	if err := store.MarkDeliveredOnAliasFallback(ctx, first.ID, "a", time.Now(), bad, fallback); err == nil {
		t.Fatal("неверная квитанция резервной доставки принята")
	}
	receipt := application.DeliveryReceipt{Kind: "telegram", ChatID: -1002, ThreadID: 0, MessageIDs: []int64{900}}
	if err := store.MarkDeliveredOnAliasFallback(ctx, first.ID, "a", time.Now(), receipt, fallback); err != nil {
		t.Fatal(err)
	}
	// Отмена привязки влияет только на новые исходные записи, не на редакцию.
	rules.UnknownAliases = nil
	rules.Version++
	edit := makeEvent("alias-edit", "неизвестная команда", 2)
	plan, err = rules.Resolve(edit)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, edit, plan); err != nil || !accepted {
		t.Fatalf("правка: %v %v", accepted, err)
	}
	job, err := store.DeliveryJobByID(ctx, edit.ID, "general")
	if err != nil || job.Delivery.Decision.ThreadID != 0 || job.Delivery.RuleLevel != "alias" {
		t.Fatalf("редакция не сохранила фактический General: %+v, %v", job.Delivery, err)
	}
	if _, err := store.DeliveryJobByID(ctx, edit.ID, "a"); err == nil {
		t.Fatal("редакция снова направлена в удалённую тему")
	}
	if _, err := store.DeliveryJobByID(ctx, edit.ID, "w"); err != nil {
		t.Fatalf("Word потерян после резервной доставки: %v", err)
	}
	changedName := makeEvent("alias-renamed-in-eavc", "другое подразделение", 3)
	plan, err = rules.Resolve(changedName)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, changedName, plan); err != nil || !accepted {
		t.Fatalf("изменение названия EAVC: %v %v", accepted, err)
	}
	cleanup, err := store.DeliveryJobByID(ctx, changedName.ID, "w")
	if err != nil || cleanup.Delivery.RuleLevel != "cleanup" {
		t.Fatalf("смена имени с тем же UUID каталога не сняла Word: %+v, %v", cleanup.Delivery, err)
	}
	// Новая запись с тем же названием после отмены уже не наследует старую привязку.
	fresh := makeEvent("alias-fresh", "неизвестная команда", 4)
	fresh.Source.UUID = "different-source"
	plan, err = rules.Resolve(fresh)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Recognized || len(plan.Deliveries) != 1 || plan.Deliveries[0].Destination.ID != "general" {
		t.Fatalf("новая запись не вернулась в текущий General: %+v", plan)
	}
}
