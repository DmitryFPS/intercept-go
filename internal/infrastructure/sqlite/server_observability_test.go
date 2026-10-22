package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
)

func openObservabilityStore(t *testing.T) (*ServerStore, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewServerStore(ctx, db)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	client := application.RegisteredClient{
		ID: "textile", Number: "01", Name: "Текстиль",
		SenderBotID: 12, SenderBotUsername: "textile_bot",
		ChatID: -1001, ThreadID: 10, KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	return store, ctx
}

func TestServerObservabilityTracksOutageAndRecovery(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	started := time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC)
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, true, started); err != nil {
		t.Fatal(err)
	}
	page, err := store.HistoryPage(ctx, "clients", 30, 0, started.Add(time.Minute))
	if err != nil || len(page.Clients) != 1 || page.Clients[0].Quality != "good" {
		t.Fatalf("неожиданное состояние связи: %+v err=%v", page, err)
	}
	if err := store.EvaluateClientHealth(ctx, started.Add(6*time.Minute), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	page, err = store.HistoryPage(ctx, "clients", 30, 0, started.Add(6*time.Minute))
	if err != nil || page.Clients[0].Status != "offline" || len(page.Incidents) != 1 || page.Incidents[0].RecoveredAt != nil {
		t.Fatalf("сбой не зафиксирован: %+v err=%v", page, err)
	}
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, false, started.Add(7*time.Minute)); err != nil {
		t.Fatal(err)
	}
	page, err = store.HistoryPage(ctx, "clients", 30, 0, started.Add(7*time.Minute))
	if err != nil || page.Clients[0].Status != "online" || page.Incidents[0].RecoveredAt == nil {
		t.Fatalf("восстановление не зафиксировано: %+v err=%v", page, err)
	}
}

func TestOlderTransportSignalCannotRegressClientHealth(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, true, now); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, false, now.Add(-4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	page, err := store.HistoryPage(ctx, "clients", 30, 0, now.Add(time.Minute))
	if err != nil || len(page.Clients) != 1 || page.Clients[0].DelaySeconds != 60 || page.Clients[0].LastEventAt != nil {
		t.Fatalf("старый сигнал ухудшил состояние: %+v err=%v", page, err)
	}
}

func TestServerHistoryClearDoesNotDeletePendingQueue(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	event := serverTestEvent()
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, event, plan); err != nil || !accepted {
		t.Fatalf("accept=%v err=%v", accepted, err)
	}
	if err := store.MarkDelivered(ctx, event.ID, plan.Deliveries[0].Destination.ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	received, err := store.HistoryPage(ctx, "received", 30, 0, time.Now().UTC())
	if err != nil || received.Total != 1 {
		t.Fatalf("история приёма: %+v err=%v", received, err)
	}
	delivered, err := store.HistoryPage(ctx, "delivered", 30, 0, time.Now().UTC())
	if err != nil || delivered.Total != 1 {
		t.Fatalf("история доставки: %+v err=%v", delivered, err)
	}
	pending, err := store.HistoryPage(ctx, "pending", 30, 0, time.Now().UTC())
	if err != nil || pending.Total != 1 || pending.Clearable {
		t.Fatalf("текущая очередь: %+v err=%v", pending, err)
	}
	if err := store.ClearHistory(ctx, "pending", time.Now().UTC()); err == nil {
		t.Fatal("активную очередь удалось очистить")
	}
	if err := store.ClearHistory(ctx, "delivered", time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	delivered, err = store.HistoryPage(ctx, "delivered", 30, 0, time.Now().UTC().Add(2*time.Second))
	if err != nil || delivered.Total != 0 {
		t.Fatalf("история доставки не очищена: %+v err=%v", delivered, err)
	}
	pending, err = store.HistoryPage(ctx, "pending", 30, 0, time.Now().UTC().Add(2*time.Second))
	if err != nil || pending.Total != 1 {
		t.Fatalf("очистка истории затронула очередь: %+v err=%v", pending, err)
	}
}

func TestTransportDeletionQueueSurvivesUntilCompletion(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	now := time.Now().UTC()
	if err := store.QueueTransportDeletion(ctx, 77, now); err != nil {
		t.Fatal(err)
	}
	items, err := store.DueTransportDeletions(ctx, now.Add(2*time.Minute), 10)
	if err != nil || len(items) != 1 || items[0] != 77 {
		t.Fatalf("повтор удаления не сохранён: items=%v err=%v", items, err)
	}
	if err := store.CompleteTransportDeletion(ctx, 77); err != nil {
		t.Fatal(err)
	}
	items, err = store.DueTransportDeletions(ctx, now.Add(3*time.Minute), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("завершённое удаление осталось в очереди: items=%v err=%v", items, err)
	}
}

func TestIncidentRetentionUsesRecoveryTime(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	now := time.Now().UTC()
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO client_connection_incidents(client_id,started_at,recovered_at,reason)
		VALUES('textile',?,?,?)`, stamp(now.Add(-120*24*time.Hour)), stamp(now.Add(-time.Hour)), "test")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.PurgeOld(ctx, now.Add(-30*24*time.Hour), now.Add(-90*24*time.Hour), 100, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if result.Incidents != 0 {
		t.Fatalf("недавнее восстановление ошибочно удалено: %+v", result)
	}
	var count int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_connection_incidents").Scan(&count); err != nil || count != 1 {
		t.Fatalf("история восстановления потеряна: count=%d err=%v", count, err)
	}
}
