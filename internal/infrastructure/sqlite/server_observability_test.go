package sqlite

import (
	"context"
	"fmt"
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
	filtered, err := store.HistoryPage(ctx, "clients", 30, 0, started.Add(time.Minute), HistoryFilter{Quality: "offline"})
	if err != nil || len(filtered.Clients) != 0 {
		t.Fatalf("фильтр качества связи не применён: %+v err=%v", filtered, err)
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

func TestDelayedSignalCannotRecoverIncidentBeforeItStarted(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	started := time.Date(2026, 10, 23, 7, 0, 0, 0, time.UTC)
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, true, started); err != nil {
		t.Fatal(err)
	}
	if err := store.EvaluateClientHealth(ctx, started.Add(6*time.Minute), 5*time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, false, started.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	page, err := store.HistoryPage(ctx, "clients", 30, 0, started.Add(6*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if page.Clients[0].Status != "offline" || page.Incidents[0].RecoveredAt != nil {
		t.Fatalf("задержанный пакет ошибочно восстановил связь: %#v %#v", page.Clients[0], page.Incidents[0])
	}
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, false, started.Add(7*time.Minute)); err != nil {
		t.Fatal(err)
	}
	page, err = store.HistoryPage(ctx, "clients", 30, 0, started.Add(7*time.Minute))
	if err != nil || page.Incidents[0].RecoveredAt == nil || page.Incidents[0].RecoveredAt.Before(page.Incidents[0].StartedAt) {
		t.Fatalf("свежее восстановление записано неверно: %#v, %v", page.Incidents[0], err)
	}
}

func TestRepairIncidentTimesReopensLatestInvalidIncident(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	started := time.Date(2026, 10, 23, 10, 7, 25, 0, time.UTC)
	invalidRecovery := time.Date(2026, 10, 23, 10, 3, 47, 0, time.UTC)
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, true, invalidRecovery); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO client_connection_incidents(client_id,started_at,recovered_at,reason)
		VALUES('textile',?,?,?)`, stamp(started), stamp(invalidRecovery), "Нет контрольного пакета или данных более 5 минут"); err != nil {
		t.Fatal(err)
	}
	if err := repairIncidentTimes(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	var recovered *string
	if err := store.db.QueryRowContext(ctx, `
		SELECT recovered_at FROM client_connection_incidents
		WHERE client_id='textile' ORDER BY started_at DESC LIMIT 1`).Scan(&recovered); err != nil {
		t.Fatal(err)
	}
	if recovered != nil {
		t.Fatalf("последний нелогичный сбой должен быть снова открыт, recovered_at=%q", *recovered)
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
	if len(received.Items) != 1 || received.Items[0].SourceRecord != event.Source.ID ||
		received.Items[0].SourceAt == nil || received.Items[0].SourceMode != string(event.Mode) {
		t.Fatalf("история не содержит полезные сведения EAVC: %+v", received.Items)
	}
	delivered, err := store.HistoryPage(ctx, "delivered", 30, 0, time.Now().UTC())
	if err != nil || delivered.Total != 1 {
		t.Fatalf("история доставки: %+v err=%v", delivered, err)
	}
	filtered, err := store.HistoryPage(ctx, "delivered", 30, 0, time.Now().UTC(), HistoryFilter{
		ClientID: "textile", DestinationID: plan.Deliveries[0].Destination.ID, Query: "Текстиль",
	})
	if err != nil || filtered.Total != 1 {
		t.Fatalf("комбинированный фильтр доставки: %+v err=%v", filtered, err)
	}
	future := time.Now().UTC().Add(24 * time.Hour)
	filtered, err = store.HistoryPage(ctx, "delivered", 30, 0, time.Now().UTC(), HistoryFilter{From: &future})
	if err != nil || filtered.Total != 0 {
		t.Fatalf("фильтр периода доставки: %+v err=%v", filtered, err)
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

func TestAllDashboardHistoryFilters(t *testing.T) {
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
	if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
		t.Fatalf("accept=%v err=%v", accepted, acceptErr)
	}
	pendingEvent := serverTestEvent()
	pendingEvent.ID = "event-pending"
	pendingEvent.Source.UUID = "source-pending"
	pendingPlan, err := rules.Resolve(pendingEvent)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, acceptErr := store.Accept(ctx, pendingEvent, pendingPlan); acceptErr != nil || !accepted {
		t.Fatalf("accept pending=%v err=%v", accepted, acceptErr)
	}
	now := time.Now().UTC()
	if err := store.RecordClientSignalByTransport(ctx, 12, -1001, 10, true, now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, event.ID, "a", now); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDeliveryExpired(ctx, event.ID, "w", now, "older_than_delivery_window"); err != nil {
		t.Fatal(err)
	}
	if err := store.Quarantine(ctx, application.IncomingPacket{
		SenderBotID: 999, SenderBotUsername: "foreign_bot", ChatID: -1001, ThreadID: 77, Payload: []byte("rejected"),
	}, "unknown_client"); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		kind   string
		filter HistoryFilter
		count  func(ServerHistoryPage) int
	}{
		{name: "рабочие точки", kind: "clients", filter: HistoryFilter{Query: "Текстиль", ClientID: "textile", Quality: "good"}, count: func(page ServerHistoryPage) int { return len(page.Clients) }},
		{name: "получено", kind: "received", filter: HistoryFilter{Query: "event-1", ClientID: "textile"}, count: func(page ServerHistoryPage) int { return len(page.Items) }},
		{name: "ожидает отправки", kind: "pending", filter: HistoryFilter{Query: "event-pending", ClientID: "textile", DestinationID: "w"}, count: func(page ServerHistoryPage) int { return len(page.Items) }},
		{name: "доставлено", kind: "delivered", filter: HistoryFilter{Query: "Текстиль", ClientID: "textile", DestinationID: "a"}, count: func(page ServerHistoryPage) int { return len(page.Items) }},
		{name: "просрочено", kind: "expired", filter: HistoryFilter{Query: "Текстиль", ClientID: "textile", DestinationID: "w"}, count: func(page ServerHistoryPage) int { return len(page.Items) }},
		{name: "отклонено", kind: "rejected", filter: HistoryFilter{Query: "foreign_bot"}, count: func(page ServerHistoryPage) int { return len(page.Items) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			page, pageErr := store.HistoryPage(ctx, test.kind, 30, 0, now.Add(time.Second), test.filter)
			if pageErr != nil || test.count(page) != 1 {
				t.Fatalf("фильтр карточки %q: page=%+v err=%v", test.kind, page, pageErr)
			}
		})
	}
	operatorSearches := []struct {
		kind  string
		query string
		want  int
	}{
		{kind: "received", query: "1 МБ 155 ОМБР", want: 2},
		{kind: "pending", query: "39-ка", want: 1},
		{kind: "delivered", query: "Аэродром", want: 1},
		{kind: "expired", query: "39-КА", want: 1},
		{kind: "rejected", query: "unknown_client", want: 1},
	}
	for _, search := range operatorSearches {
		page, pageErr := store.HistoryPage(ctx, search.kind, 30, 0, now.Add(time.Second), HistoryFilter{Query: search.query})
		if pageErr != nil || len(page.Items) != search.want || page.Total != search.want {
			t.Errorf("поиск по видимым данным карточки %q: items=%d total=%d want=%d err=%v", search.kind, len(page.Items), page.Total, search.want, pageErr)
		}
	}

	for _, kind := range []string{"clients", "received", "pending", "delivered", "expired"} {
		page, pageErr := store.HistoryPage(ctx, kind, 30, 0, now.Add(time.Second), HistoryFilter{ClientID: "missing"})
		if pageErr != nil || (len(page.Items)+len(page.Clients)) != 0 {
			t.Fatalf("карточка %q игнорирует отсутствующую рабочую точку: %+v err=%v", kind, page, pageErr)
		}
	}
	for _, kind := range []string{"pending", "delivered", "expired"} {
		page, pageErr := store.HistoryPage(ctx, kind, 30, 0, now.Add(time.Second), HistoryFilter{DestinationID: "missing"})
		if pageErr != nil || len(page.Items) != 0 {
			t.Fatalf("карточка %q игнорирует отсутствующего получателя: %+v err=%v", kind, page, pageErr)
		}
	}
	rejected, err := store.HistoryPage(ctx, "rejected", 30, 0, now.Add(time.Second), HistoryFilter{Query: "нет такого значения"})
	if err != nil || len(rejected.Items) != 0 {
		t.Fatalf("поиск карточки отклонений проигнорирован: %+v err=%v", rejected, err)
	}
}

func TestProtectedHistorySearchKeepsExactPagination(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 35; index++ {
		event := serverTestEvent()
		event.ID = fmt.Sprintf("search-event-%02d", index)
		event.Source.ID = int64(index)
		event.Source.UUID = fmt.Sprintf("search-source-%02d", index)
		plan, err := rules.Resolve(event)
		if err != nil {
			t.Fatal(err)
		}
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("accept %d=%v err=%v", index, accepted, acceptErr)
		}
	}
	page, err := store.HistoryPage(ctx, "received", 10, 10, time.Now().UTC(), HistoryFilter{Query: "1 МБ 155 ОМБР"})
	if err != nil || page.Total != 35 || len(page.Items) != 10 || page.Offset != 10 || page.Limit != 10 {
		t.Fatalf("постраничный поиск по защищённым данным: %+v err=%v", page, err)
	}
}

func TestUnicodeHistorySearchIsCaseInsensitive(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO client_connection_incidents(client_id,started_at,recovered_at,reason)
		VALUES('textile',?,?,?)`, stamp(now.Add(-time.Minute)), stamp(now), "Потеря связи"); err != nil {
		t.Fatal(err)
	}
	if err := store.Quarantine(ctx, application.IncomingPacket{
		SenderBotID: 999, SenderBotUsername: "ТекстильBot", ChatID: -1001, ThreadID: 77, Payload: []byte("rejected"),
	}, "Неизвестная точка"); err != nil {
		t.Fatal(err)
	}
	clients, err := store.HistoryPage(ctx, "clients", 30, 0, now.Add(time.Second), HistoryFilter{Query: "текстиль"})
	if err != nil || clients.Total != 1 || len(clients.Incidents) != 1 {
		t.Fatalf("кириллический поиск сбоев: %+v err=%v", clients, err)
	}
	rejected, err := store.HistoryPage(ctx, "rejected", 30, 0, now.Add(time.Second), HistoryFilter{Query: "текстиль"})
	if err != nil || rejected.Total != 1 || len(rejected.Items) != 1 {
		t.Fatalf("кириллический поиск отклонений: %+v err=%v", rejected, err)
	}
}

func TestTransportDeletionQueueSurvivesUntilCompletion(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	now := time.Now().UTC()
	if dropped, err := store.QueueTransportDeletion(ctx, 77, now); err != nil || dropped {
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

func TestTransportDeletionQueueStopsAfterAttemptLimit(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	now := time.Now().UTC()
	dropped := false
	for attempt := 0; attempt < transportDeletionMaxAttempts; attempt++ {
		var err error
		dropped, err = store.QueueTransportDeletion(ctx, 88, now.Add(time.Duration(attempt)*time.Minute))
		if err != nil {
			t.Fatal(err)
		}
	}
	if !dropped {
		t.Fatal("техническая очередь не завершилась после лимита попыток")
	}
	items, err := store.DueTransportDeletions(ctx, now.Add(48*time.Hour), 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("исчерпанное удаление осталось в очереди: %v, %v", items, err)
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
