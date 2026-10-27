package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestProtectExistingColumnKeepsOptionalEmptyValue(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "protected.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `CREATE TABLE optional_values(value BLOB NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO optional_values(value) VALUES(X''), (X'0102')`); err != nil {
		t.Fatal(err)
	}
	if err := protectExistingColumn(ctx, db, "optional_values", "value"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, `SELECT value FROM optional_values ORDER BY rowid`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var values [][]byte
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	if len(values) != 2 || len(values[0]) != 0 {
		t.Fatalf("пустое необязательное значение изменено: %#v", values)
	}
	if !bytes.HasPrefix(values[1], protectedPrefix) {
		t.Fatal("непустое значение не было защищено")
	}
}

// Повторно полученный пакет учитывается один раз, а незавершённый приём не оставляет половину данных.
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
	var storedKey []byte
	if err := db.QueryRowContext(ctx, "SELECT key_bytes FROM server_clients WHERE client_id=?", client.ID).Scan(&storedKey); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(storedKey, client.Key) || !bytes.HasPrefix(storedKey, protectedPrefix) {
		t.Fatal("ключ клиента остался открытым в SQLite")
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
	known, err := store.KnownEvent(ctx, event.ID, event.ContentDigest)
	if err != nil || !known {
		t.Fatalf("known event: %v %v", known, err)
	}
	var storedEvent []byte
	if err := db.QueryRowContext(ctx, "SELECT payload_json FROM received_events WHERE event_id=?", event.ID).Scan(&storedEvent); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(storedEvent, []byte(event.Content)) || !bytes.HasPrefix(storedEvent, protectedPrefix) {
		t.Fatal("принятое событие осталось открытым в SQLite")
	}
	accepted, err = store.Accept(ctx, event, plan)
	if err != nil || accepted {
		t.Fatalf("duplicate: %v %v", accepted, err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, time.Now().Add(time.Minute))
	if err != nil || len(jobs) != 2 {
		t.Fatalf("jobs: %#v %v", jobs, err)
	}
	pending, err := store.PendingDestinationCount(ctx, "a")
	if err != nil || pending != 1 {
		t.Fatalf("очередь получателя a: %d, %v", pending, err)
	}
	if err := store.MarkDelivered(ctx, event.ID, "a", time.Now()); err != nil {
		t.Fatal(err)
	}
	pending, err = store.PendingDestinationCount(ctx, "a")
	if err != nil || pending != 0 {
		t.Fatalf("завершённая доставка осталась в очереди: %d, %v", pending, err)
	}
	history, err := store.DestinationDeliveryCount(ctx, "a")
	if err != nil || history != 1 {
		t.Fatalf("история адреса получателя потеряна после доставки: %d, %v", history, err)
	}
	event.ContentDigest = "changed"
	if _, err := store.KnownEvent(ctx, event.ID, event.ContentDigest); err == nil {
		t.Fatal("проверка известного UUID должна обнаруживать другое содержимое")
	}
	if _, err := store.Accept(ctx, event, plan); err == nil {
		t.Fatal("один UUID с другим содержимым должен быть конфликтом")
	}
}

func TestServerTransportInboxPersistsBatchAndOffsetAtomically(t *testing.T) {
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
	now := time.Now().UTC().Truncate(time.Second)
	packets := []application.IncomingPacket{
		{UpdateID: 10, ChatID: -1001, MessageID: 110, SenderBotID: 1, Payload: []byte("packet-10"), ReceivedAt: now},
		{UpdateID: 11, ChatID: -1001, MessageID: 111, SenderBotID: 2, Payload: []byte("packet-11"), ReceivedAt: now},
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 12, packets); err != nil {
		t.Fatal(err)
	}
	raw, exists, err := store.Value(ctx, "telegram_offset")
	if err != nil || !exists || raw != "12" {
		t.Fatalf("смещение не зафиксировано с пакетами: value=%q exists=%v err=%v", raw, exists, err)
	}
	var stored []byte
	if err := db.QueryRowContext(ctx, "SELECT packet_json FROM transport_inbox WHERE update_id=10").Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stored, []byte("packet-10")) || !bytes.HasPrefix(stored, protectedPrefix) {
		t.Fatal("локальный входной пакет остался открытым в SQLite")
	}
	items, err := store.PendingTransportPackets(ctx, 25, now.Add(time.Minute))
	if err != nil || len(items) != 2 || string(items[0].Packet.Payload) != "packet-10" ||
		!items[0].Packet.ReceivedAt.Equal(now) {
		t.Fatalf("пакеты не восстановлены: %#v err=%v", items, err)
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 12, packets); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_inbox").Scan(&count); err != nil || count != 2 {
		t.Fatalf("повтор создал дубли: count=%d err=%v", count, err)
	}
}

func TestServerTransportInboxRollsBackWholeBatchWithOffset(t *testing.T) {
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
	if _, err := db.ExecContext(ctx, `
		CREATE TRIGGER reject_second_transport_packet BEFORE INSERT ON transport_inbox
		WHEN NEW.update_id=2 BEGIN SELECT RAISE(ABORT, 'test rollback'); END`); err != nil {
		t.Fatal(err)
	}
	err = store.PersistTransportBatch(ctx, "telegram_offset", 3, []application.IncomingPacket{
		{UpdateID: 1, ChatID: -1001, MessageID: 101, Payload: []byte("one")},
		{UpdateID: 2, ChatID: -1001, MessageID: 102, Payload: []byte("two")},
	})
	if err == nil {
		t.Fatal("ошибка второй записи не откатила пакет")
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_inbox").Scan(&count); err != nil || count != 0 {
		t.Fatalf("часть пакета осталась после отката: count=%d err=%v", count, err)
	}
	if _, exists, err := store.Value(ctx, "telegram_offset"); err != nil || exists {
		t.Fatalf("смещение продвинулось без всего пакета: exists=%v err=%v", exists, err)
	}
}

func TestServerTransportInboxRetryDoesNotBlockFollowingPacket(t *testing.T) {
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
	now := time.Now().UTC()
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 3, []application.IncomingPacket{
		{UpdateID: 1, ChatID: -1001, MessageID: 101, Payload: []byte("broken-temporarily")},
		{UpdateID: 2, ChatID: -1001, MessageID: 102, Payload: []byte("ready")},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkTransportRetry(ctx, 1, now.Add(time.Hour), "временный сбой"); err != nil {
		t.Fatal(err)
	}
	items, err := store.PendingTransportPackets(ctx, 25, now.Add(time.Minute))
	if err != nil || len(items) != 1 || items[0].Packet.UpdateID != 2 {
		t.Fatalf("проблемный пакет остановил следующий: %#v err=%v", items, err)
	}
}

func TestServerTransportInboxCompletionQueuesCleanupInSameTransaction(t *testing.T) {
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
	packet := application.IncomingPacket{UpdateID: 7, ChatID: -1001, MessageID: 107, Payload: []byte("done")}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 8, []application.IncomingPacket{packet}); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTransportPacket(ctx, packet.UpdateID, packet.MessageID); err != nil {
		t.Fatal(err)
	}
	var inbox, cleanup int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_inbox").Scan(&inbox)
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_cleanup WHERE message_id=?", packet.MessageID).Scan(&cleanup)
	if inbox != 0 || cleanup != 1 {
		t.Fatalf("завершение неатомарно: inbox=%d cleanup=%d", inbox, cleanup)
	}
}

func TestServerTransportInboxQuarantineIsAtomicAndDoesNotReturn(t *testing.T) {
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
	packet := application.IncomingPacket{
		UpdateID: 17, ChatID: -1001, ThreadID: 9, MessageID: 117,
		SenderBotID: 5, SenderBotUsername: "station_bot", Payload: []byte("invalid-envelope"),
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 18, []application.IncomingPacket{packet}); err != nil {
		t.Fatal(err)
	}
	if err := store.QuarantineTransportPacket(ctx, packet, "неверная подпись"); err != nil {
		t.Fatal(err)
	}
	// Повторный запуск обработчика уже не видит пакет, а очистка Telegram
	// гарантированно поставлена вместе с карантином.
	items, err := store.PendingTransportPackets(ctx, 25, time.Now().Add(time.Hour))
	if err != nil || len(items) != 0 {
		t.Fatalf("карантинный пакет вернулся в обработку: %#v err=%v", items, err)
	}
	var rejected, cleanup int
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rejected_packets").Scan(&rejected)
	_ = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_cleanup WHERE message_id=?", packet.MessageID).Scan(&cleanup)
	if rejected != 1 || cleanup != 1 {
		t.Fatalf("неатомарный карантин: rejected=%d cleanup=%d", rejected, cleanup)
	}
}

func TestServerTransportInboxRefusesOffsetRollback(t *testing.T) {
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
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 100, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 99, nil); err == nil {
		t.Fatal("смещение Telegram удалось откатить назад")
	}
	raw, _, err := store.Value(ctx, "telegram_offset")
	if err != nil || raw != "100" {
		t.Fatalf("смещение изменилось после отказа: %q err=%v", raw, err)
	}
}

func TestServerStoreMigratesRc85DatabaseToTransportInbox(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := Migrate(ctx, db, []string{
		serverMigration, serverDeliveryOrderMigration, serverStreamRepairMigration,
		serverObservabilityMigration, serverIntegrityMigration, serverHistoryIndexesMigration,
		serverDeliveryResultMigration, serverTransportAuditMigration,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServerStore(ctx, db); err != nil {
		t.Fatalf("обновление базы rc85 не прошло: %v", err)
	}
	var version int
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&version); err != nil || version != 9 {
		t.Fatalf("миграция входной очереди не применена: version=%d err=%v", version, err)
	}
}

// После изменения маршрутов сервер хранит предыдущую версию правил, чтобы её можно было восстановить.
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
	second.Unknown["a"] = domain.RouteDecision{Action: domain.RouteReview, Note: "обновлено"}
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

func TestServerStoreQueuesCleanupWhenEditedRecordLeavesWordRoute(t *testing.T) {
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
	client := application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 12,
		SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	rules := serverTestRules()
	first := serverTestEvent()
	first.SourceAt = time.Now().UTC()
	first.ExpiresAt = first.SourceAt.Add(time.Hour)
	firstPlan, err := rules.Resolve(first)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, first, firstPlan); err != nil || !accepted {
		t.Fatalf("первая версия: accepted=%v err=%v", accepted, err)
	}
	for _, delivery := range firstPlan.Deliveries {
		if err := store.MarkDelivered(ctx, first.ID, delivery.Destination.ID, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	second := first
	second.ID = "event-2"
	second.ContentDigest = "digest-2"
	second.Position.UnitName = "1мб 155 омбр"
	secondPlan, err := rules.Resolve(second)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, second, secondPlan); err != nil || !accepted {
		t.Fatalf("исправленная версия: accepted=%v err=%v", accepted, err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 {
		t.Fatalf("ожидались General и очистка Word, получено: %#v", jobs)
	}
	foundCleanup := false
	for _, job := range jobs {
		if job.Delivery.Destination.ID == "w" {
			foundCleanup = job.Delivery.Decision.Action == domain.RouteSkip && job.Delivery.RuleLevel == "cleanup"
		}
	}
	if !foundCleanup {
		t.Fatalf("задание очистки прежнего Word-маршрута не создано: %#v", jobs)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, time.Now().Add(2*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Delivery.RuleLevel != "cleanup" || jobs[0].Delivery.Destination.ID != "w" {
		t.Fatalf("очистка не должна просрочиться вместе с обычной доставкой: %#v", jobs)
	}
}

// Черновик маршрутов переживает повторное открытие базы и удаляется только после публикации.
func TestServerStorePersistsRuleDraftUntilPublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "server.db")
	db, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	draft := serverTestRules()
	draft.Positions["name:черновик"] = map[string]domain.RouteDecision{
		"a": {Action: domain.RouteDeliver, ThreadID: 77},
	}
	if err := store.SaveRuleDraft(ctx, draft); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	store, err = NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	restored, exists, err := store.RuleDraft(ctx)
	if err != nil || !exists || restored.Positions["name:черновик"]["a"].ThreadID != 77 {
		t.Fatalf("черновик не восстановлен: exists=%v rules=%+v err=%v", exists, restored, err)
	}
	if err := store.PublishRules(ctx, draft, "публикация"); err != nil {
		t.Fatal(err)
	}
	if _, exists, err := store.RuleDraft(ctx); err != nil || exists {
		t.Fatalf("опубликованный черновик остался в базе: exists=%v err=%v", exists, err)
	}
}

// Неизвестный отправитель попадает в карантин, а карантин сразу удерживается в заданных пределах.
func TestServerStoreAuthorizesTransportAndCapsQuarantineImmediately(t *testing.T) {
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
	client := application.RegisteredClient{
		ID: "client", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "Point_Bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key", Key: make([]byte, 32), Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	allowed, err := store.AuthorizeTransport(ctx, 12, "@point_bot", -1001, 10)
	if err != nil || !allowed {
		t.Fatalf("зарегистрированный бот не распознан: allowed=%v err=%v", allowed, err)
	}
	allowed, err = store.AuthorizeTransport(ctx, 999, "unknown", -1001, 10)
	if err != nil || allowed {
		t.Fatalf("посторонний бот разрешён: allowed=%v err=%v", allowed, err)
	}
	for index := 0; index < rejectedPacketLimit; index++ {
		if _, err := db.ExecContext(ctx, `
			INSERT INTO rejected_packets(sender_bot_id,sender_bot_username,chat_id,thread_id,reason,payload,received_at)
			VALUES(1,'x',-1,1,'test',x'01',?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Quarantine(ctx, application.IncomingPacket{
		SenderBotID: 12, SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		Payload: []byte("повреждённый пакет"),
	}, "неверная подпись"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rejected_packets").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != rejectedPacketLimit {
		t.Fatalf("карантин не ограничен сразу: %d", count)
	}
}

// В одном топике сообщения идут по порядку, но задержка этого топика не останавливает остальные.
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

	readyAt := time.Now().UTC().Add(time.Minute)
	jobs, err := store.PendingDeliveries(ctx, 10, readyAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].Event.ID != "event-1" || jobs[1].Event.ID != "event-3" {
		t.Fatalf("ожидались головы двух независимых топиков: %#v", jobs)
	}
	if err := store.MarkDeliveryRetry(ctx, "event-1", "telegram", readyAt.Add(time.Minute), "нет сети"); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, "event-3", "telegram", now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, readyAt)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("второе событие топика 2 не должно обгонять первое: %#v, %v", jobs, err)
	}
	if err := store.MarkDelivered(ctx, "event-1", "telegram", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, readyAt.Add(2*time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "event-2" {
		t.Fatalf("после первого ожидалось второе событие топика: %#v, %v", jobs, err)
	}
}

// Окончательная ошибка головы освобождает тот же поток немедленно, сохраняя
// причину и число реально выполненных попыток для оператора.
func TestServerStorePermanentFailureReleasesStream(t *testing.T) {
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
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	destination := domain.Destination{ID: "telegram", Name: "Группа", Kind: domain.DestinationTelegram, Enabled: true}
	for index, id := range []string{"broken-edit", "next-message"} {
		event := serverTestEvent()
		event.ID, event.Source.UUID, event.ContentDigest = id, "source-"+id, "digest-"+id
		event.CreatedAt, event.SourceAt = now.Add(time.Duration(index)*time.Second), now.Add(time.Duration(index)*time.Second)
		event.ExpiresAt = now.Add(30 * time.Minute)
		plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: destination, Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 2},
		}}}
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("принять %s: %v, %v", id, accepted, acceptErr)
		}
	}
	if err := store.MarkDeliveryFailed(ctx, "broken-edit", destination.ID, now.Add(2*time.Second), "сообщение Telegram удалено"); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, time.Now().UTC().Add(time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "next-message" {
		t.Fatalf("следующее сообщение не освобождено: %#v, %v", jobs, err)
	}
	var state, reason string
	var attempts int
	if err := db.QueryRowContext(ctx, `SELECT state,attempts,last_error FROM delivery_jobs WHERE event_id='broken-edit'`).Scan(&state, &attempts, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "expired" || attempts != 1 || reason != "сообщение Telegram удалено" {
		t.Fatalf("неверная история ошибки: state=%s attempts=%d reason=%q", state, attempts, reason)
	}
}

// Обновление с rc69 освобождает уже сохранённую ошибку, не ожидая следующего
// экспоненциального повтора и не удаляя следующие задания.
func TestServerStoreRepairsTerminalEditFromPreviousVersion(t *testing.T) {
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
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	destination := domain.Destination{ID: "telegram", Name: "Группа", Kind: domain.DestinationTelegram, Enabled: true}
	for index, id := range []string{"rc69-broken", "rc69-next"} {
		event := serverTestEvent()
		event.ID, event.Source.UUID, event.ContentDigest = id, "source-"+id, "digest-"+id
		event.CreatedAt, event.SourceAt = now.Add(time.Duration(index)*time.Second), now.Add(time.Duration(index)*time.Second)
		event.ExpiresAt = now.Add(30 * time.Minute)
		plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: destination, Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 2},
		}}}
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("принять %s: %v, %v", id, accepted, acceptErr)
		}
	}
	legacyReason := "старое сообщение Telegram невозможно исправить: telegram: Bad Request: message to edit not found (код 400)"
	if err := store.MarkDeliveryRetry(ctx, "rc69-broken", destination.ID, now.Add(time.Hour), legacyReason); err != nil {
		t.Fatal(err)
	}
	if _, err := NewServerStore(ctx, db); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, time.Now().UTC().Add(time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "rc69-next" {
		t.Fatalf("очередь rc69 не освобождена при запуске: %#v, %v", jobs, err)
	}
	var state, reason string
	if err := db.QueryRowContext(ctx, `SELECT state,last_error FROM delivery_jobs WHERE event_id='rc69-broken'`).Scan(&state, &reason); err != nil {
		t.Fatal(err)
	}
	if state != "expired" || !strings.Contains(reason, "Окончательная ошибка доставки") {
		t.Fatalf("ошибка rc69 не сохранена в истории: state=%s reason=%q", state, reason)
	}
}

// Редакция того же перехвата может изменить подразделение и тему. Даже при
// одинаковом исходном времени она обязана ждать доставку принятой ранее версии.
func TestServerStoreKeepsRevisionAfterOriginalAcrossTopics(t *testing.T) {
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
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	destination := domain.Destination{ID: "telegram", Name: "Группа", Kind: domain.DestinationTelegram, Enabled: true}
	accept := func(id, content, position string, threadID int64) {
		event := serverTestEvent()
		event.ID, event.Source.UUID, event.ContentDigest = id, "same-source", "digest-"+content
		event.Content, event.Position.UnitName = content, position
		event.CreatedAt, event.SourceAt, event.ExpiresAt = now, now, now.Add(30*time.Minute)
		plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: destination, Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: threadID},
		}}}
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("принять %s: accepted=%v err=%v", id, accepted, acceptErr)
		}
	}
	// Идентификаторы специально расположены против алфавитного порядка.
	accept("z-original", "исходный", "позиция А", 2)
	accept("a-revision", "редакция", "позиция Б", 4)
	readyAt := time.Now().UTC().Add(time.Minute)
	jobs, err := store.PendingDeliveries(ctx, 10, readyAt)
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "z-original" {
		t.Fatalf("редакция обогнала исходное сообщение: %#v, %v", jobs, err)
	}
	if err := store.MarkDelivered(ctx, "z-original", destination.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, readyAt.Add(time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "a-revision" {
		t.Fatalf("редакция не вышла после исходного сообщения: %#v, %v", jobs, err)
	}
}

// Ошибка Word-документа одного подразделения не должна задерживать документы
// остальных подразделений того же удалённого получателя.
func TestServerStoreSeparatesWordQueuesByPosition(t *testing.T) {
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
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	destination := domain.Destination{ID: "word-39", Name: "39-ка", Kind: domain.DestinationWord, Enabled: true}
	accept := func(id, source, position string, at time.Time) {
		event := serverTestEvent()
		event.ID, event.Source.UUID, event.ContentDigest = id, source, "digest-"+id
		event.Position.UnitName = position
		event.CreatedAt, event.SourceAt, event.ExpiresAt = at, at, now.Add(30*time.Minute)
		plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{{
			Destination: destination, Decision: domain.RouteDecision{Action: domain.RouteDeliver},
		}}}
		if accepted, acceptErr := store.Accept(ctx, event, plan); acceptErr != nil || !accepted {
			t.Fatalf("принять %s: %v, %v", id, accepted, acceptErr)
		}
	}
	accept("first", "source-1", "подразделение А", now)
	accept("same-position", "source-2", "подразделение А", now.Add(time.Second))
	accept("other-position", "source-3", "подразделение Б", now.Add(time.Second))
	readyAt := time.Now().UTC().Add(time.Minute)
	if err := store.MarkDeliveryRetry(ctx, "first", destination.ID, readyAt.Add(time.Minute), "файл занят"); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, readyAt)
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "other-position" {
		t.Fatalf("ожидался документ другого подразделения: %#v, %v", jobs, err)
	}
}

// Плановая очистка сохраняет ожидающие и свежие записи, удаляя только устаревшие завершённые данные.
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
