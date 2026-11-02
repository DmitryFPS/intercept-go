package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

// После перезапуска очередь сохраняется, а повторно найденный перехват не добавляется второй раз.
func TestClientQueueSurvivesReopenAndDeduplicates(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "state.db")
	now := time.Now().UTC().Truncate(time.Second)
	event := domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 1, UUID: "source-1"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeUnknown,
		Content: "данные", ContentDigest: "digest-1",
	}

	db, err := OpenState(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	queue, err := NewClientQueue(context.Background(), db)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	if inserted, err := queue.Enqueue(context.Background(), event); err != nil || !inserted {
		t.Fatalf("enqueue: %v", err)
	}
	var storedPayload []byte
	if err := db.QueryRowContext(context.Background(),
		"SELECT payload_json FROM client_events WHERE event_id=?", event.ID,
	).Scan(&storedPayload); err != nil {
		t.Fatalf("прочитать сохранённое событие: %v", err)
	}
	if bytes.Contains(storedPayload, []byte(event.Content)) || !bytes.HasPrefix(storedPayload, protectedPrefix) {
		t.Fatalf("событие осталось открытым в SQLite: %q", storedPayload)
	}
	duplicate := event
	duplicate.ID = "event-2"
	if inserted, err := queue.Enqueue(context.Background(), duplicate); err != nil || inserted {
		t.Fatalf("enqueue duplicate: %v", err)
	}
	if err := queue.SaveCursor(context.Background(), "eavc", application.Cursor{UpdatedAt: "2026", ID: 5}); err != nil {
		t.Fatalf("save cursor: %v", err)
	}
	db.Close()

	db, err = OpenState(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db.Close()
	queue, err = NewClientQueue(context.Background(), db)
	if err != nil {
		t.Fatalf("queue reopen: %v", err)
	}
	events, err := queue.Pending(context.Background(), 10, now)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(events) != 1 || events[0].Event.ID != event.ID {
		t.Fatalf("ожидалось одно исходное событие: %#v", events)
	}
	cursor, err := queue.Cursor(context.Background(), "eavc")
	if err != nil || cursor.ID != 5 {
		t.Fatalf("курсор не восстановлен: %#v, %v", cursor, err)
	}
	if err := queue.MarkUploaded(context.Background(), event.ID, 77); err != nil {
		t.Fatalf("uploaded: %v", err)
	}
	if err := queue.MarkAcknowledged(context.Background(), event.ID, now); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := queue.MarkAcknowledged(context.Background(), event.ID, now); err != nil {
		t.Fatalf("повторное подтверждение должно быть безопасным: %v", err)
	}
}

func TestClientQueueSeparatesBlocksButPreservesRecordOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id, blockUUID, text, version string, at time.Time) domain.Event {
		return domain.Event{
			ID: id, ClientID: "textile", CreatedAt: at, ObservedAt: at, ExpiresAt: at.Add(time.Hour),
			Source:   domain.SourceRecord{ID: 375, UUID: "source-375", UpdatedAt: "record-version"},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
			Content: text, ContentDigest: "digest-" + blockUUID, SourceBlockUUID: blockUUID,
			Blocks: []domain.ContentBlock{{UUID: blockUUID, Text: text, UpdatedAt: version}},
		}
	}
	first := makeEvent("event-a", "block-a", "18.46\n-Первый", "2026-10-27 18:46:00", now)
	second := makeEvent("event-b", "block-b", "18.50\n-Второй", "2026-10-27 18:50:00", now.Add(time.Second))
	for _, event := range []domain.Event{first, second} {
		if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
			t.Fatalf("блок %s не поставлен отдельно: inserted=%v err=%v", event.SourceBlockUUID, inserted, err)
		}
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_events WHERE source_uuid LIKE '%|block:%'").Scan(&count); err != nil || count != 2 {
		t.Fatalf("побочные линии не разделены: count=%d err=%v", count, err)
	}
	pending, err := queue.Pending(ctx, 10, now.Add(time.Minute))
	if err != nil || len(pending) != 1 || pending[0].Event.ID != first.ID {
		t.Fatalf("блоки одной записи нарушили порядок: %#v err=%v", pending, err)
	}
	if err := queue.MarkAcknowledged(ctx, first.ID, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	pending, err = queue.Pending(ctx, 10, now.Add(2*time.Minute))
	if err != nil || len(pending) != 1 || pending[0].Event.ID != second.ID {
		t.Fatalf("второй блок не вышел после первого: %#v err=%v", pending, err)
	}
}

// Несколько блоков одной записи могут получить одинаковые CreatedAt и общий
// миллисекундный префикс UUIDv7. Случайная часть UUID не должна становиться
// порядком отправки после сохранения очереди или перезапуска клиента.
func TestClientQueuePreservesEnqueueOrderForSameTimestampUUIDv7(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id, blockUUID, text string, sequence int) domain.Event {
		return domain.Event{
			ID: id, ClientID: "vesyoloe", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
			Source:   domain.SourceRecord{ID: 1203, UUID: "source-order", UpdatedAt: "record-version"},
			Position: domain.Position{UnitName: "1 аэмб 68 оаэмбр"}, Mode: domain.SourceModeNormal,
			Content: text, ContentDigest: "digest-" + blockUUID, SourceBlockUUID: blockUUID,
			Blocks: []domain.ContentBlock{{
				UUID: blockUUID, Sequence: sequence, Text: text,
				UpdatedAt: "2026-11-01 18:33:04",
			}},
		}
	}
	events := []domain.Event{
		makeEvent("01a197be-e468-7c80-a793-7acc4b3c6eeb", "block-1819", "18.19\n-Первый", 0),
		makeEvent("01a197be-e468-7bb9-9313-20c3c1704f20", "block-1826", "18.26\n-Второй", 1),
		makeEvent("01a197be-e468-72f8-a4ed-d1f1876fb5f9", "block-1833", "18.33\n-Третий", 2),
	}
	for _, event := range events {
		if inserted, enqueueErr := queue.Enqueue(ctx, event); enqueueErr != nil || !inserted {
			t.Fatalf("поставить %s: inserted=%v err=%v", event.ID, inserted, enqueueErr)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err = NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	for index, want := range events {
		pending, pendingErr := queue.Pending(ctx, 1, now.Add(time.Minute))
		if pendingErr != nil || len(pending) != 1 || pending[0].Event.ID != want.ID {
			t.Fatalf("шаг %d: got=%#v want=%s err=%v", index, pending, want.ID, pendingErr)
		}
		if err := queue.MarkAcknowledged(ctx, want.ID, now.Add(time.Duration(index+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
}

// Правка и удаление того же блока обязаны идти после его создания, даже когда
// все три состояния получили одну отметку времени и UUID сортируются наоборот.
func TestClientQueuePreservesCreateEditDeleteOrderAtSameTimestamp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	base := domain.Event{
		ID: "01a197be-e468-7c80-a793-7acc4b3c6eeb", ClientID: "vesyoloe",
		CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{ID: 1203, UUID: "source-edit-delete", UpdatedAt: "version-1"},
		Position: domain.Position{UnitName: "1 аэмб 68 оаэмбр"}, Mode: domain.SourceModeNormal,
		Content: "18.19\n-Исходный текст", ContentDigest: "digest-create", SourceBlockUUID: "block-a",
		Blocks: []domain.ContentBlock{{UUID: "block-a", Text: "18.19\n-Исходный текст", UpdatedAt: "2026-11-01 18:19:00"}},
	}
	edit := base
	edit.ID = "01a197be-e468-7bb9-9313-20c3c1704f20"
	edit.Source.UpdatedAt = "version-2"
	edit.Content = "18.19\n-Исправленный текст"
	edit.ContentDigest = "digest-edit"
	edit.Blocks = []domain.ContentBlock{{UUID: "block-a", Text: edit.Content, UpdatedAt: "2026-11-01 18:26:00"}}
	deleted := edit
	deleted.ID = "01a197be-e468-72f8-a4ed-d1f1876fb5f9"
	deleted.Source.UpdatedAt = "version-3"
	deleted.Content = ""
	deleted.ContentDigest = "digest-delete"
	deleted.SourceBlockDeleted = true
	deleted.Blocks = []domain.ContentBlock{{UUID: "block-a", DeletedAt: "2026-11-01 18:33:00"}}

	events := []domain.Event{base, edit, deleted}
	for _, event := range events {
		if inserted, enqueueErr := queue.Enqueue(ctx, event); enqueueErr != nil || !inserted {
			t.Fatalf("поставить %s: inserted=%v err=%v", event.ID, inserted, enqueueErr)
		}
	}
	for index, want := range events {
		pending, pendingErr := queue.Pending(ctx, 1, now.Add(time.Minute))
		if pendingErr != nil || len(pending) != 1 {
			t.Fatalf("шаг %d: got=%#v err=%v", index, pending, pendingErr)
		}
		got := pending[0].Event
		if got.ID != want.ID || got.Content != want.Content || got.SourceBlockDeleted != want.SourceBlockDeleted {
			t.Fatalf("шаг %d: got=(%s,%q,%v) want=(%s,%q,%v)", index,
				got.ID, got.Content, got.SourceBlockDeleted, want.ID, want.Content, want.SourceBlockDeleted)
		}
		if err := queue.MarkAcknowledged(ctx, got.ID, now.Add(time.Duration(index+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
}

// SQLite сам выдаёт enqueue_seq внутри INSERT; параллельные циклы чтения не
// должны получить одинаковые номера и потерять событие из-за UNIQUE-конфликта.
func TestClientQueueAssignsUniqueEnqueueSequenceConcurrently(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	const eventCount = 24
	errors := make(chan error, eventCount)
	var wait sync.WaitGroup
	for index := 0; index < eventCount; index++ {
		index := index
		wait.Add(1)
		go func() {
			defer wait.Done()
			event := domain.Event{
				ID: fmt.Sprintf("concurrent-%02d", index), ClientID: "vesyoloe",
				CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
				Source:   domain.SourceRecord{UUID: fmt.Sprintf("source-%02d", index)},
				Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
				Content: "блок", ContentDigest: fmt.Sprintf("digest-%02d", index),
			}
			inserted, enqueueErr := queue.Enqueue(ctx, event)
			if enqueueErr != nil {
				errors <- enqueueErr
				return
			}
			if !inserted {
				errors <- fmt.Errorf("событие %s не вставлено", event.ID)
			}
		}()
	}
	wait.Wait()
	close(errors)
	for enqueueErr := range errors {
		t.Fatal(enqueueErr)
	}
	var total, distinct int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*), COUNT(DISTINCT enqueue_seq) FROM client_events`).Scan(&total, &distinct); err != nil {
		t.Fatal(err)
	}
	if total != eventCount || distinct != eventCount {
		t.Fatalf("последовательности: total=%d distinct=%d want=%d", total, distinct, eventCount)
	}
}

// Граничная запись EAVC перечитывается на каждом цикле. Точный дубль должен
// отсеиваться до AUTOINCREMENT, чтобы не создавать постоянные записи в WAL.
func TestClientQueueDuplicateDoesNotAdvanceEnqueueSequence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "event-original", ClientID: "vesyoloe", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{UUID: "source-duplicate", UpdatedAt: "source-version"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "блок", ContentDigest: "digest-duplicate",
	}
	if inserted, enqueueErr := queue.Enqueue(ctx, event); enqueueErr != nil || !inserted {
		t.Fatalf("первая вставка: inserted=%v err=%v", inserted, enqueueErr)
	}
	var before int64
	if err := db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='client_events'`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	duplicate := event
	duplicate.ID = "event-duplicate"
	if inserted, enqueueErr := queue.Enqueue(ctx, duplicate); enqueueErr != nil || inserted {
		t.Fatalf("точный дубль: inserted=%v err=%v", inserted, enqueueErr)
	}
	var after int64
	if err := db.QueryRowContext(ctx, `SELECT seq FROM sqlite_sequence WHERE name='client_events'`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("дубль сдвинул AUTOINCREMENT: before=%d after=%d", before, after)
	}
}

// Обновление существующего клиента восстанавливает порядок старой очереди по
// порядку вставки строк, не доверяя лексикографическому порядку event_id.
func TestClientQueueMigrationBackfillsEnqueueOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations := clientMigrationPlan()
	if err := Migrate(ctx, db, migrations[:len(migrations)-1]); err != nil {
		t.Fatal(err)
	}
	ids := []string{"event-z", "event-a", "event-m"}
	storedPayloads := make(map[string][]byte, len(ids))
	for index, id := range ids {
		event := domain.Event{
			ID: id, ClientID: "vesyoloe", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
			Source:   domain.SourceRecord{ID: 42, UUID: "legacy-source"},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
			Content: "блок", ContentDigest: "digest-" + id,
		}
		plain, marshalErr := json.Marshal(event)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		payload, protectErr := protectDatabaseValue(plain)
		if protectErr != nil {
			t.Fatal(protectErr)
		}
		storedPayloads[id] = append([]byte(nil), payload...)
		if _, err := db.ExecContext(ctx, `
			INSERT INTO client_events(
				event_id, source_uuid, source_version, stream_key, content_digest,
				payload_json, state, next_attempt_at, expires_at, created_at
			) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?)`,
			id, event.Source.UUID, now.Format(time.RFC3339Nano), "source:"+event.Source.UUID,
			event.ContentDigest, payload, now.Format(time.RFC3339Nano),
			now.Add(time.Hour).Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)); err != nil {
			t.Fatalf("legacy insert %d: %v", index, err)
		}
	}
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	var previousSequence int64
	for _, id := range ids {
		var sequence int64
		var payload []byte
		var state string
		var attempts int
		if err := db.QueryRowContext(ctx, `
			SELECT enqueue_seq, payload_json, state, attempts
			FROM client_events WHERE event_id=?`, id).Scan(&sequence, &payload, &state, &attempts); err != nil {
			t.Fatal(err)
		}
		if sequence <= previousSequence || !bytes.Equal(payload, storedPayloads[id]) || state != "pending" || attempts != 0 {
			t.Fatalf("миграция %s: seq=%d previous=%d payload_equal=%v state=%s attempts=%d",
				id, sequence, previousSequence, bytes.Equal(payload, storedPayloads[id]), state, attempts)
		}
		previousSequence = sequence
	}
	for index, want := range ids {
		pending, pendingErr := queue.Pending(ctx, 1, now.Add(time.Minute))
		if pendingErr != nil || len(pending) != 1 || pending[0].Event.ID != want {
			t.Fatalf("миграция, шаг %d: got=%#v want=%s err=%v", index, pending, want, pendingErr)
		}
		if err := queue.MarkAcknowledged(ctx, want, now.Add(time.Duration(index+1)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM client_events WHERE event_id=?`, ids[len(ids)-1]); err != nil {
		t.Fatal(err)
	}
	if err := queue.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	newEvent := domain.Event{
		ID: "event-new", ClientID: "vesyoloe", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{ID: 42, UUID: "legacy-source"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "новый блок", ContentDigest: "digest-new",
	}
	if inserted, enqueueErr := queue.Enqueue(ctx, newEvent); enqueueErr != nil || !inserted {
		t.Fatalf("вставить после удаления и VACUUM: inserted=%v err=%v", inserted, enqueueErr)
	}
	var newSequence int64
	if err := db.QueryRowContext(ctx, `SELECT enqueue_seq FROM client_events WHERE event_id=?`, newEvent.ID).Scan(&newSequence); err != nil {
		t.Fatal(err)
	}
	if newSequence <= previousSequence {
		t.Fatalf("AUTOINCREMENT переиспользовал номер: got=%d previous_max=%d", newSequence, previousSequence)
	}
}

// Перестройка таблицы выполняется одной транзакцией. Даже если обновление
// оборвётся после переноса данных, SQLite должен вернуть схему и строки v10.
func TestClientQueueMigrationFailureRollsBackLegacyTable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations := clientMigrationPlan()
	if err := Migrate(ctx, db, migrations[:len(migrations)-1]); err != nil {
		t.Fatal(err)
	}
	payload := []byte{0, 1, 2, 3, 0xff}
	createdAt := now.Add(-time.Hour).Format(time.RFC3339Nano)
	nextAttemptAt := now.Add(time.Minute).Format(time.RFC3339Nano)
	expiresAt := now.Add(time.Hour).Format(time.RFC3339Nano)
	uploadedAt := now.Add(-time.Minute).Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO client_events(
			event_id, source_uuid, source_version, stream_key, content_digest,
			payload_json, state, attempts, next_attempt_at, expires_at,
			telegram_message_id, center_transport_message_id, last_error,
			created_at, uploaded_at, acknowledged_at, expired_at
		) VALUES(?, ?, ?, ?, ?, ?, 'uploaded', 3, ?, ?, 123, 456, ?, ?, ?, NULL, NULL)`,
		"legacy-event", "legacy-source", "legacy-version", "source:legacy-source",
		"legacy-digest", payload, nextAttemptAt, expiresAt, "temporary failure",
		createdAt, uploadedAt); err != nil {
		t.Fatal(err)
	}

	brokenPlan := append([]string(nil), migrations...)
	brokenPlan[len(brokenPlan)-1] += "\nSELECT * FROM migration_failure_missing_table;"
	if err := Migrate(ctx, db, brokenPlan); err == nil || !strings.Contains(err.Error(), "migration_failure_missing_table") {
		t.Fatalf("ожидалась искусственная ошибка миграции, получено: %v", err)
	}

	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != len(migrations)-1 {
		t.Fatalf("ошибочная миграция записана в журнал: got=%d want=%d", version, len(migrations)-1)
	}
	var renamedTableCount int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type='table' AND name='client_events_v10'`).Scan(&renamedTableCount); err != nil {
		t.Fatal(err)
	}
	if renamedTableCount != 0 {
		t.Fatalf("после отката осталась временная таблица: %d", renamedTableCount)
	}
	columns, err := db.QueryContext(ctx, `PRAGMA table_info(client_events)`)
	if err != nil {
		t.Fatal(err)
	}
	hasEnqueueSequence := false
	for columns.Next() {
		var id, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := columns.Scan(&id, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			columns.Close()
			t.Fatal(err)
		}
		if name == "enqueue_seq" {
			hasEnqueueSequence = true
		}
	}
	if err := columns.Close(); err != nil {
		t.Fatal(err)
	}
	if hasEnqueueSequence {
		t.Fatal("после отката в схеме v10 остался enqueue_seq")
	}
	var matchingRows int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM client_events
		WHERE event_id='legacy-event'
		  AND source_uuid='legacy-source'
		  AND source_version='legacy-version'
		  AND stream_key='source:legacy-source'
		  AND content_digest='legacy-digest'
		  AND payload_json=?
		  AND state='uploaded'
		  AND attempts=3
		  AND next_attempt_at=?
		  AND expires_at=?
		  AND telegram_message_id=123
		  AND center_transport_message_id=456
		  AND last_error='temporary failure'
		  AND created_at=?
		  AND uploaded_at=?
		  AND acknowledged_at IS NULL
		  AND expired_at IS NULL`,
		payload, nextAttemptAt, expiresAt, createdAt, uploadedAt).Scan(&matchingRows); err != nil {
		t.Fatal(err)
	}
	if matchingRows != 1 {
		t.Fatalf("исходная строка изменилась при откате: count=%d", matchingRows)
	}
	var integrity string
	if err := db.QueryRowContext(ctx, `PRAGMA integrity_check`).Scan(&integrity); err != nil {
		t.Fatal(err)
	}
	if integrity != "ok" {
		t.Fatalf("integrity_check после отката: %s", integrity)
	}
}

func TestClientQueueSeedsBlockBaselinesFromLegacyRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	legacy := domain.Event{
		ID: "legacy", ClientID: "textile", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{ID: 386, UUID: "source-386", UpdatedAt: "2026-10-27 15:59:29"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "15.57\n-Первый\n15.59\n-Второй", ContentDigest: "legacy-digest",
	}
	if inserted, err := queue.Enqueue(ctx, legacy); err != nil || !inserted {
		t.Fatalf("legacy enqueue: inserted=%v err=%v", inserted, err)
	}
	makeBlock := func(id, uuid, version string) domain.Event {
		text := version + "\n-Блок"
		return domain.Event{
			ID: id, ClientID: "textile", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
			Source: legacy.Source, Position: legacy.Position, Mode: legacy.Mode,
			Content: text, ContentDigest: "digest-" + uuid, SourceBlockUUID: uuid,
			Blocks: []domain.ContentBlock{{UUID: uuid, Text: text, UpdatedAt: version}},
		}
	}
	blocks := []domain.Event{
		makeBlock("block-event-a", "block-a", "2026-10-27 15:57:00"),
		makeBlock("block-event-b", "block-b", "2026-10-27 15:59:29"),
	}
	migrated, err := queue.SeedBlockBaselinesFromLegacy(ctx, "source-386", legacy.Source.UpdatedAt, blocks)
	if err != nil || !migrated {
		t.Fatalf("поблочная база не создана: migrated=%v err=%v", migrated, err)
	}
	for _, event := range blocks {
		if inserted, err := queue.Enqueue(ctx, event); err != nil || inserted {
			t.Fatalf("старый блок переиздан: %s inserted=%v err=%v", event.SourceBlockUUID, inserted, err)
		}
	}
}

func TestClientQueueSuppressesOnlyProvenArchiveReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	openQueue := func(t *testing.T) (*sql.DB, *ClientQueue) {
		t.Helper()
		db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
		if err != nil {
			t.Fatal(err)
		}
		queue, err := NewClientQueue(ctx, db)
		if err != nil {
			db.Close()
			t.Fatal(err)
		}
		return db, queue
	}
	makeEvent := func(id, blockUUID, version, digest string) domain.Event {
		text := "19.40\n-" + blockUUID
		return domain.Event{
			ID: id, ClientID: "textile", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
			Source:   domain.SourceRecord{ID: 389, UUID: "source-389", UpdatedAt: "2026-10-27T19:45:00Z"},
			Position: domain.Position{UnitName: "1 мб 155 омбр"}, Mode: domain.SourceModeNormal,
			Content: text, ContentDigest: digest, SourceBlockUUID: blockUUID,
			Blocks: []domain.ContentBlock{{UUID: blockUUID, Text: text, UpdatedAt: version}},
		}
	}
	archive := []domain.Event{
		makeEvent("archive-a", "legacy-a", "2026-10-27T19:45:00Z", "archive-digest-a"),
		makeEvent("archive-b", "legacy-b", "2026-10-27T19:45:00Z", "archive-digest-b"),
	}

	t.Run("complete cleared history", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		active := []domain.Event{
			makeEvent("active-a", "block-a", "2026-10-27T19:40:00Z", "active-digest-a"),
			makeEvent("active-b", "block-b", "2026-10-27T19:45:00Z", "active-digest-b"),
		}
		for _, event := range active {
			if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
				t.Fatalf("активный блок не сохранён: inserted=%v err=%v", inserted, err)
			}
			if err := queue.MarkAcknowledged(ctx, event.ID, now); err != nil {
				t.Fatal(err)
			}
		}
		if removed, err := queue.ClearTerminal(ctx, "acknowledged"); err != nil || removed != 2 {
			t.Fatalf("история не очищена: removed=%d err=%v", removed, err)
		}
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(
			ctx, "source-389", "2026-10-27T19:45:00Z", archive,
		)
		if err != nil || !matched {
			t.Fatalf("доказанный архивный повтор не распознан: matched=%v err=%v", matched, err)
		}
		for _, event := range archive {
			if inserted, err := queue.Enqueue(ctx, event); err != nil || inserted {
				t.Fatalf("архивный дубль попал в очередь: inserted=%v err=%v", inserted, err)
			}
		}
	})

	t.Run("partial history is not suppressed", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		partial := makeEvent("active-a", "block-a", "2026-10-27T19:45:00Z", "active-digest-a")
		if inserted, err := queue.Enqueue(ctx, partial); err != nil || !inserted {
			t.Fatal(err)
		}
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(
			ctx, "source-389", "2026-10-27T19:45:00Z", archive,
		)
		if err != nil || matched {
			t.Fatalf("неполная история ошибочно подавлена: matched=%v err=%v", matched, err)
		}
	})

	t.Run("missing latest version is not suppressed", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		for _, event := range []domain.Event{
			makeEvent("active-a", "block-a", "2026-10-27T19:39:00Z", "active-digest-a"),
			makeEvent("active-b", "block-b", "2026-10-27T19:40:00Z", "active-digest-b"),
		} {
			if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
				t.Fatal(err)
			}
		}
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(
			ctx, "source-389", "2026-10-27T19:45:00Z", archive,
		)
		if err != nil || matched {
			t.Fatalf("неизвестная последняя версия ошибочно подавлена: matched=%v err=%v", matched, err)
		}
	})

	t.Run("partial logical version is recovered", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		const first = "time:d1:0003:occurrence-a"
		const second = "time:d1:0004:occurrence-b"
		const oldVersion = "2026-10-27T19:44:00Z"
		const newVersion = "2026-10-27T19:45:00Z"
		for _, event := range []domain.Event{
			makeEvent("old-a", first, oldVersion, "old-digest-a"),
			makeEvent("old-b", second, oldVersion, "old-digest-b"),
			makeEvent("new-a", first, newVersion, "new-digest-a"),
		} {
			if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
				t.Fatalf("не удалось подготовить историю %s: inserted=%v err=%v", event.ID, inserted, err)
			}
		}
		archive := []domain.Event{
			makeEvent("archive-a", first, newVersion, "new-digest-a"),
			makeEvent("archive-b", second, newVersion, "new-digest-b"),
		}
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(ctx, "source-389", newVersion, archive)
		if err != nil || matched {
			t.Fatalf("частично записанная версия подавлена: matched=%v err=%v", matched, err)
		}
		if inserted, err := queue.Enqueue(ctx, archive[0]); err != nil || inserted {
			t.Fatalf("уже записанный блок продублирован: inserted=%v err=%v", inserted, err)
		}
		if inserted, err := queue.Enqueue(ctx, archive[1]); err != nil || !inserted {
			t.Fatalf("пропущенный при аварии блок потерян: inserted=%v err=%v", inserted, err)
		}
	})

	t.Run("archive metadata loss does not duplicate a queued logical block", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		const block = "time:d1:0003:occurrence-a"
		const version = "2026-10-27T19:45:00Z"
		active := makeEvent("active-a", block, version, "")
		active.Position.CatalogUUID = "catalog-a"
		active.ContentDigestVersion = domain.ContentDigestVersionCurrent
		active.ContentDigest = active.CanonicalContentDigest()
		if inserted, err := queue.Enqueue(ctx, active); err != nil || !inserted {
			t.Fatalf("не удалось подготовить активный блок: inserted=%v err=%v", inserted, err)
		}
		archive := active
		archive.ID = "archive-a"
		archive.Position.CatalogUUID = ""
		archive.Mode = domain.SourceModeUnknown
		archive.ContentDigest = archive.CanonicalContentDigest()
		if archive.ContentDigest == active.ContentDigest {
			t.Fatal("потеря режима и каталога не изменила отпечаток")
		}
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(ctx, "source-389", version, []domain.Event{archive})
		if err != nil || !matched {
			t.Fatalf("неизменённый архивный блок не распознан: matched=%v err=%v", matched, err)
		}
		if inserted, err := queue.Enqueue(ctx, archive); err != nil || inserted {
			t.Fatalf("архивный блок продублирован: inserted=%v err=%v", inserted, err)
		}
	})

	t.Run("restored snapshot metadata suppresses cleared archive", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		const block = "time:d1:0003:occurrence-a"
		const version = "2026-10-27T19:45:00Z"
		active := makeEvent("active-a", block, version, "")
		active.Position.CatalogUUID = "catalog-a"
		active.ContentDigestVersion = domain.ContentDigestVersionCurrent
		active.ContentDigest = active.CanonicalContentDigest()
		if inserted, err := queue.Enqueue(ctx, active); err != nil || !inserted {
			t.Fatalf("активный блок не сохранён: inserted=%v err=%v", inserted, err)
		}
		if err := queue.MarkAcknowledged(ctx, active.ID, now); err != nil {
			t.Fatal(err)
		}
		if removed, err := queue.ClearTerminal(ctx, "acknowledged"); err != nil || removed != 1 {
			t.Fatalf("очередь не очищена: removed=%d err=%v", removed, err)
		}
		archive := active
		archive.ID = "archive-a"
		archive.ContentDigest = archive.CanonicalContentDigest()
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(ctx, "source-389", version, []domain.Event{archive})
		if err != nil || !matched {
			t.Fatalf("очищенная версия не распознана: matched=%v err=%v", matched, err)
		}
		if inserted, err := queue.Enqueue(ctx, archive); err != nil || inserted {
			t.Fatalf("очищенный архивный блок продублирован: inserted=%v err=%v", inserted, err)
		}
	})

	t.Run("same logical version with different digest is not suppressed", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		const block = "time:d1:0003:occurrence-a"
		const version = "2026-10-27T19:45:00Z"
		if inserted, err := queue.Enqueue(ctx, makeEvent("active-a", block, version, "active-digest")); err != nil || !inserted {
			t.Fatalf("не удалось подготовить историю: inserted=%v err=%v", inserted, err)
		}
		archive := makeEvent("archive-a", block, version, "changed-digest")
		archive.Content = "19.40\n-Исправленный текст"
		archive.Blocks[0].Text = archive.Content
		matched, err := queue.SeedArchiveBaselinesFromSeenBlocks(ctx, "source-389", version, []domain.Event{archive})
		if err != nil || matched {
			t.Fatalf("неизвестный отпечаток подавлен: matched=%v err=%v", matched, err)
		}
		if inserted, err := queue.Enqueue(ctx, archive); err != nil || !inserted {
			t.Fatalf("новая редакция потеряна: inserted=%v err=%v", inserted, err)
		}
	})

	t.Run("legacy metadata requires every retained payload", func(t *testing.T) {
		db, queue := openQueue(t)
		defer db.Close()
		const version = "2026-10-27T19:45:00Z"
		first := makeEvent("active-a", "time:d1:0003:occurrence-a", version, "digest-a")
		second := makeEvent("active-b", "time:d1:0004:occurrence-b", version, "digest-b")
		first.Position.CatalogUUID, second.Position.CatalogUUID = "catalog-a", "catalog-a"
		item := application.SourceItem{Record: first.Source, Position: first.Position, Mode: domain.SourceModeUnknown, Archived: true}
		item.Position.CatalogUUID = ""
		snapshot := application.SourceTimeSnapshot{SourceID: first.Source.ID, SourceUUID: first.Source.UUID,
			Blocks: []application.SourceTimeStateEntry{
				{Active: true, Block: first.Blocks[0]}, {Active: true, Block: second.Blocks[0]},
			},
		}
		if inserted, err := queue.Enqueue(ctx, first); err != nil || !inserted {
			t.Fatalf("первый блок не сохранён: inserted=%v err=%v", inserted, err)
		}
		if _, _, found, err := queue.RecoverArchivedTimeMetadata(ctx, item, snapshot); err != nil || found {
			t.Fatalf("частичная история принята: found=%v err=%v", found, err)
		}
		if inserted, err := queue.Enqueue(ctx, second); err != nil || !inserted {
			t.Fatalf("второй блок не сохранён: inserted=%v err=%v", inserted, err)
		}
		mode, catalog, found, err := queue.RecoverArchivedTimeMetadata(ctx, item, snapshot)
		if err != nil || !found || mode != domain.SourceModeNormal || catalog != "catalog-a" {
			t.Fatalf("метаданные всех блоков не восстановлены: mode=%q catalog=%q found=%v err=%v", mode, catalog, found, err)
		}
		item.Record.UpdatedAt = "2026-10-27T19:46:00Z"
		if _, _, found, err := queue.RecoverArchivedTimeMetadata(ctx, item, snapshot); err != nil || found {
			t.Fatalf("старая версия записи ошибочно подтвердила новый маршрут: found=%v err=%v", found, err)
		}
	})
}

// An unreadable retained event must stop the EAVC scan before its cursor moves.
// Otherwise an archived record can be skipped permanently when the next scan
// starts from a newer source timestamp.
func TestArchivedMetadataDecodeFailureKeepsCursorForRetry(t *testing.T) {
	testArchivedMetadataReadFailure(t, []byte("unreadable payload"), "открыть сохранённое событие", false)
}

func TestArchivedMetadataInvalidJSONQuarantinesAndAdvancesCursor(t *testing.T) {
	protected, err := protectDatabaseValue([]byte("{"))
	if err != nil {
		t.Fatal(err)
	}
	testArchivedMetadataReadFailure(t, protected, "", true)
}

func testArchivedMetadataReadFailure(t *testing.T, badPayload []byte, wantError string, invalidJSON bool) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 31, 10, 0, 0, 0, time.UTC)
	version := now.Format(time.RFC3339Nano)
	content := "10:00\n-Архивный перехват"
	block := domain.ContentBlock{UUID: "active-block", Key: "10:00", Text: content, UpdatedAt: version}
	active := domain.Event{
		ID: "active-archive-block", ClientID: "textile",
		CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{ID: 389, UUID: "source-archive", UpdatedAt: version},
		Position: domain.Position{CatalogUUID: "catalog-a", UnitName: "Игровая точка", Frequency: "119.001", GroupCode: "group-a"},
		Mode:     domain.SourceModeNormal, Content: content, SourceBlockUUID: block.UUID,
		Blocks: []domain.ContentBlock{block},
	}
	active.ContentDigest = active.CanonicalContentDigest()
	if inserted, err := queue.Enqueue(ctx, active); err != nil || !inserted {
		t.Fatalf("не удалось сохранить исходный блок: inserted=%v err=%v", inserted, err)
	}
	snapshot := application.SourceTimeSnapshot{
		SourceID: active.Source.ID, SourceUUID: active.Source.UUID, SourceVersion: version,
		Blocks: []application.SourceTimeStateEntry{{
			OccurrenceID: "occurrence-a", CanonicalTime: "10:00", Active: true, Block: block,
		}},
	}
	if err := queue.SaveSourceTimeState(ctx, "uuid:source-archive", application.SourceTimeStateRecord{Snapshot: &snapshot}); err != nil {
		t.Fatal(err)
	}
	archive := application.SourceItem{
		Record: active.Source, Position: active.Position, Mode: domain.SourceModeUnknown,
		Content: content, Blocks: []domain.ContentBlock{{Key: "10:00", Text: content, UpdatedAt: version}},
		Archived: true, ArchiveMetadataAmbiguous: true, ObservedAt: now,
	}
	archive.Position.CatalogUUID = ""
	next := application.Cursor{UpdatedAt: version, ID: active.Source.ID}
	items := []application.SourceItem{archive}
	if invalidJSON {
		later := application.SourceItem{
			Record:   domain.SourceRecord{ID: 390, UUID: "source-later", UpdatedAt: version},
			Position: active.Position, Mode: domain.SourceModeNormal,
			Content: "10:01\n-Следующий перехват", ObservedAt: now,
		}
		items = append(items, later)
		next.ID = later.Record.ID
	}
	service := application.ClientService{
		ClientID: "textile", Source: archiveCursorSource{items: items, next: next},
		Queue: queue, State: queue, TimeState: queue, BatchSize: 10,
		CatchUpExisting: true, Now: func() time.Time { return now },
	}
	var protected []byte
	if err := db.QueryRowContext(ctx, "SELECT payload_json FROM client_events WHERE event_id=?", active.ID).Scan(&protected); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE client_events SET payload_json=? WHERE event_id=?", badPayload, active.ID); err != nil {
		t.Fatal(err)
	}
	if invalidJSON {
		queued, err := service.CollectOnce(ctx)
		if err != nil || queued != 1 {
			t.Fatalf("повреждённый архив остановил следующую запись: queued=%d err=%v", queued, err)
		}
		if cursor, err := queue.Cursor(ctx, "eavc"); err != nil || cursor != next {
			t.Fatalf("курсор не прошёл повреждённый архив: cursor=%+v err=%v", cursor, err)
		}
		state, exists, err := queue.LoadSourceTimeState(ctx, "uuid:source-archive")
		if err != nil || !exists || state.Conflict == nil || state.Conflict.Kind != "archive_metadata_unverifiable" ||
			state.Snapshot == nil || state.Snapshot.SourcePosition != nil {
			t.Fatalf("архив не изолирован явным конфликтом: state=%+v exists=%v err=%v", state, exists, err)
		}
		var archiveEvents, laterEvents int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_events WHERE source_uuid LIKE 'source-archive|block:%' AND event_id<>?", active.ID).Scan(&archiveEvents); err != nil {
			t.Fatal(err)
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_events WHERE source_uuid='source-later'").Scan(&laterEvents); err != nil {
			t.Fatal(err)
		}
		if archiveEvents != 0 || laterEvents != 1 {
			t.Fatalf("архивное событие отправлено или следующий перехват потерян: archive=%d later=%d", archiveEvents, laterEvents)
		}
		return
	}
	if _, err := service.CollectOnce(ctx); err == nil || !strings.Contains(err.Error(), wantError) {
		t.Fatalf("ошибка чтения архивной истории проигнорирована или неверно классифицирована: %v", err)
	}
	if cursor, err := queue.Cursor(ctx, "eavc"); err != nil || cursor != (application.Cursor{}) {
		t.Fatalf("курсор сдвинулся после ошибки: cursor=%+v err=%v", cursor, err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE client_events SET payload_json=? WHERE event_id=?", protected, active.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CollectOnce(ctx); err != nil {
		t.Fatalf("повтор после восстановления не обработан: %v", err)
	}
	if cursor, err := queue.Cursor(ctx, "eavc"); err != nil || cursor != next {
		t.Fatalf("курсор не продвинулся после успешного повтора: cursor=%+v err=%v", cursor, err)
	}
	state, exists, err := queue.LoadSourceTimeState(ctx, "uuid:source-archive")
	if err != nil || !exists || state.Conflict != nil || state.Snapshot == nil || state.Snapshot.SourcePosition == nil {
		t.Fatalf("маршрут архивного блока не восстановлен: state=%+v exists=%v err=%v", state, exists, err)
	}
}

type archiveCursorSource struct {
	items []application.SourceItem
	next  application.Cursor
}

func (source archiveCursorSource) Scan(_ context.Context, after application.Cursor, _ int) ([]application.SourceItem, application.Cursor, error) {
	if after == source.next {
		return nil, after, nil
	}
	return source.items, source.next, nil
}

func TestClientQueueCleansRC88ExpiredMigrationArtifact(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	legacy := domain.Event{
		ID: "legacy", ClientID: "textile", CreatedAt: now, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{ID: 386, UUID: "source-386", UpdatedAt: "2026-10-27 15:59:29"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "15.59\n-Старый перехват", ContentDigest: "legacy-digest",
	}
	if inserted, err := queue.Enqueue(ctx, legacy); err != nil || !inserted {
		t.Fatal(err)
	}
	artifact := domain.Event{
		ID: "rc88-artifact", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		SourceAt: now.Add(-time.Hour), ExpiresAt: now.Add(-30 * time.Minute), FreshnessIssue: domain.FreshnessExpired,
		Source: legacy.Source, Position: legacy.Position, Mode: legacy.Mode,
		Content: "15.59\n-Старый перехват", ContentDigest: "block-digest", SourceBlockUUID: "block-a",
		Blocks: []domain.ContentBlock{{UUID: "block-a", Text: "15.59\n-Старый перехват", UpdatedAt: "2026-10-27 15:59:29"}},
	}
	if inserted, err := queue.Enqueue(ctx, artifact); err != nil || !inserted {
		t.Fatalf("не создан тестовый остаток rc88: inserted=%v err=%v", inserted, err)
	}
	if _, err := NewClientQueue(ctx, db); err != nil {
		t.Fatalf("повторное открытие очереди: %v", err)
	}
	var expired, baseline int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_events WHERE event_id=?", artifact.ID).Scan(&expired); err != nil || expired != 0 {
		t.Fatalf("ложный остаток не очищен: count=%d err=%v", expired, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM client_cleared_events WHERE source_uuid=?`, artifact.SourceLineageKey()).Scan(&baseline); err != nil || baseline != 1 {
		t.Fatalf("идентичность очищенного блока потеряна: count=%d err=%v", baseline, err)
	}
}

// Пока ранняя версия записи ждёт повтора, более поздняя версия той же записи
// не должна её обогнать, в том числе после исправления названия подразделения.
func TestClientQueueDoesNotLetLaterEventOvertakeRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	first := domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		ExpiresAt: now.Add(time.Minute), Source: domain.SourceRecord{UUID: "source-1"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "первое", ContentDigest: "digest-1",
	}
	second := first
	second.ID = "event-2"
	second.Position.UnitName = "исправленная позиция"
	second.Content = "второе"
	second.ContentDigest = "digest-2"
	second.CreatedAt = now.Add(time.Second)
	second.ExpiresAt = now.Add(30 * time.Minute)
	if _, err := queue.Enqueue(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkRetry(ctx, first.ID, now.Add(time.Minute), "временная ошибка"); err != nil {
		t.Fatal(err)
	}
	jobs, err := queue.Pending(ctx, 10, now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("второе событие обогнало первое: %#v", jobs)
	}
	if _, err := queue.ExpireDue(ctx, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	jobs, err = queue.Pending(ctx, 10, now.Add(2*time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != second.ID {
		t.Fatalf("после истечения первого ожидалось второе: %#v, %v", jobs, err)
	}
}

// Если одно подразделение временно не отправляется, первое событие другого
// подразделения всё равно должно выйти из очереди этой же точки.
func TestClientQueueDoesNotBlockAnotherPosition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	first := domain.Event{
		ID: "blocked", ClientID: "textile", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{UUID: "source-1"},
		Position: domain.Position{UnitName: "первое подразделение"},
		Mode:     domain.SourceModeNormal, Content: "первое", ContentDigest: "digest-1",
	}
	second := first
	second.ID = "independent"
	second.Source.UUID = "source-2"
	second.Position.UnitName = "другое подразделение"
	second.ContentDigest = "digest-2"
	second.CreatedAt = now.Add(time.Second)
	if _, err := queue.Enqueue(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkRetry(ctx, first.ID, now.Add(time.Minute), "нет связи"); err != nil {
		t.Fatal(err)
	}
	jobs, err := queue.Pending(ctx, 4, now.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Event.ID != second.ID {
		t.Fatalf("ожидалось независимое событие, получено %#v", jobs)
	}
}

// Новые события независимых подразделений важнее профилактического повтора
// уже загруженного пакета. Иначе короткий интервал повтора забивает Telegram
// дубликатами и вытесняет свежую очередь при большом потоке.
func TestClientQueuePrioritizesPendingOverUploadedRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	uploaded := domain.Event{
		ID: "uploaded", ClientID: "point", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{UUID: "source-uploaded"},
		Position: domain.Position{UnitName: "первое подразделение"}, Mode: domain.SourceModeNormal,
		Content: "загружено", ContentDigest: "digest-uploaded",
	}
	pending := uploaded
	pending.ID = "pending"
	pending.Source.UUID = "source-pending"
	pending.Position.UnitName = "второе подразделение"
	pending.Content = "ожидает"
	pending.ContentDigest = "digest-pending"
	pending.CreatedAt = now.Add(time.Second)
	if _, err := queue.Enqueue(ctx, uploaded); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkUploaded(ctx, uploaded.ID, 77); err != nil {
		t.Fatal(err)
	}
	jobs, err := queue.Pending(ctx, 1, time.Now().UTC().Add(uploadedRetryBaseInterval+time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Event.ID != pending.ID {
		t.Fatalf("свежая очередь вытеснена повтором: %#v", jobs)
	}
}

func TestClientQueueDoesNotRetryUploadedTooFrequently(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "uploaded", ClientID: "point", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Source: domain.SourceRecord{UUID: "source-uploaded"}, Position: domain.Position{UnitName: "позиция"},
		Mode: domain.SourceModeNormal, Content: "данные", ContentDigest: "digest-uploaded",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkUploaded(ctx, event.ID, 77); err != nil {
		t.Fatal(err)
	}
	jobs, err := queue.Pending(ctx, 10, time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 0 {
		t.Fatalf("загруженное событие повторено раньше срока: %#v", jobs)
	}
	jobs, err = queue.Pending(ctx, 10, time.Now().UTC().Add(uploadedRetryBaseInterval+time.Minute))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != event.ID {
		t.Fatalf("событие не вернулось для страховочного повтора: %#v, %v", jobs, err)
	}
}

func TestUploadedRetryDelayBacksOffAndCaps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 20 * time.Minute},
		{attempt: 1, want: 20 * time.Minute},
		{attempt: 2, want: 40 * time.Minute},
		{attempt: 3, want: 80 * time.Minute},
		{attempt: 4, want: 2 * time.Hour},
		{attempt: 20, want: 2 * time.Hour},
	}
	for _, test := range tests {
		if got := uploadedRetryDelay(test.attempt); got != test.want {
			t.Fatalf("попытка %d: получено %s, ожидалось %s", test.attempt, got, test.want)
		}
	}
}

// Обновление ранней сборки пересчитывает потоки из содержимого событий и восстанавливает
// порядок версий одной записи даже при изменении её названия.
func TestClientQueueRepairsLegacyStreamKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	path := filepath.Join(t.TempDir(), "state.db")
	db, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	first := domain.Event{
		ID: "legacy-1", ClientID: "point", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Source: domain.SourceRecord{UUID: "source-1"}, Position: domain.Position{UnitName: "одно подразделение"},
		Mode: domain.SourceModeNormal, Content: "первое", ContentDigest: "digest-1",
	}
	second := first
	second.ID, second.ContentDigest = "legacy-2", "digest-2"
	second.Position.UnitName = "исправленное подразделение"
	second.CreatedAt = now.Add(time.Second)
	if _, err := queue.Enqueue(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, second); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE client_events SET stream_key='name:legacy'"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version=6"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err = NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkRetry(ctx, first.ID, now.Add(time.Minute), "нет связи"); err != nil {
		t.Fatal(err)
	}
	pending, err := queue.Pending(ctx, 10, now.Add(10*time.Second))
	if err != nil || len(pending) != 0 {
		t.Fatalf("старая очередь не восстановила порядок: %#v, %v", pending, err)
	}
}

// Новая редакция сохраняется даже тогда, когда оператор вернул текст к одному из прежних вариантов.
func TestClientQueueAllowsContentToReturnToEarlierValueInNewSourceVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "event-1", ClientID: "client", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{UUID: "source", UpdatedAt: now.Format(time.RFC3339Nano)},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "значение А", ContentDigest: "digest-a",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	returned := event
	returned.ID = "event-2"
	returned.CreatedAt = now.Add(2 * time.Minute)
	returned.Source.UpdatedAt = now.Add(2 * time.Minute).Format(time.RFC3339Nano)
	if _, err := queue.Enqueue(ctx, returned); err != nil {
		t.Fatal(err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats.Pending != 2 {
		t.Fatalf("возврат к прежнему тексту потерян: %+v, %v", stats, err)
	}
}

// Очистка удаляет только давно завершённые записи и сохраняет свежие, ожидающие и ошибочные события.
func TestClientQueuePurgesOnlyOldTerminalEvents(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id string) domain.Event {
		return domain.Event{
			ID: id, ClientID: "client", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
			Source:   domain.SourceRecord{UUID: "source-" + id},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
			Content: id, ContentDigest: "digest-" + id,
		}
	}
	for _, id := range []string{"old", "recent", "pending"} {
		if _, err := queue.Enqueue(ctx, makeEvent(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := queue.MarkAcknowledged(ctx, "old", now.Add(-40*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkAcknowledged(ctx, "recent", now); err != nil {
		t.Fatal(err)
	}
	removed, err := queue.PurgeTerminal(ctx, now.Add(-30*24*time.Hour))
	if err != nil || removed != 1 {
		t.Fatalf("очистка: removed=%d err=%v", removed, err)
	}
	if inserted, err := queue.Enqueue(ctx, makeEvent("old")); err != nil || inserted {
		t.Fatalf("автоматически очищенная версия вернулась: inserted=%v err=%v", inserted, err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats.Acknowledged != 1 || stats.Pending != 1 {
		t.Fatalf("неверные остатки очереди: %+v err=%v", stats, err)
	}
}

// Запоздалое или повторное подтверждение сервера безопасно завершает нужное событие и не портит очередь.
func TestClientQueueConsumesLateAndDuplicateAcknowledgements(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "late", ClientID: "client", CreatedAt: now, ExpiresAt: now.Add(time.Minute),
		Source:   domain.SourceRecord{UUID: "source-late"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "данные", ContentDigest: "digest-late",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkExpired(ctx, event.ID, now, domain.FreshnessExpired); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []func() error{
		func() error { return queue.MarkExpired(ctx, event.ID, now, domain.FreshnessExpired) },
		func() error { return queue.MarkAcknowledged(ctx, event.ID, now) },
		func() error { return queue.MarkAcknowledged(ctx, "already-cleaned", now) },
	} {
		if err := operation(); err != nil {
			t.Fatalf("позднее подтверждение заблокировало offset: %v", err)
		}
	}
}

// Экран очереди показывает все этапы, но разрешает очищать только завершённые.
func TestClientQueuePagesAndProtectsActiveMessagesFromDeletion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	makeEvent := func(id string) domain.Event {
		return domain.Event{
			ID: id, ClientID: "client", CreatedAt: now, ObservedAt: now, SourceAt: now,
			ExpiresAt: now.Add(time.Hour), Source: domain.SourceRecord{UUID: "source-" + id},
			Position: domain.Position{UnitName: "позиция " + id}, Mode: domain.SourceModeNormal,
			Content: "текст " + id, ContentDigest: "digest-" + id,
		}
	}
	for _, id := range []string{"pending", "uploaded", "acknowledged", "expired"} {
		if _, err := queue.Enqueue(ctx, makeEvent(id)); err != nil {
			t.Fatal(err)
		}
	}
	if err := queue.MarkUploaded(ctx, "uploaded", 10); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkUploaded(ctx, "acknowledged", 11); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkAcknowledged(ctx, "acknowledged", now); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkExpired(ctx, "expired", now, domain.FreshnessExpired); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"pending", "uploaded", "acknowledged", "expired"} {
		items, total, err := queue.EventsPage(ctx, state, 20, 0)
		if err != nil || total != 1 || len(items) != 1 || items[0].Content != "текст "+state ||
			items[0].ContentKind != "text" || items[0].ContentNote == "" || items[0].SourceUUID != "source-"+state {
			t.Fatalf("неверная страница %s: total=%d items=%#v err=%v", state, total, items, err)
		}
		if (state == "uploaded" || state == "acknowledged") && items[0].TransportMessageID == nil {
			t.Fatalf("для %s потерян message_id транспорта: %#v", state, items[0])
		}
	}
	for _, state := range []string{"pending", "uploaded"} {
		if _, err := queue.ClearTerminal(ctx, state); err == nil {
			t.Fatalf("активное состояние %s удалось удалить", state)
		}
	}
	for _, state := range []string{"acknowledged", "expired"} {
		removed, err := queue.ClearTerminal(ctx, state)
		if err != nil || removed != 1 {
			t.Fatalf("завершённое состояние %s не очищено: removed=%d err=%v", state, removed, err)
		}
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats.Pending != 1 || stats.Uploaded != 1 || stats.Acknowledged != 0 || stats.Expired != 0 {
		t.Fatalf("очистка затронула активную очередь: %+v err=%v", stats, err)
	}
}

// После ручной очистки повторное чтение той же граничной версии EAVC не должно
// возвращать её в архив. Новая редакция той же исходной записи принимается.
func TestClientQueueDoesNotRestoreClearedTerminalVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "expired-original", ClientID: "client", CreatedAt: now, ObservedAt: now,
		SourceAt: now.Add(-time.Hour), ExpiresAt: now.Add(-30 * time.Minute),
		Source:   domain.SourceRecord{UUID: "source-cleared", UpdatedAt: "2026-10-27 10:00:00"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "исходный текст", ContentDigest: "digest-original",
	}
	if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
		t.Fatalf("поставить просроченную версию: inserted=%v err=%v", inserted, err)
	}
	if removed, err := queue.ClearTerminal(ctx, "expired"); err != nil || removed != 1 {
		t.Fatalf("очистить версию: removed=%d err=%v", removed, err)
	}
	repeated := event
	repeated.ID = "expired-repeat"
	if inserted, err := queue.Enqueue(ctx, repeated); err != nil || inserted {
		t.Fatalf("очищенная версия вернулась: inserted=%v err=%v", inserted, err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats.Expired != 0 {
		t.Fatalf("очищенный архив снова заполнен: %+v err=%v", stats, err)
	}

	edited := event
	edited.ID = "expired-edited"
	edited.Source.UpdatedAt = "2026-10-27 10:01:00"
	edited.Content = "исправленный текст"
	edited.ContentDigest = "digest-edited"
	if inserted, err := queue.Enqueue(ctx, edited); err != nil || !inserted {
		t.Fatalf("новая редакция ошибочно подавлена: inserted=%v err=%v", inserted, err)
	}
}

// Быстрый центр иногда подтверждает пакет до возврата Send на рабочей точке.
// Поздняя запись промежуточного состояния не должна давать ложную ошибку или
// возвращать событие из конечного состояния обратно в очередь.
func TestLateUploadDoesNotUndoAcknowledgement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "fast-ack", ClientID: "client", CreatedAt: now, ObservedAt: now, SourceAt: now,
		ExpiresAt: now.Add(time.Hour), Source: domain.SourceRecord{UUID: "source-fast-ack"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigest: "digest-fast-ack",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkAcknowledged(ctx, event.ID, now); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkUploaded(ctx, event.ID, 77); err != nil {
		t.Fatalf("поздняя отметка загрузки дала ошибку: %v", err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Acknowledged != 1 || stats.Pending != 0 || stats.Uploaded != 0 {
		t.Fatalf("подтверждение было отменено: %+v", stats)
	}
	items, total, err := queue.EventsPage(ctx, "acknowledged", 20, 0)
	if err != nil || total != 1 || len(items) != 1 {
		t.Fatalf("не удалось прочитать подтверждённое событие: total=%d items=%#v err=%v", total, items, err)
	}
	if items[0].TransportMessageID == nil || *items[0].TransportMessageID != 77 {
		t.Fatalf("быстро подтверждённое событие потеряло message_id транспорта: %#v", items[0])
	}
}

// Оператор может очистить подтверждённую запись в коротком окне между ответом
// центра и возвратом Telegram Send. Это не сетевая ошибка и не повод повторять
// уже доставленный пакет.
func TestLateUploadAfterTerminalRecordWasClearedIsSuccessful(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "fast-ack-cleared", ClientID: "client", CreatedAt: now, ObservedAt: now, SourceAt: now,
		ExpiresAt: now.Add(time.Hour), Source: domain.SourceRecord{UUID: "source-fast-ack-cleared"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigest: "digest-fast-ack-cleared",
	}
	if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
		t.Fatalf("поставить событие: inserted=%v err=%v", inserted, err)
	}
	if err := queue.MarkAcknowledged(ctx, event.ID, now); err != nil {
		t.Fatal(err)
	}
	if removed, err := queue.ClearTerminal(ctx, "acknowledged"); err != nil || removed != 1 {
		t.Fatalf("очистить подтверждённое событие: removed=%d err=%v", removed, err)
	}
	if err := queue.MarkUploaded(ctx, event.ID, 77); err != nil {
		t.Fatalf("поздняя отметка очищенной записи дала ложную ошибку: %v", err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats != (ClientStats{}) {
		t.Fatalf("очищенная запись вернулась в очередь: %+v err=%v", stats, err)
	}
}

func TestSignedTraceAcknowledgementSurvivesLateUploadAndRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "ack-before-send", ClientID: "client", CreatedAt: now, ObservedAt: now, SourceAt: now,
		ExpiresAt: now.Add(time.Hour), Source: domain.SourceRecord{UUID: "source-ack-before-send"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigest: "digest-ack-before-send",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	changed, err := queue.MarkServerDisposition(ctx, event.ID, now, "accepted", 123)
	if err != nil || !changed {
		t.Fatalf("подтверждение до ответа Send: changed=%v err=%v", changed, err)
	}
	if err := queue.MarkRetry(ctx, event.ID, now.Add(time.Minute), "sendDocument timeout"); err != nil {
		t.Fatalf("поздний тайм-аут после ACK дал ложную ошибку: %v", err)
	}
	if err := queue.MarkUploaded(ctx, event.ID, 456); err != nil {
		t.Fatalf("поздний ответ Send дал ошибку: %v", err)
	}
	changed, err = queue.MarkServerDisposition(ctx, event.ID, now.Add(time.Minute), "accepted", 999)
	if err != nil || changed {
		t.Fatalf("повтор ACK посчитан новым: changed=%v err=%v", changed, err)
	}
	items, _, err := queue.EventsPage(ctx, "acknowledged", 20, 0)
	if err != nil || len(items) != 1 || items[0].CenterMessageID == nil || *items[0].CenterMessageID != 123 ||
		items[0].UploadMessageID == nil || *items[0].UploadMessageID != 456 ||
		items[0].TransportMessageID == nil || *items[0].TransportMessageID != 123 {
		t.Fatalf("ID подтверждённого пакета потерян или заменён дублем: items=%+v err=%v", items, err)
	}
}

func TestLegacyAckCanBeBackfilledAndLateAcceptedOverridesLocalExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "legacy-backfill", ClientID: "client", CreatedAt: now, ObservedAt: now, SourceAt: now,
		ExpiresAt: now.Add(time.Hour), Source: domain.SourceRecord{UUID: "source-legacy-backfill"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigest: "digest-legacy-backfill",
	}
	if _, err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	changed, err := queue.MarkServerDisposition(ctx, event.ID, now, "accepted", 0)
	if err != nil || !changed {
		t.Fatalf("старый ACK/3 не принят: changed=%v err=%v", changed, err)
	}
	changed, err = queue.MarkServerDisposition(ctx, event.ID, now.Add(time.Second), "expired", 987)
	if err != nil || changed {
		t.Fatalf("противоречивый поздний expired ACK изменил принятое событие: changed=%v err=%v", changed, err)
	}
	intermediate, _, err := queue.EventsPage(ctx, "acknowledged", 20, 0)
	if err != nil || len(intermediate) != 1 || intermediate[0].CenterMessageID != nil {
		t.Fatalf("expired ACK ошибочно приписал ID принятому событию: items=%+v err=%v", intermediate, err)
	}
	changed, err = queue.MarkServerDisposition(ctx, event.ID, now.Add(time.Second), "accepted", 321)
	if err != nil || changed {
		t.Fatalf("поздний ACK/4 повторно посчитан: changed=%v err=%v", changed, err)
	}
	changed, err = queue.MarkServerDisposition(ctx, event.ID, now.Add(2*time.Second), "accepted", 0)
	if err != nil || changed {
		t.Fatalf("старый ACK/3 изменил терминальное состояние: changed=%v err=%v", changed, err)
	}
	items, _, err := queue.EventsPage(ctx, "acknowledged", 20, 0)
	if err != nil || len(items) != 1 || items[0].CenterMessageID == nil || *items[0].CenterMessageID != 321 || !items[0].StatusAt.Equal(now) {
		t.Fatalf("backfill стёр исходное время/ID: items=%+v err=%v", items, err)
	}
	changed, err = queue.MarkServerDisposition(ctx, "already-cleared", now, "accepted", 999)
	if err != nil || changed {
		t.Fatalf("неизвестный ACK должен быть безопасно пропущен: changed=%v err=%v", changed, err)
	}

	expired := event
	expired.ID, expired.Source.UUID, expired.ContentDigest = "expired-then-ack", "source-expired-then-ack", "digest-expired-then-ack"
	if _, err := queue.Enqueue(ctx, expired); err != nil {
		t.Fatal(err)
	}
	changed, err = queue.MarkServerDisposition(ctx, expired.ID, now.Add(30*time.Minute), "expired", 555)
	if err != nil || !changed {
		t.Fatalf("первый expired ACK не сохранён: changed=%v err=%v", changed, err)
	}
	changed, err = queue.MarkServerDisposition(ctx, expired.ID, now.Add(31*time.Minute), "accepted", 654)
	if err != nil || !changed {
		t.Fatalf("позднее достоверное accepted не исправило локальную просрочку: changed=%v err=%v", changed, err)
	}
	items, _, err = queue.EventsPage(ctx, "acknowledged", 20, 0)
	if err != nil || len(items) != 2 || items[0].EventID != expired.ID || items[0].Reason != "" ||
		items[0].CenterMessageID == nil || *items[0].CenterMessageID != 654 {
		t.Fatalf("поздний accepted не заменил expired ID/состояние: items=%+v err=%v", items, err)
	}
	locallyExpired := event
	locallyExpired.ID, locallyExpired.Source.UUID, locallyExpired.ContentDigest = "local-expired-then-ack", "source-local-expired", "digest-local-expired"
	if _, err := queue.Enqueue(ctx, locallyExpired); err != nil {
		t.Fatal(err)
	}
	if err := queue.MarkExpired(ctx, locallyExpired.ID, now.Add(30*time.Minute), domain.FreshnessExpired); err != nil {
		t.Fatal(err)
	}
	changed, err = queue.MarkServerDisposition(ctx, locallyExpired.ID, now.Add(31*time.Minute), "accepted", 777)
	if err != nil || !changed {
		t.Fatalf("поздний accepted после локальной просрочки не принят: changed=%v err=%v", changed, err)
	}
}

func TestClientClearedEventsArePrunedByAgeAndLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	insertCleared := func(uuid string, clearedAt time.Time) {
		t.Helper()
		if _, err := db.ExecContext(ctx, `
			INSERT INTO client_cleared_events(source_uuid, source_version, content_digest, cleared_at)
			VALUES(?, ?, ?, ?)`, uuid, "version", "digest", clearedAt.Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	insertCleared("too-old", now.Add(-31*24*time.Hour))
	insertCleared("recent-1", now.Add(-3*time.Hour))
	insertCleared("recent-2", now.Add(-2*time.Hour))
	insertCleared("recent-3", now.Add(-time.Hour))

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pruneClientClearedEvents(ctx, tx, now, 30*24*time.Hour, 2); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_cleared_events").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("число меток после ограничения=%d, ожидалось 2", count)
	}
	for _, uuid := range []string{"recent-2", "recent-3"} {
		event := domain.Event{
			ID: "repeat-" + uuid, ClientID: "client", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
			Source:   domain.SourceRecord{UUID: uuid, UpdatedAt: "version"},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
			Content: "текст", ContentDigest: "digest",
		}
		if inserted, err := queue.Enqueue(ctx, event); err != nil || inserted {
			t.Fatalf("свежая очищенная версия %s вернулась: inserted=%v err=%v", uuid, inserted, err)
		}
	}
}
