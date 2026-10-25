package sqlite

import (
	"bytes"
	"context"
	"path/filepath"
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
		if err != nil || total != 1 || len(items) != 1 || items[0].Content != "текст "+state {
			t.Fatalf("неверная страница %s: total=%d items=%#v err=%v", state, total, items, err)
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
}
