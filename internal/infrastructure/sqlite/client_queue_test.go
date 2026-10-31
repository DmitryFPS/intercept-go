package sqlite

import (
	"bytes"
	"context"
	"database/sql"
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
