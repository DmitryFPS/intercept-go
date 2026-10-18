package sqlite

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

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
	if err := queue.Enqueue(context.Background(), event); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	duplicate := event
	duplicate.ID = "event-2"
	if err := queue.Enqueue(context.Background(), duplicate); err != nil {
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
	second.Source.UUID = "source-2"
	second.Content = "второе"
	second.ContentDigest = "digest-2"
	second.CreatedAt = now.Add(time.Second)
	second.ExpiresAt = now.Add(30 * time.Minute)
	if err := queue.Enqueue(ctx, first); err != nil {
		t.Fatal(err)
	}
	if err := queue.Enqueue(ctx, second); err != nil {
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
	if err := queue.Enqueue(ctx, event); err != nil {
		t.Fatal(err)
	}
	returned := event
	returned.ID = "event-2"
	returned.CreatedAt = now.Add(2 * time.Minute)
	returned.Source.UpdatedAt = now.Add(2 * time.Minute).Format(time.RFC3339Nano)
	if err := queue.Enqueue(ctx, returned); err != nil {
		t.Fatal(err)
	}
	stats, err := queue.Stats(ctx)
	if err != nil || stats.Pending != 2 {
		t.Fatalf("возврат к прежнему тексту потерян: %+v, %v", stats, err)
	}
}

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
		if err := queue.Enqueue(ctx, makeEvent(id)); err != nil {
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
	if err := queue.Enqueue(ctx, event); err != nil {
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
