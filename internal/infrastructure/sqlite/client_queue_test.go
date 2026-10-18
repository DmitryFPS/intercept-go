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
	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
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
	if len(events) != 1 || events[0].ID != event.ID {
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
