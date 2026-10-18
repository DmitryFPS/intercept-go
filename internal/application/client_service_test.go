package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

func TestClientServiceSeparatesUploadFromServerAcknowledgement(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	queue := newMemoryClientState()
	sender := &memorySender{}
	service := ClientService{
		ClientID: "textile", Source: &memorySource{items: []SourceItem{{
			Record:   domain.SourceRecord{ID: 1, UUID: "source-1"},
			Position: domain.Position{UnitName: "1 мб 155 омбр"},
			Mode:     domain.SourceModeUnknown, Content: "данные", ObservedAt: now,
		}}},
		Queue: queue, State: queue, Sender: sender,
		KeyID: "key-1", Key: make([]byte, 32), BatchSize: 10,
		CatchUpExisting: true, Now: func() time.Time { return now },
	}
	queued, err := service.CollectOnce(context.Background())
	if err != nil || queued != 1 {
		t.Fatalf("collect: %d, %v", queued, err)
	}
	sent, err := service.DispatchOnce(context.Background())
	if err != nil || sent != 1 {
		t.Fatalf("dispatch: %d, %v", sent, err)
	}
	if queue.states[queue.events[0].ID] != "uploaded" {
		t.Fatalf("загрузка не должна означать подтверждение: %#v", queue.states)
	}
	sender.acks = []Acknowledgement{{EventID: queue.events[0].ID, AcceptedAt: now}}
	acknowledged, err := service.ReceiveAcknowledgements(context.Background())
	if err != nil || acknowledged != 1 {
		t.Fatalf("ack: %d, %v", acknowledged, err)
	}
	if queue.states[queue.events[0].ID] != "acknowledged" {
		t.Fatalf("событие не подтверждено: %#v", queue.states)
	}
}

func TestClientServiceSkipsExistingRowsOnFirstStart(t *testing.T) {
	t.Parallel()

	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: &memorySource{items: []SourceItem{{
			Record:   domain.SourceRecord{ID: 1, UUID: "old"},
			Position: domain.Position{UnitName: "позиция"}, Content: "старое",
		}}},
		Queue: queue, State: queue, Sender: &memorySender{},
		KeyID: "key", Key: make([]byte, 32), BatchSize: 10,
		CatchUpExisting: false, Now: time.Now,
	}
	queued, err := service.CollectOnce(context.Background())
	if err != nil || queued != 0 || len(queue.events) != 0 {
		t.Fatalf("старые строки не должны отправляться: %d %#v %v", queued, queue.events, err)
	}
	if queue.values[eavcInitializedName] != "true" {
		t.Fatal("базовая линия не завершена")
	}
}

type memorySource struct {
	items []SourceItem
}

func (source *memorySource) Scan(_ context.Context, _ Cursor, limit int) ([]SourceItem, Cursor, error) {
	items := source.items
	if len(items) > limit {
		items = items[:limit]
	}
	source.items = source.items[len(items):]
	cursor := Cursor{}
	if len(items) > 0 {
		cursor = Cursor{UpdatedAt: "2026", ID: items[len(items)-1].Record.ID}
	}
	return items, cursor, nil
}

type memorySender struct {
	acks []Acknowledgement
}

func (sender *memorySender) Send(_ context.Context, _ protocol.Envelope) (int64, error) {
	return 77, nil
}

func (sender *memorySender) Acknowledgements(_ context.Context, offset int64) ([]Acknowledgement, int64, error) {
	return sender.acks, offset + int64(len(sender.acks)) + 1, nil
}

type memoryClientState struct {
	events  []domain.Event
	states  map[string]string
	cursors map[string]Cursor
	values  map[string]string
}

func newMemoryClientState() *memoryClientState {
	return &memoryClientState{
		states: make(map[string]string), cursors: make(map[string]Cursor), values: make(map[string]string),
	}
}

func (state *memoryClientState) Enqueue(_ context.Context, event domain.Event) error {
	state.events = append(state.events, event)
	state.states[event.ID] = "pending"
	return nil
}

func (state *memoryClientState) Pending(_ context.Context, _ int, _ time.Time) ([]domain.Event, error) {
	var pending []domain.Event
	for _, event := range state.events {
		if state.states[event.ID] != "acknowledged" {
			pending = append(pending, event)
		}
	}
	return pending, nil
}

func (state *memoryClientState) MarkUploaded(_ context.Context, eventID string, _ int64) error {
	if _, exists := state.states[eventID]; !exists {
		return errors.New("not found")
	}
	state.states[eventID] = "uploaded"
	return nil
}

func (state *memoryClientState) MarkAcknowledged(_ context.Context, eventID string, _ time.Time) error {
	if _, exists := state.states[eventID]; !exists {
		return errors.New("not found")
	}
	state.states[eventID] = "acknowledged"
	return nil
}

func (state *memoryClientState) MarkRetry(_ context.Context, eventID string, _ time.Time, _ string) error {
	if _, exists := state.states[eventID]; !exists {
		return errors.New("not found")
	}
	return nil
}

func (state *memoryClientState) Cursor(_ context.Context, name string) (Cursor, error) {
	return state.cursors[name], nil
}

func (state *memoryClientState) SaveCursor(_ context.Context, name string, cursor Cursor) error {
	state.cursors[name] = cursor
	return nil
}

func (state *memoryClientState) Value(_ context.Context, name string) (string, bool, error) {
	value, exists := state.values[name]
	return value, exists, nil
}

func (state *memoryClientState) SaveValue(_ context.Context, name, value string) error {
	state.values[name] = value
	return nil
}
