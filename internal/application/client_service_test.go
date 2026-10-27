package application

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

type testRetryAfterError struct{ delay time.Duration }

func (err testRetryAfterError) Error() string             { return "429" }
func (err testRetryAfterError) RetryAfter() time.Duration { return err.delay }

func TestRetryDelayUsesTelegramRetryAfter(t *testing.T) {
	if got := retryDelayFor(testRetryAfterError{delay: 41 * time.Second}, 1); got != 42*time.Second {
		t.Fatalf("неверная пауза после 429: %s", got)
	}
	if got := retryDelayFor(errors.New("offline"), 1); got != 5*time.Second {
		t.Fatalf("обычная ошибка потеряла экспоненциальную паузу: %s", got)
	}
}

// Если файл передан в транспортный чат, он остаётся в очереди до отдельного подтверждения сервера.
func TestClientServiceSeparatesUploadFromServerAcknowledgement(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	queue := newMemoryClientState()
	sender := &memorySender{}
	service := ClientService{
		ClientID: "textile", Source: &memorySource{items: []SourceItem{{
			Record:   domain.SourceRecord{ID: 1, UUID: "source-1", UpdatedAt: now.Format(time.RFC3339Nano)},
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
	if err != nil || acknowledged.Events != 1 {
		t.Fatalf("ack: %+v, %v", acknowledged, err)
	}
	if queue.states[queue.events[0].ID] != "acknowledged" {
		t.Fatalf("событие не подтверждено: %#v", queue.states)
	}
}

// Источник намеренно перечитывает граничную секунду, чтобы не потерять быструю
// правку. Точный повтор уже сохранённой версии не должен считаться новой
// постановкой и засорять журнал ложными событиями.
func TestClientServiceDoesNotCountBoundaryReplayAsNewEvent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 23, 20, 0, 0, 0, time.UTC)
	item := SourceItem{
		Record: domain.SourceRecord{
			ID: 42, UUID: "source-42", UpdatedAt: "2026-10-23 20:00:00",
		},
		Position: domain.Position{UnitName: "1 мб 155 омбр"},
		Mode:     domain.SourceModeNormal, Content: "20.00\n-Проверка", ObservedAt: now,
	}
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: &replaySource{item: item}, Queue: queue, State: queue,
		Sender: &memorySender{}, KeyID: "key", Key: make([]byte, 32), BatchSize: 10,
		CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("первая версия не поставлена в очередь: queued=%d err=%v", queued, err)
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("точный повтор ошибочно посчитан новым: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 {
		t.Fatalf("точный повтор продублирован в очереди: %d", len(queue.events))
	}
}

// Добавление UUID блоков в rc82 не должно менять прежний отпечаток rc81.
// Иначе после обновления граничное перечитывание возвращает старые записи в
// «Не отправлены старше 30 минут» как будто это новые редакции.
func TestClientServiceBlockMetadataKeepsRC81ContentDigest(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 27, 14, 0, 0, 0, time.UTC)
	base := SourceItem{
		Record:   domain.SourceRecord{ID: 345, UUID: "record-345", UpdatedAt: "2026-10-27 14:00:00"},
		Position: domain.Position{UnitName: "1 аэмб 68 оаэмбр", Frequency: "119.009", GroupCode: "E2E"},
		Content:  "10.39\n-Первый блок", Mode: domain.SourceModeNormal, ObservedAt: now,
	}
	service := ClientService{ClientID: "textile", Now: func() time.Time { return now }}
	legacy, err := service.newEvent(base)
	if err != nil {
		t.Fatal(err)
	}
	withUUID := base
	withUUID.Blocks = []domain.ContentBlock{{UUID: "01a17-block", Key: "10.39", Sequence: 0, Text: base.Content, Hash: "hash", UpdatedAt: "2026-10-27T14:00:00Z"}}
	modern, err := service.newEvent(withUUID)
	if err != nil {
		t.Fatal(err)
	}
	if modern.ContentDigest != legacy.ContentDigest {
		t.Fatalf("UUID блока изменил отпечаток rc81: %s != %s", modern.ContentDigest, legacy.ContentDigest)
	}
}

// EAVC сохраняет новую карточку и её текст разными запросами. Пустой
// промежуточный черновик нельзя считать отдельным перехватом: заполненная
// версия той же записи будет найдена при повторном чтении граничной секунды.
func TestClientServiceWaitsForDraftContent(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 10, 24, 0, 12, 7, 0, time.UTC)
	item := SourceItem{
		Record: domain.SourceRecord{
			ID: 1431, UUID: "source-1431", UpdatedAt: "2026-10-24 00:12:07",
		},
		Position: domain.Position{UnitName: "управление 25 овдбр"},
		Mode:     domain.SourceModeNormal, ObservedAt: now,
	}
	source := &replaySource{item: item}
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "selidovo", Source: source, Queue: queue, State: queue,
		Sender: &memorySender{}, KeyID: "key", Key: make([]byte, 32), BatchSize: 10,
		CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("пустой черновик попал в очередь: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 0 {
		t.Fatalf("пустой черновик сохранён как событие: %#v", queue.events)
	}

	source.item.Content = "00.12\n-Готовый текст перехвата"
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("заполненная версия не попала в очередь: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.ID != 1431 {
		t.Fatalf("ожидалась одна заполненная версия: %#v", queue.events)
	}
}

// При первом запуске клиент запоминает старые записи EAVC и отправляет только появившиеся после запуска.
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

func TestClientServiceUsesStableInitialBaseline(t *testing.T) {
	t.Parallel()
	queue := newMemoryClientState()
	source := &baselineSource{
		head: Cursor{UpdatedAt: "2026-10-23 10:00:00", ID: 1},
		oldItem: SourceItem{
			Record:   domain.SourceRecord{ID: 1, UUID: "old", UpdatedAt: "2026-10-23 10:00:00"},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal, Content: "старая запись",
		},
		newItem: SourceItem{
			Record:   domain.SourceRecord{ID: 2, UUID: "new", UpdatedAt: "2026-10-23 10:00:01"},
			Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal, Content: "новая запись",
		},
	}
	service := ClientService{
		ClientID: "textile", Source: source, Queue: queue, State: queue, Sender: &memorySender{},
		KeyID: "key", Key: make([]byte, 32), BatchSize: 10, CatchUpExisting: false, Now: time.Now,
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("базовая линия не создана: queued=%d err=%v", queued, err)
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("запись после базовой линии потеряна: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.ID != 2 {
		t.Fatalf("в очередь попали неверные данные: %#v", queue.events)
	}
}

func TestClientServiceSkipsInvalidRowAndQueuesFollowingRecord(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: &memorySource{items: []SourceItem{
			{Record: domain.SourceRecord{ID: 1, UUID: "broken", UpdatedAt: "2026-10-23 12:00:00"}, Content: "нет подразделения", Mode: domain.SourceModeNormal, ObservedAt: now},
			{Record: domain.SourceRecord{ID: 2, UUID: "valid", UpdatedAt: "2026-10-23 12:00:01"}, Position: domain.Position{UnitName: "1 мб 155 омбр"}, Content: "корректная запись", Mode: domain.SourceModeNormal, ObservedAt: now},
		}},
		Queue: queue, State: queue, Sender: &memorySender{}, KeyID: "key", Key: make([]byte, 32),
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	queued, err := service.CollectOnce(context.Background())
	if err != nil || queued != 1 {
		t.Fatalf("invalid row blocked collection: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.ID != 2 {
		t.Fatalf("following valid record was lost: %#v", queue.events)
	}
	if cursor := queue.cursors[eavcCursorName]; cursor.ID != 2 {
		t.Fatalf("source cursor did not advance: %+v", cursor)
	}
}

// Ответ на проверку связи сохраняется отдельно и не пытается подтвердить
// отсутствующее рабочее событие в очереди.
func TestClientServiceStoresTransportProbeSeparately(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	queue := newMemoryClientState()
	sender := &memorySender{acks: []Acknowledgement{{EventID: "probe-id", AcceptedAt: now, Status: "probe"}}}
	service := ClientService{Queue: queue, State: queue, Sender: sender}
	count, err := service.ReceiveAcknowledgements(context.Background())
	if err != nil || count.Events != 0 || !count.Probes["probe-id"].Equal(now) {
		t.Fatalf("ответ проверки не обработан: batch=%+v err=%v", count, err)
	}
	probeID, probeAt, exists, err := service.LastTransportProbe(context.Background())
	if err != nil || !exists || probeID != "probe-id" || !probeAt.Equal(now) {
		t.Fatalf("ответ проверки не сохранён: id=%q at=%s exists=%v err=%v", probeID, probeAt, exists, err)
	}
}

func TestClientServiceKeepsEveryProbeAcknowledgementInOneBatch(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Truncate(time.Second)
	queue := newMemoryClientState()
	sender := &memorySender{acks: []Acknowledgement{
		{EventID: "manual-probe", AcceptedAt: now, Status: "probe"},
		{EventID: "heartbeat-probe", AcceptedAt: now.Add(time.Second), Status: "probe"},
	}}
	service := ClientService{Queue: queue, State: queue, Sender: sender}
	batch, err := service.ReceiveAcknowledgements(context.Background())
	if err != nil || len(batch.Probes) != 2 {
		t.Fatalf("подтверждения смешались или потерялись: batch=%+v err=%v", batch, err)
	}
	if !batch.Probes["manual-probe"].Equal(now) {
		t.Fatalf("ручное подтверждение потеряно: %+v", batch.Probes)
	}
}

type memorySource struct {
	items []SourceItem
}

type replaySource struct {
	item SourceItem
}

func (source *replaySource) Scan(_ context.Context, _ Cursor, _ int) ([]SourceItem, Cursor, error) {
	return []SourceItem{source.item}, Cursor{UpdatedAt: source.item.Record.UpdatedAt, ID: source.item.Record.ID}, nil
}

type baselineSource struct {
	head    Cursor
	oldItem SourceItem
	newItem SourceItem
}

func (source *baselineSource) Head(context.Context) (Cursor, error) {
	return source.head, nil
}

func (source *baselineSource) Scan(_ context.Context, after Cursor, _ int) ([]SourceItem, Cursor, error) {
	if after != source.head {
		return nil, after, nil
	}
	next := Cursor{UpdatedAt: source.newItem.Record.UpdatedAt, ID: source.newItem.Record.ID}
	return []SourceItem{source.oldItem, source.newItem}, next, nil
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

func (state *memoryClientState) Enqueue(_ context.Context, event domain.Event) (bool, error) {
	for _, existing := range state.events {
		if sourceVersionKey(existing) == sourceVersionKey(event) {
			return false, nil
		}
	}
	state.events = append(state.events, event)
	state.states[event.ID] = "pending"
	return true, nil
}

func sourceVersionKey(event domain.Event) string {
	sourceKey := event.Source.UUID
	if sourceKey == "" {
		sourceKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	return sourceKey + "\x00" + event.Source.UpdatedAt + "\x00" + event.ContentDigest
}

func (state *memoryClientState) Pending(_ context.Context, limit int, _ time.Time) ([]QueuedEvent, error) {
	var pending []QueuedEvent
	for _, event := range state.events {
		if state.states[event.ID] != "acknowledged" && state.states[event.ID] != "expired" {
			pending = append(pending, QueuedEvent{Event: event})
			if len(pending) == limit {
				break
			}
		}
	}
	return pending, nil
}

func (state *memoryClientState) ExpireDue(_ context.Context, now time.Time) (int, error) {
	count := 0
	for _, event := range state.events {
		if event.Expired(now) && state.states[event.ID] != "acknowledged" && state.states[event.ID] != "expired" {
			state.states[event.ID] = "expired"
			count++
		}
	}
	return count, nil
}

func (state *memoryClientState) MarkExpired(_ context.Context, eventID string, _ time.Time, _ string) error {
	if _, exists := state.states[eventID]; !exists {
		return errors.New("not found")
	}
	state.states[eventID] = "expired"
	return nil
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
