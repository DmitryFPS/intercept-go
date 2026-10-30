package telegram

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func newAmbiguousTelegramDestination(t *testing.T) (*Destination, *atomic.Int32) {
	t.Helper()
	var accepted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			http.Error(writer, "неожиданный метод", http.StatusNotFound)
			return
		}
		_, _ = io.Copy(io.Discard, request.Body)
		call := accepted.Add(1)
		writer.Header().Set("Content-Type", "application/json")
		if call == 1 {
			// POST принят, но HTTP 500 не доказывает, что Telegram не создал сообщение.
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(writer, `{"ok":false,"error_code":500,"description":"Internal Server Error"}`)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": 900 + call},
		})
	}))
	t.Cleanup(server.Close)
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	return &Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}, &accepted
}

func TestTelegramSendCreationIsUncertainForServerErrors(t *testing.T) {
	t.Parallel()
	for _, scenario := range []struct {
		name       string
		status     int
		body       string
		definitely bool
	}{
		{"HTTP 400", 400, `{"ok":false,"error_code":400,"description":"bad request"}`, true},
		{"HTTP 200 with Bot code 400", 200, `{"ok":false,"error_code":400,"description":"bad request"}`, true},
		{"HTTP 400 with body 500", 400, `{"ok":false,"error_code":500,"description":"internal"}`, false},
		{"HTTP 429", 429, `{"ok":false,"error_code":429,"description":"retry"}`, true},
		{"HTTP 429 with body 500", 429, `{"ok":false,"error_code":500,"description":"internal"}`, false},
		{"HTTP 500", 500, `{"ok":false,"error_code":500,"description":"internal"}`, false},
		{"HTTP 503 with body 429", 503, `{"ok":false,"error_code":429,"description":"retry"}`, false},
		{"HTTP 200 with Bot code 500", 200, `{"ok":false,"error_code":500,"description":"internal"}`, false},
		{"malformed 500", 500, `not-json`, false},
		{"missing message ID", 200, `{"ok":true,"result":{}}`, false},
		{"wrong chat ID", 200, `{"ok":true,"result":{"message_id":901,"chat":{"id":-2002}}}`, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(scenario.status)
				_, _ = io.WriteString(writer, scenario.body)
			}))
			defer server.Close()
			api, err := NewWithBaseURL("123:token", server.URL, server.Client())
			if err != nil {
				t.Fatal(err)
			}
			api.sends.globalInterval = 0
			api.sends.groupInterval = 0
			_, err = api.SendHTML(context.Background(), -1001, 42, "проверка")
			if err == nil || telegramSendDefinitelyNotCreated(err) != scenario.definitely {
				t.Fatalf("классификация отправки: err=%v definite=%v want=%v", err, telegramSendDefinitelyNotCreated(err), scenario.definitely)
			}
		})
	}
}

func TestUnknownSendStateSurvivesInactiveCompaction(t *testing.T) {
	t.Parallel()
	items := make(map[string]destinationItem, maxTelegramInactiveItems+2)
	for index := 0; index < maxTelegramInactiveItems+1; index++ {
		items[fmt.Sprintf("inactive-%d", index)] = destinationItem{
			UpdatedAt: time.Unix(int64(index), 0),
			Blocks:    map[string]destinationBlock{"block": {Suppressed: true}},
		}
	}
	items["unresolved"] = destinationItem{
		UpdatedAt: time.Unix(0, 0),
		Blocks: map[string]destinationBlock{"block": {
			Suppressed: true, UnknownOutcome: true,
			SuppressedReason: "исход отправки Telegram неизвестен",
		}},
	}
	path := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(path, destinationState{Items: items}); err != nil {
		t.Fatal(err)
	}
	state, err := loadDestinationState(path)
	if err != nil {
		t.Fatal(err)
	}
	if !state.Items["unresolved"].Blocks["block"].UnknownOutcome {
		t.Fatal("неопределённая публикация вытеснена очисткой старого состояния")
	}
}

func TestUnknownSecondChunkKeepsConfirmedFirstChunk(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/editMessageText") {
			_, _ = io.WriteString(writer, `{"ok":true,"result":true}`)
			return
		}
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			http.Error(writer, "unexpected method", http.StatusNotFound)
			return
		}
		call := calls.Add(1)
		if call == 2 {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(writer, `{"ok":false,"error_code":500,"description":"internal"}`)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": 800 + call},
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	path := filepath.Join(t.TempDir(), "state.json")
	destination := Destination{API: api, ChatID: -1001, StatePath: path}
	t0 := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("long-event", "2026-10-28 12:00:00",
		"12.00\n"+strings.Repeat("-Длинная контрольная строка. (10001)\n", 420), t0)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	expected := len(formatBlockChunks(event, event.Blocks[0], delivery, "", "", ""))
	if expected < 2 {
		t.Fatalf("нужны минимум две части, получено %d", expected)
	}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("неоднозначная вторая часть должна вернуть ошибку")
	}
	state, err := loadDestinationState(path)
	if err != nil {
		t.Fatal(err)
	}
	block := state.Items["point-01|source-1"].Blocks["block-1"]
	if len(block.MessageIDs) != 1 || block.MessageIDs[0] != 801 || !block.SendPending || calls.Load() != 2 {
		t.Fatalf("известная первая часть или барьер второй потеряны: block=%#v calls=%d", block, calls.Load())
	}
	destination = Destination{API: api, ChatID: -1001, StatePath: path}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("вторая часть должна остаться в карантине: %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("создан возможный дубль второй части: %d", calls.Load())
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(receipt.Blocks) != 1 || !receipt.Blocks[0].UnknownOutcome ||
		receipt.Blocks[0].UnconfirmedPart != 2 || len(receipt.Blocks[0].MessageIDs) != 1 ||
		receipt.Blocks[0].MessageIDs[0] != 801 {
		t.Fatalf("частичная квитанция потеряла известную первую часть: receipt=%+v err=%v", receipt, err)
	}
	allowed, err := destination.AllowRepublish(event, "block-1")
	if err != nil || !allowed {
		t.Fatalf("ручное разрешение после сверки не сработало: allowed=%v err=%v", allowed, err)
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	state, err = loadDestinationState(path)
	if err != nil {
		t.Fatal(err)
	}
	block = state.Items["point-01|source-1"].Blocks["block-1"]
	if len(block.MessageIDs) != expected || block.MessageIDs[0] != 801 ||
		calls.Load() != int32(expected+1) || block.UnknownOutcome {
		t.Fatalf("ручное продолжение должно сохранить первую часть: block=%#v calls=%d", block, calls.Load())
	}
}

func ambiguousTelegramEvent(id, version, text string, createdAt time.Time) domain.Event {
	return domain.Event{
		ID: id, ClientID: "point-01", CreatedAt: createdAt, SourceAt: createdAt,
		Source:          domain.SourceRecord{UUID: "source-1"},
		Position:        domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{UUID: "block-1", Key: "12.00", Text: text, UpdatedAt: version}},
	}
}

// После принятого POST даже корректный ответ Bot API 500 не доказывает
// отсутствие публикации. Перезапуск и новая редакция не вправе создать дубль;
// оператор может разрешить одну новую отправку после проверки темы Telegram.
func TestDestinationHTTP500UnknownOutcomeSurvivesRestartAndRevision(t *testing.T) {
	t.Parallel()
	destination, accepted := newAmbiguousTelegramDestination(t)
	t0 := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("event-1", "2026-10-28 12:00:00", "12.00\n-Первая редакция", t0)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	err := destination.Deliver(context.Background(), event, delivery)
	if err == nil || telegramSendDefinitelyNotCreated(err) {
		t.Fatalf("принятый POST с HTTP 500 ошибочно признан безопасным для автоматического повтора: %v", err)
	}
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if !block.SendPending || len(block.MessageIDs) != 0 || accepted.Load() != 1 {
		t.Fatalf("неопределённая публикация не сохранена до перезапуска: block=%#v sends=%d", block, accepted.Load())
	}

	// Новый экземпляр воспроизводит перезапуск Центра после сохранения маркера.
	destination = &Destination{API: destination.API, ChatID: destination.ChatID, StatePath: destination.StatePath}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("повтор должен изолировать неопределённую публикацию: %v", err)
	}
	if accepted.Load() != 1 {
		t.Fatalf("повтор создал возможный дубль: sends=%d", accepted.Load())
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(receipt.Blocks) != 1 || !receipt.Blocks[0].UnknownOutcome ||
		receipt.Blocks[0].UnconfirmedPart != 1 || len(receipt.Blocks[0].MessageIDs) != 0 {
		t.Fatalf("неопределённый исход отсутствует в частичной квитанции: receipt=%+v err=%v", receipt, err)
	}
	revision := ambiguousTelegramEvent("event-2", "2026-10-28 12:01:00", "12.00\n-Уточнённая редакция", t0.Add(time.Minute))
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil && !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неожиданная ошибка изоляции новой редакции: %v", err)
	}
	current, err = loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	block = current.Items["point-01|source-1"].Blocks["block-1"]
	if accepted.Load() != 1 || !block.Suppressed || len(block.MessageIDs) != 0 {
		t.Fatalf("новая редакция автоматически опубликовала блок с неизвестным исходом: block=%#v sends=%d", block, accepted.Load())
	}

	allowed, err := destination.AllowRepublish(revision, "block-1")
	if err != nil || !allowed {
		t.Fatalf("после проверки Telegram ручная публикация не разрешена: allowed=%v err=%v", allowed, err)
	}
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil {
		t.Fatalf("ручная публикация не выполнена: %v", err)
	}
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil {
		t.Fatalf("идемпотентная проверка повторной публикации завершилась ошибкой: %v", err)
	}
	current, err = loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	block = current.Items["point-01|source-1"].Blocks["block-1"]
	if accepted.Load() != 2 || block.Suppressed || len(block.MessageIDs) != 1 || block.MessageIDs[0] != 902 {
		t.Fatalf("ручное разрешение должно дать ровно одну новую публикацию: block=%#v sends=%d", block, accepted.Load())
	}
}

// Состояние предыдущей версии уже могло содержать Suppressed после потери
// ответа, но ещё не содержать отдельного поля причины неопределённого исхода.
func TestDestinationLegacyUnknownOutcomeRemainsSuppressedOnRevision(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			http.Error(writer, "неожиданный метод", http.StatusNotFound)
			return
		}
		sends.Add(1)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": 903},
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	t0 := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-1": {
				Suppressed: true, SuppressedAt: t0.Format(time.RFC3339Nano),
				SuppressedReason: "исход отправки Telegram неизвестен после прерывания; автоматический дубль запрещён",
				SourceVersion:    "2026-10-28 12:00:00", SourceEventAt: t0, ThreadID: 42,
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	revision := ambiguousTelegramEvent("event-legacy-revision", "2026-10-28 12:01:00", "12.00\n-Новая редакция", t0.Add(time.Minute))
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil && !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неожиданная ошибка при старом подавленном состоянии: %v", err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if sends.Load() != 0 || !block.Suppressed || len(block.MessageIDs) != 0 {
		t.Fatalf("старая неопределённая публикация ожила от редакции: block=%#v sends=%d", block, sends.Load())
	}
	allowed, err := destination.AllowRepublish(revision, "block-1")
	if err != nil || !allowed {
		t.Fatalf("ручное разрешение старой записи не работает: allowed=%v err=%v", allowed, err)
	}
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil {
		t.Fatalf("ручная публикация старой записи не выполнена: %v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("ожидалась одна ручная отправка, получено %d", sends.Load())
	}
}

// EAVC может удалить блок до того, как станет известен исход отправки.
// Неизвестный message_id удалить невозможно, но новое изменение того же UUID
// не должно бесконтрольно опубликовать второй экземпляр.
func TestDestinationUnknownOutcomeSurvivesSourceDeletion(t *testing.T) {
	t.Parallel()
	destination, accepted := newAmbiguousTelegramDestination(t)
	t0 := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("event-1", "2026-10-28 12:00:00", "12.00\n-Первая редакция", t0)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("первая неопределённая отправка должна вернуть ошибку")
	}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неопределённый исход не изолирован: %v", err)
	}
	deletion := ambiguousTelegramEvent("event-delete", "2026-10-28 12:01:00", "12.00\n-Первая редакция", t0.Add(time.Minute))
	deletion.SourceBlockDeleted = true
	deletion.HasActiveBlocks = false
	deletion.Blocks[0].DeletedAt = "2026-10-28 12:01:00"
	if err := destination.Deliver(context.Background(), deletion, delivery); err != nil && !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неожиданная ошибка изоляции EAVC-удаления: %v", err)
	}
	// Изоляция одного блока не должна останавливать самостоятельный соседний
	// блок той же записи: у него свой UUID и свой подтверждённый message_id.
	sibling := ambiguousTelegramEvent("event-sibling", "2026-10-28 12:01:30", "12.01\n-Соседний блок", t0.Add(90*time.Second))
	sibling.SourceBlockUUID = "block-2"
	sibling.Blocks[0].UUID = "block-2"
	sibling.Blocks[0].Key = "12.01"
	if err := destination.Deliver(context.Background(), sibling, delivery); err != nil {
		t.Fatalf("соседний блок ошибочно остановлен из-за неизвестного исхода другого блока: %v", err)
	}
	if accepted.Load() != 2 {
		t.Fatalf("соседний блок не дошёл до Telegram: sends=%d", accepted.Load())
	}
	revision := ambiguousTelegramEvent("event-after-delete", "2026-10-28 12:02:00", "12.00\n-Блок снова изменён", t0.Add(2*time.Minute))
	if err := destination.Deliver(context.Background(), revision, delivery); err != nil && !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неожиданная ошибка после EAVC-удаления: %v", err)
	}
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if accepted.Load() != 2 || len(block.MessageIDs) != 0 {
		t.Fatalf("после EAVC-удаления неопределённая публикация создала дубль: block=%#v sends=%d", block, accepted.Load())
	}
	siblingBlock := current.Items["point-01|source-1"].Blocks["block-2"]
	if len(siblingBlock.MessageIDs) != 1 || siblingBlock.MessageIDs[0] != 902 {
		t.Fatalf("подтверждённая публикация соседнего блока потеряна: %#v", siblingBlock)
	}
}
