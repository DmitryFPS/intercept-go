package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/domain"
)

// Неизвестное название выделяется заметной пометкой, а исходный ввод оператора остаётся виден без исправлений.
func TestFormatReviewIsVisibleAndKeepsOriginalName(t *testing.T) {
	t.Parallel()

	event := domain.Event{
		ID: "event", ClientID: "client", CreatedAt: time.Now(),
		Source:   domain.SourceRecord{ID: 1},
		Position: domain.Position{UnitName: `2об3 обр ОсН "Спартан" НГУ`, Frequency: "100"},
		Mode:     domain.SourceModeUnknown, Content: "сообщение", ContentDigest: "digest",
	}
	text := FormatEvent(event, true)
	if !strings.HasPrefix(text, "🔴 НОВОЕ ИЛИ ОШИБОЧНОЕ ПОДРАЗДЕЛЕНИЕ") {
		t.Fatalf("пометка не заметна: %q", text)
	}
	if !strings.Contains(text, event.Position.UnitName) {
		t.Fatal("исходное название должно сохраняться без исправления")
	}
}

// Сообщение сохраняет привычные заголовок и позывные в том же виде, что использовала старая программа.
func TestFormatEventMatchesLegacyHeaderAndCallsigns(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		CreatedAt: time.Date(2026, 10, 18, 21, 0, 0, 0, time.UTC),
		SourceAt:  time.Date(2026, 10, 18, 21, 0, 0, 0, time.UTC),
		Position:  domain.Position{UnitName: `3 шб 425 ошп "Скала"`, Frequency: "162.0000", GroupCode: "53525"},
		Content:   "16.39\n-Привет. (90861)",
		Blocks: []domain.ContentBlock{{
			Key: "16.39", Text: "16.39\n-Привет. (90861)",
			Callsigns: []domain.Callsign{{Code: "90861", Label: "Лаки"}},
		}},
	}
	want := "ОВЧ радиосеть\n<b>3 шб 425 ошп \"Скала\"</b>\n" +
		"Частота 162.0000 МГц (групповой ID 53525 AES-256)\n" +
		"Дата: 19.10.2026\n\n<b>Лаки (90861)</b>\n\n16.39\n-Привет. (90861)"
	if got := FormatEvent(event, false); got != want {
		t.Fatalf("формат отличается\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

// При изменении перехвата существующие сообщения редактируются или удаляются, а дубликаты не создаются.
func TestDestinationEditsAndDeletesBlocksWithoutDuplicates(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("декодировать %s: %v", method, err)
		}
		if method == "sendMessage" {
			if body["message_thread_id"] != float64(42) || body["parse_mode"] != "HTML" {
				t.Fatalf("неверная отправка: %#v", body)
			}
			nextID++
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": nextID}})
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{
			{Key: "12.00", Sequence: 0, Text: "12.00\n-Первое"},
			{Key: "12.01", Sequence: 1, Text: "12.01\n-Второе"},
		},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks = []domain.ContentBlock{{Key: "12.00", Sequence: 0, Text: "12.00\n-Первое исправлено"}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "sendMessage", "editMessageText", "deleteMessage"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
	if len(destination.locks) != 0 {
		t.Fatalf("служебные блокировки не освобождены: %d", len(destination.locks))
	}
}

// Новое сообщение создаётся только когда Telegram окончательно запретил правку старого; временная ошибка приводит к повтору.
func TestDestinationSendsReplacementOnlyAfterTerminalEditError(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		if method == "editMessageText" {
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message can't be edited",
			})
			return
		}
		nextID++
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": nextID},
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position: domain.Position{UnitName: "позиция"},
		Blocks:   []domain.ContentBlock{{Key: "12.00", Text: "12.00\n-Первое"}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "editMessageText", "sendMessage"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
}

// Сведения о давно отправленных сообщениях удаляются, а свежие остаются доступными для последующего редактирования.
func TestDestinationStateDropsRecordsOlderThanRetention(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.json")
	payload, err := json.Marshal(destinationState{Items: map[string]destinationItem{
		"old":    {UpdatedAt: time.Now().UTC().Add(-40 * 24 * time.Hour)},
		"recent": {UpdatedAt: time.Now().UTC()},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := loadDestinationState(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(current.Items) != 1 {
		t.Fatalf("устаревшее состояние не очищено: %#v", current.Items)
	}
	if _, exists := current.Items["recent"]; !exists {
		t.Fatal("актуальное состояние удалено")
	}
}
