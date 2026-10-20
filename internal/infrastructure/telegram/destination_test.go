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

// Сообщение дословно повторяет предоставленный пользователем рабочий образец:
// номер радиосети, карточки корреспондентов и /номер/ перед каждой репликой.
func TestFormatEventMatchesApprovedTelegramExample(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		CreatedAt: time.Date(2026, 10, 20, 17, 34, 0, 0, time.UTC),
		SourceAt:  time.Date(2026, 10, 20, 17, 34, 0, 0, time.UTC),
		Source:    domain.SourceRecord{UpdatedAt: "2026-10-20T17:34:00+03:00"},
		Position:  domain.Position{UnitName: "Гольназ", Frequency: "159.3000", GroupCode: "18902"},
		Content:   "17.30\n-Монако Торбе на связь. (52325)\n-На связи. (52345)\n-До Олимпа дотянулися, сказал им копать. (52325)\n-Плюс. (52345)",
		Blocks: []domain.ContentBlock{{
			Key: "17.30", Text: "17.30\n-Монако Торбе на связь. (52325)\n-На связи. (52345)\n-До Олимпа дотянулися, сказал им копать. (52325)\n-Плюс. (52345)",
			Callsigns: []domain.Callsign{
				{Number: 1, Code: "52345", Label: "Монако", Description: "ОД на КНП Гольназ, мужской голос, украинская речь"},
				{Number: 6, Code: "52325", Label: "Торба", Description: "Старший группы Гольназ, мужской голос, украинская речь"},
			},
		}},
	}
	want := "Радиосеть <b>Гольназ</b> - 39\n" +
		"159.3000 МГц (ID 18902 AES-256)\n" +
		"20.10.2026г.\n\n" +
		"1. «<b>Монако</b>» (id: 52345) ОД на КНП Гольназ, мужской голос, украинская речь;\n" +
		"6. «<b>Торба</b>» (id: 52325) Старший группы Гольназ, мужской голос, украинская речь;\n\n" +
		"<b>17:30</b>\n" +
		"/6/ Монако Торбе на связь.\n" +
		"/1/ На связи.\n" +
		"/6/ До Олимпа дотянулися, сказал им копать.\n" +
		"/1/ Плюс."
	if got := FormatEventForNetwork(event, "39", false); got != want {
		t.Fatalf("формат отличается\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestCustomMessageTemplateControlsLayout(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		SourceAt: time.Date(2026, 10, 20, 17, 34, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "Гольназ", Frequency: "159.3000", GroupCode: "18902"},
		Blocks: []domain.ContentBlock{{
			Text:      "17.30\n-На связи. (52345)",
			Callsigns: []domain.Callsign{{Number: 1, Code: "52345", Label: "Монако"}},
		}},
	}
	template := "{{DATE}} | {{UNIT}} | {{NETWORK}} | {{FREQUENCY}} | {{GROUP_ID}}\n{{CORRESPONDENTS}}\n---\n{{TRANSCRIPT}}"
	got := FormatEventWithTemplate(event, "39", template, false)
	for _, expected := range []string{"20.10.2026 | Гольназ | 39 | 159.3000 | 18902", "«<b>Монако</b>»", "---", "<b>17:30</b>"} {
		if !strings.Contains(got, expected) {
			t.Fatalf("пользовательский шаблон не применён, нет %q:\n%s", expected, got)
		}
	}
}

func TestMessageTemplateValidationListsMissingMarkers(t *testing.T) {
	t.Parallel()
	if err := ValidateMessageTemplate("{{UNIT}}"); err == nil || !strings.Contains(err.Error(), "{{TRANSCRIPT}}") {
		t.Fatalf("ожидалась понятная ошибка отсутствующих меток: %v", err)
	}
	if err := ValidateMessageTemplate(DefaultMessageTemplate); err != nil {
		t.Fatalf("стандартный шаблон не прошёл проверку: %v", err)
	}
}

func TestFormatDeferredEventKeepsMarkerAndOriginalDate(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		CreatedAt: time.Date(2026, 10, 20, 20, 0, 0, 0, time.UTC),
		Source:    domain.SourceRecord{UpdatedAt: "2026-10-19 03:51:00"},
		Position: domain.Position{
			UnitName: "2 об 3 обр ОсН НГ (отложенный режим)", Frequency: "159.5750", GroupCode: "3",
		},
		Mode: domain.SourceModeDeferred,
		Blocks: []domain.ContentBlock{{
			Key: "03.51", Text: "03.51\n-На приём. (783693)",
			Callsigns: []domain.Callsign{{Number: 15, Code: "783693", Label: "Марс"}},
		}},
	}
	got := FormatEventForNetwork(event, "39", false)
	for _, expected := range []string{
		"Радиосеть <b>2 об 3 обр ОсН НГ (отложенный режим)</b> - 39",
		"19.10.2026г.", "15. «<b>Марс</b>»", "<b>03:51</b>", "/15/ На приём.",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("в отложенном сообщении нет %q:\n%s", expected, got)
		}
	}
}

// При изменении перехвата существующие сообщения редактируются или удаляются, а дубликаты не создаются.
func TestDestinationEditsAndDeletesBlocksWithoutDuplicates(t *testing.T) {
	t.Parallel()
	var methods []string
	var texts []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("декодировать %s: %v", method, err)
		}
		if value, ok := body["text"].(string); ok {
			texts = append(texts, value)
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
	if len(texts) != 3 || !strings.Contains(texts[2], "Первое исправлено") {
		t.Fatalf("исправленный текст не передан через editMessageText: %#v", texts)
	}
	if len(destination.locks) != 0 {
		t.Fatalf("служебные блокировки не освобождены: %d", len(destination.locks))
	}
}

// Если Telegram окончательно запретил правку старого сообщения, новое не
// создаётся, но доставка остаётся ошибочной и не теряет исправление молча.
func TestDestinationDoesNotReplaceMessageAfterTerminalEditError(t *testing.T) {
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
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("невозможная правка не должна считаться доставленной")
	}
	want := []string{"sendMessage", "editMessageText"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
}

// Ответ «текст не изменён» означает, что предыдущая попытка уже успела
// исправить сообщение; такой повтор можно безопасно завершить без ожидания.
func TestDestinationAcceptsAlreadyAppliedTelegramEdit(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	call := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		call++
		if method == "sendMessage" {
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 101},
			})
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": false, "error_code": 400, "description": "Bad Request: message is not modified",
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
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
		t.Fatalf("уже выполненная правка должна считаться успешной: %v", err)
	}
	if call != 2 {
		t.Fatalf("ожидались отправка и одна правка, вызовов: %d", call)
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
