package telegram

import (
	"context"
	"encoding/json"
	"encoding/xml"
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
		"1. «<b>Торба</b>» (id: 52325) Старший группы Гольназ, мужской голос, украинская речь;\n" +
		"2. «<b>Монако</b>» (id: 52345) ОД на КНП Гольназ, мужской голос, украинская речь;\n\n" +
		"<b>17:30</b>\n" +
		"/1/ Монако Торбе на связь.\n" +
		"/2/ На связи.\n" +
		"/1/ До Олимпа дотянулися, сказал им копать.\n" +
		"/2/ Плюс."
	if got := FormatEventForNetwork(event, "39", false); got != want {
		t.Fatalf("формат отличается\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestFormatEventKeepsSeveralTimeBlocksInOneMessage(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		SourceAt: time.Date(2026, 10, 20, 17, 34, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "Гольназ", Frequency: "159.3000", GroupCode: "18902"},
		Blocks: []domain.ContentBlock{
			{
				Key: "17.30", Sequence: 0, Text: "17.30\n-На связи. (52345)\n-Слышу уверенно. (52325)",
				Callsigns: []domain.Callsign{
					{Number: 1, Code: "52345", Label: "Монако"},
					{Number: 2, Code: "52325", Label: "Торба"},
				},
			},
			{
				Key: "20.51", Sequence: 1, Text: "20.51\n-Принял, продолжаю работу. (61021)",
				Callsigns: []domain.Callsign{{Number: 3, Code: "61021", Label: "Рубин"}},
			},
			{
				Key: "21.14", Sequence: 2, Text: "21.14\n-Доложите обстановку. (61022)\n-Без изменений. (52345)",
				Callsigns: []domain.Callsign{{Number: 4, Code: "61022", Label: "Вектор"}},
			},
		},
	}
	got := FormatEventForNetwork(event, "313", false)
	for _, expected := range []string{
		"1. «<b>Монако</b>»", "2. «<b>Торба</b>»", "3. «<b>Рубин</b>»", "4. «<b>Вектор</b>»",
		"<b>17:30</b>\n/1/ На связи.\n/2/ Слышу уверенно.",
		"<b>20:51</b>\n/3/ Принял, продолжаю работу.",
		"<b>21:14</b>\n/4/ Доложите обстановку.\n/1/ Без изменений.",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("в объединённом сообщении нет %q:\n%s", expected, got)
		}
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

func TestMessageTemplateValidationRejectsUnsafeHTML(t *testing.T) {
	t.Parallel()
	for _, invalid := range []string{
		strings.Replace(DefaultMessageTemplate, "<b>", `<b onclick="x">`, 1),
		strings.Replace(DefaultMessageTemplate, "<b>", "<script>", 1),
		strings.Replace(DefaultMessageTemplate, "</b>", "</i>", 1),
	} {
		if err := ValidateMessageTemplate(invalid); err == nil {
			t.Fatalf("опасное или повреждённое HTML-оформление принято: %q", invalid)
		}
	}
}

func TestTelegramLengthBoundaryAndBalancedChunks(t *testing.T) {
	t.Parallel()
	for _, size := range []int{4095, 4096, 4097} {
		parts := splitHTMLMessage(strings.Repeat("я", size), maxTelegramMessageRunes)
		want := 1
		if size > maxTelegramMessageRunes {
			want = 2
		}
		if len(parts) != want {
			t.Fatalf("длина %d: частей %d, ожидалось %d", size, len(parts), want)
		}
	}
	event := domain.Event{
		SourceAt: time.Now(), Position: domain.Position{UnitName: "Точка", Frequency: "100.100", GroupCode: "A1"},
		Blocks: []domain.ContentBlock{{Text: "12.00\n" + strings.Repeat("Длинный текст & < >. ", 900), Callsigns: []domain.Callsign{{Number: 1, Code: "1", Label: "Марс", Description: strings.Repeat("описание ", 350)}}}},
	}
	chunks := formatEventChunks(event, domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver}}, "313", "СВОЯ-ЗАЩИТА", DefaultMessageTemplate)
	if len(chunks) < 2 {
		t.Fatal("длинное итоговое сообщение должно быть разделено")
	}
	for index, chunk := range chunks {
		if runeLen(chunk) > maxTelegramMessageRunes {
			t.Fatalf("часть %d длиннее лимита: %d", index+1, runeLen(chunk))
		}
		var root struct{}
		if err := xml.Unmarshal([]byte("<root>"+chunk+"</root>"), &root); err != nil {
			t.Fatalf("часть %d содержит несбалансированный HTML: %v", index+1, err)
		}
	}
}

func TestMissingDescriptionIsNotInvented(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		SourceAt: time.Now(), Position: domain.Position{UnitName: "Точка"},
		Blocks: []domain.ContentBlock{{Text: "12.00\n-Принято. (7)", Callsigns: []domain.Callsign{{Number: 7, Code: "7", Label: "Марс"}}}},
	}
	message := FormatEventForNetwork(event, "313", false)
	if !strings.Contains(message, "сведения не указаны") || strings.Contains(message, "мужской голос") {
		t.Fatalf("программа подставила неподтверждённые сведения: %s", message)
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
		"19.10.2026г.", "1. «<b>Марс</b>»", "<b>03:51</b>", "/1/ На приём.",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("в отложенном сообщении нет %q:\n%s", expected, got)
		}
	}
}

// При изменении перехвата одно объединённое сообщение редактируется, а
// исчезнувший временной блок не оставляет отдельный дубликат.
func TestDestinationEditsCombinedRecordWithoutDuplicates(t *testing.T) {
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
		SourceAt: time.Date(2026, 10, 21, 8, 40, 3, 0, time.UTC),
		Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{
			{Key: "12.00", Sequence: 0, Text: "12.00\n-Первое"},
			{Key: "12.01", Sequence: 1, Text: "12.01\n-Второе"},
		},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 1 || !strings.Contains(texts[0], "<b>12:00</b>") || !strings.Contains(texts[0], "<b>12:01</b>") {
		t.Fatalf("временные блоки не объединены в первом сообщении: %#v", texts)
	}
	if strings.Contains(texts[0], "(отредактировано") {
		t.Fatalf("у первой отправки не должно быть отметки редактирования: %q", texts[0])
	}
	event.Blocks = []domain.ContentBlock{{Key: "12.00", Sequence: 0, Text: "12.00\n-Первое исправлено"}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "editMessageText"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
	if len(texts) != 2 || !strings.Contains(texts[1], "Первое исправлено") ||
		!strings.HasSuffix(texts[1], "(отредактировано 11:40:03)") {
		t.Fatalf("исправленный текст не передан через editMessageText: %#v", texts)
	}
	if !strings.Contains(texts[1], "Первое исправлено\n\n(отредактировано 11:40:03)") {
		t.Fatalf("перед отметкой редактирования должна быть одна пустая строка: %q", texts[1])
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 2 {
		t.Fatalf("неизменённая запись после правки отправлена повторно: %v", methods)
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

func TestDestinationDoesNotReuseMessageIDsAfterTargetChange(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{
		Scope: "old-target",
		Items: map[string]destinationItem{"textile|source-1": {
			ThreadID: 42,
			Blocks:   map[string]destinationBlock{"__record__": {MessageIDs: []int64{77}}},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": 101},
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1002, StatePath: statePath, StateScope: "new-target"}
	event := domain.Event{
		ClientID: "textile", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position: domain.Position{UnitName: "позиция"},
		Blocks:   []domain.ContentBlock{{Key: "12.00", Text: "12.00\n-Текст"}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 1 || methods[0] != "sendMessage" {
		t.Fatalf("старый ID использован в новом назначении: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	if err != nil || current.Scope != "new-target" {
		t.Fatalf("новая область состояния не сохранена: %#v err=%v", current, err)
	}
}
