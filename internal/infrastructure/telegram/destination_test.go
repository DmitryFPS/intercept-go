package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
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

func TestFormatReviewSuggestionUsesWarningIcon(t *testing.T) {
	t.Parallel()

	event := domain.Event{
		ID: "event", ClientID: "client", CreatedAt: time.Now(),
		Source:   domain.SourceRecord{ID: 1},
		Position: domain.Position{UnitName: "1 мб 155 омб", Frequency: "100"},
		Mode:     domain.SourceModeUnknown, Content: "15:03\n/1/ Проверка", ContentDigest: "digest",
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{
		Action: domain.RouteReview,
		Note:   "⚠️ Возможно, имелось в виду: 1 мб 155 омбр",
	}}
	text := formatBlock(event, combinedEventBlock(event), delivery, "", "", "", "")
	if !strings.Contains(text, "⚠️ Возможно, имелось в виду: 1 мб 155 омбр") {
		t.Fatalf("предупреждение о возможном совпадении не выделено: %q", text)
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
		"159.3000 МГц (гр. ID 18902 AES-256)\n" +
		"20.10.2026г.\n\n" +
		"1.«<b>Торба</b>» (id: 52325) – Старший группы Гольназ, мужской голос, украинская речь;\n" +
		"2.«<b>Монако</b>» (id: 52345) – ОД на КНП Гольназ, мужской голос, украинская речь;\n\n" +
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
				Callsigns: []domain.Callsign{
					{Number: 3, Code: "61021", Label: "Рубин"},
					{Number: 1, Code: "52345", Label: "Монако", Description: "ОД, мужской голос"},
				},
			},
			{
				Key: "21.14", Sequence: 2, Text: "21.14\n-Доложите обстановку. (61022)\n-Без изменений. (52345)",
				Callsigns: []domain.Callsign{{Number: 4, Code: "61022", Label: "Вектор"}},
			},
		},
	}
	got := FormatEventForNetwork(event, "313", false)
	for _, expected := range []string{
		"1.«<b>Монако</b>»", "2.«<b>Торба</b>»", "3.«<b>Рубин</b>»", "4.«<b>Вектор</b>»",
		"<b>17:30</b>\n/1/ На связи.\n/2/ Слышу уверенно.",
		"<b>20:51</b>\n/3/ Принял, продолжаю работу.",
		"<b>21:14</b>\n/4/ Доложите обстановку.\n/1/ Без изменений.",
		"1.«<b>Монако</b>» (id: 52345) – ОД, мужской голос;",
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

func TestLegacyDefaultTemplateMigratesToWordRadioNetworkLabel(t *testing.T) {
	t.Parallel()
	legacy := strings.Replace(DefaultMessageTemplate, "МГц (гр. ID", "МГц (ID", 1)
	migrated := MigrateMessageTemplate(legacy)
	if !strings.Contains(migrated, "МГц (гр. ID {{GROUP_ID}} {{PROTECTION}})") {
		t.Fatalf("старый шаблон не приведён к общему с Word виду: %q", migrated)
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
	if !strings.Contains(message, "1.«<b>Марс</b>» (id: 7);") ||
		strings.Contains(message, "сведения не указаны") || strings.Contains(message, ") – ;") ||
		strings.Contains(message, "мужской голос") {
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
		"19.10.2026г.", "1.«<b>Марс</b>»", "<b>03:51</b>", "/1/ На приём.",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("в отложенном сообщении нет %q:\n%s", expected, got)
		}
	}
}

// Каждый UUID-блок публикуется отдельно. Правка одного блока редактирует только
// его message_id, а удаление соседнего не создаёт нового сообщения.
func TestDestinationKeepsIndependentBlocksWithoutDuplicates(t *testing.T) {
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
			{UUID: "block-1", Key: "12.00", Sequence: 0, Text: "12.00\n-Первое"},
			{UUID: "block-2", Key: "12.01", Sequence: 1, Text: "12.01\n-Второе"},
		},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(texts) != 2 || !strings.Contains(texts[0], "<b>12:00</b>") ||
		strings.Contains(texts[0], "<b>12:01</b>") || !strings.Contains(texts[1], "<b>12:01</b>") {
		t.Fatalf("UUID-блоки не разделены: %#v", texts)
	}
	if strings.Contains(texts[0], "(отредактировано") {
		t.Fatalf("у первой отправки не должно быть отметки редактирования: %q", texts[0])
	}
	event.Blocks = []domain.ContentBlock{{UUID: "block-1", Key: "12.00", Sequence: 0, Text: "12.00\n-Первое исправлено"}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "sendMessage", "editMessageText", "deleteMessage"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
	if len(texts) != 3 || !strings.Contains(texts[2], "Первое исправлено") ||
		!strings.HasSuffix(texts[2], "(отредактировано 11:40:03)") {
		t.Fatalf("исправленный текст не передан через editMessageText: %#v", texts)
	}
	if !strings.Contains(texts[2], "Первое исправлено\n\n(отредактировано 11:40:03)") {
		t.Fatalf("перед отметкой редактирования должна быть одна пустая строка: %q", texts[2])
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 4 {
		t.Fatalf("неизменённая запись после правки отправлена повторно: %v", methods)
	}
	if len(destination.locks) != 0 {
		t.Fatalf("служебные блокировки не освобождены: %d", len(destination.locks))
	}
}

// Удалённая вручную публикация не восстанавливается автоматически. Её UUID
// подавляется, а параллельная обработка не создаёт повторов и не блокирует очередь.
func TestDestinationSuppressesManuallyDeletedMessage(t *testing.T) {
	t.Parallel()
	var sends, edits, deletes atomic.Int32
	var nextID atomic.Int64
	nextID.Store(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("декодировать %s: %v", method, err)
			return
		}
		switch method {
		case "sendMessage":
			sends.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": nextID.Add(1)},
			})
		case "editMessageText":
			edits.Add(1)
			if int64(body["message_id"].(float64)) == 101 {
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found",
				})
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		case "deleteMessage":
			deletes.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message to delete not found",
			})
		default:
			http.Error(writer, "неожиданный метод", http.StatusNotFound)
		}
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	var logs bytes.Buffer
	statePath := filepath.Join(t.TempDir(), "state.json")
	destination := Destination{
		API: api, ChatID: -1001, StatePath: statePath,
		Logger: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		SourceAt: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "позиция"},
		Blocks:   []domain.ContentBlock{{Key: "12.00", Text: "12.00\n-Первое"}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	event.SourceAt = event.SourceAt.Add(time.Minute)
	start := make(chan struct{})
	errorsFound := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			errorsFound <- destination.Deliver(context.Background(), event, delivery)
		}()
	}
	close(start)
	workers.Wait()
	close(errorsFound)
	for deliveryErr := range errorsFound {
		if deliveryErr != nil {
			t.Fatalf("восстановление завершилось ошибкой: %v", deliveryErr)
		}
	}
	if sends.Load() != 1 || edits.Load() != 1 || deletes.Load() != 1 {
		t.Fatalf("удалённое сообщение не должно восстанавливаться: sends=%d edits=%d deletes=%d", sends.Load(), edits.Load(), deletes.Load())
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["12.00"]
	if !block.Suppressed || len(block.MessageIDs) != 0 || len(block.DeletedMessageIDs) != 1 || block.DeletedMessageIDs[0] != 101 {
		t.Fatalf("удалённый message_id не зафиксирован как подавленный: %#v", block)
	}
	if !strings.Contains(logs.String(), "не будет восстановлена автоматически") {
		t.Fatalf("в журнале нет результата подавления: %s", logs.String())
	}
	event.Blocks[0].Text = "12.00\n-Третья редакция"
	event.SourceAt = event.SourceAt.Add(time.Minute)
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 1 || edits.Load() != 1 {
		t.Fatalf("следующая версия восстановила удалённое сообщение: sends=%d edits=%d", sends.Load(), edits.Load())
	}
}

// Подавление удалённого сообщения переживает перезапуск. Даже новая редакция
// того же block_uuid не создаёт публикацию, пока оператор явно не разрешит её.
func TestDestinationKeepsDeletedMessageSuppressedAfterRestart(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		switch method {
		case "sendMessage":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 101},
			})
		case "editMessageText":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found",
			})
		case "deleteMessage":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message to delete not found",
			})
		}
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		SourceAt: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "позиция"},
		Blocks:   []domain.ContentBlock{{UUID: "block-1", Key: "12.00", Text: "12.00\n-Первое"}},
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	restarted := Destination{API: api, ChatID: -1001, StatePath: statePath}
	event.Blocks[0].Text = "12.00\n-Третья редакция"
	if err := restarted.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "editMessageText", "deleteMessage"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("после перезапуска удалённая публикация восстановлена: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	if err != nil || !current.Items["point-01|source-1"].Blocks["block-1"].Suppressed {
		t.Fatalf("подавление не сохранено: %#v err=%v", current, err)
	}
}

func TestDestinationRepublishesOnlyExplicitlySelectedBlock(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		switch method {
		case "sendMessage":
			nextID++
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": nextID}})
		case "editMessageText":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found",
			})
		case "deleteMessage":
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": false, "error_code": 400, "description": "Bad Request: message to delete not found",
			})
		}
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{
			{UUID: "block-1", Sequence: 0, Text: "12.00\n-Первое"},
			{UUID: "block-2", Sequence: 1, Text: "12.01\n-Второе"},
		},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	allowed, err := destination.AllowRepublish("block-1")
	if err != nil || !allowed {
		t.Fatalf("повторная публикация не разрешена: allowed=%v err=%v", allowed, err)
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	first := current.Items["point-01|source-1"].Blocks["block-1"]
	second := current.Items["point-01|source-1"].Blocks["block-2"]
	if first.Suppressed || !reflect.DeepEqual(first.MessageIDs, []int64{103}) ||
		!reflect.DeepEqual(second.MessageIDs, []int64{102}) {
		t.Fatalf("неверная привязка после ручной публикации: first=%#v second=%#v", first, second)
	}
	want := []string{"sendMessage", "sendMessage", "editMessageText", "deleteMessage", "sendMessage"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("чужой блок затронут при повторной публикации: %v", methods)
	}
}

// Если сообщение существует, но Telegram окончательно запретил его правку,
// новое не создаётся: иначе в теме остались бы две публикации.
func TestDestinationDoesNotReplaceExistingUneditableMessage(t *testing.T) {
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
	err = destination.Deliver(context.Background(), event, delivery)
	if err == nil {
		t.Fatal("невозможная правка не должна считаться доставленной")
	}
	if !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неисправимая правка ошибочно назначит повтор: %v", err)
	}
	want := []string{"sendMessage", "editMessageText"}
	if strings.Join(methods, ",") != strings.Join(want, ",") {
		t.Fatalf("неожиданные действия Telegram: %v", methods)
	}
}

func TestTerminalEditErrorClassification(t *testing.T) {
	t.Parallel()
	for _, description := range []string{
		"Bad Request: message to edit not found",
		"Bad Request: message can't be edited",
		"Bad Request: MESSAGE_ID_INVALID",
	} {
		if !terminalEditError(&apiError{code: 400, description: description}) {
			t.Errorf("окончательная ошибка не распознана: %s", description)
		}
	}
	if terminalEditError(&apiError{code: 500, description: "message to edit not found"}) {
		t.Fatal("временная серверная ошибка Telegram ошибочно завершит доставку")
	}
	if terminalEditError(&apiError{code: 400, description: "Bad Request: chat not found"}) {
		t.Fatal("исправимая настройкой ошибка чата ошибочно завершит доставку")
	}
}

func TestMissingEditErrorClassification(t *testing.T) {
	t.Parallel()
	for _, description := range []string{
		"Bad Request: message to edit not found",
		"Bad Request: MESSAGE_ID_INVALID",
	} {
		if !missingEditError(&apiError{code: 400, description: description}) {
			t.Errorf("отсутствующее сообщение не распознано: %s", description)
		}
	}
	if missingEditError(&apiError{code: 400, description: "Bad Request: message can't be edited"}) {
		t.Fatal("существующее нередактируемое сообщение ошибочно будет заменено")
	}
	if missingEditError(&apiError{code: 500, description: "message to edit not found"}) {
		t.Fatal("временная серверная ошибка Telegram ошибочно запустит восстановление")
	}
}

func TestDestinationCleanupRemovesPreviousMessageAndState(t *testing.T) {
	t.Parallel()
	deleted := int64(0)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if strings.HasSuffix(request.URL.Path, "/deleteMessage") {
			deleted = int64(body["message_id"].(float64))
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"textile|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"__record__": {MessageIDs: []int64{101}},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	event := domain.Event{ClientID: "textile", Source: domain.SourceRecord{UUID: "source-1"}}
	cleanup := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteSkip}}
	if err := destination.Deliver(context.Background(), event, cleanup); err != nil {
		t.Fatal(err)
	}
	if deleted != 101 {
		t.Fatalf("прежнее сообщение не удалено: %d", deleted)
	}
	current, err := loadDestinationState(statePath)
	if err != nil || len(current.Items) != 0 {
		t.Fatalf("состояние прежнего сообщения осталось: %#v, %v", current.Items, err)
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

// Компактная связь с Telegram message_id хранится без срока: EAVC допускает
// позднюю правку, которая должна изменить прежнее сообщение, а не создать дубль.
func TestDestinationStateKeepsOldIdentityForLateEdit(t *testing.T) {
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
	if len(current.Items) != 2 {
		t.Fatalf("старая связь с сообщением потеряна: %#v", current.Items)
	}
	if _, exists := current.Items["old"]; !exists {
		t.Fatal("старая связь с message_id удалена")
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

func TestDestinationMigratesLegacyTokenScopeWithoutLosingMessageIDs(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{
		Scope: "legacy-token-scope",
		Items: map[string]destinationItem{"event": {ThreadID: 2}},
	}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{
		StatePath: statePath, StateScope: "stable-bot-scope", LegacyStateScope: "legacy-token-scope",
	}
	current, err := destination.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if current.Scope != "stable-bot-scope" || len(current.Items) != 1 {
		t.Fatalf("миграция области потеряла историю сообщений: %#v", current)
	}
}

func TestDestinationReceiptReturnsFinalTelegramMessageIDs(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	event := domain.Event{ClientID: "textile", Source: domain.SourceRecord{UUID: "source-1"}}
	if err := saveDestinationState(statePath, destinationState{
		Items: map[string]destinationItem{"textile|source-1": {
			ThreadID: 17,
			Blocks: map[string]destinationBlock{
				"block-2": {Sequence: 1, MessageIDs: []int64{702}},
				"block-1": {Sequence: 0, MessageIDs: []int64{701}},
				"block-3": {Sequence: 2, MessageIDs: []int64{703}, Suppressed: true},
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{ChatID: -1003943357633, StatePath: statePath}
	receipt, err := destination.DeliveryReceipt(event, domain.PlannedDelivery{})
	if err != nil || receipt.Kind != "telegram" || receipt.ChatID != destination.ChatID ||
		receipt.ThreadID != 17 || len(receipt.MessageIDs) != 2 || receipt.MessageIDs[0] != 701 {
		t.Fatalf("неверный результат Telegram-доставки: %#v err=%v", receipt, err)
	}
}

func TestDestinationMigratesRC81CombinedMessageToUUIDBlocks(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 17, Blocks: map[string]destinationBlock{
			"__record__": {Hash: "old", SourceHash: "old", MessageIDs: []int64{701}},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		if method == "sendMessage" {
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 702}})
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	event := domain.Event{
		ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{
			{UUID: "block-1", Sequence: 0, Text: "12.00\n-Первое"},
			{UUID: "block-2", Sequence: 1, Text: "12.01\n-Второе"},
		},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 17}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(methods, []string{"editMessageText", "sendMessage"}) {
		t.Fatalf("неверная миграция rc81: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	blocks := current.Items["point-01|source-1"].Blocks
	if _, exists := blocks["__record__"]; exists ||
		!reflect.DeepEqual(blocks["block-1"].MessageIDs, []int64{701}) ||
		!reflect.DeepEqual(blocks["block-2"].MessageIDs, []int64{702}) {
		t.Fatalf("message_id потеряны при миграции: %#v", blocks)
	}
}
