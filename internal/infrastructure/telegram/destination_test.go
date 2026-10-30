package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
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

func TestFormatEventNormalizesAllMarkedTimeHeaders(t *testing.T) {
	t.Parallel()
	event := domain.Event{
		SourceAt: time.Date(2026, 10, 28, 10, 45, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "Игровая команда", Frequency: "100.100", GroupCode: "GAME-1"},
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.15\n-Начало раунда. (7)\n10.18#\n-Уточнение. (7)\n10.28#1\n-Финиш. (7)",
			Callsigns: []domain.Callsign{{Code: "7", Label: "Игрок"}},
		}},
	}
	got := FormatEventForNetwork(event, "Тест", false)
	for _, expected := range []string{
		"<b>10:15</b>", "<b>10:18</b>", "<b>10:28</b>",
		"/1/ Начало раунда.", "/1/ Уточнение.", "/1/ Финиш.",
	} {
		if !strings.Contains(got, expected) {
			t.Fatalf("в Telegram нет %q:\n%s", expected, got)
		}
	}
	if strings.Contains(got, "10.18#") || strings.Contains(got, "10.28#1") {
		t.Fatalf("служебная решётка попала в Telegram: %s", got)
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

func TestSplitHTMLMessageLargeInputUsesBoundedAllocations(t *testing.T) {
	// Один допустимый блок EAVC может содержать 1 МиБ текста. Разбиение не
	// должно создавать новую строку для каждого символа такого блока.
	message := strings.Repeat("я", 1<<19)
	var chunks []string
	allocations := testing.AllocsPerRun(1, func() {
		chunks = splitHTMLMessage(message, maxTelegramMessageRunes-48)
	})
	if allocations > 5000 {
		t.Fatalf("слишком много аллокаций при разбиении 1 МиБ: %.0f", allocations)
	}
	if len(chunks) < 2 || strings.Join(chunks, "") != message {
		t.Fatalf("длинный текст разделён с потерями: частей %d", len(chunks))
	}
	for index, chunk := range chunks {
		if runeLen(chunk) > maxTelegramMessageRunes-48 {
			t.Fatalf("часть %d превышает лимит", index+1)
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

// Само ручное удаление не обнаруживается и не создаёт публикацию заново.
// Первая следующая содержательная редакция восстанавливает тот же UUID один раз.
func TestDestinationRestoresManuallyDeletedMessageOnNewContent(t *testing.T) {
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
		ID: "event-v1", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
		CreatedAt:       time.Date(2026, 10, 26, 12, 0, 0, 1, time.UTC),
		SourceAt:        time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC),
		Position:        domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Key: "12.00", Text: "12.00\n-Первое", UpdatedAt: "2026-10-26 12:00:00",
		}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	// Telegram не присылает боту событие ручного удаления. Точный повтор v1
	// должен остаться локальным no-op и не пытаться восстановить публикацию.
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	event.Blocks[0].UpdatedAt = "2026-10-26 12:01:00"
	event.ID = "event-v2"
	event.CreatedAt = event.CreatedAt.Add(time.Minute)
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
	if sends.Load() != 2 || edits.Load() != 1 || deletes.Load() != 1 {
		t.Fatalf("новая редакция не восстановлена ровно один раз: sends=%d edits=%d deletes=%d", sends.Load(), edits.Load(), deletes.Load())
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if block.Suppressed || !reflect.DeepEqual(block.MessageIDs, []int64{102}) ||
		!reflect.DeepEqual(block.DeletedMessageIDs, []int64{101}) || block.SourceVersion != "2026-10-26 12:01:00" {
		t.Fatalf("новый и удалённый message_id связаны неверно: %#v", block)
	}
	if !strings.Contains(logs.String(), "восстанавливается новой редакцией") {
		t.Fatalf("в журнале нет результата восстановления: %s", logs.String())
	}
	event.Blocks[0].Text = "12.00\n-Третья редакция"
	event.Blocks[0].UpdatedAt = "2026-10-26 12:02:00"
	event.ID = "event-v3"
	event.CreatedAt = event.CreatedAt.Add(time.Minute)
	event.SourceAt = event.SourceAt.Add(time.Minute)
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 2 || edits.Load() != 2 {
		t.Fatalf("следующая версия не изменила восстановленное сообщение: sends=%d edits=%d", sends.Load(), edits.Load())
	}
}

// Перезапуск не восстанавливает удалённую публикацию по точному повтору, но
// следующая содержательная версия создаёт новую привязку и дальше редактируется.
func TestDestinationRestoresDeletedMessageAfterRestartOnlyForNewVersion(t *testing.T) {
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
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": nextID},
			})
		case "editMessageText":
			if int64(body["message_id"].(float64)) == 101 {
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"ok": false, "error_code": 400, "description": "Bad Request: message to edit not found",
				})
			} else {
				_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
			}
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
		ID: "event-v1", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
		CreatedAt:       time.Date(2026, 10, 26, 12, 0, 0, 1, time.UTC),
		SourceAt:        time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC),
		Position:        domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Key: "12.00", Text: "12.00\n-Первое", UpdatedAt: "2026-10-26 12:00:00",
		}},
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	restarted := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := restarted.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.ID = "event-v2"
	event.CreatedAt = event.CreatedAt.Add(time.Minute)
	event.SourceAt = event.SourceAt.Add(time.Minute)
	event.Blocks[0].Text = "12.00\n-Первое исправлено"
	event.Blocks[0].UpdatedAt = "2026-10-26 12:01:00"
	if err := restarted.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	restartedAgain := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := restartedAgain.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	event.ID = "event-v3"
	event.CreatedAt = event.CreatedAt.Add(time.Minute)
	event.SourceAt = event.SourceAt.Add(time.Minute)
	event.Blocks[0].Text = "12.00\n-Третья редакция"
	event.Blocks[0].UpdatedAt = "2026-10-26 12:02:00"
	if err := restartedAgain.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	want := []string{"sendMessage", "editMessageText", "deleteMessage", "sendMessage", "editMessageText"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("неверные операции после перезапуска: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if err != nil || block.Suppressed || !reflect.DeepEqual(block.MessageIDs, []int64{102}) || block.SourceVersion != "2026-10-26 12:02:00" {
		t.Fatalf("восстановленная привязка не сохранена: %#v err=%v", current, err)
	}
}

func TestDestinationRepublishesOnlyExplicitlySelectedBlock(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(102)
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
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-1": {
				DeletedMessageIDs: []int64{101}, Suppressed: true,
				SuppressedReason: "сообщение удалено вручную в Telegram",
				SourceVersion:    "2026-10-26 12:00:00",
			},
			"block-2": {MessageIDs: []int64{102}, SourceHash: "second", Sequence: 1},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ID: "manual-v1", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"}, CreatedAt: time.Now(),
		Position:        domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Sequence: 0, Text: "12.00\n-Первое", UpdatedAt: "2026-10-26 12:00:00",
		}},
	}
	allowed, err := destination.AllowRepublish(event, "block-1")
	if err != nil || !allowed {
		t.Fatalf("повторная публикация не разрешена: allowed=%v err=%v", allowed, err)
	}
	// Повтор команды до фактической доставки не должен снять разрешение:
	// интерфейс или сеть могут безопасно повторить один и тот же запрос.
	permitReceipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(permitReceipt.Blocks) != 1 {
		t.Fatalf("разрешение не отражено в квитанции: receipt=%+v err=%v", permitReceipt, err)
	}
	allowed, err = destination.ResumeManualRepublish(event, "block-1", permitReceipt.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("повторное разрешение неидемпотентно: allowed=%v err=%v", allowed, err)
	}
	if err := destination.DeliverManually(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	first := current.Items["point-01|source-1"].Blocks["block-1"]
	second := current.Items["point-01|source-1"].Blocks["block-2"]
	if first.Suppressed || !reflect.DeepEqual(first.MessageIDs, []int64{103}) ||
		!reflect.DeepEqual(first.DeletedMessageIDs, []int64{101}) ||
		!reflect.DeepEqual(second.MessageIDs, []int64{102}) {
		t.Fatalf("неверная привязка после ручной публикации: first=%#v second=%#v", first, second)
	}
	want := []string{"sendMessage"}
	if !reflect.DeepEqual(methods, want) {
		t.Fatalf("чужой блок затронут при повторной публикации: %v", methods)
	}
}

func TestDestinationDoesNotDuplicateUncertainSendAfterRestart(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/sendMessage") {
			sends.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 301},
			})
			return
		}
		http.Error(writer, "неожиданный метод", http.StatusNotFound)
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {Blocks: map[string]destinationBlock{
			"block-1": {
				SendPending: true, SendPendingAt: "2026-10-28T12:00:00Z",
				SourceVersion: "2026-10-28 12:00:00",
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	event := domain.Event{
		ID: "event-1", ClientID: "point-01", CreatedAt: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
		Source: domain.SourceRecord{UUID: "source-1"}, Position: domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "12.00\n-Первое", UpdatedAt: "2026-10-28 12:00:00",
		}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("неопределённая отправка не изолирована: %v", err)
	}
	if sends.Load() != 0 {
		t.Fatalf("после неопределённой отправки создан автоматический дубль: sends=%d", sends.Load())
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if !block.Suppressed || block.SendPending || !strings.Contains(block.SuppressedReason, "дубль запрещён") {
		t.Fatalf("неопределённая попытка не получила безопасное состояние: %#v", block)
	}
	allowed, err := destination.AllowRepublish(event, "block-1")
	if err != nil || !allowed {
		t.Fatalf("оператор не может разрешить публикацию после проверки Telegram: allowed=%v err=%v", allowed, err)
	}
	if err := destination.DeliverManually(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if sends.Load() != 1 {
		t.Fatalf("ручное разрешение не создало ровно одно сообщение: sends=%d", sends.Load())
	}
}

func TestDestinationManualRepublishIsScopedToClientAndRecord(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {Blocks: map[string]destinationBlock{
			"shared-block": {Suppressed: true, DeletedMessageIDs: []int64{101}},
		}},
		"point-02|source-2": {Blocks: map[string]destinationBlock{
			"shared-block": {Suppressed: true, DeletedMessageIDs: []int64{202}},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{ChatID: -1001, StatePath: statePath}
	allowed, err := destination.AllowRepublish(domain.Event{
		ClientID: "point-02", Source: domain.SourceRecord{UUID: "source-2"},
	}, "shared-block")
	if err != nil || !allowed {
		t.Fatalf("выбранный блок второй точки не разрешён: allowed=%v err=%v", allowed, err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Items["point-01|source-1"].Blocks["shared-block"].Suppressed ||
		current.Items["point-02|source-2"].Blocks["shared-block"].Suppressed {
		t.Fatalf("повторная публикация затронула чужую точку: %#v", current.Items)
	}
}

func TestDestinationManualRepublishPreservesKnownParts(t *testing.T) {
	t.Parallel()
	var sends atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/editMessageText"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			id := 701 + sends.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": id},
			})
		default:
			http.Error(writer, "неожиданный метод", http.StatusNotFound)
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	client.sends.globalInterval = 0
	client.sends.groupInterval = 0
	event := domain.Event{
		ID: "partial-manual", ClientID: "point-01",
		CreatedAt: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
		SourceAt:  time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
		Source:    domain.SourceRecord{UUID: "source-1"},
		Position:  domain.Position{UnitName: "позиция"}, SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Sequence: 0, UpdatedAt: "2026-10-28 12:00:00",
			Text: "12.00\n" + strings.Repeat("-Длинная строка. (10001)\n", 420),
		}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	chunks := formatBlockChunksWithEditedAt(event, event.Blocks[0], delivery, "", "", "", event.SourceAt)
	if len(chunks) < 2 {
		t.Fatalf("контрольный текст не разбит на части: %d", len(chunks))
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-1": {
				MessageIDs: []int64{701}, ExpectedMessageCount: len(chunks),
				Suppressed: true, SuppressedReason: "исход следующей части неизвестен",
				SourceVersion: event.SourceVersion(), SourceEventAt: event.CreatedAt,
				ThreadID: 42, SendPendingIndex: 1,
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: client, ChatID: -1001, StatePath: statePath}
	allowed, err := destination.AllowRepublish(event, "block-1")
	if err != nil || !allowed {
		t.Fatalf("ручной повтор не разрешён: allowed=%v err=%v", allowed, err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(current.Items["point-01|source-1"].Blocks["block-1"].MessageIDs, []int64{701}) {
		t.Fatal("известная первая часть потеряна до повтора")
	}
	if err := destination.DeliverManually(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(receipt.Blocks) != 1 || !receipt.Blocks[0].Complete ||
		len(receipt.Blocks[0].MessageIDs) != len(chunks) || receipt.Blocks[0].MessageIDs[0] != 701 ||
		int(sends.Load()) != len(chunks)-1 {
		t.Fatalf("повтор создал дубль известной части: receipt=%#v sends=%d err=%v", receipt, sends.Load(), err)
	}
}

func TestDestinationDoesNotAllowRepublishWhileSendOutcomeIsUnknown(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {Blocks: map[string]destinationBlock{
			"block-1": {
				SendPending: true, SendPendingIndex: 1, SendPendingAt: "2026-10-28T12:00:00Z",
				DeletedMessageIDs: []int64{101}, SourceVersion: "2026-10-28 12:00:00",
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{ChatID: -1001, StatePath: statePath}
	allowed, err := destination.AllowRepublish(domain.Event{
		ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
	}, "block-1")
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("неопределённая Telegram-отправка ошибочно разрешена к повтору")
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if !block.SendPending || block.SendPendingIndex != 1 {
		t.Fatalf("защитный маркер неопределённой отправки очищен: %#v", block)
	}
}

func TestDestinationResumesMissingChunksWithoutCompletingPartialPublication(t *testing.T) {
	t.Parallel()
	var sendCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		switch method {
		case "editMessageText":
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		case "sendMessage":
			call := sendCalls.Add(1)
			if call == 2 {
				writer.WriteHeader(http.StatusTooManyRequests)
				_ = json.NewEncoder(writer).Encode(map[string]any{
					"ok": false, "error_code": 429, "description": "временное ограничение частоты Telegram",
					"parameters": map[string]any{"retry_after": 1},
				})
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 700 + call},
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
	statePath := filepath.Join(t.TempDir(), "state.json")
	event := domain.Event{
		ID: "event-long", ClientID: "point-01", CreatedAt: time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC),
		Source: domain.SourceRecord{UUID: "source-long"}, Position: domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-long", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{
			UUID: "block-long", Sequence: 0, UpdatedAt: "2026-10-28 12:00:00",
			Text: "12.00\n" + strings.Repeat("-Длинная контрольная строка для проверки частей. (10001)\n", 420),
		}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	expected := len(formatBlockChunks(event, event.Blocks[0], delivery, "", "", ""))
	if expected < 2 {
		t.Fatalf("тестовый перехват не разбит на части: %d", expected)
	}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("первая доставка должна остановиться на второй части")
	}
	partial, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(partial.Blocks) != 1 {
		t.Fatalf("не удалось прочитать частичную квитанцию: %#v err=%v", partial, err)
	}
	if partial.Blocks[0].Complete || partial.Blocks[0].ExpectedMessageCount != expected ||
		!reflect.DeepEqual(partial.Blocks[0].MessageIDs, []int64{701}) {
		t.Fatalf("частичная публикация ошибочно завершена: %#v", partial.Blocks[0])
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatalf("недостающие части не продолжены: %v", err)
	}
	complete, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(complete.Blocks) != 1 || !complete.Blocks[0].Complete {
		t.Fatalf("публикация не стала полной: %#v err=%v", complete, err)
	}
	if complete.Blocks[0].MessageIDs[0] != 701 ||
		len(complete.Blocks[0].MessageIDs) != complete.Blocks[0].ExpectedMessageCount {
		t.Fatalf("первая часть продублирована или потеряна: %#v", complete.Blocks[0])
	}
}

func TestDestinationRecoversRC97EditedChunkCountAtTelegramBoundary(t *testing.T) {
	t.Parallel()
	editedAt := time.Date(2026, 10, 28, 15, 45, 30, 0, time.UTC)
	event := domain.Event{
		ID: "event-boundary", ClientID: "point-01", CreatedAt: editedAt, SourceAt: editedAt,
		Source: domain.SourceRecord{UUID: "source-boundary"}, Position: domain.Position{UnitName: "позиция"},
		SourceBlockUUID: "block-boundary", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{UUID: "block-boundary", Sequence: 0, UpdatedAt: "2026-10-28 15:45:30"}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	var baseChunks, editedChunks []string
	for runes := 3000; runes <= 5000; runes++ {
		event.Blocks[0].Text = "15.45\n-" + strings.Repeat("А", runes) + " (10001)"
		baseChunks = formatBlockChunks(event, event.Blocks[0], delivery, "", "", "")
		editedChunks = formatBlockChunksWithEditedAt(event, event.Blocks[0], delivery, "", "", "", editedAt)
		if len(baseChunks) == 1 && len(editedChunks) == 2 {
			break
		}
	}
	if len(baseChunks) != 1 || len(editedChunks) != 2 {
		t.Fatalf("не найдена контрольная граница Telegram: base=%d edited=%d", len(baseChunks), len(editedChunks))
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-boundary": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-boundary": {
				Hash: hashChunks(editedChunks), SourceHash: hashChunks(baseChunks),
				SourceVersion: event.SourceVersion(), SourceEventAt: event.CreatedAt,
				// rc97 сохранял хеш отредактированных частей, но ещё не имел
				// ExpectedMessageCount. После обновления хвост обязан продолжиться.
				ThreadID: 42, MessageIDs: []int64{701},
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		if method == "editMessageText" {
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
			return
		}
		if method == "sendMessage" {
			sends.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": 702},
			})
			return
		}
		http.Error(writer, "неожиданный метод", http.StatusNotFound)
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatalf("пограничный хвост не продолжен: %v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("ожидалась ровно одна недостающая часть, отправлено %d", sends.Load())
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(receipt.Blocks) != 1 || !receipt.Blocks[0].Complete ||
		!reflect.DeepEqual(receipt.Blocks[0].MessageIDs, []int64{701, 702}) {
		t.Fatalf("пограничная публикация не завершена: %#v err=%v", receipt, err)
	}
}

func TestDestinationScopedEventNeverChangesSiblingBlock(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:])
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-a": {SourceHash: "old-a", Hash: "old-a", MessageIDs: []int64{101}, Sequence: 0},
			"block-b": {SourceHash: "old-b", Hash: "old-b", MessageIDs: []int64{102}, Sequence: 1},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ID: "event-a", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
		Position: domain.Position{UnitName: "позиция"}, SourceBlockUUID: "block-a",
		HasActiveBlocks: true,
		Blocks:          []domain.ContentBlock{{UUID: "block-a", Text: "12.00\n-Первый исправлен"}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(receipt.Blocks) != 1 || receipt.Blocks[0].BlockUUID != "block-a" ||
		!reflect.DeepEqual(receipt.MessageIDs, []int64{101}) {
		t.Fatalf("квитанция смешала соседние блоки: %#v err=%v", receipt, err)
	}
	event.ID = "delete-a"
	event.SourceBlockDeleted = true
	event.HasActiveBlocks = true
	event.Blocks[0].DeletedAt = "2026-10-27 19:00:00"
	event.Blocks[0].Text = ""
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	item := current.Items["point-01|source-1"]
	deleted := item.Blocks["block-a"]
	if !deleted.SourceDeleted || len(deleted.MessageIDs) != 0 || !reflect.DeepEqual(deleted.DeletedMessageIDs, []int64{101}) ||
		!reflect.DeepEqual(item.Blocks["block-b"].MessageIDs, []int64{102}) {
		t.Fatalf("удаление одного блока затронуло соседний: %#v", item)
	}
	if want := []string{"editMessageText", "deleteMessage"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("неверные Telegram-операции: got=%v want=%v", methods, want)
	}
}

func TestDestinationUndeletableBlockIsPermanentFailure(t *testing.T) {
	t.Parallel()
	var deletes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/deleteMessage") {
			http.Error(writer, "неожиданный метод", http.StatusNotFound)
			return
		}
		deletes.Add(1)
		writer.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": false, "error_code": 400, "description": "Bad Request: message can't be deleted",
		})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-a": {MessageIDs: []int64{101}, SourceVersion: "2026-10-27 18:00:00", ThreadID: 42},
			"block-b": {MessageIDs: []int64{102}, SourceVersion: "2026-10-27 18:00:00", ThreadID: 42},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	event := domain.Event{
		ID: "delete-a", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
		CreatedAt: time.Date(2026, 10, 27, 19, 0, 0, 0, time.UTC),
		Position:  domain.Position{UnitName: "позиция"}, SourceBlockUUID: "block-a",
		SourceBlockDeleted: true, HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{UUID: "block-a", DeletedAt: "2026-10-27 19:00:00"}},
	}
	err = destination.Deliver(context.Background(), event, delivery)
	if err == nil || !application.IsPermanentDeliveryError(err) || !strings.Contains(err.Error(), "can't be deleted") {
		t.Fatalf("невозможное удаление не классифицировано как окончательная ошибка: %v", err)
	}
	if deletes.Load() != 1 {
		t.Fatalf("ожидалась одна попытка удаления, получено %d", deletes.Load())
	}
	current, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	item := current.Items["point-01|source-1"]
	if item.Blocks["block-a"].SourceDeleted ||
		!reflect.DeepEqual(item.Blocks["block-a"].MessageIDs, []int64{101}) ||
		!reflect.DeepEqual(item.Blocks["block-b"].MessageIDs, []int64{102}) {
		t.Fatalf("отказ Telegram нельзя записывать как успешное удаление: %#v", item)
	}
}

func TestDestinationSourceTombstoneRejectsLateEditAndAllowsNewerRestore(t *testing.T) {
	t.Parallel()
	var methods, texts []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if text, ok := body["text"].(string); ok {
			texts = append(texts, text)
		}
		if method == "sendMessage" {
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
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	destination := Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	created := time.Date(2026, 10, 28, 10, 0, 0, 1, time.UTC)
	event := domain.Event{
		ID: "v1", ClientID: "point-01", CreatedAt: created, SourceAt: created,
		Source: domain.SourceRecord{UUID: "source-1"}, Position: domain.Position{UnitName: "Игровая команда"},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{UUID: "block-1", Text: "10.00\n-Старт", UpdatedAt: "2026-10-28 10:00:00"}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	deletion := event
	deletion.ID = "v3-delete"
	deletion.CreatedAt = created.Add(3 * time.Minute)
	deletion.SourceAt = deletion.CreatedAt
	deletion.SourceBlockDeleted = true
	deletion.HasActiveBlocks = false
	deletion.Blocks = []domain.ContentBlock{{UUID: "block-1", DeletedAt: "2026-10-28 10:03:00"}}
	if err := destination.Deliver(context.Background(), deletion, delivery); err != nil {
		t.Fatal(err)
	}
	stale := event
	stale.ID = "v2-late"
	stale.CreatedAt = created.Add(2 * time.Minute)
	stale.SourceAt = stale.CreatedAt
	stale.Blocks[0].Text = "10.00\n-Старая запоздалая редакция"
	stale.Blocks[0].UpdatedAt = "2026-10-28 10:02:00"
	if err := destination.Deliver(context.Background(), stale, delivery); err != nil {
		t.Fatal(err)
	}
	restored := event
	restored.ID = "v4-restore"
	restored.CreatedAt = created.Add(4 * time.Minute)
	restored.SourceAt = restored.CreatedAt
	restored.Blocks[0].Text = "10.00\n-Новая версия после восстановления источника"
	restored.Blocks[0].UpdatedAt = "2026-10-28 10:04:00"
	if err := destination.Deliver(context.Background(), restored, delivery); err != nil {
		t.Fatal(err)
	}
	if err := destination.Deliver(context.Background(), deletion, delivery); err != nil {
		t.Fatal(err)
	}
	if want := []string{"sendMessage", "deleteMessage", "sendMessage"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("устаревшая версия изменила Telegram: got=%v want=%v", methods, want)
	}
	if len(texts) != 2 || !strings.Contains(texts[1], "Новая версия после восстановления источника\n\n(отредактировано 13:04:00)") {
		t.Fatalf("восстановленная версия оформлена неверно: %#v", texts)
	}
	current, err := loadDestinationState(destination.StatePath)
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if err != nil || block.SourceDeleted || !reflect.DeepEqual(block.MessageIDs, []int64{102}) ||
		!reflect.DeepEqual(block.DeletedMessageIDs, []int64{101}) || block.SourceVersion != "2026-10-28 10:04:00" {
		t.Fatalf("неверное состояние после tombstone/restore: %#v err=%v", block, err)
	}
}

func TestDestinationVersionedStateRejectsLateThreadMove(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:])
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	storedAt := time.Date(2026, 10, 28, 11, 0, 0, 0, time.UTC)
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {
			ThreadID: 42, UpdatedAt: storedAt,
			Blocks: map[string]destinationBlock{
				"block-1": {
					SourceHash: "current", MessageIDs: []int64{101},
					SourceVersion: "2026-10-28 11:00:00", SourceEventAt: storedAt,
				},
			},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	event := domain.Event{
		ID: "late-v1", ClientID: "point-01", CreatedAt: storedAt.Add(-time.Minute),
		Source: domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:59:00"}, SourceBlockUUID: "block-1",
		Position: domain.Position{UnitName: "Игровая команда"},
		Blocks:   []domain.ContentBlock{{UUID: "block-1", Text: "10.59\n-Старая версия"}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 99}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 0 {
		t.Fatalf("запоздалое событие rc90 изменило Telegram: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	item := current.Items["point-01|source-1"]
	if err != nil || item.ThreadID != 42 || !reflect.DeepEqual(item.Blocks["block-1"].MessageIDs, []int64{101}) {
		t.Fatalf("запоздалое событие изменило legacy-состояние: %#v err=%v", item, err)
	}
}

func TestDestinationVersionedStateRejectsLateEditInSameThread(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:])
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	storedAt := time.Date(2026, 10, 28, 11, 0, 0, 0, time.UTC)
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {
			ThreadID: 42, UpdatedAt: storedAt,
			Blocks: map[string]destinationBlock{
				"block-1": {
					SourceHash: "current", MessageIDs: []int64{101},
					SourceVersion: "2026-10-28 11:00:00", SourceEventAt: storedAt,
				},
			},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	event := domain.Event{
		ID: "late-v1", ClientID: "point-01", CreatedAt: storedAt.Add(-time.Minute),
		Source: domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:59:00"}, SourceBlockUUID: "block-1",
		Position: domain.Position{UnitName: "Игровая команда"},
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.59\n-Старая версия", UpdatedAt: "2026-10-28 10:59:00",
		}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 0 {
		t.Fatalf("запоздалая правка откатила ту же тему: %v", methods)
	}
}

func TestDestinationLegacyStateAcceptsNewBacklogRevisionOlderThanLocalWrite(t *testing.T) {
	t.Parallel()
	var methods []string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods = append(methods, request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:])
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	statePath := filepath.Join(t.TempDir(), "state.json")
	localWriteAt := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {
			ThreadID: 42, UpdatedAt: localWriteAt,
			Blocks: map[string]destinationBlock{
				"block-1": {SourceHash: "legacy-content", MessageIDs: []int64{101}},
			},
		},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	eventCreatedAt := localWriteAt.Add(-2 * time.Hour)
	event := domain.Event{
		ID: "backlog-v2", ClientID: "point-01", CreatedAt: eventCreatedAt, SourceAt: eventCreatedAt,
		Source:          domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:00:00"},
		SourceBlockUUID: "block-1", Position: domain.Position{UnitName: "Игровая команда"},
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.00\n-Новая редакция из очереди", UpdatedAt: "2026-10-28 10:00:00",
		}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(methods, []string{"editMessageText"}) {
		t.Fatalf("новая backlog-редакция не обновила legacy-публикацию: %v", methods)
	}
	current, err := loadDestinationState(statePath)
	block := current.Items["point-01|source-1"].Blocks["block-1"]
	if err != nil || block.SourceVersion != "2026-10-28 10:00:00" || !block.SourceEventAt.Equal(eventCreatedAt) {
		t.Fatalf("истинная ревизия backlog не закреплена: %#v err=%v", block, err)
	}
}

func TestDestinationLegacyCleanupUsesIndependentBlockBaselines(t *testing.T) {
	t.Parallel()
	var deleted []int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(request.Body).Decode(&body)
		if strings.HasSuffix(request.URL.Path, "/deleteMessage") {
			deleted = append(deleted, int64(body["message_id"].(float64)))
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	baseline := time.Date(2026, 10, 28, 11, 0, 0, 0, time.UTC)
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, UpdatedAt: baseline, Blocks: map[string]destinationBlock{
			"block-a": {MessageIDs: []int64{101}}, "block-b": {MessageIDs: []int64{102}},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{API: api, ChatID: -1001, StatePath: statePath}
	cleanup := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteSkip}}
	for index, blockUUID := range []string{"block-a", "block-b"} {
		event := domain.Event{
			ID: fmt.Sprintf("delete-%d", index), ClientID: "point-01",
			CreatedAt: baseline.Add(time.Duration(index+1) * time.Minute),
			Source:    domain.SourceRecord{UUID: "source-1"}, SourceBlockUUID: blockUUID,
			SourceBlockDeleted: true,
			Blocks: []domain.ContentBlock{{
				UUID: blockUUID, DeletedAt: baseline.Add(time.Duration(index+1) * time.Minute).Format("2006-01-02 15:04:05"),
			}},
		}
		if err := destination.Deliver(context.Background(), event, cleanup); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(deleted, []int64{101, 102}) {
		t.Fatalf("второй cleanup ошибочно стал stale: %v", deleted)
	}
	current, err := loadDestinationState(statePath)
	item := current.Items["point-01|source-1"]
	if err != nil || !item.Blocks["block-a"].SourceDeleted || !item.Blocks["block-b"].SourceDeleted {
		t.Fatalf("не оба блока зафиксированы удалёнными: %#v err=%v", item, err)
	}
}

func TestDestinationThreadMoveNeverDeletesSiblingBlock(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		if method == "sendMessage" {
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
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	destination := Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	makeEvent := func(id, blockUUID, version, text string, created time.Time) domain.Event {
		return domain.Event{
			ID: id, ClientID: "point-01", CreatedAt: created, SourceAt: created,
			Source: domain.SourceRecord{UUID: "source-1"}, SourceBlockUUID: blockUUID,
			Position: domain.Position{UnitName: "Игровая команда"},
			Blocks:   []domain.ContentBlock{{UUID: blockUUID, Text: text, UpdatedAt: version}},
		}
	}
	thread42 := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	thread99 := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 99}}
	a1 := makeEvent("a-v1", "block-a", "2026-10-28 10:01:00", "10.01\n-A1", base.Add(time.Minute))
	b1 := makeEvent("b-v1", "block-b", "2026-10-28 10:01:00", "10.01\n-B1", base.Add(time.Minute))
	if err := destination.Deliver(context.Background(), a1, thread42); err != nil {
		t.Fatal(err)
	}
	if err := destination.Deliver(context.Background(), b1, thread42); err != nil {
		t.Fatal(err)
	}
	b2 := makeEvent("b-v2", "block-b", "2026-10-28 10:02:00", "10.01\n-B2", base.Add(2*time.Minute))
	if err := destination.Deliver(context.Background(), b2, thread99); err != nil {
		t.Fatal(err)
	}
	a0 := makeEvent("a-v0-late", "block-a", "2026-10-28 10:00:00", "10.01\n-A0", base)
	if err := destination.Deliver(context.Background(), a0, thread42); err != nil {
		t.Fatal(err)
	}
	current, err := loadDestinationState(destination.StatePath)
	item := current.Items["point-01|source-1"]
	if err != nil || !reflect.DeepEqual(item.Blocks["block-a"].MessageIDs, []int64{101}) ||
		item.Blocks["block-a"].ThreadID != 42 || !reflect.DeepEqual(item.Blocks["block-b"].MessageIDs, []int64{103}) ||
		item.Blocks["block-b"].ThreadID != 99 {
		t.Fatalf("смена темы повредила соседний блок: %#v err=%v", item, err)
	}
	if want := []string{"sendMessage", "sendMessage", "deleteMessage", "sendMessage"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("неверные Telegram-операции при поблочном переносе: got=%v want=%v", methods, want)
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
		if !terminalEditError(&apiError{code: 400, httpStatus: 400, description: description}) {
			t.Errorf("окончательная ошибка не распознана: %s", description)
		}
	}
	if terminalEditError(&apiError{code: 500, httpStatus: 500, description: "message to edit not found"}) {
		t.Fatal("временная серверная ошибка Telegram ошибочно завершит доставку")
	}
	if terminalEditError(&apiError{code: 400, httpStatus: 500, description: "message to edit not found"}) {
		t.Fatal("противоречивый ответ HTTP 500 ошибочно завершит доставку")
	}
	if terminalEditError(&apiError{code: 400, httpStatus: 400, description: "Bad Request: chat not found"}) {
		t.Fatal("исправимая настройкой ошибка чата ошибочно завершит доставку")
	}
}

func TestMissingEditErrorClassification(t *testing.T) {
	t.Parallel()
	for _, description := range []string{
		"Bad Request: message to edit not found",
		"Bad Request: MESSAGE_ID_INVALID",
	} {
		if !missingEditError(&apiError{code: 400, httpStatus: 400, description: description}) {
			t.Errorf("отсутствующее сообщение не распознано: %s", description)
		}
	}
	if missingEditError(&apiError{code: 400, httpStatus: 400, description: "Bad Request: message can't be edited"}) {
		t.Fatal("существующее нередактируемое сообщение ошибочно будет заменено")
	}
	if missingEditError(&apiError{code: 500, httpStatus: 500, description: "message to edit not found"}) {
		t.Fatal("временная серверная ошибка Telegram ошибочно запустит восстановление")
	}
	if missingEditError(&apiError{code: 400, httpStatus: 500, description: "message to edit not found"}) ||
		unchangedEditError(&apiError{code: 400, httpStatus: 500, description: "message is not modified"}) ||
		missingDeleteError(&apiError{code: 400, httpStatus: 500, description: "message to delete not found"}) {
		t.Fatal("ответ HTTP 500 нельзя считать подтверждением исправления или удаления")
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

func TestDestinationStateBoundsDiagnosticDeletionHistory(t *testing.T) {
	t.Parallel()
	blocks := make(map[string]destinationBlock)
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 4100; index++ {
		blocks[fmt.Sprintf("block-%04d", index)] = destinationBlock{
			SourceDeleted: true, SourceEventAt: base.Add(time.Duration(index) * time.Second),
		}
	}
	compactTelegramTombstones(blocks)
	if len(blocks) != 4096 {
		t.Fatalf("надгробия Telegram не ограничены: %d", len(blocks))
	}
	if _, exists := blocks["block-0000"]; exists {
		t.Fatal("самое старое надгробие не вытеснено")
	}
	var ids []int64
	for value := int64(1); value <= 100; value++ {
		ids = appendUniqueIDs(ids, value)
	}
	if len(ids) != 64 || ids[0] != 37 || ids[len(ids)-1] != 100 {
		t.Fatalf("история message_id не ограничена последними 64: %v", ids)
	}
}

func TestDestinationRecordCleanupRejectsDelayedScopedEventAndAllowsNewerRevision(t *testing.T) {
	t.Parallel()
	var methods []string
	nextID := int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
		methods = append(methods, method)
		if method == "sendMessage" {
			nextID++
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"message_id": nextID},
			})
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
	destination := Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "v1", ClientID: "point-01", CreatedAt: base, SourceAt: base,
		Source:   domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:00:00"},
		Position: domain.Position{UnitName: "Игровая команда"}, SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.00\n-Первая версия", UpdatedAt: "2026-10-28 10:00:00",
		}},
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	cleanup := event
	cleanup.ID = "v3-cleanup"
	cleanup.CreatedAt = base.Add(3 * time.Minute)
	cleanup.SourceAt = cleanup.CreatedAt
	cleanup.Source.UpdatedAt = "2026-10-28 10:03:00"
	cleanup.SourceBlockUUID = ""
	cleanup.Blocks = nil
	if err := destination.Deliver(context.Background(), cleanup, domain.PlannedDelivery{
		Decision: domain.RouteDecision{Action: domain.RouteSkip},
	}); err != nil {
		t.Fatal(err)
	}
	// Барьер удаления записи хранится отдельно от компактируемых неактивных
	// публикаций. Имитируем длительную работу после очистки: целевая запись
	// должна исчезнуть из Items, но её монотонный барьер обязан сохраниться.
	compacted, err := loadDestinationState(destination.StatePath)
	targetKey := "point-01|source-1"
	target := compacted.Items[targetKey]
	target.UpdatedAt = base.Add(-time.Hour)
	compacted.Items[targetKey] = target
	for index := 0; index < maxTelegramInactiveItems+4; index++ {
		compacted.Items[fmt.Sprintf("other-%05d", index)] = destinationItem{
			UpdatedAt: base.Add(time.Duration(index) * time.Second),
			Blocks: map[string]destinationBlock{
				"deleted": {SourceDeleted: true, SourceEventAt: base},
			},
		}
	}
	if err := saveDestinationState(destination.StatePath, compacted); err != nil {
		t.Fatal(err)
	}
	compacted, err = loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := compacted.Items[targetKey]; exists {
		t.Fatal("целевая неактивная запись не прошла штатную компактацию")
	}
	if barrier := compacted.RecordTombstones[targetKey]; barrier.SourceVersion != "2026-10-28 10:03:00" {
		t.Fatalf("долговечный барьер потерян после 4096 надгробий: %#v", barrier)
	}
	equal := event
	equal.ID = "v3-equal-delayed"
	equal.CreatedAt = cleanup.CreatedAt
	equal.SourceAt = cleanup.SourceAt
	equal.Source.UpdatedAt = "2026-10-28 10:03:00"
	equal.Blocks[0].UpdatedAt = "2026-10-28 10:03:00"
	equal.Blocks[0].Text = "10.00\n-Равная удалению запоздалая версия"
	if err := destination.Deliver(context.Background(), equal, delivery); err != nil {
		t.Fatal(err)
	}
	delayed := event
	delayed.ID = "v2-delayed"
	delayed.CreatedAt = base.Add(2 * time.Minute)
	delayed.SourceAt = delayed.CreatedAt
	delayed.Source.UpdatedAt = "2026-10-28 10:02:00"
	delayed.Blocks[0].UpdatedAt = "2026-10-28 10:02:00"
	delayed.Blocks[0].Text = "10.00\n-Запоздалая версия"
	if err := destination.Deliver(context.Background(), delayed, delivery); err != nil {
		t.Fatal(err)
	}
	newer := event
	newer.ID = "v4-new"
	newer.CreatedAt = base.Add(4 * time.Minute)
	newer.SourceAt = newer.CreatedAt
	newer.Source.UpdatedAt = "2026-10-28 10:04:00"
	newer.Blocks[0].UpdatedAt = "2026-10-28 10:04:00"
	newer.Blocks[0].Text = "10.00\n-Новая версия после удаления записи"
	if err := destination.Deliver(context.Background(), newer, delivery); err != nil {
		t.Fatal(err)
	}
	if want := []string{"sendMessage", "deleteMessage", "sendMessage"}; !reflect.DeepEqual(methods, want) {
		t.Fatalf("запоздалое событие воскресило публикацию: got=%v want=%v", methods, want)
	}
	current, err := loadDestinationState(destination.StatePath)
	item := current.Items["point-01|source-1"]
	barrier := current.RecordTombstones["point-01|source-1"]
	if err != nil || barrier.SourceVersion != "2026-10-28 10:03:00" ||
		!reflect.DeepEqual(item.Blocks["block-1"].MessageIDs, []int64{102}) {
		t.Fatalf("барьер удаления записи или новая публикация не сохранены: item=%#v barrier=%#v err=%v", item, barrier, err)
	}
}

func TestDestinationStateCompactsOnlyInactiveItemsAndEnforcesHardLimits(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	current := destinationState{Items: make(map[string]destinationItem)}
	for index := 0; index < maxTelegramInactiveItems+4; index++ {
		current.Items[fmt.Sprintf("inactive-%05d", index)] = destinationItem{
			RecordCleanupSourceVersion: "2026-10-01 00:00:00",
			UpdatedAt:                  base.Add(time.Duration(index) * time.Second),
		}
	}
	current.Items["active"] = destinationItem{
		UpdatedAt: base,
		Blocks: map[string]destinationBlock{
			"block-1": {MessageIDs: []int64{777}},
		},
	}
	compactTelegramInactiveItems(&current)
	if len(current.Items) != maxTelegramInactiveItems+1 {
		t.Fatalf("неактивное состояние не ограничено: %d", len(current.Items))
	}
	if _, exists := current.Items["active"]; !exists {
		t.Fatal("активная привязка Telegram была вытеснена")
	}
	if _, exists := current.Items["inactive-00000"]; exists {
		t.Fatal("самый старый неактивный барьер не вытеснен")
	}
	if _, exists := current.Items[fmt.Sprintf("inactive-%05d", maxTelegramInactiveItems+3)]; !exists {
		t.Fatal("новый барьер удаления потерян при компактации")
	}

	limited := destinationState{Items: map[string]destinationItem{
		"a": {Blocks: map[string]destinationBlock{"block": {MessageIDs: []int64{1}}}},
		"b": {Blocks: map[string]destinationBlock{"block": {MessageIDs: []int64{2}}}},
	}}
	if err := enforceTelegramActiveItemLimit(limited, 1); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("жёсткий предел активного состояния не дал постоянную ошибку: %v", err)
	}
	limited.RecordTombstones = map[string]destinationRecordTombstone{
		"a": {}, "b": {},
	}
	if err := enforceTelegramRecordTombstoneLimit(limited, 1); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("жёсткий предел долговечных барьеров не дал постоянную ошибку: %v", err)
	}
	path := filepath.Join(t.TempDir(), "oversized.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxTelegramStateBytes + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDestinationState(path); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("слишком большой state не дал постоянную эксплуатационную ошибку: %v", err)
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
