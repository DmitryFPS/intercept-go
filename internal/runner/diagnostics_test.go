package runner

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestDiagnosticLogRedactsOperationalIdentifiers(t *testing.T) {
	source := strings.NewReader(`{"time":"2026-10-23T10:00:00Z","msg":"received","source_uuid":"secret-uuid","source_id":42,"event_id":"event-secret","trace_id":"trace-safe-123","transport_message_id":88,"destination_id":"telegram-secret","thread_id":42,"telegram_message_id":99,"sender_bot":"secret_bot","client_name":"Текстиль","client_number":"01","position":"1 мб 155 омбр","error":"open C:\\secret\\main.sqlite: denied","content_kind":"text","content_chars":321,"content_blocks":4,"source_mode":"normal","new_event":true,"status":{"invite_link":"https://t.me/+secret","eavc_path":"C:\\secret\\main.sqlite","queue":3}}` + "\n" +
		`{"time":"2026-10-23T10:00:01Z","msg":"delivered","event_id":"event-secret","telegram_message_id":99}` + "\n")
	var output bytes.Buffer
	if err := copyRedactedDiagnosticLog(&output, source); err != nil {
		t.Fatal(err)
	}
	text := output.String()
	for _, secret := range []string{"secret-uuid", "event-secret", "telegram-secret", "secret_bot", "Текстиль", "1 мб 155 омбр", "https://t.me/+secret", `C:\\secret\\main.sqlite`, `"source_id"`, `"thread_id"`, `"error"`} {
		if strings.Contains(text, secret) {
			t.Fatalf("диагностический журнал содержит %q: %s", secret, text)
		}
	}
	if !strings.Contains(text, `"queue":3`) || !strings.Contains(text, `"msg":"received"`) {
		t.Fatalf("полезные технические поля потеряны: %s", text)
	}
	for _, expected := range []string{`"content_kind":"text"`, `"content_chars":321`, `"content_blocks":4`, `"source_mode":"normal"`, `"new_event":true`} {
		if !strings.Contains(text, expected) {
			t.Fatalf("диагностика потеряла характеристику содержимого %s: %s", expected, text)
		}
	}
	for _, expected := range []string{`"trace_id":"trace-safe-123"`, `"transport_message_id":88`} {
		if !strings.Contains(text, expected) {
			t.Errorf("сквозной технический идентификатор потерян: %s", expected)
		}
	}
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if records[0]["event_ref"] == "" || records[0]["event_ref"] != records[1]["event_ref"] ||
		records[0]["telegram_message_ref"] != records[1]["telegram_message_ref"] {
		t.Fatalf("обезличенные ссылки не связывают одну цепочку: %#v", records)
	}
}

func TestDiagnosticLogDoesNotCopyUnknownPlainText(t *testing.T) {
	var output bytes.Buffer
	if err := copyRedactedDiagnosticLog(&output, strings.NewReader("token=secret-value\n")); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "secret-value") {
		t.Fatalf("неструктурированная строка попала в диагностику: %s", output.String())
	}
}

func TestUpdateDiagnosticsKeepUsefulResultAndRedactToken(t *testing.T) {
	t.Parallel()
	token := "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcd"
	var logOutput bytes.Buffer
	updateLog := `{"time":"2026-10-24T10:00:00Z","state":"rollback","from":"rc54","to":"rc55","message":"Версия восстановлена","error":"bot ` + token + ` rejected"}` + "\n"
	if err := copyRedactedDiagnosticLogWithUpdates(&logOutput, strings.NewReader(updateLog), true); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`"message":"Версия восстановлена"`, `"error":"bot *** rejected"`} {
		if !strings.Contains(logOutput.String(), expected) {
			t.Fatalf("в журнале обновления нет %s: %s", expected, logOutput.String())
		}
	}
	if strings.Contains(logOutput.String(), token) {
		t.Fatal("токен остался в журнале обновления")
	}

	var resultOutput bytes.Buffer
	result := "{\n  \"state\": \"success\",\n  \"from_version\": \"rc54\",\n  \"to_version\": \"rc55\",\n  \"message\": \"Обновление установлено\"\n}"
	if err := copyRedactedDiagnosticJSON(&resultOutput, strings.NewReader(result), true); err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{"from_version", "to_version", "Обновление установлено"} {
		if !strings.Contains(resultOutput.String(), expected) {
			t.Fatalf("результат обновления потерял %q: %s", expected, resultOutput.String())
		}
	}
}
