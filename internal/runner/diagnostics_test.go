package runner

import (
	"bytes"
	"strings"
	"testing"
)

func TestDiagnosticLogRedactsOperationalIdentifiers(t *testing.T) {
	source := strings.NewReader(`{"time":"2026-10-23T10:00:00Z","msg":"received","source_uuid":"secret-uuid","source_id":42,"event_id":"event-secret","destination_id":"telegram-secret","thread_id":42,"telegram_message_id":99,"sender_bot":"secret_bot","client_name":"Текстиль","client_number":"01","position":"1 мб 155 омбр","error":"open C:\\secret\\main.sqlite: denied","status":{"invite_link":"https://t.me/+secret","eavc_path":"C:\\secret\\main.sqlite","queue":3}}` + "\n")
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
