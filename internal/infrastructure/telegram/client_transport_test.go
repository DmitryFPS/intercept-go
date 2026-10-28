package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"intercept/internal/application"
)

// Двусторонняя проверка считается успешной только по ответу настроенного
// серверного бота с точным служебным префиксом.
func TestClientTransportRecognizesProbeAcknowledgement(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	probeID := "018f22d2-3f5c-7abc-8123-0123456789ab"
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true,
			"result": []any{map[string]any{
				"update_id": 5,
				"message": map[string]any{
					"message_id": 10, "date": 1_700_000_000,
					"from": map[string]any{"id": 7, "is_bot": true, "username": "server_bot"},
					"chat": map[string]any{"id": 7, "type": "private"},
					"text": probeAcknowledgementText(probeID, key),
				},
			}},
		})
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	transport := ClientTransport{API: client, ServerBotID: 7, ServerBotUsername: "server_bot", Key: key}
	acks, next, err := transport.Acknowledgements(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].Status != "probe" || next != 6 {
		t.Fatalf("неожиданный ответ: %#v, offset=%d", acks, next)
	}
}

func TestClientTransportRejectsUnsignedAndTamperedAcknowledgements(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	texts := []string{
		"IC-ACK/2 event-1 accepted",
		AcknowledgementText("event-1", "accepted", key) + "00",
	}
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		result := make([]any, 0, len(texts))
		for index, value := range texts {
			result = append(result, map[string]any{
				"update_id": index + 1,
				"message": map[string]any{
					"message_id": index + 10, "date": 1_700_000_000,
					"from": map[string]any{"id": 7, "is_bot": true, "username": "server_bot"},
					"chat": map[string]any{"id": 7, "type": "private"}, "text": value,
				},
			})
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": result})
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	transport := ClientTransport{API: client, ServerBotID: 7, ServerBotUsername: "server_bot", Key: key}
	acks, _, err := transport.Acknowledgements(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 0 {
		t.Fatalf("поддельные подтверждения приняты: %#v", acks)
	}
}

func TestVerifyProbeRejectsTamperingAndReplay(t *testing.T) {
	t.Parallel()
	key := make([]byte, 32)
	probeAt := time.Now().UTC().Truncate(time.Second)
	packet := application.IncomingPacket{
		ProbeClientID: "textile", ProbeID: "018f22d2-3f5c-7abc-8123-0123456789ab",
		ProbeSentAt: probeAt, SentAt: probeAt.Add(10 * time.Second),
		ProbeShift: application.EmptyShiftSnapshot(),
	}
	packet.ProbeMAC = probeMAC(key, packet.ProbeClientID, packet.ProbeID, packet.ProbeSentAt.Format(time.RFC3339), packet.ProbeShift)
	if !VerifyProbe(packet, key) {
		t.Fatal("подлинный свежий контрольный пакет отклонён")
	}
	tampered := packet
	tampered.ProbeClientID = "selidovo"
	if VerifyProbe(tampered, key) {
		t.Fatal("подмена точки не обнаружена")
	}
	tamperedShift := packet
	tamperedShift.ProbeShift = application.ShiftSnapshot{
		ReadState: application.ShiftReadOK,
		Active: []application.ShiftSession{{
			SessionID: 1, SessionUUID: "session-1", PositionName: "Позиция",
			OpenedAt: probeAt.Add(-time.Hour),
		}},
	}
	if VerifyProbe(tamperedShift, key) {
		t.Fatal("подмена состояния смены не обнаружена")
	}
	replayed := packet
	replayed.SentAt = probeAt.Add(3 * time.Minute)
	if VerifyProbe(replayed, key) {
		t.Fatal("повтор старого подписанного пакета не обнаружен")
	}
}
