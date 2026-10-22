package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
)

// Сервер отвергает файл от неизвестного бота ещё до загрузки его содержимого.
func TestServerReceiverRejectsUnknownBotBeforeDownload(t *testing.T) {
	t.Parallel()
	var downloads atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true,
				"result": []any{map[string]any{
					"update_id": 7,
					"message": map[string]any{
						"message_id": 15, "message_thread_id": 9,
						"from":     map[string]any{"id": 999, "is_bot": true, "username": "unknown_bot"},
						"chat":     map[string]any{"id": -1001, "type": "supergroup"},
						"document": map[string]any{"file_id": "malicious", "file_name": "x.icevent", "file_size": 100},
					},
				}},
			})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			downloads.Add(1)
			http.Error(writer, "getFile не должен вызываться", http.StatusInternalServerError)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return false, nil },
	}
	packets, next, err := receiver.Packets(context.Background(), 0)
	if err != nil || len(packets) != 0 || next != 8 {
		t.Fatalf("неожиданный результат: packets=%#v next=%d err=%v", packets, next, err)
	}
	if downloads.Load() != 0 {
		t.Fatal("сервер запросил файл неизвестного бота")
	}
}

// Проверочный файл от зарегистрированной точки распознаётся отдельно и не
// передаётся в обработку рабочих перехватов.
func TestServerReceiverRecognizesTransportProbe(t *testing.T) {
	t.Parallel()
	probeID := "018f22d2-3f5c-7abc-8123-0123456789ab"
	key := make([]byte, 32)
	sentAt := time.Now().UTC().Truncate(time.Second)
	probe := probePayload{Schema: probeSchema, ClientID: "textile", ProbeID: probeID, SentAt: sentAt.Format(time.RFC3339)}
	probe.MAC = probeMAC(key, probe.ClientID, probe.ProbeID, probe.SentAt)
	api := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": []any{map[string]any{
				"update_id": 8, "message": map[string]any{
					"message_id": 16, "message_thread_id": 9, "date": sentAt.Unix(),
					"from":     map[string]any{"id": 101, "is_bot": true, "username": "textile_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "probe", "file_name": probeID + ".icprobe", "file_size": 100},
				},
			}}})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": "probe.json"}})
		case strings.Contains(request.URL.Path, "/file/bot"):
			_ = json.NewEncoder(writer).Encode(probe)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer api.Close()
	client, err := NewWithBaseURL("123:token", api.URL, api.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
	}
	packets, _, err := receiver.Packets(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(packets) != 1 || packets[0].ProbeID != probeID || !VerifyProbe(packets[0], key) {
		t.Fatalf("проверка не распознана: %#v", packets)
	}
}
