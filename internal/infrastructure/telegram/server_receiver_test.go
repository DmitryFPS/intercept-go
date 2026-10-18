package telegram

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

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
