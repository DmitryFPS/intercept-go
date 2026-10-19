package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type failingRoundTripper struct{ message string }

func (transport failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New(transport.message)
}

// Сетевая ошибка стандартной библиотеки может содержать полный URL Bot API;
// токен должен быть удалён ещё до передачи ошибки в журнал.
func TestTelegramClientRemovesTokenFromTransportError(t *testing.T) {
	t.Parallel()
	const token = "123456789:abcdefghijklmnopqrstuvwxyzABCDE"
	httpClient := &http.Client{Transport: failingRoundTripper{message: "Post https://api.telegram.org/bot" + token + "/getMe: offline"}}
	client, err := New(token, httpClient)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetMe(context.Background())
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("токен остался в ошибке: %v", err)
	}
}

// Клиент отправляет пакет именно в настроенный транспортный чат и в топик своей точки.
func TestTelegramClientSendsDocumentToExactTopic(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendDocument") {
			t.Fatalf("неожиданный метод: %s", request.URL.Path)
		}
		if err := request.ParseMultipartForm(1024 * 1024); err != nil {
			t.Fatalf("multipart: %v", err)
		}
		if request.FormValue("chat_id") != "-100123" || request.FormValue("message_thread_id") != "42" {
			t.Fatalf("неверное назначение: %#v", request.Form)
		}
		file, _, err := request.FormFile("document")
		if err != nil {
			t.Fatalf("document: %v", err)
		}
		payload, _ := io.ReadAll(file)
		if string(payload) != "encrypted" {
			t.Fatalf("неверный файл: %q", payload)
		}
		json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": 77, "chat": map[string]any{"id": -100123}},
		})
	}))
	defer server.Close()

	client, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	message, err := client.SendDocument(context.Background(), -100123, 42, "event.icevent", []byte("encrypted"), "IC/1")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if message.MessageID != 77 {
		t.Fatalf("неверный message_id: %d", message.MessageID)
	}
}

// Даже при ошибке Telegram секретный токен бота не должен попасть в журнал или сообщение оператору.
func TestTelegramClientDoesNotExposeTokenInError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusUnauthorized)
		io.WriteString(writer, `{"ok":false,"error_code":401,"description":"Unauthorized"}`)
	}))
	defer server.Close()
	const token = "123:very-secret-token"
	client, _ := NewWithBaseURL(token, server.URL, server.Client())
	_, err := client.GetMe(context.Background())
	if err == nil || strings.Contains(err.Error(), token) {
		t.Fatalf("ошибка должна быть безопасной: %v", err)
	}
}
