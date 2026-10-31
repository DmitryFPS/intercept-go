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
	"time"
)

type failingRoundTripper struct{ message string }

func (transport failingRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New(transport.message)
}

func TestTelegramClientReturnsRetryAfterForRateLimit(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(writer, `{"ok":false,"error_code":429,"description":"Too Many Requests","parameters":{"retry_after":17}}`)
	}))
	defer server.Close()
	client, _ := NewWithBaseURL("123:token", server.URL, server.Client())
	_, err := client.GetMe(context.Background())
	var limited interface{ RetryAfter() time.Duration }
	if !errors.As(err, &limited) || limited.RetryAfter() != 17*time.Second {
		t.Fatalf("retry_after не сохранён: %v", err)
	}
}

func TestSendLimiterKeepsGroupBelowTwentyMessagesPerMinute(t *testing.T) {
	limiter := newSendLimiter()
	limiter.globalInterval = 0
	limiter.groupInterval = 35 * time.Millisecond
	started := time.Now()
	if err := limiter.wait(context.Background(), -1001); err != nil {
		t.Fatal(err)
	}
	if err := limiter.wait(context.Background(), -1001); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 30*time.Millisecond {
		t.Fatalf("сообщения одной группы отправлены слишком быстро: %s", elapsed)
	}
}

func TestSendLimiterHonorsSharedCooldown(t *testing.T) {
	limiter := newSendLimiter()
	limiter.globalInterval = 0
	limiter.privateInterval = 0
	limiter.blockFor(35 * time.Millisecond)
	started := time.Now()
	if err := limiter.wait(context.Background(), 1001); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 30*time.Millisecond {
		t.Fatalf("общая пауза Telegram не соблюдена: %s", elapsed)
	}
}

func TestTopicFailsWhenProbeMessageCannotBeDeleted(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(request.URL.Path, "/sendMessage") {
			io.WriteString(writer, `{"ok":true,"result":{"message_id":17,"chat":{"id":-1001}}}`)
			return
		}
		writer.WriteHeader(http.StatusBadRequest)
		io.WriteString(writer, `{"ok":false,"error_code":400,"description":"message can't be deleted"}`)
	}))
	defer server.Close()
	client, _ := NewWithBaseURL("123:token", server.URL, server.Client())
	err := client.TestTopic(context.Background(), -1001, 42)
	if err == nil || !strings.Contains(err.Error(), "не удалено") {
		t.Fatalf("неудачное удаление должно остановить настройку: %v", err)
	}
}

func TestTopicProbeRejectsWrongReturnedThreadAndDeletesProbe(t *testing.T) {
	t.Parallel()
	deleted := false
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		if strings.HasSuffix(request.URL.Path, "/sendMessage") {
			io.WriteString(writer, `{"ok":true,"result":{"message_id":17,"message_thread_id":99,"chat":{"id":-1001}}}`)
			return
		}
		if strings.HasSuffix(request.URL.Path, "/deleteMessage") {
			deleted = true
			io.WriteString(writer, `{"ok":true,"result":true}`)
			return
		}
		t.Errorf("неожиданный метод: %s", request.URL.Path)
	}))
	defer server.Close()
	client, _ := NewWithBaseURL("123:token", server.URL, server.Client())
	client.sends.globalInterval = 0
	client.sends.groupInterval = 0
	err := client.TestTopic(context.Background(), -1001, 42)
	if err == nil || !strings.Contains(err.Error(), "в тему 99 вместо 42") || !deleted {
		t.Fatalf("чужая тема не остановила публикацию или проба не удалена: deleted=%v err=%v", deleted, err)
	}
}

func TestSendHTMLDoesNotConfirmWrongReturnedThread(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		io.WriteString(writer, `{"ok":true,"result":{"message_id":17,"message_thread_id":99,"chat":{"id":-1001}}}`)
	}))
	defer server.Close()
	client, _ := NewWithBaseURL("123:token", server.URL, server.Client())
	message, err := client.SendHTML(context.Background(), -1001, 42, "Проверка")
	var attempt *sendAttemptError
	if !errors.As(err, &attempt) || !attempt.attempted || !strings.Contains(err.Error(), "message_id 17 в теме 99 вместо запрошенной 42") {
		t.Fatalf("ответ из чужой темы ошибочно подтверждён: %v", err)
	}
	if message.MessageID != 17 || message.MessageThreadID != 99 || message.Chat.ID != -1001 {
		t.Fatalf("известный ответ Telegram потерян: %+v", message)
	}
	observed, ok := attempt.ObservedMessage()
	if !ok || observed != message {
		t.Fatalf("ошибка не содержит данные для ручной сверки: ok=%v observed=%+v", ok, observed)
	}
}

func TestSendHTMLPreservesKnownIDFromUnexpectedChat(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		io.WriteString(writer, `{"ok":true,"result":{"message_id":18,"message_thread_id":42,"chat":{"id":-2002}}}`)
	}))
	defer server.Close()
	client, _ := NewWithBaseURL("123:token", server.URL, server.Client())
	message, err := client.SendHTML(context.Background(), -1001, 42, "Проверка")
	var attempt *sendAttemptError
	if !errors.As(err, &attempt) || !attempt.attempted || message.MessageID != 18 || message.Chat.ID != -2002 {
		t.Fatalf("ID отправки в другой чат потерян: message=%+v err=%v", message, err)
	}
	observed, ok := attempt.ObservedMessage()
	if !ok || observed != message {
		t.Fatalf("ошибка не содержит ID для сверки: observed=%+v ok=%v", observed, ok)
	}
}

func TestPrivateRecipientLimiterSeparatesBots(t *testing.T) {
	t.Parallel()
	first := privateRecipientLimiterID("@First_Point_Bot")
	if first <= 0 || first != privateRecipientLimiterID("first_point_bot") {
		t.Fatalf("идентификатор одного бота должен быть положительным и стабильным: %d", first)
	}
	if first == privateRecipientLimiterID("second_point_bot") {
		t.Fatal("разные боты ошибочно делят ограничение одного личного диалога")
	}
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
