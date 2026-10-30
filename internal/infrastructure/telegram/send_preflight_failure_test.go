package telegram

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

type sendFailureRoundTrip func(*http.Request) (*http.Response, error)

func (roundTrip sendFailureRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

// DNS и dial завершаются до записи HTTP-запроса. Маркер неопределённой
// отправки в этих случаях не должен запрещать безопасный повтор.
func TestSendHTMLPreRequestNetworkFailuresAreDefinitelyNotCreated(t *testing.T) {
	const token = "123456:secret-for-redaction-test"
	for _, scenario := range []struct {
		name string
		err  error
	}{
		{name: "DNS", err: &net.DNSError{Err: "no such host", Name: "api.telegram.org"}},
		{name: "dial", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			client, err := NewWithBaseURL(token, "https://api.telegram.org", &http.Client{
				Transport: sendFailureRoundTrip(func(request *http.Request) (*http.Response, error) {
					return nil, &url.Error{Op: "Post", URL: request.URL.String(), Err: scenario.err}
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, err = client.SendHTML(context.Background(), -1001, 42, "проверка")
			var attempt *sendAttemptError
			if !errors.As(err, &attempt) || attempt.attempted || !telegramSendDefinitelyNotCreated(err) {
				t.Fatalf("сбой до HTTP-запроса ошибочно признан неопределённой отправкой: %v", err)
			}
			if strings.Contains(err.Error(), token) {
				t.Fatalf("токен Telegram попал в текст ошибки: %v", err)
			}
		})
	}
}

// Сервер действительно получил POST, но оборвал соединение до ответа.
// Клиент не знает, была ли публикация создана, и не вправе повторить её сам.
func TestSendHTMLAcceptedRequestWithoutResponseRemainsUncertain(t *testing.T) {
	var accepted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		accepted.Add(1)
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	defer server.Close()
	client, err := NewWithBaseURL("123:secret", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.SendHTML(context.Background(), -1001, 42, "проверка")
	var attempt *sendAttemptError
	if !errors.As(err, &attempt) || !attempt.attempted || telegramSendDefinitelyNotCreated(err) {
		t.Fatalf("принятый POST без ответа ошибочно признан безопасным для повтора: %v", err)
	}
	if accepted.Load() != 1 {
		t.Fatalf("ожидался один принятый запрос, получено %d", accepted.Load())
	}
}

// После DNS-сбоя Destination должен снять SendPending, повторить тот же блок
// и создать ровно одну публикацию; ручное снятие подавления не требуется.
func TestDestinationRetriesPreRequestDNSFailureWithoutDuplicate(t *testing.T) {
	const token = "123456:secret-for-destination-test"
	var calls int
	client, err := NewWithBaseURL(token, "https://api.telegram.org", &http.Client{
		Transport: sendFailureRoundTrip(func(request *http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				return nil, &url.Error{Op: "Post", URL: request.URL.String(), Err: &net.DNSError{
					Err: "no such host", Name: "api.telegram.org",
				}}
			}
			return &http.Response{
				StatusCode: http.StatusOK, Header: make(http.Header), Request: request,
				Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":314}}`)),
			}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "telegram-state.json")
	destination := Destination{API: client, ChatID: -1001, StatePath: statePath}
	event := domain.Event{
		ID: "event-1", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-1"},
		CreatedAt: time.Now().UTC(), Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{{UUID: "block-a", Key: "12.00", Text: "12.00\n-Проверка"}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("первая отправка должна вернуть ошибку DNS")
	} else if strings.Contains(err.Error(), token) {
		t.Fatalf("токен Telegram попал в ошибку Destination: %v", err)
	}
	state, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := state.Items["point-01|source-1"].Blocks["block-a"]
	if block.SendPending || block.Suppressed || len(block.MessageIDs) != 0 {
		t.Fatalf("сбой до запроса оставил блок подавленным или неопределённым: %#v", block)
	}
	// Новый экземпляр воспроизводит запуск Центра после аварийного завершения.
	destination = Destination{API: client, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatalf("безопасный повтор после DNS-сбоя не состоялся: %v", err)
	}
	if err := destination.Deliver(context.Background(), event, delivery); err != nil {
		t.Fatalf("идемпотентная проверка завершилась ошибкой: %v", err)
	}
	state, err = loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block = state.Items["point-01|source-1"].Blocks["block-a"]
	if calls != 2 || len(block.MessageIDs) != 1 || block.MessageIDs[0] != 314 {
		t.Fatalf("ожидалась одна публикация после одного DNS-сбоя: calls=%d block=%#v", calls, block)
	}
}

// Если Telegram принял запрос, но ответ потерялся, SendPending остаётся
// записанным. Повторный проход изолирует блок и не создаёт возможный дубль.
func TestDestinationKeepsPendingAfterAcceptedRequestLosesResponse(t *testing.T) {
	var accepted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = io.Copy(io.Discard, request.Body)
		accepted.Add(1)
		connection, _, err := writer.(http.Hijacker).Hijack()
		if err == nil {
			_ = connection.Close()
		}
	}))
	defer server.Close()
	client, err := NewWithBaseURL("123:secret", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(t.TempDir(), "telegram-state.json")
	destination := Destination{API: client, ChatID: -1001, StatePath: statePath}
	event := domain.Event{
		ID: "event-uncertain", ClientID: "point-01", Source: domain.SourceRecord{UUID: "source-uncertain"},
		CreatedAt: time.Now().UTC(), Position: domain.Position{UnitName: "позиция"},
		Blocks: []domain.ContentBlock{{UUID: "block-uncertain", Key: "12.00", Text: "12.00\n-Проверка"}},
	}
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("потерянный ответ должен вернуть ошибку доставки")
	}
	state, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block := state.Items["point-01|source-uncertain"].Blocks["block-uncertain"]
	if !block.SendPending || block.Suppressed || len(block.MessageIDs) != 0 {
		t.Fatalf("неопределённый исход потерял маркер SendPending: %#v", block)
	}
	destination = Destination{API: client, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("повтор должен изолировать неопределённую отправку без дубля: %v", err)
	}
	if accepted.Load() != 1 {
		t.Fatalf("повтор ошибочно отправил второй запрос: %d", accepted.Load())
	}
	state, err = loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	block = state.Items["point-01|source-uncertain"].Blocks["block-uncertain"]
	if !block.Suppressed || block.SendPending {
		t.Fatalf("неопределённая отправка не изолирована: %#v", block)
	}
}
