package telegram

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

type manualResponseRoundTrip func(*http.Request) (*http.Response, error)

func (roundTrip manualResponseRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return roundTrip(request)
}

type cancelOnReadBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (body *cancelOnReadBody) Read(p []byte) (int, error) {
	n, err := body.ReadCloser.Read(p)
	if n > 0 && body.cancel != nil {
		body.cancel()
		body.cancel = nil
	}
	return n, err
}

func TestManualPermitSurvivesRestartWithoutAutomaticPost(t *testing.T) {
	t.Parallel()
	destination, sends := newAmbiguousTelegramDestination(t)
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("v1", "2026-10-28 12:00:00", "12.00\n-Первый текст", at)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("ожидался неизвестный исход первой отправки")
	}
	before, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || len(before.Blocks) != 1 || before.Blocks[0].SendAttemptToken == "" {
		t.Fatalf("потерян маркер первой попытки: receipt=%+v err=%v", before, err)
	}
	allowed, err := destination.AllowRepublishWithConfirmation(event, "block-1", 1, before.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("разрешение не сохранено: allowed=%v err=%v", allowed, err)
	}
	permit, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || !permit.Blocks[0].ManualRepublishPending || permit.Blocks[0].UnknownOutcome {
		t.Fatalf("нужен отдельный ручной барьер до POST: receipt=%+v err=%v", permit, err)
	}
	revision := ambiguousTelegramEvent("v2", "2026-10-28 12:01:00", "12.00\n-Новая редакция", at.Add(time.Minute))
	destination = &Destination{API: destination.API, ChatID: destination.ChatID, StatePath: destination.StatePath}
	for _, candidate := range []domain.Event{event, revision} {
		if err := destination.Deliver(context.Background(), candidate, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
			t.Fatalf("обычный worker обошёл ручной барьер: err=%v", err)
		}
	}
	if sends.Load() != 1 {
		t.Fatalf("до ручного продолжения создан дубль: sends=%d", sends.Load())
	}
	allowed, err = destination.ResumeManualRepublish(revision, "block-1", permit.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("после перезапуска ручное разрешение недоступно: allowed=%v err=%v", allowed, err)
	}
	if err := destination.DeliverManually(context.Background(), revision, delivery); err != nil {
		t.Fatal(err)
	}
	after, err := destination.DeliveryReceipt(revision, delivery)
	if err != nil || sends.Load() != 2 || !after.Blocks[0].Complete || after.Blocks[0].ManualRepublishPending {
		t.Fatalf("ручное продолжение не завершилось однократной отправкой: receipt=%+v sends=%d err=%v", after, sends.Load(), err)
	}
}

func TestManualPermitSurvivesRestartBetweenConfirmedParts(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sends atomic.Int32
	transport := manualResponseRoundTrip(func(request *http.Request) (*http.Response, error) {
		var body io.ReadCloser
		switch {
		case strings.HasSuffix(request.URL.Path, "/editMessageText"):
			body = io.NopCloser(strings.NewReader(`{"ok":true,"result":true}`))
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			call := sends.Add(1)
			body = io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"result":{"message_id":%d}}`, 801+call)))
			if call == 1 {
				// The first new part was accepted, then the operator's request
				// ended before a send intent for the next part could be saved.
				body = &cancelOnReadBody{ReadCloser: body, cancel: cancel}
			}
		default:
			return nil, fmt.Errorf("unexpected Telegram method %s", request.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body, Request: request}, nil
	})
	api, err := NewWithBaseURL("123:token", "https://telegram.invalid", &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("long-manual", "2026-10-28 12:00:00",
		"12.00\n"+strings.Repeat("-Длинная строка. (10001)\n", 900), at)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	chunks := formatBlockChunksWithEditedAt(event, event.Blocks[0], delivery, "", "", "", at)
	if len(chunks) < 3 {
		t.Fatalf("нужны минимум три части, получено %d", len(chunks))
	}
	statePath := filepath.Join(t.TempDir(), "state.json")
	const permitToken = "permit-before-crash"
	if err := saveDestinationState(statePath, destinationState{Items: map[string]destinationItem{
		"point-01|source-1": {ThreadID: 42, Blocks: map[string]destinationBlock{
			"block-1": {
				MessageIDs: []int64{801}, ExpectedMessageCount: len(chunks),
				SourceVersion: event.SourceVersion(), SourceEventAt: event.CreatedAt, ThreadID: 42,
				ManualRepublishPending: true, ManualRepublishToken: permitToken, SendPendingIndex: 1,
			},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := &Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.DeliverManually(ctx, event, delivery); !errors.Is(err, context.Canceled) {
		t.Fatalf("после подтверждённой части ожидалась остановка запроса: %v", err)
	}
	state, err := loadDestinationState(statePath)
	if err != nil {
		t.Fatal(err)
	}
	partial := state.Items["point-01|source-1"].Blocks["block-1"]
	if sends.Load() != 1 || len(partial.MessageIDs) != 2 || partial.MessageIDs[1] != 802 ||
		partial.SendPending || !partial.ManualRepublishPending || partial.ManualRepublishToken != permitToken {
		t.Fatalf("между частями не сохранено ручное разрешение: block=%+v sends=%d", partial, sends.Load())
	}
	destination = &Destination{API: api, ChatID: -1001, StatePath: statePath}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("после restart обычная доставка должна ждать ручного продолжения: %v", err)
	}
	if sends.Load() != 1 {
		t.Fatalf("обычная доставка создала следующую часть: sends=%d", sends.Load())
	}
	allowed, err := destination.ResumeManualRepublish(event, "block-1", permitToken)
	if err != nil || !allowed {
		t.Fatalf("сохранённое разрешение не возобновилось: allowed=%v err=%v", allowed, err)
	}
	if err := destination.DeliverManually(context.Background(), event, delivery); err != nil {
		t.Fatal(err)
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil || !receipt.Blocks[0].Complete || receipt.Blocks[0].ManualRepublishPending ||
		len(receipt.Blocks[0].MessageIDs) != len(chunks) || sends.Load() != int32(len(chunks)-1) {
		t.Fatalf("ручная публикация после restart не завершилась: receipt=%+v sends=%d err=%v", receipt, sends.Load(), err)
	}
}

func TestManualPermitPreservesUnknownSecondPartAndFinalizesShrink(t *testing.T) {
	t.Parallel()
	var sends, edits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/editMessageText"):
			edits.Add(1)
			_, _ = writer.Write([]byte(`{"ok":true,"result":true}`))
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			call := sends.Add(1)
			if call == 2 {
				writer.WriteHeader(http.StatusInternalServerError)
				_, _ = writer.Write([]byte(`{"ok":false,"error_code":500,"description":"internal"}`))
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"message_id": 800 + call}})
		default:
			http.Error(writer, "unexpected method", http.StatusNotFound)
		}
	}))
	defer server.Close()
	api, err := NewWithBaseURL("123:token", server.URL, server.Client())
	if err != nil {
		t.Fatal(err)
	}
	api.sends.globalInterval = 0
	api.sends.groupInterval = 0
	destination := &Destination{API: api, ChatID: -1001, StatePath: filepath.Join(t.TempDir(), "state.json")}
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	long := ambiguousTelegramEvent("long", "2026-10-28 12:00:00", "12.00\n"+strings.Repeat("-Длинная строка. (10001)\n", 420), at)
	short := ambiguousTelegramEvent("short", "2026-10-28 12:01:00", "12.00\n-Короткая редакция", at.Add(time.Minute))
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), long, delivery); err == nil || sends.Load() != 2 {
		t.Fatalf("нужна неизвестная вторая часть: sends=%d err=%v", sends.Load(), err)
	}
	before, err := destination.DeliveryReceipt(short, delivery)
	if err != nil || len(before.Blocks) != 1 || before.Blocks[0].ExpectedMessageCount < 2 ||
		before.Blocks[0].UnconfirmedPart != 2 || len(before.Blocks[0].MessageIDs) != 1 {
		t.Fatalf("новая редакция обнулила сверку прежней второй части: receipt=%+v err=%v", before, err)
	}
	allowed, err := destination.AllowRepublishWithConfirmation(short, "block-1", 2, before.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("подтверждённое отсутствие второй части отклонено: allowed=%v err=%v", allowed, err)
	}
	permit, err := destination.DeliveryReceipt(short, delivery)
	if err != nil || !permit.Blocks[0].ManualRepublishPending || permit.Blocks[0].UnconfirmedPart != 2 ||
		len(permit.Blocks[0].MessageIDs) != 1 {
		t.Fatalf("ручное разрешение потеряло индекс или известную часть: receipt=%+v err=%v", permit, err)
	}
	destination = &Destination{API: api, ChatID: -1001, StatePath: destination.StatePath}
	if err := destination.Deliver(context.Background(), short, delivery); err == nil || !application.IsPermanentDeliveryError(err) || sends.Load() != 2 {
		t.Fatalf("обычный worker создал дубль после сокращения: sends=%d err=%v", sends.Load(), err)
	}
	allowed, err = destination.ResumeManualRepublish(short, "block-1", permit.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("разрешение после restart недоступно: allowed=%v err=%v", allowed, err)
	}
	if err := destination.DeliverManually(context.Background(), short, delivery); err != nil {
		t.Fatal(err)
	}
	after, err := destination.DeliveryReceipt(short, delivery)
	if err != nil || !after.Blocks[0].Complete || after.Blocks[0].ManualRepublishPending ||
		len(after.Blocks[0].MessageIDs) != 1 || after.Blocks[0].MessageIDs[0] != 801 || sends.Load() != 2 || edits.Load() == 0 {
		t.Fatalf("сокращение 2→1 не завершило разрешение без нового POST: receipt=%+v sends=%d edits=%d err=%v", after, sends.Load(), edits.Load(), err)
	}
}

func TestManualPermitIsRevokedWhenSourceBlockDeleted(t *testing.T) {
	t.Parallel()
	destination, sends := newAmbiguousTelegramDestination(t)
	at := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	event := ambiguousTelegramEvent("v1", "2026-10-28 12:00:00", "12.00\n-Начало", at)
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}}
	if err := destination.Deliver(context.Background(), event, delivery); err == nil {
		t.Fatal("ожидался неизвестный исход")
	}
	receipt, err := destination.DeliveryReceipt(event, delivery)
	if err != nil {
		t.Fatal(err)
	}
	allowed, err := destination.AllowRepublishWithConfirmation(event, "block-1", 1, receipt.Blocks[0].SendAttemptToken)
	if err != nil || !allowed {
		t.Fatalf("ручное разрешение не сохранено: allowed=%v err=%v", allowed, err)
	}
	deleted := ambiguousTelegramEvent("v2", "2026-10-28 12:01:00", "", at.Add(time.Minute))
	deleted.SourceBlockDeleted = true
	deleted.HasActiveBlocks = false
	deleted.Blocks[0].DeletedAt = "2026-10-28 12:01:00"
	if err := destination.Deliver(context.Background(), deleted, delivery); err != nil {
		t.Fatal(err)
	}
	state, err := loadDestinationState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	block := state.Items["point-01|source-1"].Blocks["block-1"]
	if block.ManualRepublishPending || block.ManualRepublishToken != "" || !block.SourceDeleted || sends.Load() != 1 {
		t.Fatalf("удаление не отозвало неиспользованное разрешение: block=%+v sends=%d", block, sends.Load())
	}
	restored := ambiguousTelegramEvent("v3", "2026-10-28 12:02:00", "12.00\n-Восстановлено", at.Add(2*time.Minute))
	if err := destination.Deliver(context.Background(), restored, delivery); err != nil || sends.Load() != 2 {
		t.Fatalf("новая активная редакция осталась в ручном барьере: sends=%d err=%v", sends.Load(), err)
	}
}
