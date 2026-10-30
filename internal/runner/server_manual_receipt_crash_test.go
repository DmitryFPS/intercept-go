package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/telegram"
)

// Имитирует остановку Центра после успешного ответа Bot API и записи ID в JSON
// шлюза, но до переноса результата ручной публикации в SQLite.
func TestExpiredTelegramHistoryRecoversManualPostBeforeSQLiteReceipt(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	server, err := NewServer(ctx, baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const (
		destinationID = "telegram"
		botToken      = "123:token"
		chatID        = int64(-100123)
		threadID      = int64(42)
		messageID     = int64(902)
	)
	server.config = config.Server{Destinations: map[string]config.DestinationConfig{
		destinationID: {Kind: domain.DestinationTelegram, BotToken: botToken, ChatID: chatID},
	}}
	if err := server.Store.SaveClient(ctx, application.RegisteredClient{
		ID: "point-01", Number: "01", Name: "Текстиль", SenderBotID: 101,
		SenderBotUsername: "point_bot", ChatID: chatID, ThreadID: 2,
		KeyID: "key-01", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	content := "12:00\n-Проверка восстановления публикации"
	event := domain.Event{
		ID: "event-crash-recovery", ClientID: "point-01",
		CreatedAt: now, ObservedAt: now, SourceAt: now, ExpiresAt: now.Add(30 * time.Minute),
		Source:   domain.SourceRecord{ID: 1, UUID: "source-1"},
		Position: domain.Position{UnitName: "Текстиль"}, Mode: domain.SourceModeNormal,
		Content: content, Blocks: []domain.ContentBlock{{UUID: "block-1", Key: "12:00", Text: content}},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	delivery := domain.PlannedDelivery{
		Destination: domain.Destination{ID: destinationID, Name: "Получатель", Kind: domain.DestinationTelegram, Enabled: true},
		Decision:    domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: threadID},
	}
	plan := domain.DeliveryPlan{EventID: event.ID, RuleSetVersion: 1, Deliveries: []domain.PlannedDelivery{delivery}}
	if accepted, err := server.Store.Accept(ctx, event, plan); err != nil || !accepted {
		t.Fatalf("создать задание: accepted=%v err=%v", accepted, err)
	}

	var posts atomic.Int32
	bot := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if !strings.HasSuffix(request.URL.Path, "/sendMessage") {
			http.Error(writer, "unexpected method", http.StatusNotFound)
			return
		}
		call := posts.Add(1)
		if call == 1 {
			writer.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(writer, `{"ok":false,"error_code":500,"description":"internal"}`)
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"ok": true, "result": map[string]any{"message_id": messageID, "chat": map[string]any{"id": chatID}},
		})
	}))
	defer bot.Close()
	api, err := telegram.NewWithBaseURL(botToken, bot.URL, bot.Client())
	if err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(baseDir, "data", "telegram-"+safeID(destinationID)+".json")
	scope := telegramStateScope(0, botToken, chatID)
	gateway := &telegram.Destination{
		API: api, ChatID: chatID, StatePath: statePath,
		StateScope: scope, LegacyStateScope: scope,
	}
	if err := gateway.Deliver(ctx, event, delivery); err == nil {
		t.Fatal("первая отправка должна иметь неопределённый исход")
	}
	unknown, err := gateway.DeliveryReceipt(event, delivery)
	if err != nil || len(unknown.Blocks) != 1 || !unknown.Blocks[0].UnknownOutcome ||
		unknown.Blocks[0].SendAttemptToken == "" {
		t.Fatalf("нет маркера спорной отправки: receipt=%+v err=%v", unknown, err)
	}
	if err := server.Store.MarkDeliveryFailed(ctx, event.ID, destinationID, now, "исход отправки Telegram неизвестен"); err != nil {
		t.Fatal(err)
	}
	allowed, err := gateway.AllowRepublishWithConfirmation(
		event, "block-1", 1, unknown.Blocks[0].SendAttemptToken,
	)
	if err != nil || !allowed {
		t.Fatalf("ручное разрешение отклонено: allowed=%v err=%v", allowed, err)
	}
	permit, err := gateway.DeliveryReceipt(event, delivery)
	if err != nil || len(permit.Blocks) != 1 || !permit.Blocks[0].ManualRepublishPending {
		t.Fatalf("разрешение не сохранено в шлюзе: receipt=%+v err=%v", permit, err)
	}
	if err := server.Store.SaveManualDeliveryReceipt(ctx, event.ID, destinationID, permit); err != nil {
		t.Fatal(err)
	}
	if err := gateway.DeliverManually(ctx, event, delivery); err != nil {
		t.Fatalf("ручная отправка: %v", err)
	}
	if posts.Load() != 2 {
		t.Fatalf("ожидалось две попытки POST, получено %d", posts.Load())
	}

	// Новый экземпляр читает только файл: успешный ID пережил остановку процесса.
	restartedGateway := &telegram.Destination{
		ChatID: chatID, StatePath: statePath, StateScope: scope, LegacyStateScope: scope,
	}
	durable, err := restartedGateway.DeliveryReceipt(event, delivery)
	if err != nil || len(durable.MessageIDs) != 1 || durable.MessageIDs[0] != messageID ||
		len(durable.Blocks) != 1 || !durable.Blocks[0].Complete {
		t.Fatalf("успешный POST не сохранён в JSON шлюза: receipt=%+v err=%v", durable, err)
	}
	before, err := server.Store.HistoryPage(ctx, "expired", 10, 0, time.Now().UTC())
	if err != nil || before.Total != 1 || len(before.Items) != 1 || len(before.Items[0].TelegramIDs) != 0 {
		t.Fatalf("SQLite уже содержит результат до восстановления: page=%+v err=%v", before, err)
	}

	expired, err := server.History(ctx, "expired", 10, 0, HistoryFilter{})
	if err != nil || expired.Total != 0 {
		t.Fatalf("просроченное задание не восстановлено: page=%+v err=%v", expired, err)
	}
	delivered, err := server.History(ctx, "delivered", 10, 0, HistoryFilter{})
	if err != nil || delivered.Total != 1 || len(delivered.Items) != 1 ||
		delivered.Items[0].ID != event.ID || len(delivered.Items[0].TelegramIDs) != 1 ||
		delivered.Items[0].TelegramIDs[0] != messageID {
		t.Fatalf("SQLite не получил подтверждённый message_id: page=%+v err=%v", delivered, err)
	}
	if posts.Load() != 2 {
		t.Fatalf("восстановление создало повторный POST: posts=%d", posts.Load())
	}
}
