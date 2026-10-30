package runner

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

// A history refresh must not use an old gateway permit after a manual
// publication has already committed the new Telegram message ID to SQLite.
func TestExpiredHistoryDoesNotOverwriteConcurrentManualPublication(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	server, err := NewServer(ctx, baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	const (
		destinationID = "telegram"
		botToken      = "123:race-test"
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
	content := "12:00\n-Проверка параллельной публикации"
	event := domain.Event{
		ID: "event-history-race", ClientID: "point-01",
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

	bot := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(writer, `{"ok":false,"error_code":500,"description":"internal"}`)
	}))
	defer bot.Close()
	api, err := telegram.NewWithBaseURL(botToken, bot.URL, bot.Client())
	if err != nil {
		t.Fatal(err)
	}
	scope := telegramStateScope(0, botToken, chatID)
	gateway := &telegram.Destination{
		API: api, ChatID: chatID,
		StatePath:  filepath.Join(baseDir, "data", "telegram-"+safeID(destinationID)+".json"),
		StateScope: scope, LegacyStateScope: scope,
	}
	if err := gateway.Deliver(ctx, event, delivery); err == nil {
		t.Fatal("первая отправка должна иметь неопределённый исход")
	}
	unknown, err := gateway.DeliveryReceipt(event, delivery)
	if err != nil || len(unknown.Blocks) != 1 || !unknown.Blocks[0].UnknownOutcome {
		t.Fatalf("не получен маркер неопределённой отправки: receipt=%+v err=%v", unknown, err)
	}
	if err := server.Store.MarkDeliveryFailed(ctx, event.ID, destinationID, now, "исход отправки Telegram неизвестен"); err != nil {
		t.Fatal(err)
	}
	allowed, err := gateway.AllowRepublishWithConfirmation(event, "block-1", 1, unknown.Blocks[0].SendAttemptToken)
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

	// Model the moment after Telegram POST and SQLite commit. The gateway file
	// deliberately still contains the old permit, as in a stale history read.
	complete := application.DeliveryReceipt{
		Kind: "telegram", ChatID: chatID, ThreadID: threadID, MessageIDs: []int64{messageID},
		Blocks: []application.DeliveryBlockReceipt{{
			BlockUUID: "block-1", Complete: true, MessageIDs: []int64{messageID}, ExpectedMessageCount: 1,
		}},
	}

	server.adminMu.Lock()
	locked := true
	defer func() {
		if locked {
			server.adminMu.Unlock()
		}
	}()
	type historyResult struct {
		page stateDB.ServerHistoryPage
		err  error
	}
	started := make(chan struct{})
	done := make(chan historyResult, 1)
	go func() {
		close(started)
		page, err := server.History(ctx, "expired", 10, 0, HistoryFilter{})
		done <- historyResult{page: page, err: err}
	}()
	<-started
	select {
	case result := <-done:
		if result.err != nil || result.page.Total != 1 {
			t.Fatalf("пока идёт ручная публикация, история должна читаться без сверки: page=%+v err=%v", result.page, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("чтение истории заблокировалось ручной публикацией")
	}
	if err := server.Store.ReplaceDeliveryReceipt(ctx, event.ID, destinationID, now.Add(time.Second), complete); err != nil {
		t.Fatal(err)
	}
	server.adminMu.Unlock()
	locked = false
	delivered, err := server.History(ctx, "delivered", 10, 0, HistoryFilter{})
	if err != nil || delivered.Total != 1 || len(delivered.Items) != 1 ||
		len(delivered.Items[0].TelegramIDs) != 1 || delivered.Items[0].TelegramIDs[0] != messageID {
		t.Fatalf("результат публикации потерян из-за устаревшей квитанции: page=%+v err=%v", delivered, err)
	}
}
