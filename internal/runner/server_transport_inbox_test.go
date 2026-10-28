package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
	"intercept/internal/protocol"
)

// Даже без запущенной бизнес-обработки приёмник сначала сохраняет файл и
// offset в SQLite. Это и есть граница, после которой суточная очистка Telegram
// больше не может удалить единственную копию пакета.
func TestTransportIntakePersistsWithoutProcessor(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"schema_version":1}`)
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			var input struct {
				Offset int64 `json:"offset"`
			}
			_ = json.NewDecoder(request.Body).Decode(&input)
			result := []any{}
			if input.Offset == 0 {
				result = append(result, map[string]any{"update_id": 1, "message": map[string]any{
					"message_id": 101, "message_thread_id": 9, "date": time.Now().Unix(),
					"from":     map[string]any{"id": 11, "is_bot": true, "username": "station_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "event", "file_name": "event.icevent", "file_size": len(payload)},
				}})
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": result})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"file_path": "event.icevent"},
			})
		case strings.Contains(request.URL.Path, "/file/bot"):
			_, _ = writer.Write(payload)
		default:
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		}
	}))
	defer apiServer.Close()

	store := newRunnerTransportStore(t)
	client, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := &telegram.ServerReceiver{
		API: client, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.transportIntakeLoop(ctx, receiver)
	}()
	waitUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(waitUntil) {
		stats, statsErr := store.Stats(context.Background())
		offset, exists, valueErr := store.Value(context.Background(), serverOffsetName)
		if statsErr == nil && valueErr == nil && exists && offset == "2" && stats.TransportInbox == 1 {
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatal("скачанный пакет не был зафиксирован независимо от обработчика")
}

// Окончательно повреждённый конверт уходит в карантин, но следующий корректный
// элемент той же локальной очереди всё равно принимается.
func TestTransportInboxQuarantinesPoisonAndProcessesFollowingPacket(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newRunnerTransportStore(t)
	key := bytes.Repeat([]byte{0x64}, 32)
	client := application.RegisteredClient{
		ID: "station", Number: "001", Name: "Точка", SenderBotID: 11,
		SenderBotUsername: "station_bot", ChatID: -1001, ThreadID: 9,
		KeyID: "key-1", Key: key, Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	rules := domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"telegram": {ID: "telegram", Name: "Получатель", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("подразделение"): {
				"telegram": {Action: domain.RouteDeliver, ThreadID: 7},
			},
		},
		Unknown: map[string]domain.RouteDecision{"telegram": {Action: domain.RouteReview}},
	}
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	event := domain.Event{
		ID: "event-valid", ClientID: client.ID, CreatedAt: now, ObservedAt: now,
		SourceAt: now, ExpiresAt: now.Add(30 * time.Minute),
		Source:   domain.SourceRecord{ID: 1, UUID: "source-valid"},
		Position: domain.Position{UnitName: "подразделение"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	envelope, err := protocol.Seal(event, client.KeyID, key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	validPayload, _ := json.Marshal(envelope)
	legacyEnvelope := envelope
	legacyEnvelope.Schema = "intercept-envelope/v1"
	legacyPayload, _ := json.Marshal(legacyEnvelope)
	packets := []application.IncomingPacket{
		{UpdateID: 1, SenderBotID: 11, SenderBotUsername: "station_bot", ChatID: -1001, ThreadID: 9,
			MessageID: 101, SentAt: now, ReceivedAt: now, Payload: []byte("повреждено")},
		{UpdateID: 2, SenderBotID: 11, SenderBotUsername: "station_bot", ChatID: -1001, ThreadID: 9,
			MessageID: 102, SentAt: now, ReceivedAt: now, Payload: legacyPayload},
		{UpdateID: 3, SenderBotID: 11, SenderBotUsername: "station_bot", ChatID: -1001, ThreadID: 9,
			MessageID: 103, SentAt: now, ReceivedAt: now, Payload: validPayload},
	}
	if err := store.PersistTransportBatch(ctx, serverOffsetName, 4, packets); err != nil {
		t.Fatal(err)
	}

	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		result := any(true)
		if strings.HasSuffix(request.URL.Path, "/sendMessage") {
			result = map[string]any{
				"message_id": 999, "date": time.Now().Unix(),
				"chat": map[string]any{"id": 1, "type": "private"},
			}
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": result})
	}))
	defer apiServer.Close()
	telegramClient, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := &telegram.ServerReceiver{
		API: telegramClient, ChatID: -1001, MaxBytes: 1024,
		Observe: func(application.TransportObservation) {},
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	reception := &application.ServerService{
		Clients: store, Rules: store, Store: store, Now: time.Now, DeliveryWindow: time.Hour,
	}
	loopContext, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.transportInboxLoop(loopContext, receiver, reception)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		stats, statsErr := store.Stats(context.Background())
		if statsErr == nil && stats.TransportInbox == 0 && stats.Events == 1 && stats.Rejected == 2 {
			cancel()
			<-done
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	stats, _ := store.Stats(context.Background())
	t.Fatalf("ядовитый пакет задержал следующий: %+v", stats)
}

func newRunnerTransportStore(t *testing.T) *sqlite.ServerStore {
	t.Helper()
	db, err := sqlite.OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := sqlite.NewServerStore(context.Background(), db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
