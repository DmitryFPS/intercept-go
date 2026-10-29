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
	"sync/atomic"
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

// После фиксации пакета и offset в локальной БД исчезновение файла Telegram
// больше не влияет на приём: обработчик использует сохранённый payload.
func TestTransportInboxProcessesPacketAfterTelegramFileDisappears(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := newRunnerTransportStore(t)
	key := bytes.Repeat([]byte{0x37}, 32)
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
		ID: "event-persisted", ClientID: client.ID, CreatedAt: now, ObservedAt: now,
		SourceAt: now, ExpiresAt: now.Add(30 * time.Minute),
		Source:   domain.SourceRecord{ID: 1, UUID: "source-persisted"},
		Position: domain.Position{UnitName: "подразделение"}, Mode: domain.SourceModeNormal,
		Content: "текст", ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	envelope, err := protocol.Seal(event, client.KeyID, key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	var fileGone atomic.Bool
	var fileLookups, acknowledgements atomic.Int32
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
					"message_id": 101, "message_thread_id": 9, "date": now.Unix(),
					"from":     map[string]any{"id": 11, "is_bot": true, "username": "station_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "event", "file_name": "event.icevent", "file_size": len(payload)},
				}})
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": result})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			fileLookups.Add(1)
			if fileGone.Load() {
				writer.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "error_code": 404, "description": "Not Found"})
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"file_path": "event.icevent"},
			})
		case strings.Contains(request.URL.Path, "/file/bot"):
			if fileGone.Load() {
				http.NotFound(writer, request)
				return
			}
			_, _ = writer.Write(payload)
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			acknowledgements.Add(1)
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{
				"message_id": 999, "date": now.Unix(), "chat": map[string]any{"id": 1, "type": "private"},
			}})
		case strings.HasSuffix(request.URL.Path, "/deleteMessage"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer apiServer.Close()
	telegramClient, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	reception := &application.ServerService{
		Clients: store, Rules: store, Store: store, Now: func() time.Time { return now },
		DeliveryWindow: 120 * time.Minute,
	}
	receiver := &telegram.ServerReceiver{
		API: telegramClient, ChatID: -1001, MaxBytes: int64(len(payload) + 1),
		Authorize:    func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
		Authenticate: reception.AuthenticatePacket,
		Observe:      func(application.TransportObservation) {},
	}
	packets, next, err := receiver.Packets(ctx, 0)
	if err != nil || len(packets) != 1 || next != 2 {
		t.Fatalf("исходный пакет не скачан: count=%d next=%d err=%v", len(packets), next, err)
	}
	if err := store.PersistTransportBatch(ctx, serverOffsetName, next, packets); err != nil {
		t.Fatal(err)
	}
	fileGone.Store(true)
	if _, err := telegramClient.GetFile(ctx, "event"); err == nil {
		t.Fatal("имитатор продолжает отдавать удалённый транспортный файл")
	}
	lookupsBeforeProcessing := fileLookups.Load()
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	server.processTransportInboxItem(ctx, receiver, reception, packets[0], 0, make(map[string]time.Time))
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	offset, exists, err := store.Value(ctx, serverOffsetName)
	if err != nil || !exists || offset != "2" || stats.Events != 1 || stats.TransportInbox != 0 ||
		stats.Rejected != 0 || acknowledgements.Load() != 1 || fileLookups.Load() != lookupsBeforeProcessing {
		t.Fatalf("пакет потерян после удаления файла: stats=%+v offset=%q exists=%v ack=%d file_lookups=%d→%d err=%v",
			stats, offset, exists, acknowledgements.Load(), lookupsBeforeProcessing, fileLookups.Load(), err)
	}
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

// Уже скачанный префикс пакетов сохраняется до повтора временно недоступного
// следующего файла. Его удаление в Telegram после ошибки не должно лишать
// Центр единственной локальной копии.
func TestServerTransportIntakePersistsDownloadedPrefixOnTransientSuffix(t *testing.T) {
	store := newRunnerTransportStore(t)
	var failedSuffix atomic.Bool
	bAttempt := make(chan struct{}, 1)
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": []any{
				map[string]any{"update_id": 11, "message": map[string]any{
					"message_id": 101, "message_thread_id": 9, "date": time.Now().Unix(),
					"from":     map[string]any{"id": 11, "is_bot": true, "username": "station_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "first", "file_name": "first.icevent", "file_size": 5},
				}},
				map[string]any{"update_id": 12, "message": map[string]any{
					"message_id": 102, "message_thread_id": 9, "date": time.Now().Unix(),
					"from":     map[string]any{"id": 11, "is_bot": true, "username": "station_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "second", "file_name": "second.icevent", "file_size": 6},
				}},
			}})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			var input struct {
				FileID string `json:"file_id"`
			}
			_ = json.NewDecoder(request.Body).Decode(&input)
			if input.FileID == "second" {
				failedSuffix.Store(true)
				select {
				case bAttempt <- struct{}{}:
				default:
				}
				writer.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(writer).Encode(map[string]any{"ok": false, "error_code": 500, "description": "temporary"})
				return
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{"file_path": "first.icevent"}})
		case strings.Contains(request.URL.Path, "/file/bot"):
			_, _ = writer.Write([]byte("first"))
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer apiServer.Close()
	api, err := telegram.NewWithBaseURL("123:token", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	receiver := &telegram.ServerReceiver{
		API: api, ChatID: -1001, MaxBytes: 1024,
		Authorize: func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); server.transportIntakeLoop(ctx, receiver) }()
	defer func() { cancel(); <-done }()
	select {
	case <-bAttempt:
	case <-time.After(2 * time.Second):
		t.Fatal("fake Telegram API did not deliver the transient suffix error")
	}
	if !failedSuffix.Load() {
		t.Fatal("transient suffix was not exercised")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats, statsErr := store.Stats(context.Background())
		offset, exists, valueErr := store.Value(context.Background(), serverOffsetName)
		if statsErr == nil && valueErr == nil && exists && offset == "12" && stats.TransportInbox == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats, _ := store.Stats(context.Background())
	offset, exists, _ := store.Value(context.Background(), serverOffsetName)
	t.Fatalf("downloaded first file not durable after second failed: offset=%q exists=%v inbox=%d", offset, exists, stats.TransportInbox)
}
