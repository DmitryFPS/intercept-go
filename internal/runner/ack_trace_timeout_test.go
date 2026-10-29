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
	"sync"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

// Telegram может принять документ, но не вернуть ответ отправителю. В этом
// случае только подтверждение Центра сообщает Клиенту действительный ID.
func TestAckTraceRestoresMessageIDAfterAcceptedUploadTimesOut(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	key := bytes.Repeat([]byte{0x42}, 32)
	event := domain.Event{
		ID: "event-timeout-after-accept", ClientID: "station", CreatedAt: now,
		ObservedAt: now, SourceAt: now, ExpiresAt: now.Add(30 * time.Minute),
		Source:   domain.SourceRecord{ID: 1, UUID: "source-timeout-after-accept", UpdatedAt: now.Format(time.RFC3339Nano)},
		Position: domain.Position{UnitName: "подразделение"}, Mode: domain.SourceModeNormal,
		Content: "10.15\n-Проверка", ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()

	clientDB, err := sqlite.OpenState(filepath.Join(t.TempDir(), "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer clientDB.Close()
	clientQueue, err := sqlite.NewClientQueue(ctx, clientDB)
	if err != nil {
		t.Fatal(err)
	}
	if inserted, err := clientQueue.Enqueue(ctx, event); err != nil || !inserted {
		t.Fatalf("поставить исходное событие в очередь: inserted=%v err=%v", inserted, err)
	}

	var mu sync.Mutex
	var document []byte
	var ackText string
	var documentSends int
	apiServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(request.URL.Path, "/sendDocument"):
			if err := request.ParseMultipartForm(4 << 20); err != nil {
				t.Errorf("разобрать документ: %v", err)
				return
			}
			file, _, err := request.FormFile("document")
			if err != nil {
				t.Errorf("открыть документ: %v", err)
				return
			}
			payload, err := io.ReadAll(file)
			_ = file.Close()
			if err != nil {
				t.Errorf("прочитать документ: %v", err)
				return
			}
			mu.Lock()
			document = payload // Telegram уже принял пакет с message_id=123.
			documentSends++
			mu.Unlock()
			<-request.Context().Done() // Но ответ теряется до получения Клиентом.
		case strings.HasSuffix(request.URL.Path, "/getUpdates"):
			var input struct {
				Offset int64 `json:"offset"`
			}
			_ = json.NewDecoder(request.Body).Decode(&input)
			mu.Lock()
			payload := append([]byte(nil), document...)
			ack := ackText
			mu.Unlock()
			updates := []any{}
			if input.Offset == 0 && strings.Contains(request.URL.Path, "/bot22:server/") && len(payload) > 0 {
				updates = append(updates, map[string]any{"update_id": 1, "message": map[string]any{
					"message_id": 123, "message_thread_id": 9, "date": now.Unix(),
					"from":     map[string]any{"id": 11, "is_bot": true, "username": "station_bot"},
					"chat":     map[string]any{"id": -1001, "type": "supergroup"},
					"document": map[string]any{"file_id": "event", "file_name": event.ID + ".icevent", "file_size": len(payload)},
				}})
			}
			if input.Offset == 0 && strings.Contains(request.URL.Path, "/bot11:station/") && ack != "" {
				updates = append(updates, map[string]any{"update_id": 1, "message": map[string]any{
					"message_id": 321, "date": now.Unix(),
					"from": map[string]any{"id": 22, "is_bot": true, "username": "server_bot"},
					"chat": map[string]any{"id": 11, "type": "private"}, "text": ack,
				}})
			}
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": updates})
		case strings.HasSuffix(request.URL.Path, "/getFile"):
			_ = json.NewEncoder(writer).Encode(map[string]any{
				"ok": true, "result": map[string]any{"file_path": "event.icevent"},
			})
		case strings.Contains(request.URL.Path, "/file/bot"):
			mu.Lock()
			payload := append([]byte(nil), document...)
			mu.Unlock()
			_, _ = writer.Write(payload)
		case strings.HasSuffix(request.URL.Path, "/sendMessage"):
			var input struct {
				Text string `json:"text"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Errorf("разобрать подтверждение: %v", err)
				return
			}
			mu.Lock()
			ackText = input.Text
			mu.Unlock()
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": map[string]any{
				"message_id": 321, "date": now.Unix(), "chat": map[string]any{"id": 11, "type": "private"},
			}})
		case strings.HasSuffix(request.URL.Path, "/deleteMessage"):
			_ = json.NewEncoder(writer).Encode(map[string]any{"ok": true, "result": true})
		default:
			http.Error(writer, "unexpected request", http.StatusNotFound)
		}
	}))
	defer apiServer.Close()

	stationAPI, err := telegram.NewWithBaseURL("11:station", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	stationTransport := &telegram.ClientTransport{
		API: stationAPI, ChatID: -1001, ThreadID: 9,
		ServerBotID: 22, ServerBotUsername: "server_bot", Key: key,
	}
	station := &application.ClientService{
		ClientID: "station", Queue: clientQueue, State: clientQueue,
		Sender: stationTransport, KeyID: "key-1", Key: key,
		Now: func() time.Time { return now }, AttemptTimeout: 40 * time.Millisecond,
	}
	if sent, err := station.DispatchOnce(ctx); err != nil || sent != 0 {
		t.Fatalf("неоднозначный тайм-аут должен оставить событие для повтора: sent=%d err=%v", sent, err)
	}
	mu.Lock()
	acceptedPayload := append([]byte(nil), document...)
	sends := documentSends
	mu.Unlock()
	if sends != 1 || len(acceptedPayload) == 0 {
		t.Fatalf("Telegram не сохранил единственный пакет: sends=%d bytes=%d", sends, len(acceptedPayload))
	}
	pending, total, err := clientQueue.EventsPage(ctx, "pending", 10, 0)
	if err != nil || total != 1 || pending[0].TransportMessageID != nil {
		t.Fatalf("до ACK Клиент не должен знать ID: total=%d events=%+v err=%v", total, pending, err)
	}

	store := newRunnerTransportStore(t)
	registered := application.RegisteredClient{
		ID: "station", Number: "001", Name: "Точка", SenderBotID: 11,
		SenderBotUsername: "station_bot", ChatID: -1001, ThreadID: 9,
		KeyID: "key-1", Key: key, Active: true,
	}
	if err := store.SaveClient(ctx, registered); err != nil {
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
	serverAPI, err := telegram.NewWithBaseURL("22:server", apiServer.URL, apiServer.Client())
	if err != nil {
		t.Fatal(err)
	}
	reception := &application.ServerService{
		Clients: store, Rules: store, Store: store, Now: func() time.Time { return now },
		DeliveryWindow: 120 * time.Minute,
	}
	receiver := &telegram.ServerReceiver{
		API: serverAPI, ChatID: -1001, MaxBytes: 4 << 20,
		Authorize:    func(context.Context, application.IncomingPacket) (bool, error) { return true, nil },
		Authenticate: reception.AuthenticatePacket,
		Observe:      func(application.TransportObservation) {},
	}
	packets, next, err := receiver.Packets(ctx, 0)
	if err != nil || len(packets) != 1 || next != 2 || packets[0].MessageID != 123 {
		t.Fatalf("Центр не скачал единственный пакет: count=%d next=%d err=%v", len(packets), next, err)
	}
	if err := store.PersistTransportBatch(ctx, serverOffsetName, next, packets); err != nil {
		t.Fatal(err)
	}
	server := &Server{Store: store, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	server.processTransportInboxItem(ctx, receiver, reception, packets[0], 0, make(map[string]time.Time))
	mu.Lock()
	ack := ackText
	mu.Unlock()
	if want := telegram.AcknowledgementTraceText(event.ID, "accepted", 123, key); ack != want {
		t.Fatalf("Центр не подтвердил свой ID подписанным ACK/4: got=%q want=%q", ack, want)
	}

	acknowledged, err := station.ReceiveAcknowledgements(ctx)
	if err != nil || acknowledged.Events != 1 {
		t.Fatalf("Клиент не принял ACK/4: result=%+v err=%v", acknowledged, err)
	}
	items, total, err := clientQueue.EventsPage(ctx, "acknowledged", 10, 0)
	if err != nil || total != 1 || items[0].CenterMessageID == nil ||
		*items[0].CenterMessageID != 123 || items[0].TransportMessageID == nil ||
		*items[0].TransportMessageID != 123 || items[0].UploadMessageID != nil {
		t.Fatalf("ID, увиденный Центром, не восстановлен в истории Клиента: total=%d items=%+v err=%v", total, items, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Events != 1 || stats.TransportInbox != 0 || stats.Rejected != 0 {
		t.Fatalf("Центр не завершил единственное событие: stats=%+v err=%v", stats, err)
	}
	station.Now = func() time.Time { return now.Add(time.Minute) }
	if sent, err := station.DispatchOnce(ctx); err != nil || sent != 0 {
		t.Fatalf("после ACK Клиент повторно отправил принятое событие: sent=%d err=%v", sent, err)
	}
	mu.Lock()
	sends = documentSends
	mu.Unlock()
	if sends != 1 {
		t.Fatalf("после ACK возник дубликат отправки: sends=%d", sends)
	}
}
