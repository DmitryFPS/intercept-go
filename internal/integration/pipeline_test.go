package integration_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/protocol"
)

type recordingGateway struct {
	mu     sync.Mutex
	events []domain.Event
}

func (gateway *recordingGateway) Deliver(_ context.Context, event domain.Event, _ domain.PlannedDelivery) error {
	gateway.mu.Lock()
	defer gateway.mu.Unlock()
	gateway.events = append(gateway.events, event)
	return nil
}

func TestEncryptedPacketIsAcceptedRoutedAndDeliveredOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := stateDB.OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	store, err := stateDB.NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	key := []byte("01234567890123456789012345678901")
	client := application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Текстиль", SenderBotID: 101,
		SenderBotUsername: "textile_transport_bot", ChatID: -100500, ThreadID: 77,
		KeyID: "key-1", Key: key, Active: true,
	}
	if err := store.SaveClient(ctx, client); err != nil {
		t.Fatal(err)
	}
	rules := domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"shabashka": {ID: "shabashka", Name: "Шабашка", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{},
		Unknown: map[string]domain.RouteDecision{
			"shabashka": {Action: domain.RouteReview},
		},
	}
	if err := store.PublishRules(ctx, rules, "интеграционный тест"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	event := domain.Event{
		ID: "0199f399-8e00-7000-8000-000000000001", ClientID: client.ID,
		CreatedAt: now, ObservedAt: now, Source: domain.SourceRecord{ID: 42, UUID: "source-42"},
		Position: domain.Position{UnitName: "новое подразделение"}, Mode: domain.SourceModeNormal,
		Content: "текст перехвата", ContentDigest: "digest-42",
	}
	envelope, err := protocol.Seal(event, client.KeyID, key, now, nil)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	receiver := application.ServerService{Clients: store, Rules: store, Store: store}
	packet := application.IncomingPacket{
		SenderBotID: client.SenderBotID, SenderBotUsername: client.SenderBotUsername,
		ChatID: client.ChatID, ThreadID: client.ThreadID, Payload: payload,
	}
	first, err := receiver.Receive(ctx, packet)
	if err != nil || !first.Accepted {
		t.Fatalf("первый пакет должен быть принят: result=%+v err=%v", first, err)
	}
	duplicate, err := receiver.Receive(ctx, packet)
	if err != nil || duplicate.Accepted {
		t.Fatalf("повтор должен быть подтверждён без второй записи: result=%+v err=%v", duplicate, err)
	}

	gateway := &recordingGateway{}
	delivery := application.DeliveryService{
		Queue: store, Gateways: map[string]application.DeliveryGateway{"shabashka": gateway},
		Now: func() time.Time { return now.Add(time.Minute) },
	}
	count, err := delivery.RunOnce(ctx, 10)
	if err != nil || count != 1 {
		t.Fatalf("ожидалась одна доставка: count=%d err=%v", count, err)
	}
	count, err = delivery.RunOnce(ctx, 10)
	if err != nil || count != 0 {
		t.Fatalf("повторной доставки быть не должно: count=%d err=%v", count, err)
	}
	if len(gateway.events) != 1 || gateway.events[0].ID != event.ID {
		t.Fatalf("шлюз получил неожиданные события: %+v", gateway.events)
	}
}
