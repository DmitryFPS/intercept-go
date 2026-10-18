package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

func TestServerServiceChecksTelegramIdentityBeforeAccept(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x31}, 32)
	event := serverServiceEvent()
	envelope, err := protocol.Seal(event, "key-1", key, time.Now(), bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	payload, _ := json.Marshal(envelope)
	store := &memoryServerStore{}
	service := ServerService{
		Clients: memoryRegistry{client: RegisteredClient{
			ID: "textile", Name: "Текстиль", SenderBotID: 11, SenderBotUsername: "textile_bot",
			ChatID: -1001, ThreadID: 42, KeyID: "key-1", Key: key, Active: true,
		}},
		Rules: memoryRules{rules: serverServiceRules()}, Store: store,
	}
	packet := IncomingPacket{
		SenderBotID: 11, SenderBotUsername: "textile_bot", ChatID: -1001, ThreadID: 42, Payload: payload,
	}
	result, err := service.Receive(context.Background(), packet)
	if err != nil || !result.Accepted || store.accepted != 1 {
		t.Fatalf("receive: %#v %v", result, err)
	}
	packet.SenderBotID = 12
	if _, err := service.Receive(context.Background(), packet); err == nil {
		t.Fatal("чужой Telegram-бот должен быть отклонён")
	}
	if store.accepted != 1 {
		t.Fatal("отклонённый пакет не должен попадать в хранилище")
	}
}

func TestDeliveryServiceKeepsDestinationFailuresIndependent(t *testing.T) {
	t.Parallel()

	queue := &memoryDeliveryQueue{jobs: []DeliveryJob{
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "ok"}}},
		{Event: serverServiceEvent(), Delivery: domain.PlannedDelivery{Destination: domain.Destination{ID: "fail"}}},
	}}
	service := DeliveryService{
		Queue: queue,
		Gateways: map[string]DeliveryGateway{
			"ok": memoryGateway{}, "fail": memoryGateway{err: errors.New("нет сети")},
		},
		Now: time.Now,
	}
	count, err := service.RunOnce(context.Background(), 10)
	if err != nil || count != 1 {
		t.Fatalf("run: %d %v", count, err)
	}
	if len(queue.delivered) != 1 || len(queue.retried) != 1 {
		t.Fatalf("состояния направлений смешались: %#v %#v", queue.delivered, queue.retried)
	}
}

type memoryRegistry struct{ client RegisteredClient }

func (registry memoryRegistry) Client(_ context.Context, id string) (RegisteredClient, error) {
	if id != registry.client.ID {
		return RegisteredClient{}, errors.New("not found")
	}
	return registry.client, nil
}

func (registry memoryRegistry) SaveClient(_ context.Context, _ RegisteredClient) error { return nil }

type memoryRules struct{ rules domain.RuleSet }

func (rules memoryRules) ActiveRules(_ context.Context) (domain.RuleSet, error) {
	return rules.rules, nil
}
func (rules memoryRules) PublishRules(_ context.Context, _ domain.RuleSet, _ string) error {
	return nil
}

type memoryServerStore struct{ accepted int }

func (store *memoryServerStore) Accept(_ context.Context, _ domain.Event, _ domain.DeliveryPlan) (bool, error) {
	store.accepted++
	return true, nil
}

type memoryGateway struct{ err error }

func (gateway memoryGateway) Deliver(_ context.Context, _ domain.Event, _ domain.PlannedDelivery) error {
	return gateway.err
}

type memoryDeliveryQueue struct {
	jobs      []DeliveryJob
	delivered []string
	retried   []string
}

func (queue *memoryDeliveryQueue) PendingDeliveries(_ context.Context, _ int, _ time.Time) ([]DeliveryJob, error) {
	return queue.jobs, nil
}

func (queue *memoryDeliveryQueue) MarkDelivered(_ context.Context, eventID, destinationID string, _ time.Time) error {
	queue.delivered = append(queue.delivered, eventID+":"+destinationID)
	return nil
}

func (queue *memoryDeliveryQueue) MarkDeliveryRetry(_ context.Context, eventID, destinationID string, _ time.Time, _ string) error {
	queue.retried = append(queue.retried, eventID+":"+destinationID)
	return nil
}

func serverServiceEvent() domain.Event {
	now := time.Now().UTC()
	return domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 1, UUID: "source"},
		Position: domain.Position{UnitName: "позиция"}, Mode: domain.SourceModeNormal,
		Content: "данные", ContentDigest: "digest",
	}
}

func serverServiceRules() domain.RuleSet {
	return domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"d": {ID: "d", Name: "Получатель", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:" + domain.NormalizeUnitName("позиция"): {"d": {Action: domain.RouteDeliver}},
		},
		Unknown: map[string]domain.RouteDecision{"d": {Action: domain.RouteReview}},
	}
}
