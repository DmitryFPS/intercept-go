package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

// IncomingPacket описывает Telegram-метаданные до расшифрования.
type IncomingPacket struct {
	SenderBotID       int64
	SenderBotUsername string
	ChatID            int64
	ThreadID          int64
	Payload           []byte
}

// PacketResult нужен приёмнику для безопасного подтверждения клиенту.
type PacketResult struct {
	EventID           string
	ClientBotUsername string
	Accepted          bool
	Status            string
}

// ServerService проверяет входные пакеты и создаёт независимые задания.
type ServerService struct {
	Clients ClientRegistry
	Rules   RuleRepository
	Store   ServerStore
	Now     func() time.Time
}

func (service *ServerService) Receive(ctx context.Context, packet IncomingPacket) (PacketResult, error) {
	if len(packet.Payload) == 0 || len(packet.Payload) > 4*1024*1024 {
		return PacketResult{}, errors.New("недопустимый размер транспортного пакета")
	}
	decoder := json.NewDecoder(strings.NewReader(string(packet.Payload)))
	decoder.DisallowUnknownFields()
	var envelope protocol.Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return PacketResult{}, errors.New("транспортный пакет повреждён")
	}
	client, err := service.Clients.Client(ctx, envelope.ClientID)
	if err != nil {
		return PacketResult{}, errors.New("точка не зарегистрирована")
	}
	if !client.Active {
		return PacketResult{}, errors.New("точка отключена")
	}
	if client.SenderBotID != packet.SenderBotID ||
		!sameUsername(client.SenderBotUsername, packet.SenderBotUsername) {
		return PacketResult{}, errors.New("Telegram-бот не принадлежит указанной точке")
	}
	if client.ChatID != packet.ChatID || client.ThreadID != packet.ThreadID {
		return PacketResult{}, errors.New("пакет поступил не из темы зарегистрированной точки")
	}
	if client.KeyID != envelope.KeyID {
		return PacketResult{}, errors.New("ключ пакета не зарегистрирован")
	}
	event, err := protocol.Open(envelope, client.Key)
	if err != nil {
		return PacketResult{}, err
	}
	if event.ClientID != client.ID {
		return PacketResult{}, errors.New("ID точки внутри события не совпадает с регистрацией")
	}
	if service.Now == nil {
		service.Now = time.Now
	}
	if event.Expired(service.Now()) {
		return PacketResult{
			EventID: event.ID, ClientBotUsername: client.SenderBotUsername,
			Status: "expired",
		}, nil
	}
	rules, err := service.Rules.ActiveRules(ctx)
	if err != nil {
		return PacketResult{}, fmt.Errorf("получить маршруты: %w", err)
	}
	plan, err := rules.Resolve(event)
	if err != nil {
		return PacketResult{}, fmt.Errorf("построить маршрут: %w", err)
	}
	accepted, err := service.Store.Accept(ctx, event, plan)
	if err != nil {
		return PacketResult{}, err
	}
	return PacketResult{
		EventID: event.ID, ClientBotUsername: client.SenderBotUsername,
		Accepted: accepted, Status: "accepted",
	}, nil
}

// DeliveryService выполняет ожидающие задания и не смешивает их состояния.
type DeliveryService struct {
	Queue    DeliveryQueue
	Gateways map[string]DeliveryGateway
	Now      func() time.Time
}

func (service *DeliveryService) RunOnce(ctx context.Context, limit int) (int, error) {
	if service.Now == nil {
		service.Now = time.Now
	}
	jobs, err := service.Queue.PendingDeliveries(ctx, limit, service.Now())
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, job := range jobs {
		now := service.Now().UTC()
		if job.Event.Expired(now) {
			if err := service.Queue.MarkDeliveryExpired(
				ctx, job.Event.ID, job.Delivery.Destination.ID, now, domain.FreshnessExpired,
			); err != nil {
				return delivered, err
			}
			continue
		}
		gateway, exists := service.Gateways[job.Delivery.Destination.ID]
		if !exists {
			reason := "для направления не настроен адаптер доставки"
			if err := service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
				service.Now().Add(retryDelay(job.Attempts+1)), reason); err != nil {
				return delivered, err
			}
			continue
		}
		if err := gateway.Deliver(ctx, job.Event, job.Delivery); err != nil {
			if markErr := service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
				service.Now().Add(retryDelay(job.Attempts+1)), err.Error()); markErr != nil {
				return delivered, fmt.Errorf("доставка: %v; сохранить повтор: %w", err, markErr)
			}
			continue
		}
		if err := service.Queue.MarkDelivered(ctx, job.Event.ID, job.Delivery.Destination.ID, service.Now()); err != nil {
			return delivered, err
		}
		delivered++
	}
	return delivered, nil
}

func sameUsername(left, right string) bool {
	normalize := func(value string) string {
		return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "@"))
	}
	return normalize(left) != "" && normalize(left) == normalize(right)
}
