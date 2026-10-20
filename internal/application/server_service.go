package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
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
	MessageID         int64
	Payload           []byte
	ProbeID           string
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
	Clients        ClientRegistry
	Rules          RuleRepository
	Store          ServerStore
	Now            func() time.Time
	DeliveryWindow time.Duration
}

// Receive проверяет происхождение и содержимое пакета, после чего надёжно
// сохраняет событие вместе с рассчитанными маршрутами.
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
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return PacketResult{}, errors.New("после транспортного пакета найдены лишние данные")
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
	now := service.Now().UTC()
	// Подтверждаем уже сохранённый пакет даже после истечения исходных 30 минут.
	// Такое возможно, если первое подтверждение Telegram потерялось. Проверка
	// дайджеста не позволяет подменить содержимое под известным UUID.
	known, err := service.Store.KnownEvent(ctx, event.ID, event.ContentDigest)
	if err != nil {
		return PacketResult{}, err
	}
	if known {
		return PacketResult{
			EventID: event.ID, ClientBotUsername: client.SenderBotUsername,
			Status: "accepted",
		}, nil
	}
	if event.Expired(now) {
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
	deliveryWindow := service.DeliveryWindow
	if deliveryWindow <= 0 {
		deliveryWindow = 10 * time.Minute
	}
	// После своевременного приёма транспорт больше не отвечает за срок. Сервер
	// получает собственное ограниченное окно, чтобы завершить внешние доставки.
	event.ExpiresAt = now.Add(deliveryWindow)
	event.FreshnessIssue = ""
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
	Queue          DeliveryQueue
	Gateways       map[string]DeliveryGateway
	Now            func() time.Time
	MaxConcurrent  int
	AttemptTimeout time.Duration
	Logger         *slog.Logger
}

// RunOnce выполняет доступные задания параллельно, сохраняя независимое
// состояние каждого получателя.
func (service *DeliveryService) RunOnce(ctx context.Context, limit int) (int, error) {
	if service.Now == nil {
		service.Now = time.Now
	}
	jobs, err := service.Queue.PendingDeliveries(ctx, limit, service.Now())
	if err != nil {
		return 0, err
	}
	maxConcurrent := service.MaxConcurrent
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}
	if maxConcurrent > len(jobs) {
		maxConcurrent = len(jobs)
	}
	type jobResult struct {
		delivered bool
		err       error
	}
	results := make(chan jobResult, len(jobs))
	semaphore := make(chan struct{}, maxConcurrent)
	var workers sync.WaitGroup
	for _, job := range jobs {
		job := job
		workers.Add(1)
		go func() {
			defer workers.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()
			delivered, err := service.deliverOne(ctx, job)
			results <- jobResult{delivered: delivered, err: err}
		}()
	}
	workers.Wait()
	close(results)
	delivered := 0
	var firstError error
	for result := range results {
		if result.delivered {
			delivered++
		}
		if result.err != nil && firstError == nil {
			firstError = result.err
		}
	}
	return delivered, firstError
}

func (service *DeliveryService) deliverOne(ctx context.Context, job DeliveryJob) (bool, error) {
	now := service.Now().UTC()
	if job.Event.Expired(now) {
		if service.Logger != nil {
			service.Logger.Warn("доставка отменена: событие старше допустимого срока",
				"event_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
				"position", job.Event.Position.UnitName, "expires_at", job.Event.ExpiresAt,
			)
		}
		return false, service.Queue.MarkDeliveryExpired(
			ctx, job.Event.ID, job.Delivery.Destination.ID, now, domain.FreshnessExpired,
		)
	}
	gateway, exists := service.Gateways[job.Delivery.Destination.ID]
	if !exists {
		reason := "для направления не настроен адаптер доставки"
		if service.Logger != nil {
			service.Logger.Error(reason, "event_id", job.Event.ID,
				"destination_id", job.Delivery.Destination.ID)
		}
		return false, service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
			now.Add(retryDelay(job.Attempts+1)), reason)
	}
	timeout := service.AttemptTimeout
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	attemptContext, cancel := context.WithTimeout(ctx, timeout)
	err := gateway.Deliver(attemptContext, job.Event, job.Delivery)
	cancel()
	if err != nil {
		next := now.Add(retryDelayFor(err, job.Attempts+1))
		if markErr := service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
			next, err.Error()); markErr != nil {
			return false, fmt.Errorf("доставка: %v; сохранить повтор: %w", err, markErr)
		}
		if service.Logger != nil {
			service.Logger.Warn("доставка не удалась, назначен повтор",
				"event_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
				"thread_id", job.Delivery.Decision.ThreadID, "attempt", job.Attempts+1,
				"next_attempt_at", next, "error", err,
			)
		}
		return false, nil
	}
	if err := service.Queue.MarkDelivered(ctx, job.Event.ID, job.Delivery.Destination.ID, service.Now()); err != nil {
		return false, err
	}
	if service.Logger != nil {
		service.Logger.Info("событие доставлено",
			"event_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
			"thread_id", job.Delivery.Decision.ThreadID, "attempt", job.Attempts+1,
		)
	}
	return true, nil
}

func sameUsername(left, right string) bool {
	normalize := func(value string) string {
		return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "@"))
	}
	return normalize(left) != "" && normalize(left) == normalize(right)
}
