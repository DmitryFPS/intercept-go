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
	"unicode/utf8"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

// IncomingPacket описывает Telegram-метаданные до расшифрования.
type IncomingPacket struct {
	UpdateID          int64
	SenderBotID       int64
	SenderBotUsername string
	ChatID            int64
	ThreadID          int64
	MessageID         int64
	SentAt            time.Time
	// ReceivedAt фиксируется сразу после скачивания файла Telegram. Проверка
	// исходной свежести опирается на этот момент, а не на возможную задержку
	// последующей обработки локальной очереди Центра.
	ReceivedAt    time.Time
	Payload       []byte
	ProbeID       string
	ProbeClientID string
	ProbeSentAt   time.Time
	ProbeMAC      string
	ProbeShift    ShiftSnapshot
}

// TransportObservation описывает только технический след Telegram-обновления.
// Текст, подпись и содержимое пользовательских файлов сюда не попадают.
type TransportObservation struct {
	UpdateID          int64
	MessageID         int64
	ChatID            int64
	ThreadID          int64
	SenderBotID       int64
	SenderBotUsername string
	MessageKind       string
	Outcome           string
	Reason            string
	EventID           string
	ClientID          string
	Counted           bool
	Rank              int
	SentAt            time.Time
	ObservedAt        time.Time
}

// ErrClientNotFound позволяет отличить неизвестную точку от временного сбоя
// реестра. Неизвестный отправитель отклоняется окончательно, а ошибка БД
// должна оставить транспортный пакет для следующей попытки.
var ErrClientNotFound = errors.New("точка не найдена")

// PermanentPacketError означает, что повтор того же байтового пакета никогда
// не сделает его допустимым. Только такие ошибки можно помещать в карантин и
// окончательно подтверждать транспортному каналу.
type PermanentPacketError struct{ Err error }

func (err *PermanentPacketError) Error() string { return err.Err.Error() }
func (err *PermanentPacketError) Unwrap() error { return err.Err }

func permanentPacketError(message string) error {
	return &PermanentPacketError{Err: errors.New(message)}
}

// IsPermanentPacketError сообщает приёмнику, можно ли безопасно удалить пакет.
func IsPermanentPacketError(err error) bool {
	var permanent *PermanentPacketError
	return errors.As(err, &permanent)
}

// PacketResult нужен приёмнику для безопасного подтверждения клиенту.
type PacketResult struct {
	EventID            string
	ClientID           string
	ClientBotUsername  string
	AcknowledgementKey []byte
	AckTraceV4         bool
	Accepted           bool
	Status             string
	ContentKind        string
	ContentChars       int
	ContentBlocks      int
	SourceMode         string
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
	event, client, err := service.openAuthenticatedPacket(ctx, packet)
	if err != nil {
		return PacketResult{}, err
	}
	if err := event.Validate(); err != nil {
		return PacketResult{}, permanentPacketError("нарушена структура события: " + err.Error())
	}
	if service.Now == nil {
		service.Now = time.Now
	}
	now := service.Now().UTC()
	if err := domain.ValidateRevisionClock(event, now); err != nil {
		return PacketResult{}, permanentPacketError("нарушена совместимость времени EAVC: " + err.Error())
	}
	if err := event.ValidateContentDigest(); err != nil {
		return PacketResult{}, permanentPacketError(err.Error())
	}
	freshnessAt := now
	if !packet.ReceivedAt.IsZero() && !packet.ReceivedAt.After(now.Add(time.Minute)) {
		freshnessAt = packet.ReceivedAt.UTC()
	}
	// Подтверждаем уже сохранённый пакет даже после истечения исходных 30 минут.
	// Такое возможно, если первое подтверждение Telegram потерялось. Проверка
	// дайджеста не позволяет подменить содержимое под известным UUID.
	known, err := service.Store.KnownEvent(ctx, event.ID, event.ClientID, event.ContentDigest)
	if err != nil {
		if errors.Is(err, ErrEventIDConflict) {
			return PacketResult{}, &PermanentPacketError{Err: err}
		}
		return PacketResult{}, err
	}
	if known {
		return PacketResult{
			EventID: event.ID, ClientID: client.ID, ClientBotUsername: client.SenderBotUsername,
			AcknowledgementKey: append([]byte(nil), client.Key...),
			AckTraceV4:         event.AckTraceV4,
			Status:             "accepted", ContentKind: eventContentKind(event),
			ContentChars: utf8.RuneCountInString(event.Content), ContentBlocks: len(event.Blocks),
			SourceMode: string(event.Mode),
		}, nil
	}
	if event.Expired(freshnessAt) {
		return PacketResult{
			EventID: event.ID, ClientID: client.ID, ClientBotUsername: client.SenderBotUsername,
			AcknowledgementKey: append([]byte(nil), client.Key...),
			AckTraceV4:         event.AckTraceV4,
			Status:             "expired", ContentKind: eventContentKind(event),
			ContentChars: utf8.RuneCountInString(event.Content), ContentBlocks: len(event.Blocks),
			SourceMode: string(event.Mode),
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
		deliveryWindow = 30 * time.Minute
	}
	// После своевременного приёма транспорт больше не отвечает за срок. Сервер
	// получает собственное ограниченное окно, чтобы завершить внешние доставки.
	if event.SourceBlockDeleted {
		// Удаление — это корректирующая операция, а не новая публикация. Она
		// обязана дождаться восстановления Telegram/SMB и не оставлять старый
		// результат только из-за длительного внешнего сбоя.
		event.ExpiresAt = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
	} else {
		event.ExpiresAt = now.Add(deliveryWindow)
	}
	event.FreshnessIssue = ""
	accepted, err := service.Store.Accept(ctx, event, plan)
	if err != nil {
		if errors.Is(err, ErrSourceLineageLimit) || errors.Is(err, ErrSourceRevisionConflict) || errors.Is(err, ErrEventIDConflict) {
			return PacketResult{}, &PermanentPacketError{Err: err}
		}
		return PacketResult{}, err
	}
	return PacketResult{
		EventID: event.ID, ClientID: client.ID, ClientBotUsername: client.SenderBotUsername,
		AcknowledgementKey: append([]byte(nil), client.Key...),
		AckTraceV4:         event.AckTraceV4,
		Accepted:           accepted, Status: "accepted", ContentKind: eventContentKind(event),
		ContentChars: utf8.RuneCountInString(event.Content), ContentBlocks: len(event.Blocks),
		SourceMode: string(event.Mode),
	}, nil
}

// AuthenticatePacket проверяет только ключ и подпись AES-GCM до записи во
// входную очередь. Подлинные пакеты с ошибками контракта остаются в очереди
// для штатного карантина и объяснения причины оператору.
func (service *ServerService) AuthenticatePacket(ctx context.Context, packet IncomingPacket) error {
	envelope, client, err := service.authenticatedEnvelopeClient(ctx, packet)
	if err != nil {
		return err
	}
	if err := protocol.Authenticate(envelope, client.Key); err != nil {
		return &PermanentPacketError{Err: err}
	}
	return nil
}

func (service *ServerService) openAuthenticatedPacket(ctx context.Context, packet IncomingPacket) (domain.Event, RegisteredClient, error) {
	envelope, client, err := service.authenticatedEnvelopeClient(ctx, packet)
	if err != nil {
		return domain.Event{}, RegisteredClient{}, err
	}
	event, err := protocol.Open(envelope, client.Key)
	if err != nil {
		return domain.Event{}, RegisteredClient{}, &PermanentPacketError{Err: err}
	}
	if event.ClientID != client.ID {
		return domain.Event{}, RegisteredClient{}, permanentPacketError("ID точки внутри события не совпадает с регистрацией")
	}
	return event, client, nil
}

func (service *ServerService) authenticatedEnvelopeClient(ctx context.Context, packet IncomingPacket) (protocol.Envelope, RegisteredClient, error) {
	if len(packet.Payload) == 0 || len(packet.Payload) > 4*1024*1024 {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("недопустимый размер транспортного пакета")
	}
	decoder := json.NewDecoder(strings.NewReader(string(packet.Payload)))
	decoder.DisallowUnknownFields()
	var envelope protocol.Envelope
	if err := decoder.Decode(&envelope); err != nil {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("транспортный пакет повреждён")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("после транспортного пакета найдены лишние данные")
	}
	client, err := service.Clients.Client(ctx, envelope.ClientID)
	if err != nil {
		if errors.Is(err, ErrClientNotFound) {
			return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("точка не зарегистрирована")
		}
		return protocol.Envelope{}, RegisteredClient{}, fmt.Errorf("прочитать регистрацию точки: %w", err)
	}
	if !client.Active {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("точка отключена")
	}
	if client.SenderBotID != packet.SenderBotID ||
		!sameUsername(client.SenderBotUsername, packet.SenderBotUsername) {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("telegram-бот не принадлежит указанной точке")
	}
	if client.ChatID != packet.ChatID || client.ThreadID != packet.ThreadID {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("пакет поступил не из темы зарегистрированной точки")
	}
	if client.KeyID != envelope.KeyID {
		return protocol.Envelope{}, RegisteredClient{}, permanentPacketError("ключ пакета не зарегистрирован")
	}
	return envelope, client, nil
}

func eventContentKind(event domain.Event) string {
	if event.SourceBlockDeleted {
		return "deleted_block"
	}
	if strings.TrimSpace(event.Content) == "" {
		return "empty_text"
	}
	return "text"
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
	jobs = limitParallelWordDestinations(jobs)
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

// limitParallelWordDestinations оставляет в одном проходе не более одной
// операции для каждого Word-получателя. Telegram-темы по-прежнему могут
// отправляться параллельно, а DOCX не конкурируют за общий файл состояния и
// не тратят серверное окно на бесполезные повторы «операция ещё выполняется».
func limitParallelWordDestinations(jobs []DeliveryJob) []DeliveryJob {
	seenWord := make(map[string]struct{})
	selected := make([]DeliveryJob, 0, len(jobs))
	for _, job := range jobs {
		if job.Delivery.Destination.Kind == domain.DestinationWord {
			key := job.Delivery.Destination.ID
			if _, exists := seenWord[key]; exists {
				continue
			}
			seenWord[key] = struct{}{}
		}
		selected = append(selected, job)
	}
	return selected
}

func (service *DeliveryService) deliverOne(ctx context.Context, job DeliveryJob) (bool, error) {
	now := service.Now().UTC()
	// Корректирующее удаление уже опубликованного результата не ограничено
	// сроком исходного перехвата: оно должно выполняться и после долгого сбоя.
	if job.Delivery.RuleLevel != "cleanup" && job.Event.Expired(now) {
		if service.Logger != nil {
			service.Logger.Warn("доставка отменена: событие старше допустимого срока",
				"event_id", job.Event.ID, "trace_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
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
			service.Logger.Error(reason, "event_id", job.Event.ID, "trace_id", job.Event.ID,
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
		// Ошибка всей доставки не означает, что Telegram не подтвердил ни
		// одной части. Сохраняем известные ID и неопределённый исход до
		// перевода задания в повтор или окончательную ошибку.
		if provider, ok := gateway.(DeliveryReceiptProvider); ok {
			if queue, ok := service.Queue.(PartialDeliveryReceiptQueue); ok {
				receipt, receiptErr := provider.DeliveryReceipt(job.Event, job.Delivery)
				if receiptErr != nil {
					if service.Logger != nil {
						service.Logger.Warn("частичная квитанция доставки недоступна",
							"event_id", job.Event.ID, "trace_id", job.Event.ID,
							"destination_id", job.Delivery.Destination.ID, "error", receiptErr)
					}
				} else if saveErr := queue.SavePartialDeliveryReceipt(
					ctx, job.Event.ID, job.Delivery.Destination.ID, receipt,
				); saveErr != nil {
					return false, fmt.Errorf("доставка: %v; сохранить частичную квитанцию: %w", err, saveErr)
				}
			}
		}
		if IsPermanentDeliveryError(err) {
			if markErr := service.Queue.MarkDeliveryFailed(ctx, job.Event.ID, job.Delivery.Destination.ID,
				now, err.Error()); markErr != nil {
				return false, fmt.Errorf("доставка окончательно невозможна: %v; сохранить результат: %w", err, markErr)
			}
			if service.Logger != nil {
				service.Logger.Error("доставка окончательно невозможна, очередь продолжена",
					"event_id", job.Event.ID, "trace_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
					"thread_id", job.Delivery.Decision.ThreadID, "attempt", job.Attempts+1,
					"error", err,
				)
			}
			return false, nil
		}
		next := now.Add(retryDelayFor(err, job.Attempts+1))
		if markErr := service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
			next, err.Error()); markErr != nil {
			return false, fmt.Errorf("доставка: %v; сохранить повтор: %w", err, markErr)
		}
		if service.Logger != nil {
			service.Logger.Warn("доставка не удалась, назначен повтор",
				"event_id", job.Event.ID, "trace_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
				"thread_id", job.Delivery.Decision.ThreadID, "attempt", job.Attempts+1,
				"next_attempt_at", next, "error", err,
			)
		}
		return false, nil
	}
	receipt := DeliveryReceipt{}
	receiptReady := false
	if provider, ok := gateway.(DeliveryReceiptProvider); ok {
		var receiptErr error
		receipt, receiptErr = provider.DeliveryReceipt(job.Event, job.Delivery)
		if receiptErr != nil {
			// Публикация уже выполнена, но без устойчивой квитанции нельзя
			// завершать задание: внешний message_id навсегда исчез бы из
			// трассировки. Адаптер повторно сверит своё состояние без дубля.
			next := now.Add(retryDelay(job.Attempts + 1))
			if markErr := service.Queue.MarkDeliveryRetry(ctx, job.Event.ID, job.Delivery.Destination.ID,
				next, "Не удалось прочитать квитанцию доставки; повторная сверка состояния назначена"); markErr != nil {
				return false, fmt.Errorf("доставка выполнена, но квитанция недоступна: %v; сохранить повтор: %w", receiptErr, markErr)
			}
			if service.Logger != nil {
				service.Logger.Warn("доставка выполнена, квитанция будет прочитана повторно",
					"event_id", job.Event.ID, "trace_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
					"next_attempt_at", next, "error", receiptErr)
			}
			return false, nil
		} else {
			receiptReady = true
		}
	}
	markErr := error(nil)
	if queue, ok := service.Queue.(DeliveryReceiptQueue); ok && receiptReady {
		markErr = queue.MarkDeliveredWithReceipt(
			ctx, job.Event.ID, job.Delivery.Destination.ID, service.Now(), receipt,
		)
	} else {
		markErr = service.Queue.MarkDelivered(ctx, job.Event.ID, job.Delivery.Destination.ID, service.Now())
	}
	if markErr != nil {
		return false, markErr
	}
	if service.Logger != nil {
		// При резервной доставке фактическая тема может отличаться от темы
		// в плане. Для поиска сообщения в Telegram нужен ID из квитанции.
		actualThreadID := job.Delivery.Decision.ThreadID
		if receiptReady && receipt.ThreadID != 0 {
			actualThreadID = receipt.ThreadID
		}
		fields := []any{
			"event_id", job.Event.ID, "trace_id", job.Event.ID, "destination_id", job.Delivery.Destination.ID,
			"thread_id", actualThreadID, "planned_thread_id", job.Delivery.Decision.ThreadID,
			"attempt", job.Attempts + 1,
			"telegram_message_count", len(receipt.MessageIDs), "final_message_ids", receipt.MessageIDs,
		}
		if len(receipt.MessageIDs) > 0 {
			fields = append(fields, "telegram_message_id", receipt.MessageIDs[0])
		}
		service.Logger.Info("событие доставлено", fields...)
	}
	return true, nil
}

func sameUsername(left, right string) bool {
	normalize := func(value string) string {
		return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "@"))
	}
	return normalize(left) != "" && normalize(left) == normalize(right)
}
