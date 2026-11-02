package runner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/docx"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

const (
	transportInboxWorkerCount   = 3
	transportInboxLeaseDuration = 2 * time.Minute
	transportInboxLeaseRenewal  = 30 * time.Second
	transportInboxLeaseTimeout  = 5 * time.Second
)

func (server *Server) restartWorker(parent context.Context) error {
	server.lifecycleMu.Lock()
	defer server.lifecycleMu.Unlock()
	protected, err := secrets.Load(server.configPath())
	if err != nil {
		return err
	}
	cfg, err := config.DecodeServer(bytes.NewReader(protected))
	if err != nil {
		return err
	}
	for id, destination := range cfg.Destinations {
		if strings.TrimSpace(destination.Protection) == "" {
			destination.Protection = telegram.DefaultProtection
		}
		if destination.Kind == domain.DestinationTelegram {
			destination.MessageTemplate = telegram.MigrateMessageTemplate(destination.MessageTemplate)
		}
		cfg.Destinations[id] = destination
	}
	server.stopWorkerLocked()
	workerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	server.mu.Lock()
	server.workerStop = cancel
	server.workerDone = done
	server.config = cloneServerConfig(cfg)
	server.status.Configured = true
	server.status.State = "starting"
	server.status.Title = "Запуск центра"
	server.status.Detail = "Проверяем связь и получателей."
	server.status.InviteLink = cfg.Transport.InviteLink
	server.status.Destinations = destinationIDs(cfg.Destinations)
	server.status.Processing = true
	server.mu.Unlock()
	go func() {
		defer close(done)
		server.run(workerContext, cfg)
	}()
	return nil
}

func (server *Server) stopWorker() {
	server.lifecycleMu.Lock()
	defer server.lifecycleMu.Unlock()
	server.stopWorkerLocked()
}

func (server *Server) stopWorkerLocked() {
	server.mu.Lock()
	cancel, done := server.workerStop, server.workerDone
	server.workerStop = nil
	server.workerDone = nil
	server.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
	server.update(func(status *ServerStatus) { status.Processing = false })
}

func (server *Server) run(ctx context.Context, cfg config.Server) {
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		server.fail(
			"Неверные настройки Telegram",
			"Проверьте токен бота центра на странице «Связь».",
			err,
		)
		return
	}
	// Доставка уже принятых заданий не зависит от входящего Telegram polling.
	// Запускаем очередь до проверки webhook, чтобы временная недоступность
	// транспортного бота не блокировала Word и другие готовые направления.
	gateways := server.gatewaysWithTransportAPI(cfg, api)
	delivery := &application.DeliveryService{
		Queue: server.Store, Gateways: gateways, Now: time.Now,
		MaxConcurrent: 4, AttemptTimeout: 2 * time.Minute, Logger: server.Logger,
	}
	transportAudit := newTransportAuditSink(
		server.Store, server.Logger, filepath.Join(server.BaseDir, "data", "transport-audit-spool"),
		transportAuditBufferSize,
	)
	var background sync.WaitGroup
	var transportProducers sync.WaitGroup
	background.Add(2)
	go func() {
		defer background.Done()
		server.deliveryLoop(ctx, delivery)
	}()
	go func() {
		defer background.Done()
		transportAudit.run()
	}()
	defer func() {
		// Сначала дожидаемся всех обработчиков, которые способны вызвать
		// receiver.Observe. Только после этого можно закрыть вход аудита: иначе
		// одновременная остановка и классификация пакета приводили к отправке в
		// уже закрытый канал и аварийному завершению Центра.
		transportProducers.Wait()
		transportAudit.shutdown()
		background.Wait()
	}()
	for {
		// Накопившиеся обновления нельзя удалять: событие могло быть отправлено
		// точкой непосредственно перед остановкой центра и к следующей попытке
		// клиента уже выйти из разрешённого окна доставки.
		if err := api.DeleteWebhook(ctx, false); err == nil {
			break
		} else {
			server.warn(
				"Telegram временно недоступен",
				"Настройки и очереди сохранены. Программа повторит подключение автоматически.",
				err,
			)
		}
		if !waitForRetry(ctx, 5*time.Second) {
			return
		}
	}
	reception := &application.ServerService{
		Clients: server.Store, Rules: server.Store, Store: server.Store,
		Now: time.Now, DeliveryWindow: cfg.DeliveryTimeout(),
	}
	receiver := &telegram.ServerReceiver{
		API: api, ChatID: cfg.Transport.ChatID, MaxBytes: 4 * 1024 * 1024, Logger: server.Logger,
		Authorize: func(ctx context.Context, packet application.IncomingPacket) (bool, error) {
			return server.Store.AuthorizeTransport(
				ctx, packet.SenderBotID, packet.SenderBotUsername, packet.ChatID, packet.ThreadID,
			)
		},
		Authenticate: func(ctx context.Context, packet application.IncomingPacket) error {
			if packet.ProbeID == "" {
				return reception.AuthenticatePacket(ctx, packet)
			}
			client, err := server.Store.Client(ctx, packet.ProbeClientID)
			if errors.Is(err, application.ErrClientNotFound) {
				return &application.PermanentPacketError{Err: errors.New("точка контрольного пакета не зарегистрирована")}
			}
			if err != nil {
				return err
			}
			if !client.Active || client.SenderBotID != packet.SenderBotID ||
				!strings.EqualFold(strings.TrimPrefix(client.SenderBotUsername, "@"), strings.TrimPrefix(packet.SenderBotUsername, "@")) ||
				client.ChatID != packet.ChatID || client.ThreadID != packet.ThreadID ||
				!telegram.VerifyProbe(packet, client.Key) {
				return &application.PermanentPacketError{Err: errors.New("контрольный пакет не прошёл проверку подлинности")}
			}
			return nil
		},
	}
	receiver.Observe = func(item application.TransportObservation) {
		transportAudit.submit(item)
	}
	// Даже файлы, отброшенные до расшифровки (исчезнувшие, чужие или
	// повреждённые), очищаются через ту же долговечную очередь, что и принятые.
	receiver.Discard = func(discardContext context.Context, messageID int64) error {
		return server.enqueueTransportPacketDeletion(discardContext, receiver.ChatID, messageID)
	}
	server.update(func(status *ServerStatus) {
		status.State = "running"
		status.Title = "Центр работает"
		status.Detail = "Получаем данные точек и выполняем маршруты."
		status.TransportOK = true
	})
	// Скачивание и разбор разделены долговечной локальной границей. Приёмник
	// может быстро освободить суточную Telegram-очередь, даже если проверка,
	// подтверждение или конечные получатели временно задерживаются.
	transportContext, stopTransport := context.WithCancel(ctx)
	transportProducers.Add(2)
	go func() {
		defer transportProducers.Done()
		server.transportInboxLoop(transportContext, receiver, reception)
	}()
	go func() {
		defer transportProducers.Done()
		server.transportCleanupLoop(transportContext, receiver)
	}()
	server.transportIntakeLoop(transportContext, receiver)
	stopTransport()
}

func (server *Server) transportIntakeLoop(ctx context.Context, receiver *telegram.ServerReceiver) {
	offset := int64(0)
	for ctx.Err() == nil {
		raw, exists, err := server.Store.Value(ctx, serverOffsetName)
		if err != nil {
			server.warn("Не удалось прочитать очередь Центра",
				"Локальные данные сохранены; чтение будет повторено автоматически.", err)
			if !waitForRetry(ctx, 2*time.Second) {
				return
			}
			continue
		}
		if exists {
			parsed, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil {
				server.fail("Повреждено смещение Telegram", "Запуск остановлен для защиты от потери пакетов.", parseErr)
				return
			}
			offset = parsed
		}
		break
	}
	for ctx.Err() == nil {
		packets, next, err := receiver.Packets(ctx, offset)
		// Даже если следующий файл временно недоступен, уже скачанный
		// префикс должен стать долговечным до повторной попытки.
		if err == nil || next > offset || len(packets) > 0 {
			if persistErr := server.Store.PersistTransportBatch(ctx, serverOffsetName, next, packets); persistErr != nil {
				server.warn("Не удалось сохранить входящие пакеты",
					"Telegram-смещение не изменено. Центр повторит скачивание без потери данных.", persistErr)
				if !waitForRetry(ctx, 2*time.Second) {
					return
				}
				continue
			}
			offset = next
			if len(packets) > 0 {
				server.Logger.Info("пакеты сохранены в локальной входной очереди",
					"packets", len(packets), "next_transport_offset", next)
			}
		}
		if err != nil {
			if ctx.Err() == nil {
				server.warn("Telegram временно недоступен",
					"Связь прервалась. Программа повторит запрос автоматически; локальная очередь сохранена.", err)
			}
			if !waitForRetry(ctx, 5*time.Second) {
				return
			}
			continue
		}
		server.transportRecovered()
	}
}

func (server *Server) transportInboxLoop(
	ctx context.Context, receiver *telegram.ServerReceiver, reception *application.ServerService,
) {
	tracker := newAcknowledgementTracker(make(map[string]time.Time))
	var workers sync.WaitGroup
	workers.Add(transportInboxWorkerCount)
	for range transportInboxWorkerCount {
		go func() {
			defer workers.Done()
			server.transportInboxWorker(ctx, receiver, reception, tracker)
		}()
	}
	workers.Wait()
}

func (server *Server) transportInboxWorker(
	ctx context.Context,
	receiver *telegram.ServerReceiver,
	reception *application.ServerService,
	tracker *acknowledgementTracker,
) {
	for ctx.Err() == nil {
		items, err := server.Store.ClaimTransportPackets(
			ctx, 1, time.Now().UTC(), transportInboxLeaseDuration,
		)
		if err != nil {
			if ctx.Err() == nil {
				server.Logger.Error("не удалось арендовать пакет локальной входной очереди", "error", err)
			}
			if !waitForRetry(ctx, 500*time.Millisecond) {
				return
			}
			continue
		}
		if len(items) == 0 {
			if !waitForRetry(ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		for _, item := range items {
			if ctx.Err() != nil {
				server.releaseTransportLease(item.Packet.UpdateID, item.LeaseToken)
				return
			}
			itemContext, cancelItem := context.WithCancelCause(ctx)
			renewalDone := make(chan error, 1)
			go func() {
				renewalDone <- server.keepTransportLease(
					itemContext, cancelItem, item.Packet.UpdateID, item.LeaseToken,
				)
			}()
			server.processTransportInboxItemWithLease(
				itemContext, receiver, reception, item.Packet, item.Attempts, item.LeaseToken, tracker,
			)
			cancelItem(context.Canceled)
			renewalErr := <-renewalDone
			if renewalErr != nil && ctx.Err() == nil && !errors.Is(renewalErr, stateDB.ErrTransportLeaseLost) {
				server.Logger.Warn("не удалось продлить аренду входного пакета; обработка безопасно остановлена",
					"transport_update_id", item.Packet.UpdateID, "error", renewalErr)
			}
			// Complete/Retry/Quarantine уже сняли аренду атомарно. Повторный release
			// безопасен и нужен для отмены HTTP-запроса или ошибки heartbeat.
			server.releaseTransportLease(item.Packet.UpdateID, item.LeaseToken)
			if ctx.Err() != nil {
				return
			}
		}
	}
}

func (server *Server) keepTransportLease(
	ctx context.Context,
	cancelProcessing context.CancelCauseFunc,
	updateID int64,
	leaseToken string,
) error {
	ticker := time.NewTicker(transportInboxLeaseRenewal)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			renewContext, cancelRenewal := context.WithTimeout(ctx, transportInboxLeaseTimeout)
			_, err := server.Store.RenewTransportPacketLease(
				renewContext, updateID, leaseToken, time.Now().UTC(), transportInboxLeaseDuration,
			)
			cancelRenewal()
			if err != nil {
				// После утраты аренды обработчик не имеет права продолжать внешние
				// действия. Все завершающие SQL-операции дополнительно сверяют token.
				cancelProcessing(err)
				return err
			}
		}
	}
}

func (server *Server) processTransportInboxItem(
	ctx context.Context,
	receiver *telegram.ServerReceiver,
	reception *application.ServerService,
	packet application.IncomingPacket,
	attempts int,
	recentAcknowledgements map[string]time.Time,
) {
	server.processTransportInboxItemWithLease(
		ctx, receiver, reception, packet, attempts, "",
		newAcknowledgementTracker(recentAcknowledgements),
	)
}

func (server *Server) processTransportInboxItemWithLease(
	ctx context.Context,
	receiver *telegram.ServerReceiver,
	reception *application.ServerService,
	packet application.IncomingPacket,
	attempts int,
	leaseToken string,
	acknowledgements *acknowledgementTracker,
) {
	retry := func(reason string, err error) {
		if ctx.Err() != nil {
			return
		}
		next := time.Now().UTC().Add(transportInboxRetryDelay(attempts))
		var retryErr error
		if leaseToken == "" {
			retryErr = server.Store.MarkTransportRetry(ctx, packet.UpdateID, next, err.Error())
		} else {
			retryErr = server.Store.MarkTransportRetryLease(ctx, packet.UpdateID, next, err.Error(), leaseToken)
		}
		if retryErr != nil && ctx.Err() == nil && !errors.Is(retryErr, stateDB.ErrTransportLeaseLost) {
			server.Logger.Error("не удалось сохранить повтор входного пакета",
				"transport_update_id", packet.UpdateID, "error", retryErr)
		}
		server.Logger.Warn(reason, "transport_update_id", packet.UpdateID,
			"transport_message_id", packet.MessageID, "next_attempt_at", next, "error", err)
	}
	complete := func() bool {
		var err error
		if leaseToken == "" {
			err = server.Store.CompleteTransportPacket(ctx, packet.UpdateID, packet.ChatID, packet.MessageID)
		} else {
			err = server.Store.CompleteTransportPacketLease(
				ctx, packet.UpdateID, packet.ChatID, packet.MessageID, leaseToken,
			)
		}
		if err != nil {
			if errors.Is(err, stateDB.ErrTransportLeaseLost) {
				server.Logger.Warn("аренда входного пакета завершилась в другом обработчике",
					"transport_update_id", packet.UpdateID)
				return false
			}
			retry("не удалось завершить локальный входной пакет", err)
			return false
		}
		return true
	}

	if packet.ProbeID != "" {
		client, err := server.Store.Client(ctx, packet.ProbeClientID)
		if err != nil && !errors.Is(err, application.ErrClientNotFound) {
			retry("временная ошибка проверки контрольного пакета", err)
			return
		}
		valid := err == nil && client.Active &&
			client.SenderBotID == packet.SenderBotID &&
			strings.EqualFold(strings.TrimPrefix(client.SenderBotUsername, "@"), strings.TrimPrefix(packet.SenderBotUsername, "@")) &&
			client.ChatID == packet.ChatID && client.ThreadID == packet.ThreadID &&
			telegram.VerifyProbe(packet, client.Key) && recentTransportPacket(packet.SentAt, time.Now().UTC())
		if !valid {
			receiver.Observe(transportObservation(packet, "probe_packet", "probe_rejected",
				"Контрольный пакет не прошёл проверку подлинности или свежести", "", "", false, 200))
			server.Logger.Warn("отклонён недостоверный контрольный пакет",
				"sender_bot", packet.SenderBotUsername, "error", err)
			complete()
			return
		}
		shiftChange, err := server.Store.RecordClientShiftSnapshot(
			ctx, packet.ProbeClientID, packet.ProbeSentAt, packet.ProbeShift,
		)
		if err != nil {
			// История смен — дополнительная диагностическая функция. Её локальная
			// ошибка не должна задерживать подтверждение связи, копить контрольные
			// пакеты или останавливать обработку перехватов. Следующий probe снова
			// попробует сохранить актуальный снимок.
			server.Logger.Error("не удалось сохранить состояние смены рабочей точки; связь и обработка перехватов продолжаются",
				"client_id", packet.ProbeClientID, "probe_id", packet.ProbeID, "error", err)
		} else if shiftChange.Conflict {
			server.Logger.Error("состояние смены EAVC противоречит уже подтверждённой истории; прежнее состояние сохранено",
				"client_id", packet.ProbeClientID, "probe_id", packet.ProbeID)
		} else if shiftChange.Opened > 0 || shiftChange.Closed > 0 {
			server.Logger.Info("обновлена история смен рабочей точки",
				"client_id", packet.ProbeClientID, "opened", shiftChange.Opened, "closed", shiftChange.Closed,
				"probe_id", packet.ProbeID)
		}
		if err := server.Store.RecordClientSignalByTransport(
			ctx, packet.SenderBotID, packet.ChatID, packet.ThreadID, true, packet.SentAt,
		); err != nil {
			server.Logger.Warn("не удалось обновить состояние связи с точкой", "error", err)
		}
		if err := receiver.AcknowledgeProbe(ctx, packet.SenderBotUsername, packet.ProbeID, client.Key); err != nil {
			retry("не удалось подтвердить проверку связи", err)
			return
		}
		server.Logger.Info("двусторонняя связь с точкой проверена",
			"sender_bot", packet.SenderBotUsername, "thread_id", packet.ThreadID)
		receiver.Observe(transportObservation(packet, "probe_packet", "probe_valid",
			"Проверка связи выполнена; рабочие счётчики не изменены", "", packet.ProbeClientID, false, 200))
		complete()
		return
	}

	result, err := reception.Receive(ctx, packet)
	if err != nil {
		if !application.IsPermanentPacketError(err) {
			receiver.Observe(transportObservation(packet, "event_packet", "processing_retry",
				"Временная ошибка обработки; локальный пакет будет повторён", "", "", false, 30))
			retry("временная ошибка приёма локального пакета", err)
			return
		}
		receiver.Observe(transportObservation(packet, "event_packet", "quarantined",
			"Пакет окончательно отклонён и помещён в карантин", "", "", false, 200))
		server.Logger.Warn("пакет помещён в карантин", "error", err,
			"sender_bot_id", packet.SenderBotID, "content_kind", "unverified")
		var quarantineErr error
		if leaseToken == "" {
			quarantineErr = server.Store.QuarantineTransportPacket(ctx, packet, err.Error())
		} else {
			quarantineErr = server.Store.QuarantineTransportPacketLease(ctx, packet, err.Error(), leaseToken)
		}
		if quarantineErr != nil {
			retry("не удалось сохранить пакет в карантин", quarantineErr)
		}
		return
	}

	outcome := "processed_duplicate"
	reason := "Повтор уже принятого события; рабочие счётчики не изменены"
	if result.Status == "expired" {
		outcome = "event_expired"
		reason = "Пакет скачан после допустимого срока; в рабочие события не добавлен"
	} else if result.Accepted {
		outcome = "event_accepted"
		reason = "Создано новое принятое событие"
	}
	receiver.Observe(transportObservation(packet, "event_packet", outcome, reason,
		result.EventID, result.ClientID, result.Accepted, 200))
	if result.Accepted && recentTransportPacket(packet.SentAt, time.Now().UTC()) {
		if err := server.Store.RecordClientSignalByTransport(
			ctx, packet.SenderBotID, packet.ChatID, packet.ThreadID, false, packet.SentAt,
		); err != nil {
			server.Logger.Warn("не удалось обновить состояние связи с точкой", "error", err)
		}
	}
	server.Logger.Info("локальный транспортный пакет обработан",
		"event_id", result.EventID, "trace_id", result.EventID,
		"transport_message_id", packet.MessageID, "transport_update_id", packet.UpdateID,
		"downloaded_at", packet.ReceivedAt, "status", result.Status,
		"new_event", result.Accepted, "sender_bot", result.ClientBotUsername,
		"content_kind", result.ContentKind, "content_chars", result.ContentChars,
		"content_blocks", result.ContentBlocks, "source_mode", result.SourceMode,
	)
	acknowledgedAt := time.Now().UTC()
	ackKey := acknowledgementKey(result)
	if acknowledgements.reserve(ackKey, acknowledgedAt) {
		if err := receiver.Acknowledge(
			ctx, result.ClientBotUsername, result.EventID, result.Status, result.AcknowledgementKey,
			packet.MessageID, result.AckTraceV4,
		); err != nil {
			acknowledgements.release(ackKey)
			retry("не удалось подтвердить событие рабочей точке", err)
			return
		}
		acknowledgements.complete(ackKey, acknowledgedAt)
	}
	if !complete() {
		return
	}
	now := time.Now().UTC()
	server.update(func(status *ServerStatus) { status.LastReceived = &now })
}

func transportInboxRetryDelay(attempts int) time.Duration {
	if attempts < 0 {
		attempts = 0
	}
	if attempts > 7 {
		attempts = 7
	}
	delay := 5 * time.Second * time.Duration(1<<attempts)
	if delay > 10*time.Minute {
		return 10 * time.Minute
	}
	return delay
}

func transportObservation(
	packet application.IncomingPacket, kind, outcome, reason, eventID, clientID string, counted bool, rank int,
) application.TransportObservation {
	return application.TransportObservation{
		UpdateID: packet.UpdateID, MessageID: packet.MessageID, ChatID: packet.ChatID,
		ThreadID: packet.ThreadID, SenderBotID: packet.SenderBotID,
		SenderBotUsername: packet.SenderBotUsername, MessageKind: kind, Outcome: outcome,
		Reason: reason, EventID: eventID, ClientID: clientID, Counted: counted, Rank: rank,
		SentAt: packet.SentAt, ObservedAt: time.Now().UTC(),
	}
}

const duplicateAcknowledgementInterval = 90 * time.Second

type acknowledgementTracker struct {
	mu       sync.Mutex
	recent   map[string]time.Time
	inFlight map[string]struct{}
}

func newAcknowledgementTracker(recent map[string]time.Time) *acknowledgementTracker {
	if recent == nil {
		recent = make(map[string]time.Time)
	}
	return &acknowledgementTracker{recent: recent, inFlight: make(map[string]struct{})}
}

// reserve не позволяет двум параллельным пакетам одного события одновременно
// отправить одинаковый ACK. При ошибке резерв освобождается для следующей
// долговечной попытки; после успеха действует прежнее защитное окно.
func (tracker *acknowledgementTracker) reserve(key string, now time.Time) bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if _, exists := tracker.inFlight[key]; exists || !acknowledgementDue(tracker.recent, key, now) {
		return false
	}
	tracker.inFlight[key] = struct{}{}
	return true
}

func (tracker *acknowledgementTracker) release(key string) {
	tracker.mu.Lock()
	delete(tracker.inFlight, key)
	tracker.mu.Unlock()
}

func (tracker *acknowledgementTracker) complete(key string, at time.Time) {
	tracker.mu.Lock()
	tracker.recent[key] = at
	delete(tracker.inFlight, key)
	tracker.mu.Unlock()
}

// Смена исхода «просрочено» → «принято» обязана дать отдельный ACK даже внутри
// защитного окна: иначе Клиент останется в ложном терминальном состоянии.
func acknowledgementKey(result application.PacketResult) string {
	schema := "v3"
	if result.AckTraceV4 {
		schema = "v4"
	}
	return result.EventID + "|" + schema + "|" + result.Status
}

// acknowledgementDue подавляет плотную серию одинаковых ответов на дубли,
// но разрешает повтор через интервал — например, если первый ответ потерялся.
func acknowledgementDue(recent map[string]time.Time, eventID string, now time.Time) bool {
	if previous, exists := recent[eventID]; exists && now.Sub(previous) < duplicateAcknowledgementInterval {
		return false
	}
	if len(recent) > 4096 {
		cutoff := now.Add(-duplicateAcknowledgementInterval)
		for id, at := range recent {
			if at.Before(cutoff) {
				delete(recent, id)
			}
		}
	}
	return true
}

func recentTransportPacket(sentAt, now time.Time) bool {
	if sentAt.IsZero() {
		return false
	}
	delta := now.Sub(sentAt)
	return delta >= -time.Minute && delta <= 5*time.Minute
}

func (server *Server) transportRecovered() {
	server.update(func(status *ServerStatus) {
		status.TransportOK = true
		if status.State != "waiting" || (status.Title != "Telegram временно недоступен" &&
			status.Title != "Не удалось сохранить входящие пакеты" &&
			status.Title != "Не удалось прочитать очередь Центра") {
			return
		}
		status.State = "running"
		status.Title = "Центр работает"
		status.Detail = "Связь с Telegram восстановлена. Получаем данные точек и выполняем маршруты."
	})
}

func (server *Server) enqueueTransportPacketDeletion(ctx context.Context, chatID, messageID int64) error {
	if chatID == 0 || messageID <= 0 {
		return errors.New("не заданы чат или транспортное сообщение для очистки")
	}
	return server.Store.EnqueueTransportDeletion(ctx, chatID, messageID)
}

func (server *Server) releaseTransportLease(updateID int64, leaseToken string) {
	if updateID < 0 || strings.TrimSpace(leaseToken) == "" {
		return
	}
	releaseContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := server.Store.ReleaseTransportPacketLease(releaseContext, updateID, leaseToken); err != nil &&
		!errors.Is(err, stateDB.ErrTransportLeaseLost) {
		server.Logger.Warn("не удалось освободить аренду входного пакета при остановке",
			"transport_update_id", updateID, "error", err)
	}
}

func (server *Server) transportCleanupLoop(ctx context.Context, receiver *telegram.ServerReceiver) {
	nextPruneAt := time.Time{}
	for ctx.Err() == nil {
		now := time.Now().UTC()
		if !now.Before(nextPruneAt) {
			if dropped, err := server.Store.PruneTransportDeletions(ctx, now); err != nil {
				if ctx.Err() == nil {
					server.Logger.Warn("не удалось ограничить очередь удаления транспортных файлов", "error", err)
				}
			} else if dropped > 0 {
				server.Logger.Warn("устаревшие запросы на удаление транспортных файлов удалены из технической очереди",
					"count", dropped)
			}
			nextPruneAt = now.Add(time.Minute)
		}
		processed := server.retryTransportDeletions(ctx, receiver)
		if processed == 0 && !waitForRetry(ctx, 250*time.Millisecond) {
			return
		}
	}
}

func (server *Server) retryTransportDeletions(ctx context.Context, receiver *telegram.ServerReceiver) int {
	messageIDs, err := server.Store.DueTransportDeletions(ctx, receiver.ChatID, time.Now().UTC(), 25)
	if err != nil {
		if ctx.Err() == nil {
			server.Logger.Warn("не удалось прочитать очередь удаления транспортных пакетов", "error", err)
		}
		return 0
	}
	for _, messageID := range messageIDs {
		if ctx.Err() != nil {
			return len(messageIDs)
		}
		if err := receiver.DeletePacket(ctx, messageID); err != nil {
			if ctx.Err() != nil {
				return len(messageIDs)
			}
			dropped, queueErr := server.Store.QueueTransportDeletion(
				ctx, receiver.ChatID, messageID, time.Now().UTC(),
			)
			if queueErr != nil {
				server.Logger.Warn("не удалось обновить повтор удаления транспортного пакета",
					"telegram_message_id", messageID, "error", queueErr)
			} else if dropped {
				server.Logger.Error("исчерпан лимит удаления транспортного пакета",
					"telegram_message_id", messageID, "attempts", 120)
			}
			continue
		}
		if err := server.Store.CompleteTransportDeletion(ctx, receiver.ChatID, messageID); err != nil && ctx.Err() == nil {
			server.Logger.Warn("не удалось завершить очередь удаления транспортного пакета",
				"telegram_message_id", messageID, "error", err)
		}
	}
	return len(messageIDs)
}

func (server *Server) deliveryLoop(ctx context.Context, service *application.DeliveryService) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		count, err := service.RunOnce(ctx, 100)
		if err != nil && ctx.Err() == nil {
			server.warnDelivery(
				"Не удалось доставить результат",
				"Результат сохранён в очереди. Программа повторит отправку автоматически.",
				err,
			)
		}
		if count > 0 {
			now := time.Now().UTC()
			server.update(func(status *ServerStatus) { status.LastDelivery = &now })
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (server *Server) gateways(cfg config.Server) map[string]application.DeliveryGateway {
	return server.gatewaysWithTransportAPI(cfg, nil)
}

// gatewaysWithTransportAPI переиспользует клиент транспортного бота для
// направлений с тем же token. Так ACK, очистка и конечная доставка разделяют
// один sendLimiter и одну паузу RetryAfter Telegram.
func (server *Server) gatewaysWithTransportAPI(
	cfg config.Server, transportAPI *telegram.Client,
) map[string]application.DeliveryGateway {
	result := make(map[string]application.DeliveryGateway)
	telegramAPIs := make(map[string]*telegram.Client)
	if transportAPI != nil && strings.TrimSpace(cfg.Transport.BotToken) != "" {
		telegramAPIs[cfg.Transport.BotToken] = transportAPI
	}
	for id, destination := range cfg.Destinations {
		switch destination.Kind {
		case domain.DestinationTelegram:
			api := telegramAPIs[destination.BotToken]
			var err error
			if api == nil {
				api, err = telegram.New(destination.BotToken, nil)
				if err == nil {
					telegramAPIs[destination.BotToken] = api
				}
			}
			if err == nil {
				result[id] = &telegram.Destination{
					API: api, ChatID: destination.ChatID, Network: destination.Network,
					Protection:       destination.Protection,
					MessageTemplate:  destination.MessageTemplate,
					StatePath:        filepath.Join(server.BaseDir, "data", "telegram-"+safeID(id)+".json"),
					StateScope:       telegramStateScope(destination.BotID, destination.BotToken, destination.ChatID),
					LegacyStateScope: telegramStateScope(0, destination.BotToken, destination.ChatID),
					Logger:           server.Logger,
				}
			}
		case domain.DestinationWord:
			result[id] = &docx.Destination{
				TemplatePath: destination.Template, Folder: destination.Folder,
				StatePath:  filepath.Join(server.BaseDir, "data", "word-"+safeID(id)+".json"),
				StateScope: wordStateScope(destination.Folder),
				Network:    destination.Network, Protection: destination.Protection, Prefix: destination.Prefix,
				Username: destination.Username, Password: destination.Password,
			}
		}
	}
	return result
}

func telegramStateScope(botID int64, botToken string, chatID int64) string {
	identity := strings.TrimSpace(botToken)
	if botID > 0 {
		identity = "bot:" + strconv.FormatInt(botID, 10)
	}
	digest := sha256.Sum256([]byte(identity + "\x00" + strconv.FormatInt(chatID, 10)))
	return hex.EncodeToString(digest[:16])
}

func wordStateScope(folder string) string {
	normalized := strings.ToLower(filepath.Clean(strings.TrimSpace(folder)))
	digest := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(digest[:16])
}

func (server *Server) statusLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		if err := server.Store.EvaluateClientHealth(ctx, time.Now().UTC(), 5*time.Minute); err != nil && ctx.Err() == nil {
			server.Logger.Warn("не удалось оценить состояние связи с точками", "error", err)
		}
		stats, err := server.Store.Stats(ctx)
		if err == nil {
			server.update(func(status *ServerStatus) { status.Stats = stats })
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (server *Server) serverMaintenanceLoop(ctx context.Context) {
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()
	for {
		now := time.Now().UTC()
		result, err := server.Store.PurgeOld(
			ctx,
			now.Add(-serverTerminalRetention),
			now.Add(-serverRejectedRetention),
			serverRuleHistoryLimit,
			serverRejectedLimit,
		)
		if err != nil && ctx.Err() == nil {
			server.Logger.Warn("не удалось очистить технические данные сервера", "error", err)
		}
		transportRemoved, transportErr := server.Store.PurgeTransportAudit(ctx, now.Add(-serverRejectedRetention))
		if transportErr != nil && ctx.Err() == nil {
			server.Logger.Warn("не удалось ограничить журнал транспорта; рабочая обработка продолжается", "error", transportErr)
		} else {
			result.Transport = transportRemoved
		}
		if err == nil && result.Events+result.Rejected+result.Rules+result.Incidents+result.Transport > 0 {
			if err := server.Store.Compact(ctx); err != nil && ctx.Err() == nil {
				server.Logger.Warn("не удалось уплотнить базу сервера", "error", err)
			} else {
				server.Logger.Info("очищены технические данные сервера",
					"events", result.Events, "rejected", result.Rejected,
					"rules", result.Rules, "incidents", result.Incidents,
					"transport", result.Transport)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (server *Server) saveConfig(cfg config.Server) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(server.configPath()), 0o700); err != nil {
		return err
	}
	payload, _ := json.Marshal(cfg)
	return secrets.Save(server.configPath(), payload)
}

func (server *Server) configPath() string {
	return filepath.Join(server.BaseDir, "data", "server-config.dat")
}

func (server *Server) clientConfigPath(clientID string) string {
	return filepath.Join(server.BaseDir, "data", "clients", safeID(clientID)+".dat")
}

func (server *Server) clientSigningKey() (ed25519.PrivateKey, error) {
	server.mu.Lock()
	defer server.mu.Unlock()
	if len(server.signingKey) == ed25519.PrivateKeySize {
		return append(ed25519.PrivateKey(nil), server.signingKey...), nil
	}
	key, err := secrets.LoadOrCreateSigningKey(filepath.Join(server.BaseDir, "data", "server-signing-key.dat"))
	if err != nil {
		return nil, err
	}
	server.signingKey = append(ed25519.PrivateKey(nil), key...)
	return append(ed25519.PrivateKey(nil), key...), nil
}

func (server *Server) fail(title, detail string, err error) {
	server.Logger.Error(title, "error", err)
	server.update(func(status *ServerStatus) {
		status.State = "error"
		status.Title = title
		status.Detail = detail
		status.TransportOK = false
	})
}

func (server *Server) warn(title, detail string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	server.Logger.Warn(title, "error", err)
	server.update(func(status *ServerStatus) {
		status.State = "waiting"
		status.Title = title
		status.Detail = detail
		status.TransportOK = false
	})
}

// warnDelivery показывает сбой конкретного получателя, не объявляя при этом
// входящий Telegram-транспорт недоступным. Его состояние проверяется и
// восстанавливается независимым циклом transportLoop.
func (server *Server) warnDelivery(title, detail string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	server.Logger.Warn(title, "error", err)
	server.update(func(status *ServerStatus) {
		status.State = "waiting"
		status.Title = title
		status.Detail = detail
	})
}

func (server *Server) update(change func(*ServerStatus)) {
	server.mu.Lock()
	defer server.mu.Unlock()
	change(&server.status)
}

func destinationIDs(destinations map[string]config.DestinationConfig) []string {
	result := make([]string, 0, len(destinations))
	for id := range destinations {
		result = append(result, id)
	}
	return result
}

func safeID(value string) string {
	return strings.Map(func(char rune) rune {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '-' || char == '_' {
			return char
		}
		return '-'
	}, value)
}

func cloneServerConfig(source config.Server) config.Server {
	result := source
	result.Destinations = cloneDestinations(source.Destinations)
	return result
}

func cloneDestinations(source map[string]config.DestinationConfig) map[string]config.DestinationConfig {
	if source == nil {
		return nil
	}
	result := make(map[string]config.DestinationConfig, len(source))
	for id, destination := range source {
		result[id] = destination
	}
	return result
}
