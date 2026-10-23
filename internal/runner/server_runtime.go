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
	"intercept/internal/infrastructure/telegram"
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
	receiver := &telegram.ServerReceiver{
		API: api, ChatID: cfg.Transport.ChatID, MaxBytes: 4 * 1024 * 1024,
		Authorize: func(ctx context.Context, packet application.IncomingPacket) (bool, error) {
			return server.Store.AuthorizeTransport(
				ctx, packet.SenderBotID, packet.SenderBotUsername, packet.ChatID, packet.ThreadID,
			)
		},
	}
	reception := &application.ServerService{
		Clients: server.Store, Rules: server.Store, Store: server.Store,
		DeliveryWindow: cfg.DeliveryTimeout(),
	}
	delivery := &application.DeliveryService{
		Queue: server.Store, Gateways: server.gateways(cfg), Now: time.Now,
		MaxConcurrent: 4, AttemptTimeout: 2 * time.Minute, Logger: server.Logger,
	}
	server.update(func(status *ServerStatus) {
		status.State = "running"
		status.Title = "Центр работает"
		status.Detail = "Получаем данные точек и выполняем маршруты."
		status.TransportOK = true
	})
	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		server.deliveryLoop(ctx, delivery)
	}()
	// Перенастройка ждёт остановки старого цикла. Одновременные циклы могли бы
	// выбрать одно задание до фиксации результата и отправить его дважды.
	defer background.Wait()
	offset := int64(0)
	recentAcknowledgements := make(map[string]time.Time)
	if raw, exists, _ := server.Store.Value(ctx, serverOffsetName); exists {
		offset, _ = strconv.ParseInt(raw, 10, 64)
	}
	for ctx.Err() == nil {
		packets, next, err := receiver.Packets(ctx, offset)
		if err != nil {
			if ctx.Err() == nil {
				server.warn(
					"Telegram временно недоступен",
					"Связь прервалась. Программа повторит запрос автоматически; настройки и очереди сохранены.",
					err,
				)
				time.Sleep(5 * time.Second)
			}
			continue
		}
		server.transportRecovered()
		for _, packet := range packets {
			if packet.ProbeID != "" {
				client, clientErr := server.Store.Client(ctx, packet.ProbeClientID)
				valid := clientErr == nil && client.Active &&
					client.SenderBotID == packet.SenderBotID &&
					strings.EqualFold(strings.TrimPrefix(client.SenderBotUsername, "@"), strings.TrimPrefix(packet.SenderBotUsername, "@")) &&
					client.ChatID == packet.ChatID && client.ThreadID == packet.ThreadID &&
					telegram.VerifyProbe(packet, client.Key) && recentTransportPacket(packet.SentAt, time.Now().UTC())
				if !valid {
					server.Logger.Warn("отклонён недостоверный контрольный пакет",
						"sender_bot", packet.SenderBotUsername, "error", clientErr)
					server.deleteTransportPacket(ctx, receiver, packet.MessageID)
					continue
				}
				if err := server.Store.RecordClientSignalByTransport(
					ctx, packet.SenderBotID, packet.ChatID, packet.ThreadID, true, packet.SentAt,
				); err != nil {
					server.Logger.Warn("не удалось обновить состояние связи с точкой", "error", err)
				}
				if err := receiver.AcknowledgeProbe(ctx, packet.SenderBotUsername, packet.ProbeID, client.Key); err != nil {
					server.Logger.Warn("не удалось подтвердить проверку связи",
						"sender_bot", packet.SenderBotUsername, "error", err)
				} else {
					server.Logger.Info("двусторонняя связь с точкой проверена",
						"sender_bot", packet.SenderBotUsername, "thread_id", packet.ThreadID)
				}
				server.deleteTransportPacket(ctx, receiver, packet.MessageID)
				continue
			}
			result, err := reception.Receive(ctx, packet)
			if err != nil {
				server.Logger.Warn("пакет помещён в карантин", "error", err, "sender_bot_id", packet.SenderBotID)
				if quarantineErr := server.Store.Quarantine(ctx, packet, err.Error()); quarantineErr != nil {
					server.Logger.Error("не удалось сохранить пакет в карантин", "error", quarantineErr)
					continue
				}
				server.deleteTransportPacket(ctx, receiver, packet.MessageID)
				continue
			}
			if result.Accepted && recentTransportPacket(packet.SentAt, time.Now().UTC()) {
				if err := server.Store.RecordClientSignalByTransport(
					ctx, packet.SenderBotID, packet.ChatID, packet.ThreadID, false, packet.SentAt,
				); err != nil {
					server.Logger.Warn("не удалось обновить состояние связи с точкой", "error", err)
				}
			}
			server.Logger.Info("транспортный пакет обработан",
				"event_id", result.EventID, "status", result.Status,
				"new_event", result.Accepted, "sender_bot", result.ClientBotUsername,
			)
			acknowledgedAt := time.Now().UTC()
			if acknowledgementDue(recentAcknowledgements, result.EventID, acknowledgedAt) {
				if err := receiver.Acknowledge(
					ctx, result.ClientBotUsername, result.EventID, result.Status, result.AcknowledgementKey,
				); err != nil {
					server.Logger.Warn("не удалось подтвердить событие", "event_id", result.EventID, "error", err)
				} else {
					recentAcknowledgements[result.EventID] = acknowledgedAt
				}
			}
			server.deleteTransportPacket(ctx, receiver, packet.MessageID)
			now := time.Now().UTC()
			server.update(func(status *ServerStatus) { status.LastReceived = &now })
		}
		if next > offset {
			offset = next
			_ = server.Store.SaveValue(ctx, serverOffsetName, strconv.FormatInt(offset, 10))
		}
		server.retryTransportDeletions(ctx, receiver)
	}
}

const duplicateAcknowledgementInterval = 90 * time.Second

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
		if status.State != "waiting" || status.Title != "Telegram временно недоступен" {
			return
		}
		status.State = "running"
		status.Title = "Центр работает"
		status.Detail = "Связь с Telegram восстановлена. Получаем данные точек и выполняем маршруты."
	})
}

func (server *Server) deleteTransportPacket(ctx context.Context, receiver *telegram.ServerReceiver, messageID int64) {
	if messageID <= 0 {
		return
	}
	if err := receiver.DeletePacket(ctx, messageID); err != nil && ctx.Err() == nil {
		server.Logger.Warn("не удалось удалить обработанный пакет из транспортной группы",
			"telegram_message_id", messageID, "error", err)
		dropped, queueErr := server.Store.QueueTransportDeletion(ctx, messageID, time.Now().UTC())
		if queueErr != nil {
			server.Logger.Warn("не удалось сохранить повтор удаления транспортного пакета",
				"telegram_message_id", messageID, "error", queueErr)
		} else if dropped {
			server.Logger.Error("исчерпан лимит удаления транспортного пакета",
				"telegram_message_id", messageID, "attempts", 120)
		}
		return
	}
	_ = server.Store.CompleteTransportDeletion(ctx, messageID)
}

func (server *Server) retryTransportDeletions(ctx context.Context, receiver *telegram.ServerReceiver) {
	messageIDs, err := server.Store.DueTransportDeletions(ctx, time.Now().UTC(), 25)
	if err != nil {
		server.Logger.Warn("не удалось прочитать очередь удаления транспортных пакетов", "error", err)
		return
	}
	for _, messageID := range messageIDs {
		if err := receiver.DeletePacket(ctx, messageID); err != nil {
			dropped, queueErr := server.Store.QueueTransportDeletion(ctx, messageID, time.Now().UTC())
			if queueErr != nil {
				server.Logger.Warn("не удалось обновить повтор удаления транспортного пакета",
					"telegram_message_id", messageID, "error", queueErr)
			} else if dropped {
				server.Logger.Error("исчерпан лимит удаления транспортного пакета",
					"telegram_message_id", messageID, "attempts", 120)
			}
			continue
		}
		_ = server.Store.CompleteTransportDeletion(ctx, messageID)
	}
}

func (server *Server) deliveryLoop(ctx context.Context, service *application.DeliveryService) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		count, err := service.RunOnce(ctx, 100)
		if err != nil && ctx.Err() == nil {
			server.warn(
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
	result := make(map[string]application.DeliveryGateway)
	telegramAPIs := make(map[string]*telegram.Client)
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
		} else if result.Events+result.Rejected+result.Rules+result.Incidents > 0 {
			if err := server.Store.Compact(ctx); err != nil && ctx.Err() == nil {
				server.Logger.Warn("не удалось уплотнить базу сервера", "error", err)
			} else {
				server.Logger.Info("очищены технические данные сервера",
					"events", result.Events, "rejected", result.Rejected,
					"rules", result.Rules, "incidents", result.Incidents)
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
