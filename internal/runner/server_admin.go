package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/docx"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/infrastructure/share"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
	"intercept/internal/provisioning"
)

var destinationIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

// ConfigureTransport проверяет серверного бота и сохраняет транспортную группу.
func (server *Server) ConfigureTransport(ctx context.Context, setup TransportSetup) error {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	api, err := telegram.New(setup.BotToken, nil)
	if err != nil {
		return err
	}
	me, err := api.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("проверка серверного бота: %w", err)
	}
	if err := api.DeleteWebhook(ctx, false); err != nil {
		return fmt.Errorf("отключить webhook серверного бота: %w", err)
	}
	if err := api.TestTopic(ctx, setup.ChatID, 0); err != nil {
		return fmt.Errorf("проверка транспортной группы: %w", err)
	}
	server.mu.RLock()
	currentTransport := server.config.Transport
	wasConfigured := server.status.Configured
	destinations := cloneDestinations(server.config.Destinations)
	server.mu.RUnlock()
	if wasConfigured && (currentTransport.BotID != me.ID || currentTransport.ChatID != setup.ChatID) {
		clients, clientsErr := server.Store.Clients(ctx)
		if clientsErr != nil {
			return clientsErr
		}
		if len(clients) > 0 {
			return errors.New("нельзя заменить бота или транспортную группу, пока зарегистрированы рабочие точки: их настройки перестанут работать")
		}
	}
	if destinations == nil {
		destinations = make(map[string]config.DestinationConfig)
	}
	cfg := config.Server{
		Schema: config.ServerSchemaV1, UpdatedAt: time.Now().UTC(), Listen: "127.0.0.1:17632",
		DeliveryTimeoutMinutes: 120,
		Transport: config.ServerTransport{
			BotToken: setup.BotToken, BotID: me.ID, BotUsername: me.Username,
			ChatID: setup.ChatID, InviteLink: strings.TrimSpace(setup.InviteLink),
		},
		Destinations: destinations,
	}
	if err := server.saveConfig(cfg); err != nil {
		return err
	}
	return server.restartWorker(server.applicationContext())
}

// AddDestination проверяет получателя до публикации новой конфигурации.
func (server *Server) AddDestination(ctx context.Context, setup DestinationSetup) error {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	previousConfig := cloneServerConfig(cfg)
	if !server.Status().Configured {
		return errors.New("сначала настройте транспортную группу")
	}
	id := strings.TrimSpace(setup.ID)
	if !destinationIDPattern.MatchString(id) {
		return errors.New("ID направления должен содержать только латинские буквы, цифры, дефис или подчёркивание и быть не длиннее 64 символов")
	}
	name := strings.TrimSpace(setup.Name)
	if name == "" {
		return errors.New("не задано название направления")
	}
	destination := config.DestinationConfig{
		Kind: setup.Kind, BotToken: setup.BotToken, ChatID: setup.ChatID,
		MessageTemplate: strings.TrimSpace(setup.MessageTemplate),
		Template:        strings.TrimSpace(setup.Template), Folder: strings.TrimSpace(setup.Folder),
		Network: strings.TrimSpace(setup.Network), Protection: strings.TrimSpace(setup.Protection), Prefix: strings.TrimSpace(setup.Prefix),
		Username: setup.Username, Password: setup.Password,
	}
	if destination.Network == "" {
		return errors.New("не задано обозначение радиосети")
	}
	if destination.Protection == "" {
		return errors.New("не задано обозначение защиты")
	}
	existingDestination, destinationExists := cfg.Destinations[id]
	previousDeliveries, err := server.Store.DestinationDeliveryCount(ctx, id)
	if err != nil {
		return err
	}
	if !destinationExists && previousDeliveries > 0 {
		return errors.New("этот ID получателя уже использовался; создайте новый ID, чтобы сохранить связь с историей")
	}
	if destinationExists && existingDestination.Kind == destination.Kind {
		if destination.Kind == domain.DestinationTelegram && strings.TrimSpace(destination.BotToken) == "" {
			destination.BotToken = existingDestination.BotToken
		}
		if destination.Kind == domain.DestinationWord {
			if destination.Username == "" {
				destination.Username = existingDestination.Username
			}
			if destination.Password == "" {
				destination.Password = existingDestination.Password
			}
		}
	}
	switch setup.Kind {
	case domain.DestinationTelegram:
		if destination.MessageTemplate == "" {
			destination.MessageTemplate = telegram.DefaultMessageTemplate
		}
		destination.MessageTemplate = telegram.MigrateMessageTemplate(destination.MessageTemplate)
		if err := telegram.ValidateMessageTemplate(destination.MessageTemplate); err != nil {
			return err
		}
		api, err := telegram.New(destination.BotToken, nil)
		if err != nil {
			return err
		}
		me, err := api.GetMe(ctx)
		if err != nil {
			return fmt.Errorf("проверка бота направления: %w", err)
		}
		destination.BotID = me.ID
		if err := api.TestTopic(ctx, destination.ChatID, 0); err != nil {
			return fmt.Errorf("проверка Telegram-направления: %w", err)
		}
	case domain.DestinationWord:
		if err := docx.ValidateTemplate(destination.Template); err != nil {
			return fmt.Errorf("проверка шаблона Word: %w", err)
		}
		if _, err := os.Stat(destination.Folder); err != nil {
			if err := share.Connect(ctx, destination.Folder, destination.Username, destination.Password); err != nil {
				return err
			}
		}
		if err := os.MkdirAll(destination.Folder, 0o755); err != nil {
			return fmt.Errorf("проверка папки Word: %w", err)
		}
	default:
		return errors.New("неподдерживаемый тип направления")
	}
	if destinationExists && destinationTargetChanged(existingDestination, destination) {
		pending, err := server.Store.PendingDestinationCount(ctx, id)
		if err != nil {
			return err
		}
		if pending > 0 {
			return fmt.Errorf("нельзя изменить адрес получателя: сначала дождитесь завершения %d заданий в его очереди", pending)
		}
		if previousDeliveries > 0 {
			return errors.New("нельзя изменить тип или адрес получателя с историей; создайте новый получатель")
		}
	}
	if cfg.Destinations == nil {
		cfg.Destinations = make(map[string]config.DestinationConfig)
	}
	cfg.Destinations[id] = destination
	cfg.UpdatedAt = time.Now().UTC()
	if err := server.saveConfig(cfg); err != nil {
		return err
	}
	rules, err := server.Store.ActiveRules(ctx)
	if err != nil {
		return errors.Join(err, server.saveConfig(previousConfig))
	}
	if rules.Destinations == nil {
		rules.Destinations = make(map[string]domain.Destination)
	}
	if rules.Unknown == nil {
		rules.Unknown = make(map[string]domain.RouteDecision)
	}
	existingRule, ruleExists := rules.Destinations[id]
	rules.Destinations[id] = domain.Destination{ID: id, Name: name, Kind: setup.Kind, Enabled: true}
	if _, exists := rules.Unknown[id]; !exists {
		rules.Unknown[id] = domain.RouteDecision{Action: domain.RouteSkip}
	}
	if setup.Kind == domain.DestinationTelegram {
		hasGeneral := false
		for destinationID, decision := range rules.Unknown {
			destination := rules.Destinations[destinationID]
			if destination.Enabled && destination.Kind == domain.DestinationTelegram && decision.Action != domain.RouteSkip {
				hasGeneral = true
				break
			}
		}
		if !hasGeneral {
			rules.Unknown[id] = domain.RouteDecision{Action: domain.RouteReview, ThreadID: 0}
		}
	}
	if ruleExists && existingRule.Kind != setup.Kind && setup.Kind != domain.DestinationTelegram {
		clearDestinationThreads(rules, id)
	}
	rules.Version++
	if err := server.Store.PublishRules(ctx, rules, "Настройка направления"); err != nil {
		return errors.Join(err, server.saveConfig(previousConfig))
	}
	return server.restartWorker(server.applicationContext())
}

// DeleteDestination удаляет получателя из конфигурации и всех маршрутов.
func (server *Server) DeleteDestination(ctx context.Context, id string) error {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("не задан ID направления")
	}
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	previousConfig := cloneServerConfig(cfg)
	if _, exists := cfg.Destinations[id]; !exists {
		return errors.New("направление не найдено")
	}
	pending, err := server.Store.PendingDestinationCount(ctx, id)
	if err != nil {
		return err
	}
	if pending > 0 {
		return fmt.Errorf("нельзя удалить получателя: сначала дождитесь завершения %d заданий в его очереди", pending)
	}
	rules, err := server.Store.ActiveRules(ctx)
	if err != nil {
		return err
	}
	delete(cfg.Destinations, id)
	delete(rules.Destinations, id)
	delete(rules.Unknown, id)
	for _, decisions := range rules.Positions {
		delete(decisions, id)
	}
	rules.Version++
	if err := rules.Validate(); err != nil {
		return err
	}
	cfg.UpdatedAt = time.Now().UTC()
	if err := server.saveConfig(cfg); err != nil {
		return err
	}
	if err := server.Store.PublishRules(ctx, rules, "Удаление направления"); err != nil {
		return errors.Join(err, server.saveConfig(previousConfig))
	}
	return server.restartWorker(server.applicationContext())
}

func clearDestinationThreads(rules domain.RuleSet, id string) {
	clear := func(decisions map[string]domain.RouteDecision) {
		if decision, exists := decisions[id]; exists {
			decision.ThreadID = 0
			decisions[id] = decision
		}
	}
	clear(rules.Unknown)
	for _, decisions := range rules.Positions {
		clear(decisions)
	}
}

// AddClient регистрирует точку и возвращает полностью готовый подписанный JSON.
func (server *Server) AddClient(ctx context.Context, request config.ClientRequest) ([]byte, error) {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()

	server.mu.RLock()
	cfg := server.config
	configured := server.status.Configured
	server.mu.RUnlock()
	if !configured {
		return nil, errors.New("сначала настройте транспортную группу")
	}
	api, err := telegram.New(request.BotToken, nil)
	if err != nil {
		return nil, err
	}
	me, err := api.GetMe(ctx)
	if err != nil {
		return nil, fmt.Errorf("проверка бота точки: %w", err)
	}
	if request.BotUsername != "" && !strings.EqualFold(strings.TrimPrefix(request.BotUsername, "@"), me.Username) {
		return nil, errors.New("токен принадлежит другому боту")
	}
	if err := validateSeparateTransportBot(cfg.Transport.BotID, me.ID); err != nil {
		return nil, err
	}
	request.BotUsername = me.Username
	request.ServerBotID = cfg.Transport.BotID
	request.ServerBotUsername = cfg.Transport.BotUsername
	request.ChatID = cfg.Transport.ChatID
	request.InviteLink = cfg.Transport.InviteLink
	if err := api.TestTopic(ctx, request.ChatID, request.ThreadID); err != nil {
		return nil, fmt.Errorf("бот точки не может писать в указанную тему: %w", err)
	}
	clientConfig, err := config.GenerateClient(request, time.Now().UTC(), rand.Reader)
	if err != nil {
		return nil, err
	}
	signingKey, err := server.clientSigningKey()
	if err != nil {
		return nil, fmt.Errorf("подготовить подпись настроек точки: %w", err)
	}
	clientConfig, err = config.SignClient(clientConfig, signingKey)
	if err != nil {
		return nil, err
	}
	payload, err := json.MarshalIndent(clientConfig, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("подготовить настройки точки: %w", err)
	}
	clientConfigPath := server.clientConfigPath(clientConfig.ClientID)
	if err := os.MkdirAll(filepath.Dir(clientConfigPath), 0o700); err != nil {
		return nil, fmt.Errorf("создать хранилище настроек точек: %w", err)
	}
	if err := secrets.Save(clientConfigPath, payload); err != nil {
		return nil, fmt.Errorf("сохранить резервную копию настроек точки: %w", err)
	}
	key, _ := clientConfig.Key()
	if err := server.Store.SaveClient(ctx, application.RegisteredClient{
		ID: clientConfig.ClientID, Number: clientConfig.ClientNumber, Name: clientConfig.ClientName,
		SenderBotID: me.ID, SenderBotUsername: me.Username,
		ChatID: request.ChatID, ThreadID: request.ThreadID,
		KeyID: clientConfig.Cryptography.KeyID, Key: key, Active: true,
	}); err != nil {
		_ = os.Remove(clientConfigPath)
		return nil, err
	}
	server.Logger.Info("точка добавлена", "client_id", clientConfig.ClientID, "client_number", clientConfig.ClientNumber)
	return payload, nil
}

func validateSeparateTransportBot(centerBotID, pointBotID int64) error {
	if centerBotID == pointBotID {
		return errors.New("для рабочей точки нужен отдельный бот; бот центра использовать нельзя")
	}
	return nil
}

// Clients возвращает зарегистрированные точки без их ключей шифрования.
func (server *Server) Clients(ctx context.Context) ([]application.RegisteredClient, error) {
	return server.Store.Clients(ctx)
}

// History возвращает безопасную историю выбранного счётчика обзора.
type HistoryFilter = stateDB.HistoryFilter

func (server *Server) History(ctx context.Context, kind string, limit, offset int, filter HistoryFilter) (stateDB.ServerHistoryPage, error) {
	var page stateDB.ServerHistoryPage
	var err error
	if kind == "expired" && server.adminMu.TryLock() {
		// Read the page while holding the same lock as manual publication.
		// Otherwise a stale expired row could overwrite its freshly committed
		// receipt. If the lock is busy, show history without reconciling; a later
		// request can retry. Bound both the read and reconciliation work.
		reconcileContext, cancel := context.WithTimeout(ctx, 3*time.Second)
		page, err = server.Store.HistoryPage(reconcileContext, kind, limit, offset, time.Now().UTC(), filter)
		reconciled := false
		if err == nil {
			reconciled = server.refreshExpiredTelegramReceipts(reconcileContext, &page)
		}
		server.adminMu.Unlock()
		cancel()
		if err != nil {
			return page, err
		}
		if reconciled {
			page, err = server.Store.HistoryPage(ctx, kind, limit, offset, time.Now().UTC(), filter)
		}
	} else {
		page, err = server.Store.HistoryPage(ctx, kind, limit, offset, time.Now().UTC(), filter)
	}
	if err != nil {
		return page, err
	}
	for _, warning := range page.InternalWarnings {
		if server.Logger != nil {
			server.Logger.Warn("история смен прочитана не полностью; рабочая обработка продолжается", "error", warning)
		}
	}
	return page, nil
}

// refreshExpiredTelegramReceipts reconciles only the currently accepted scoped
// event with the local gateway JSON. It makes a permit or ambiguous send
// visible after a crash between the JSON and SQLite writes. No Bot API method
// is called here.
func (server *Server) refreshExpiredTelegramReceipts(ctx context.Context, page *stateDB.ServerHistoryPage) bool {
	if page == nil || len(page.Items) == 0 {
		return false
	}
	reconciled := false
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	gateways := server.gateways(cfg)
	for index := range page.Items {
		if ctx.Err() != nil {
			break
		}
		item := &page.Items[index]
		if item.State != "expired" || item.DestinationID == "" || !item.ContentReady {
			continue
		}
		gateway, ok := gateways[item.DestinationID].(*telegram.Destination)
		if !ok {
			continue
		}
		job, err := server.Store.CurrentDeliveryJobForRepublish(ctx, item.ID, item.DestinationID)
		if err != nil || job.Event.ID != item.ID || job.Event.SourceBlockUUID == "" {
			continue
		}
		receipt, err := gateway.DeliveryReceipt(job.Event, job.Delivery)
		if err != nil {
			continue
		}
		if telegramReceiptCompletesCurrentJob(job, receipt, job.Event.SourceBlockUUID) {
			if err := server.Store.ReplaceDeliveryReceipt(ctx, job.Event.ID,
				job.Delivery.Destination.ID, time.Now().UTC(), receipt); err != nil {
				if server.Logger != nil {
					server.Logger.Warn("не удалось восстановить подтверждённую публикацию Telegram", "event_id", item.ID, "error", err)
				}
			} else {
				reconciled = true
			}
			continue
		}
		if !receiptHasManualActionBlock(receipt) {
			continue
		}
		if item.TelegramChat == strconv.FormatInt(receipt.ChatID, 10) &&
			item.TelegramTopic == receipt.ThreadID &&
			slices.Equal(item.TelegramIDs, receipt.MessageIDs) &&
			reflect.DeepEqual(item.TelegramBlocks, receipt.Blocks) {
			continue
		}
		if err := server.Store.SaveManualDeliveryReceipt(ctx,
			job.Event.ID, job.Delivery.Destination.ID, receipt); err != nil {
			if server.Logger != nil {
				server.Logger.Warn("не удалось сверить квитанцию ручной публикации с SQLite",
					"event_id", item.ID, "destination_id", item.DestinationID, "error", err)
			}
			continue
		}
		item.TelegramChat = strconv.FormatInt(receipt.ChatID, 10)
		item.TelegramTopic = receipt.ThreadID
		item.TelegramIDs = append([]int64(nil), receipt.MessageIDs...)
		item.TelegramBlocks = append([]application.DeliveryBlockReceipt(nil), receipt.Blocks...)
		item.TelegramURLs = nil
		for _, messageID := range receipt.MessageIDs {
			if link := telegramHistoryMessageURL(receipt.ChatID, messageID); link != "" {
				item.TelegramURLs = append(item.TelegramURLs, link)
			}
		}
		item.TelegramOutcomeUnknown = receiptHasUnknownBlock(receipt)
	}
	return reconciled
}

func receiptHasManualActionBlock(receipt application.DeliveryReceipt) bool {
	for _, block := range receipt.Blocks {
		if block.UnknownOutcome || block.ManualRepublishPending {
			return true
		}
	}
	return false
}

func receiptHasUnknownBlock(receipt application.DeliveryReceipt) bool {
	for _, block := range receipt.Blocks {
		if block.UnknownOutcome {
			return true
		}
	}
	return false
}

func telegramHistoryMessageURL(chatID, messageID int64) string {
	chat := strconv.FormatInt(chatID, 10)
	if messageID <= 0 || !strings.HasPrefix(chat, "-100") || len(chat) <= 4 {
		return ""
	}
	return "https://t.me/c/" + strings.TrimPrefix(chat, "-100") + "/" + strconv.FormatInt(messageID, 10)
}

// ClearHistory очищает представление завершённой истории, не затрагивая очередь.
func (server *Server) ClearHistory(ctx context.Context, kind string) error {
	if err := server.Store.ClearHistory(ctx, kind, time.Now().UTC()); err != nil {
		return err
	}
	stats, err := server.Store.Stats(ctx)
	if err == nil {
		server.update(func(status *ServerStatus) { status.Stats = stats })
	}
	return nil
}

type TelegramRepublishConfirmation struct {
	CheckedCurrentRevision bool   `json:"checked_current_revision"`
	ConfirmedPartAbsent    bool   `json:"confirmed_part_absent"`
	ManualResume           bool   `json:"manual_resume"`
	UnconfirmedPart        int    `json:"unconfirmed_part"`
	SendAttemptToken       string `json:"send_attempt_token"`
}

// RepublishTelegramBlock повторно создаёт только явно выбранный удалённый
// временной блок его актуальной редакции. ID берётся исключительно из ответа
// бота: сообщения других участников группы не используются как привязка.
func (server *Server) RepublishTelegramBlock(
	ctx context.Context, eventID, destinationID, blockUUID string, confirmation TelegramRepublishConfirmation,
) (resultErr error) {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	// Фоновая доставка и ручная команда используют один файл состояния.
	// Короткая штатная пауза исключает гонку двух экземпляров адаптера; входящие
	// пакеты при этом остаются в Telegram и будут приняты сразу после запуска.
	wasProcessing := server.Status().Processing
	if wasProcessing {
		server.stopWorker()
		defer func() {
			if restartErr := server.restartWorker(server.applicationContext()); restartErr != nil && resultErr == nil {
				resultErr = fmt.Errorf("блок опубликован, но обработку Центра не удалось возобновить: %w", restartErr)
			}
		}()
	}
	selectedEventID := strings.TrimSpace(eventID)
	destinationID = strings.TrimSpace(destinationID)
	blockUUID = strings.TrimSpace(blockUUID)
	job, err := server.Store.CurrentDeliveryJobForRepublish(ctx, selectedEventID, destinationID)
	if err != nil {
		return err
	}
	if err := validateTelegramRepublishBlock(job, blockUUID); err != nil {
		return err
	}
	if job.Event.ID != selectedEventID && server.Logger != nil {
		server.Logger.Info("ручная публикация использует актуальную редакцию вместо исторической карточки",
			"selected_event_id", selectedEventID, "current_event_id", job.Event.ID,
			"destination_id", destinationID, "block_uuid", blockUUID,
		)
	}
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	gateway, ok := server.gateways(cfg)[job.Delivery.Destination.ID].(*telegram.Destination)
	if !ok {
		return errors.New("для этой доставки не настроен Telegram")
	}
	before, err := gateway.DeliveryReceipt(job.Event, job.Delivery)
	if err != nil {
		return fmt.Errorf("проверить состояние публикации Telegram: %w", err)
	}
	if telegramReceiptCompletesCurrentJob(job, before, blockUUID) {
		return persistReceiptIndependently(func(saveContext context.Context) error {
			return server.Store.ReplaceDeliveryReceipt(saveContext, job.Event.ID,
				job.Delivery.Destination.ID, time.Now().UTC(), before)
		})
	}
	if err := server.saveTelegramManualReceipt(job, before); err != nil {
		return err
	}
	confirmedAbsentPart, err := validateTelegramUnknownRepublishConfirmation(before, job.Delivery, blockUUID, confirmation)
	if err != nil {
		return err
	}
	var allowed bool
	if confirmation.ManualResume {
		allowed, err = gateway.ResumeManualRepublish(job.Event, blockUUID, confirmation.SendAttemptToken)
	} else {
		allowed, err = gateway.AllowRepublishWithConfirmation(job.Event, blockUUID, confirmedAbsentPart, confirmation.SendAttemptToken)
	}
	if err != nil {
		return err
	}
	if !allowed {
		// Предыдущая попытка могла уже успешно создать сообщение Telegram, но
		// потерять HTTP-контекст до записи нового message_id в SQLite. В таком
		// случае повторное действие оператора не создаёт дубль, а только
		// восстанавливает результат доставки из устойчивого состояния шлюза.
		receipt, receiptErr := gateway.DeliveryReceipt(job.Event, job.Delivery)
		if receiptErr != nil {
			return fmt.Errorf("проверить состояние публикации Telegram: %w", receiptErr)
		}
		found, complete := receiptBlockPublicationState(receipt, blockUUID)
		if !found {
			return errors.New("блок не отмечен как удалённый в Telegram")
		}
		for _, block := range receipt.Blocks {
			if strings.EqualFold(block.BlockUUID, blockUUID) && block.UnknownOutcome {
				return errors.New("исход отправки Telegram по-прежнему неизвестен; проверьте спорную часть и повторите подтверждение")
			}
		}
		if complete {
			if persistErr := persistReceiptIndependently(func(saveContext context.Context) error {
				return server.Store.ReplaceDeliveryReceipt(
					saveContext, job.Event.ID, job.Delivery.Destination.ID, time.Now().UTC(), receipt,
				)
			}); persistErr != nil {
				return persistErr
			}
			server.Logger.Info("результат повторной публикации восстановлен без создания дубля",
				"event_id", job.Event.ID, "destination_id", destinationID, "block_uuid", blockUUID,
				"final_message_ids", receipt.MessageIDs,
			)
			return nil
		}
		// У длинного перехвата могла сохраниться только первая часть. Продолжаем
		// тот же набор message_id вместо ошибочного завершения или новой отправки.
		server.Logger.Warn("незавершённая публикация Telegram будет продолжена",
			"event_id", job.Event.ID, "destination_id", destinationID, "block_uuid", blockUUID,
			"known_message_ids", receipt.MessageIDs,
		)
	}
	// The permit is already durable in the gateway JSON. Mirror it before the
	// first POST so a restart can show the operator the current token.
	if allowed {
		permitted, receiptErr := gateway.DeliveryReceipt(job.Event, job.Delivery)
		if receiptErr != nil {
			return fmt.Errorf("прочитать разрешение ручной публикации: %w", receiptErr)
		}
		if err := server.saveTelegramManualReceipt(job, permitted); err != nil {
			return err
		}
	}
	if err := gateway.DeliverManually(ctx, job.Event, job.Delivery); err != nil {
		receipt, receiptErr := gateway.DeliveryReceipt(job.Event, job.Delivery)
		if receiptErr != nil {
			return fmt.Errorf("повторно опубликовать блок: %v; прочитать состояние Telegram: %w", err, receiptErr)
		}
		if saveErr := server.saveTelegramManualReceipt(job, receipt); saveErr != nil {
			return fmt.Errorf("повторно опубликовать блок: %v; сохранить текущее состояние: %w", err, saveErr)
		}
		return fmt.Errorf("повторно опубликовать блок: %w", err)
	}
	receipt, err := gateway.DeliveryReceipt(job.Event, job.Delivery)
	if err != nil {
		return fmt.Errorf("прочитать ID повторной публикации: %w", err)
	}
	if !receiptHasPublishedBlock(receipt, blockUUID) {
		if err := server.saveTelegramManualReceipt(job, receipt); err != nil {
			return err
		}
		return errors.New("повторная публикация Telegram не завершена: сохранены не все части")
	}
	if err := persistReceiptIndependently(func(saveContext context.Context) error {
		return server.Store.ReplaceDeliveryReceipt(
			saveContext, job.Event.ID, job.Delivery.Destination.ID, time.Now().UTC(), receipt,
		)
	}); err != nil {
		server.Logger.Error("Telegram опубликован, но новый message_id пока не сохранён",
			"event_id", job.Event.ID, "destination_id", destinationID, "block_uuid", blockUUID,
			"final_message_ids", receipt.MessageIDs, "error", err,
		)
		return err
	}
	server.Logger.Info("временной блок повторно опубликован оператором", "event_id", job.Event.ID, "destination_id", destinationID, "block_uuid", blockUUID)
	return nil
}

func (server *Server) saveTelegramManualReceipt(job application.DeliveryJob, receipt application.DeliveryReceipt) error {
	return persistReceiptIndependently(func(saveContext context.Context) error {
		return server.Store.SaveManualDeliveryReceipt(saveContext,
			job.Event.ID, job.Delivery.Destination.ID, receipt)
	})
}

func validateTelegramUnknownRepublishConfirmation(
	receipt application.DeliveryReceipt, delivery domain.PlannedDelivery, blockUUID string,
	confirmation TelegramRepublishConfirmation,
) (int, error) {
	for _, block := range receipt.Blocks {
		if !strings.EqualFold(strings.TrimSpace(block.BlockUUID), blockUUID) {
			continue
		}
		if block.ManualRepublishPending {
			if !confirmation.ManualResume || !confirmation.CheckedCurrentRevision ||
				confirmation.ConfirmedPartAbsent || confirmation.UnconfirmedPart != 0 ||
				confirmation.SendAttemptToken == "" || confirmation.SendAttemptToken != block.SendAttemptToken {
				return 0, errors.New("ручное разрешение изменилось; обновите историю и сверьте актуальную редакцию")
			}
			return 0, nil
		}
		if !block.UnknownOutcome {
			break
		}
		part := block.UnconfirmedPart
		if confirmation.ManualResume || !confirmation.CheckedCurrentRevision || !confirmation.ConfirmedPartAbsent ||
			confirmation.UnconfirmedPart != part || part <= 0 ||
			block.ExpectedMessageCount < part || len(block.MessageIDs) != part-1 ||
			block.SendAttemptToken == "" || confirmation.SendAttemptToken != block.SendAttemptToken ||
			receipt.ThreadID != delivery.Decision.ThreadID {
			return 0, errors.New("для ручной публикации нужна повторная сверка актуальной редакции и отсутствия спорной части в теме Telegram")
		}
		seen := make(map[int64]struct{}, len(block.MessageIDs))
		for _, messageID := range block.MessageIDs {
			if messageID <= 0 {
				return 0, errors.New("сохранённые ID частей Telegram некорректны")
			}
			if _, exists := seen[messageID]; exists {
				return 0, errors.New("сохранённые ID частей Telegram повторяются")
			}
			seen[messageID] = struct{}{}
		}
		return part, nil
	}
	if confirmation.ManualResume || confirmation.CheckedCurrentRevision || confirmation.ConfirmedPartAbsent ||
		confirmation.UnconfirmedPart != 0 || confirmation.SendAttemptToken != "" {
		return 0, errors.New("состояние публикации Telegram изменилось; обновите историю перед повторной публикацией")
	}
	return 0, nil
}

func validateTelegramRepublishBlock(job application.DeliveryJob, blockUUID string) error {
	if job.Delivery.Destination.Kind != domain.DestinationTelegram ||
		job.Delivery.RuleLevel == "cleanup" || job.Delivery.Decision.Action == domain.RouteSkip {
		return errors.New("актуальная редакция больше не публикуется в выбранной теме Telegram")
	}
	if job.Event.SourceBlockDeleted {
		return errors.New("временной блок удалён в актуальной редакции EAVC")
	}
	if sourceBlock := strings.TrimSpace(job.Event.SourceBlockUUID); sourceBlock != blockUUID {
		return errors.New("для повторной публикации нужна актуальная редакция с точным UUID временного блока")
	}
	for _, block := range job.Event.Blocks {
		if block.Identity() == blockUUID {
			if strings.TrimSpace(block.DeletedAt) != "" || strings.TrimSpace(block.Text) == "" {
				return errors.New("временной блок удалён или пуст в актуальной редакции EAVC")
			}
			return nil
		}
	}
	return errors.New("выбранный временной блок не найден в актуальной редакции EAVC")
}

func receiptHasPublishedBlock(receipt application.DeliveryReceipt, blockUUID string) bool {
	_, complete := receiptBlockPublicationState(receipt, blockUUID)
	return complete
}

func telegramReceiptCompletesCurrentJob(job application.DeliveryJob, receipt application.DeliveryReceipt, blockUUID string) bool {
	return validateTelegramRepublishBlock(job, blockUUID) == nil &&
		receipt.Kind == "telegram" && receipt.ThreadID == job.Delivery.Decision.ThreadID &&
		receiptHasPublishedBlock(receipt, blockUUID)
}

func receiptBlockPublicationState(receipt application.DeliveryReceipt, blockUUID string) (found, complete bool) {
	for _, block := range receipt.Blocks {
		if strings.EqualFold(strings.TrimSpace(block.BlockUUID), strings.TrimSpace(blockUUID)) {
			return true, block.Complete && !block.Suppressed && len(block.MessageIDs) > 0
		}
	}
	return false, false
}

// persistReceiptIndependently не связывает уже выполненную внешнюю публикацию
// с жизненным циклом браузерного HTTP-запроса. Ограниченные повторы закрывают
// кратковременную блокировку SQLite, но не могут зависнуть бесконечно.
func persistReceiptIndependently(operation func(context.Context) error) error {
	const attempts = 4
	delay := 100 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		saveContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		lastErr = operation(saveContext)
		cancel()
		if lastErr == nil {
			return nil
		}
		if attempt < attempts {
			time.Sleep(delay)
			delay *= 2
		}
	}
	return fmt.Errorf("сохранить ID повторной публикации после %d попыток: %w", attempts, lastErr)
}

// ClientConfig повторно выдаёт защищённо сохранённую конфигурацию точки.
func (server *Server) ClientConfig(ctx context.Context, clientID string) ([]byte, application.RegisteredClient, error) {
	client, err := server.Store.Client(ctx, strings.TrimSpace(clientID))
	if err != nil {
		return nil, application.RegisteredClient{}, err
	}
	payload, err := secrets.Load(server.clientConfigPath(client.ID))
	if err != nil {
		return nil, application.RegisteredClient{}, fmt.Errorf(
			"резервная копия настроек этой точки отсутствует; создайте точку заново: %w", err,
		)
	}
	if _, err := config.DecodeClient(bytes.NewReader(payload)); err != nil {
		return nil, application.RegisteredClient{}, fmt.Errorf("резервная копия настроек повреждена: %w", err)
	}
	return payload, client, nil
}

// ClientPackage собирает готовую папку рабочей точки: оператору достаточно
// распаковать ZIP на Рабочий стол и запустить EXE. Индивидуальный JSON будет
// автоматически принят при первом запуске и перенесён в защищённое хранилище.
func (server *Server) ClientActivation(ctx context.Context, clientID string) ([]byte, string, application.RegisteredClient, error) {
	payload, client, err := server.ClientConfig(ctx, clientID)
	if err != nil {
		return nil, "", application.RegisteredClient{}, err
	}
	sealed, code, err := provisioning.Seal(payload, rand.Reader)
	if err != nil {
		return nil, "", application.RegisteredClient{}, fmt.Errorf("зашифровать настройки точки: %w", err)
	}
	return sealed, code, client, nil
}

func (server *Server) ClientPackage(ctx context.Context, clientID string) ([]byte, string, application.RegisteredClient, error) {
	sealedConfig, activationCode, client, err := server.ClientActivation(ctx, clientID)
	if err != nil {
		return nil, "", application.RegisteredClient{}, err
	}
	template, err := server.verifiedStationTemplate()
	if err != nil {
		return nil, "", application.RegisteredClient{}, err
	}
	executable := template["EAVC Connect Station.exe"]
	updater := template["EAVC Connect Updater.exe"]
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	folder := "EAVC Connect Station-" + safePackageName(client.Number) + "/"
	entries := []struct {
		name string
		data []byte
	}{
		{folder + "EAVC Connect Station.exe", executable},
		{folder + "EAVC Connect Updater.exe", updater},
		{folder + provisionedClientActivationName, sealedConfig},
		{folder + "ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt", []byte(clientPackageReadme)},
	}
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		part, createErr := archive.CreateHeader(header)
		if createErr != nil {
			return nil, "", application.RegisteredClient{}, fmt.Errorf("создать комплект точки: %w", createErr)
		}
		if _, writeErr := part.Write(entry.data); writeErr != nil {
			return nil, "", application.RegisteredClient{}, fmt.Errorf("записать комплект точки: %w", writeErr)
		}
	}
	if err := archive.Close(); err != nil {
		return nil, "", application.RegisteredClient{}, fmt.Errorf("завершить комплект точки: %w", err)
	}
	return output.Bytes(), activationCode, client, nil
}

type stationTemplateManifest struct {
	Schema  int    `json:"schema"`
	Product string `json:"product"`
	Version string `json:"version"`
	Files   []struct {
		Path   string `json:"path"`
		Size   int64  `json:"size"`
		SHA256 string `json:"sha256"`
	} `json:"files"`
}

func (server *Server) verifiedStationTemplate() (map[string][]byte, error) {
	root := filepath.Join(server.BaseDir, "station-template")
	manifestPayload, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return nil, fmt.Errorf("манифест комплекта рабочей точки не найден: %w", err)
	}
	signatureText, err := os.ReadFile(filepath.Join(root, "manifest.sig"))
	if err != nil {
		return nil, fmt.Errorf("подпись комплекта рабочей точки не найдена: %w", err)
	}
	publicKey, keyErr := base64.StdEncoding.DecodeString(strings.TrimSpace(server.UpdatePublicKey))
	signature, signatureErr := base64.StdEncoding.DecodeString(strings.TrimSpace(string(signatureText)))
	if keyErr != nil || signatureErr != nil || len(publicKey) != ed25519.PublicKeySize ||
		!ed25519.Verify(ed25519.PublicKey(publicKey), manifestPayload, signature) {
		return nil, errors.New("подпись комплекта рабочей точки недействительна")
	}
	var manifest stationTemplateManifest
	if err := json.Unmarshal(manifestPayload, &manifest); err != nil {
		return nil, fmt.Errorf("прочитать манифест комплекта рабочей точки: %w", err)
	}
	if manifest.Schema != 1 || manifest.Product != "eavc-connect-station-template" || manifest.Version != server.Version {
		return nil, errors.New("комплект рабочей точки выпущен для другой версии")
	}
	expected := map[string]bool{"EAVC Connect Station.exe": true, "EAVC Connect Updater.exe": true}
	verified := make(map[string][]byte, len(expected))
	for _, item := range manifest.Files {
		if !expected[item.Path] || verified[item.Path] != nil {
			return nil, errors.New("манифест комплекта рабочей точки содержит посторонний или повторный файл")
		}
		payload, err := os.ReadFile(filepath.Join(root, item.Path))
		if err != nil {
			return nil, fmt.Errorf("файл комплекта рабочей точки не найден: %s: %w", item.Path, err)
		}
		hash := sha256.Sum256(payload)
		if int64(len(payload)) != item.Size || !strings.EqualFold(hex.EncodeToString(hash[:]), item.SHA256) {
			return nil, fmt.Errorf("целостность файла комплекта рабочей точки нарушена: %s", item.Path)
		}
		verified[item.Path] = payload
	}
	if len(verified) != len(expected) {
		return nil, errors.New("комплект рабочей точки неполон")
	}
	return verified, nil
}

func safePackageName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "point"
	}
	return strings.Map(func(char rune) rune {
		if char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' ||
			char >= '0' && char <= '9' || char >= 'А' && char <= 'я' || char == 'Ё' || char == 'ё' ||
			char == '-' || char == '_' {
			return char
		}
		return '-'
	}, value)
}

const clientPackageReadme = `EAVC CONNECT — РАБОЧАЯ ТОЧКА

1. Скопируйте всю распакованную папку на Рабочий стол.
2. Запустите «EAVC Connect Station.exe».
3. Введите код активации, который отдельно показал центр управления.
4. Если база EAVC не найдена автоматически, укажите её в блоке «Где находится EAVC».

Токен Telegram и ключ шифрования внутри комплекта защищены кодом активации.
Не храните код активации рядом с ZIP и не отправляйте их одним сообщением.
Не удаляйте папки data и logs после первого запуска: в них находятся защищённые настройки, очередь и журналы.
При неисправности нажмите «Скачать диагностику» и отправьте ZIP администратору.
`

// Rules возвращает действующую опубликованную матрицу маршрутов.
func (server *Server) Rules(ctx context.Context) (domain.RuleSet, error) {
	return server.Store.ActiveRules(ctx)
}

// RuleDraft возвращает автоматически сохранённый неопубликованный черновик.
func (server *Server) RuleDraft(ctx context.Context) (domain.RuleSet, bool, error) {
	return server.Store.RuleDraft(ctx)
}

// SaveRuleDraft проверяет и сохраняет черновик без изменения действующих маршрутов.
func (server *Server) SaveRuleDraft(ctx context.Context, rules domain.RuleSet) error {
	canonical, err := canonicalRules(rules)
	if err != nil {
		return err
	}
	return server.Store.SaveRuleDraft(ctx, canonical)
}

// PublishRules проверяет и публикует матрицу как новую целостную версию.
func (server *Server) PublishRules(ctx context.Context, rules domain.RuleSet, author string) error {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	canonical, err := canonicalRules(rules)
	if err != nil {
		return err
	}
	rules = canonical
	if err := server.validateRuleDestinations(rules); err != nil {
		return err
	}
	active, err := server.Store.ActiveRules(ctx)
	if err != nil {
		return err
	}
	if err := requireCurrentRuleVersion(rules.Version, active.Version); err != nil {
		return err
	}
	next, err := server.Store.NextRuleVersion(ctx)
	if err != nil {
		return err
	}
	rules.Version = next
	return server.Store.PublishRules(ctx, rules, author)
}

func requireCurrentRuleVersion(submitted, active int64) error {
	if submitted != active {
		return fmt.Errorf("маршруты уже изменены другим действием: на экране версия %d, действует версия %d; обновите страницу и повторите", submitted, active)
	}
	return nil
}

// RuleHistory показывает версии маршрутов без раскрытия секретных настроек.
func (server *Server) RuleHistory(ctx context.Context) ([]stateDB.RuleVersion, error) {
	return server.Store.RuleHistory(ctx)
}

// RestoreRules публикует выбранную старую матрицу как новую версию.
func (server *Server) RestoreRules(ctx context.Context, version int64, author string) error {
	server.adminMu.Lock()
	defer server.adminMu.Unlock()
	rules, err := server.Store.RulesByVersion(ctx, version)
	if err != nil {
		return err
	}
	if err := server.validateRuleDestinations(rules); err != nil {
		return fmt.Errorf("эту версию нельзя восстановить: %w", err)
	}
	next, err := server.Store.NextRuleVersion(ctx)
	if err != nil {
		return err
	}
	rules.Version = next
	return server.Store.PublishRules(ctx, rules, author+" · восстановление версии "+strconv.FormatInt(version, 10))
}

func destinationTargetChanged(previous, next config.DestinationConfig) bool {
	if previous.Kind != next.Kind {
		return true
	}
	switch next.Kind {
	case domain.DestinationTelegram:
		sameBot := previous.BotToken == next.BotToken
		if previous.BotID > 0 && next.BotID > 0 {
			sameBot = previous.BotID == next.BotID
		}
		return !sameBot || previous.ChatID != next.ChatID
	case domain.DestinationWord:
		return !strings.EqualFold(filepath.Clean(strings.TrimSpace(previous.Folder)), filepath.Clean(strings.TrimSpace(next.Folder)))
	default:
		return true
	}
}

func (server *Server) validateRuleDestinations(rules domain.RuleSet) error {
	server.mu.RLock()
	configured := cloneDestinations(server.config.Destinations)
	server.mu.RUnlock()
	for id := range rules.Destinations {
		actual, exists := configured[id]
		if !exists {
			return fmt.Errorf("получатель %q отсутствует в текущей конфигурации", id)
		}
		if rules.Destinations[id].Kind != actual.Kind {
			return fmt.Errorf("тип получателя %q в маршрутах не совпадает с его настройкой", id)
		}
	}
	for id := range configured {
		if _, exists := rules.Destinations[id]; !exists {
			return fmt.Errorf("для настроенного получателя %q отсутствует описание маршрута", id)
		}
	}
	return nil
}

func canonicalRules(rules domain.RuleSet) (domain.RuleSet, error) {
	positions := make(map[string]map[string]domain.RouteDecision, len(rules.Positions))
	for key, decisions := range rules.Positions {
		canonical := key
		switch {
		case strings.HasPrefix(key, "name:"):
			canonical = "name:" + domain.NormalizeUnitName(strings.TrimPrefix(key, "name:"))
		case strings.HasPrefix(key, "catalog:"):
			canonical = "catalog:" + strings.ToLower(strings.TrimSpace(strings.TrimPrefix(key, "catalog:")))
		}
		if _, exists := positions[canonical]; exists {
			return domain.RuleSet{}, fmt.Errorf("две позиции после нормализации имеют один ключ %q", canonical)
		}
		positions[canonical] = decisions
	}
	rules.Positions = positions
	return rules, nil
}

// SafeDestinations возвращает направления без токенов и паролей.
func (server *Server) SafeDestinations() map[string]map[string]any {
	server.mu.RLock()
	configDestinations := cloneDestinations(server.config.Destinations)
	server.mu.RUnlock()
	rules, _ := server.Store.ActiveRules(context.Background())
	result := make(map[string]map[string]any, len(configDestinations))
	for id, destination := range configDestinations {
		name := id
		if item, exists := rules.Destinations[id]; exists {
			name = item.Name
		}
		messageTemplate := destination.MessageTemplate
		if destination.Kind == domain.DestinationTelegram && strings.TrimSpace(messageTemplate) == "" {
			messageTemplate = telegram.DefaultMessageTemplate
		}
		messageTemplate = telegram.MigrateMessageTemplate(messageTemplate)
		protection := destination.Protection
		if strings.TrimSpace(protection) == "" {
			protection = telegram.DefaultProtection
		}
		result[id] = map[string]any{
			"name": name, "kind": destination.Kind, "chat_id": destination.ChatID,
			"message_template": messageTemplate,
			"template":         destination.Template, "folder": destination.Folder,
			"network": destination.Network, "protection": protection, "prefix": destination.Prefix,
		}
	}
	return result
}

// TemplateDefaults возвращает интерфейсу стандартные шаблоны без секретов.
func (server *Server) TemplateDefaults() TemplateDefaults {
	return TemplateDefaults{
		Telegram: telegram.DefaultMessageTemplate, TelegramMarkers: telegram.MessageTemplateMarkers(),
		WordPath: filepath.Join(server.BaseDir, "word_template.docx"), WordMarkers: docx.TemplateMarkers(),
	}
}

// RestoreDefaultWordTemplate возвращает файл поставки рядом с программой.
func (server *Server) RestoreDefaultWordTemplate() (string, error) {
	path := filepath.Join(server.BaseDir, "word_template.docx")
	if err := docx.RestoreDefaultTemplate(path); err != nil {
		return "", err
	}
	if err := docx.ValidateTemplate(path); err != nil {
		return "", fmt.Errorf("проверка восстановленного шаблона Word: %w", err)
	}
	return path, nil
}

// DestinationWordTemplate возвращает выбранный пользователем DOCX для просмотра.
func (server *Server) DestinationWordTemplate(id string) ([]byte, string, error) {
	server.mu.RLock()
	destination, exists := server.config.Destinations[strings.TrimSpace(id)]
	server.mu.RUnlock()
	if !exists || destination.Kind != domain.DestinationWord {
		return nil, "", errors.New("word-получатель не найден")
	}
	file, err := os.Open(destination.Template)
	if err != nil {
		return nil, "", fmt.Errorf("открыть шаблон Word: %w", err)
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, 64*1024*1024+1))
	if err != nil {
		return nil, "", fmt.Errorf("прочитать шаблон Word: %w", err)
	}
	if len(payload) > 64*1024*1024 {
		return nil, "", errors.New("шаблон Word слишком велик для скачивания")
	}
	return payload, filepath.Base(destination.Template), nil
}

// Diagnostics формирует архив состояния и журналов без конфигурации и секретов.
func (server *Server) Diagnostics(ctx context.Context) ([]byte, error) {
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	status, _ := json.MarshalIndent(server.OperationalStatus(), "", "  ")
	part, _ := archive.Create("status.json")
	_, _ = part.Write(status)
	history, err := server.RuleHistory(ctx)
	if err == nil {
		var active int64
		var latest time.Time
		for _, item := range history {
			if item.Active {
				active = item.Version
			}
			if item.CreatedAt.After(latest) {
				latest = item.CreatedAt
			}
		}
		payload, _ := json.MarshalIndent(map[string]any{"count": len(history), "active_version": active, "latest_at": latest}, "", "  ")
		part, _ = archive.Create("rule-versions.json")
		_, _ = part.Write(payload)
	}
	if err := addDiagnosticLogs(archive, server.BaseDir, "server.log"); err != nil {
		_ = archive.Close()
		return nil, err
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
