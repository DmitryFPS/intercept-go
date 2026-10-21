package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
)

// ConfigureTransport проверяет серверного бота и сохраняет транспортную группу.
func (server *Server) ConfigureTransport(ctx context.Context, setup TransportSetup) error {
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
	destinations := cloneDestinations(server.config.Destinations)
	server.mu.RUnlock()
	if destinations == nil {
		destinations = make(map[string]config.DestinationConfig)
	}
	cfg := config.Server{
		Schema: config.ServerSchemaV1, UpdatedAt: time.Now().UTC(), Listen: "127.0.0.1:17632",
		DeliveryTimeoutMinutes: 10,
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
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	if !server.Status().Configured {
		return errors.New("сначала настройте транспортную группу")
	}
	id := strings.TrimSpace(setup.ID)
	if id == "" {
		return errors.New("не задан ID направления")
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
		if _, err := api.GetMe(ctx); err != nil {
			return fmt.Errorf("проверка бота направления: %w", err)
		}
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
		return err
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
	if ruleExists && existingRule.Kind != setup.Kind && setup.Kind != domain.DestinationTelegram {
		clearDestinationThreads(rules, id)
	}
	rules.Version++
	if err := server.Store.PublishRules(ctx, rules, "Настройка направления"); err != nil {
		return err
	}
	return server.restartWorker(server.applicationContext())
}

// DeleteDestination удаляет получателя из конфигурации и всех маршрутов.
func (server *Server) DeleteDestination(ctx context.Context, id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("не задан ID направления")
	}
	server.mu.RLock()
	cfg := cloneServerConfig(server.config)
	server.mu.RUnlock()
	if _, exists := cfg.Destinations[id]; !exists {
		return errors.New("направление не найдено")
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
	for _, decisions := range rules.Modes {
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
		return err
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
	for _, decisions := range rules.Modes {
		clear(decisions)
	}
}

// AddClient регистрирует точку и возвращает полностью готовый подписанный JSON.
func (server *Server) AddClient(ctx context.Context, request config.ClientRequest) ([]byte, error) {
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
	key, _ := clientConfig.Key()
	if err := server.Store.SaveClient(ctx, application.RegisteredClient{
		ID: clientConfig.ClientID, Number: clientConfig.ClientNumber, Name: clientConfig.ClientName,
		SenderBotID: me.ID, SenderBotUsername: me.Username,
		ChatID: request.ChatID, ThreadID: request.ThreadID,
		KeyID: clientConfig.Cryptography.KeyID, Key: key, Active: true,
	}); err != nil {
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
	server.Logger.Info("точка добавлена", "client_id", clientConfig.ClientID, "client_number", clientConfig.ClientNumber)
	return payload, nil
}

// Clients возвращает зарегистрированные точки без их ключей шифрования.
func (server *Server) Clients(ctx context.Context) ([]application.RegisteredClient, error) {
	return server.Store.Clients(ctx)
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
func (server *Server) ClientPackage(ctx context.Context, clientID string) ([]byte, application.RegisteredClient, error) {
	payload, client, err := server.ClientConfig(ctx, clientID)
	if err != nil {
		return nil, application.RegisteredClient{}, err
	}
	executablePath := filepath.Join(server.BaseDir, "station-template", "EAVC Connect Station.exe")
	executable, err := os.ReadFile(executablePath)
	if err != nil {
		return nil, application.RegisteredClient{}, fmt.Errorf("шаблон программы рабочей точки не найден: %w", err)
	}
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	folder := "EAVC Connect Station-" + safePackageName(client.Number) + "/"
	entries := []struct {
		name string
		data []byte
	}{
		{folder + "EAVC Connect Station.exe", executable},
		{folder + provisionedClientConfigName, payload},
		{folder + "ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt", []byte(clientPackageReadme)},
	}
	for _, entry := range entries {
		header := &zip.FileHeader{Name: entry.name, Method: zip.Deflate}
		header.Modified = time.Date(1980, 1, 1, 0, 0, 0, 0, time.UTC)
		part, createErr := archive.CreateHeader(header)
		if createErr != nil {
			return nil, application.RegisteredClient{}, fmt.Errorf("создать комплект точки: %w", createErr)
		}
		if _, writeErr := part.Write(entry.data); writeErr != nil {
			return nil, application.RegisteredClient{}, fmt.Errorf("записать комплект точки: %w", writeErr)
		}
	}
	if err := archive.Close(); err != nil {
		return nil, application.RegisteredClient{}, fmt.Errorf("завершить комплект точки: %w", err)
	}
	return output.Bytes(), client, nil
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
3. Если база EAVC не найдена автоматически, укажите её в блоке «Где находится EAVC».

Настройки уже подготовлены администратором. Вводить токены, ключи и коды не требуется.
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
	canonical, err := canonicalRules(rules)
	if err != nil {
		return err
	}
	rules = canonical
	next, err := server.Store.NextRuleVersion(ctx)
	if err != nil {
		return err
	}
	if rules.Version < next {
		rules.Version = next
	}
	return server.Store.PublishRules(ctx, rules, author)
}

// RuleHistory показывает версии маршрутов без раскрытия секретных настроек.
func (server *Server) RuleHistory(ctx context.Context) ([]stateDB.RuleVersion, error) {
	return server.Store.RuleHistory(ctx)
}

// RestoreRules публикует выбранную старую матрицу как новую версию.
func (server *Server) RestoreRules(ctx context.Context, version int64, author string) error {
	rules, err := server.Store.RulesByVersion(ctx, version)
	if err != nil {
		return err
	}
	next, err := server.Store.NextRuleVersion(ctx)
	if err != nil {
		return err
	}
	rules.Version = next
	return server.Store.PublishRules(ctx, rules, author+" · восстановление версии "+strconv.FormatInt(version, 10))
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
	status, _ := json.MarshalIndent(server.Status(), "", "  ")
	part, _ := archive.Create("status.json")
	_, _ = part.Write(status)
	history, err := server.RuleHistory(ctx)
	if err == nil {
		payload, _ := json.MarshalIndent(history, "", "  ")
		part, _ = archive.Create("rule-versions.json")
		_, _ = part.Write(payload)
	}
	for _, name := range []string{"server.log", "server.log.1", "server.log.2"} {
		path := filepath.Join(server.BaseDir, "logs", name)
		file, openErr := os.Open(path)
		if openErr != nil {
			continue
		}
		part, _ = archive.Create(filepath.Join("logs", name))
		_, _ = io.Copy(part, io.LimitReader(file, 10*1024*1024))
		file.Close()
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}
