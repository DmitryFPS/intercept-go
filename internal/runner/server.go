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
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/defaults"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/docx"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/infrastructure/share"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

const serverOffsetName = "telegram_offset"

type ServerStatus struct {
	State        string              `json:"state"`
	Title        string              `json:"title"`
	Detail       string              `json:"detail"`
	Configured   bool                `json:"configured"`
	TransportOK  bool                `json:"transport_ok"`
	LastReceived *time.Time          `json:"last_received,omitempty"`
	LastDelivery *time.Time          `json:"last_delivery,omitempty"`
	Stats        stateDB.ServerStats `json:"stats"`
	Version      string              `json:"version"`
	InviteLink   string              `json:"invite_link,omitempty"`
	Destinations []string            `json:"destinations"`
}

type TransportSetup struct {
	BotToken   string `json:"bot_token"`
	ChatID     int64  `json:"chat_id"`
	InviteLink string `json:"invite_link"`
}

type DestinationSetup struct {
	ID       string                 `json:"id"`
	Name     string                 `json:"name"`
	Kind     domain.DestinationKind `json:"kind"`
	BotToken string                 `json:"bot_token,omitempty"`
	ChatID   int64                  `json:"chat_id,omitempty"`
	Template string                 `json:"template,omitempty"`
	Folder   string                 `json:"folder,omitempty"`
	Network  string                 `json:"network,omitempty"`
	Prefix   string                 `json:"prefix,omitempty"`
	Username string                 `json:"username,omitempty"`
	Password string                 `json:"password,omitempty"`
}

// Server управляет центральным процессом и безопасной перенастройкой.
type Server struct {
	BaseDir string
	Version string
	Logger  *slog.Logger
	Store   *stateDB.ServerStore

	mu          sync.RWMutex
	status      ServerStatus
	config      config.Server
	workerStop  context.CancelFunc
	rootContext context.Context
}

func NewServer(ctx context.Context, baseDir, version string, logger *slog.Logger) (*Server, error) {
	db, err := stateDB.OpenState(filepath.Join(baseDir, "data", "server.db"))
	if err != nil {
		return nil, err
	}
	store, err := stateDB.NewServerStore(ctx, db)
	if err != nil {
		db.Close()
		return nil, err
	}
	if _, err := store.ActiveRules(ctx); err != nil {
		rules, defaultErr := defaults.InitialRules()
		if defaultErr != nil {
			db.Close()
			return nil, defaultErr
		}
		if err := store.PublishRules(ctx, rules, "Начальная конфигурация"); err != nil {
			db.Close()
			return nil, err
		}
	}
	server := &Server{
		BaseDir: baseDir, Version: version, Logger: logger, Store: store,
		status: ServerStatus{
			State: "setup", Title: "Настройте транспорт", Detail: "Укажите серверного Telegram-бота и закрытую транспортную группу.", Version: version,
		},
	}
	return server, nil
}

func (server *Server) Start(ctx context.Context) {
	server.mu.Lock()
	server.rootContext = ctx
	server.mu.Unlock()
	if _, err := os.Stat(server.configPath()); err == nil {
		if err := server.restartWorker(ctx); err != nil {
			server.fail("Ошибка настроек сервера", err)
		}
	}
	go server.statusLoop(ctx)
}

func (server *Server) Close() {
	server.mu.Lock()
	if server.workerStop != nil {
		server.workerStop()
	}
	server.mu.Unlock()
	_ = server.Store.Close()
}

func (server *Server) Status() ServerStatus {
	server.mu.RLock()
	defer server.mu.RUnlock()
	return server.status
}

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
	destinations := server.config.Destinations
	server.mu.RUnlock()
	if destinations == nil {
		destinations = make(map[string]config.DestinationConfig)
	}
	cfg := config.Server{
		Schema: config.ServerSchemaV1, UpdatedAt: time.Now().UTC(), Listen: "127.0.0.1:17632",
		Transport: config.ServerTransport{
			BotToken: setup.BotToken, BotUsername: me.Username,
			ChatID: setup.ChatID, InviteLink: strings.TrimSpace(setup.InviteLink),
		},
		Destinations: destinations,
	}
	if err := server.saveConfig(cfg); err != nil {
		return err
	}
	return server.restartWorker(server.applicationContext())
}

func (server *Server) AddDestination(ctx context.Context, setup DestinationSetup) error {
	server.mu.RLock()
	cfg := server.config
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
		Template: strings.TrimSpace(setup.Template), Folder: strings.TrimSpace(setup.Folder),
		Network: strings.TrimSpace(setup.Network), Prefix: strings.TrimSpace(setup.Prefix),
		Username: setup.Username, Password: setup.Password,
	}
	switch setup.Kind {
	case domain.DestinationTelegram:
		api, err := telegram.New(setup.BotToken, nil)
		if err != nil {
			return err
		}
		if _, err := api.GetMe(ctx); err != nil {
			return fmt.Errorf("проверка бота направления: %w", err)
		}
		if err := api.TestTopic(ctx, setup.ChatID, 0); err != nil {
			return fmt.Errorf("проверка Telegram-направления: %w", err)
		}
	case domain.DestinationWord:
		if _, err := os.Stat(destination.Template); err != nil {
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
	if existing, exists := rules.Destinations[id]; !exists || existing.Name != name || existing.Kind != setup.Kind {
		if rules.Destinations == nil {
			rules.Destinations = make(map[string]domain.Destination)
		}
		rules.Destinations[id] = domain.Destination{ID: id, Name: name, Kind: setup.Kind, Enabled: true}
		rules.Version++
		if err := server.Store.PublishRules(ctx, rules, "Настройка направления"); err != nil {
			return err
		}
	}
	return server.restartWorker(server.applicationContext())
}

func (server *Server) applicationContext() context.Context {
	server.mu.RLock()
	defer server.mu.RUnlock()
	if server.rootContext != nil {
		return server.rootContext
	}
	return context.Background()
}

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
	request.ServerBotUsername = cfg.Transport.BotUsername
	request.ChatID = cfg.Transport.ChatID
	if err := api.TestTopic(ctx, request.ChatID, request.ThreadID); err != nil {
		return nil, fmt.Errorf("бот точки не может писать в указанную тему: %w", err)
	}
	clientConfig, err := config.GenerateClient(request, time.Now().UTC(), rand.Reader)
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
	server.Logger.Info("точка добавлена", "client_id", clientConfig.ClientID, "client_number", clientConfig.ClientNumber)
	return json.MarshalIndent(clientConfig, "", "  ")
}

func (server *Server) Clients(ctx context.Context) ([]application.RegisteredClient, error) {
	return server.Store.Clients(ctx)
}

func (server *Server) Rules(ctx context.Context) (domain.RuleSet, error) {
	return server.Store.ActiveRules(ctx)
}

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

func (server *Server) RuleHistory(ctx context.Context) ([]stateDB.RuleVersion, error) {
	return server.Store.RuleHistory(ctx)
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
		part, _ := archive.Create(filepath.Join("logs", name))
		_, _ = io.Copy(part, io.LimitReader(file, 10*1024*1024))
		file.Close()
	}
	if err := archive.Close(); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

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

func (server *Server) SafeDestinations() map[string]map[string]any {
	server.mu.RLock()
	defer server.mu.RUnlock()
	result := make(map[string]map[string]any, len(server.config.Destinations))
	for id, destination := range server.config.Destinations {
		result[id] = map[string]any{
			"kind": destination.Kind, "chat_id": destination.ChatID,
			"template": destination.Template, "folder": destination.Folder,
			"network": destination.Network, "prefix": destination.Prefix,
		}
	}
	return result
}

func (server *Server) restartWorker(parent context.Context) error {
	protected, err := secrets.Load(server.configPath())
	if err != nil {
		return err
	}
	cfg, err := config.DecodeServer(bytes.NewReader(protected))
	if err != nil {
		return err
	}
	server.mu.Lock()
	if server.workerStop != nil {
		server.workerStop()
	}
	workerContext, cancel := context.WithCancel(parent)
	server.workerStop = cancel
	server.config = cfg
	server.status.Configured = true
	server.status.State = "starting"
	server.status.Title = "Запуск сервера"
	server.status.Detail = "Проверяем транспорт и направления."
	server.status.InviteLink = cfg.Transport.InviteLink
	server.status.Destinations = destinationIDs(cfg.Destinations)
	server.mu.Unlock()
	go server.run(workerContext, cfg)
	return nil
}

func (server *Server) run(ctx context.Context, cfg config.Server) {
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		server.fail("Ошибка Telegram", err)
		return
	}
	if err := api.DeleteWebhook(ctx, false); err != nil {
		server.fail("Ошибка Telegram", err)
		return
	}
	receiver := &telegram.ServerReceiver{API: api, ChatID: cfg.Transport.ChatID, MaxBytes: 4 * 1024 * 1024}
	service := &application.ServerService{Clients: server.Store, Rules: server.Store, Store: server.Store}
	delivery := &application.DeliveryService{
		Queue: server.Store, Gateways: server.gateways(cfg), Now: time.Now,
	}
	server.update(func(status *ServerStatus) {
		status.State = "running"
		status.Title = "Сервер работает"
		status.Detail = "Получаем данные точек и выполняем маршруты."
		status.TransportOK = true
	})
	go server.deliveryLoop(ctx, delivery)
	offset := int64(0)
	if raw, exists, _ := server.Store.Value(ctx, serverOffsetName); exists {
		offset, _ = strconv.ParseInt(raw, 10, 64)
	}
	for ctx.Err() == nil {
		packets, next, err := receiver.Packets(ctx, offset)
		if err != nil {
			if ctx.Err() == nil {
				server.warn("Не удалось получить данные из Telegram", err)
				time.Sleep(5 * time.Second)
			}
			continue
		}
		for _, packet := range packets {
			result, err := service.Receive(ctx, packet)
			if err != nil {
				server.Logger.Warn("пакет помещён в карантин", "error", err, "sender_bot_id", packet.SenderBotID)
				_ = server.Store.Quarantine(ctx, packet, err.Error())
				continue
			}
			if err := receiver.Acknowledge(ctx, result.ClientBotUsername, result.EventID, result.Status); err != nil {
				server.Logger.Warn("не удалось подтвердить событие", "event_id", result.EventID, "error", err)
			}
			now := time.Now().UTC()
			server.update(func(status *ServerStatus) { status.LastReceived = &now })
		}
		if next > offset {
			offset = next
			_ = server.Store.SaveValue(ctx, serverOffsetName, strconv.FormatInt(offset, 10))
		}
	}
}

func (server *Server) deliveryLoop(ctx context.Context, service *application.DeliveryService) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		count, err := service.RunOnce(ctx, 100)
		if err != nil && ctx.Err() == nil {
			server.warn("Ошибка очереди доставки", err)
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
	for id, destination := range cfg.Destinations {
		switch destination.Kind {
		case domain.DestinationTelegram:
			api, err := telegram.New(destination.BotToken, nil)
			if err == nil {
				result[id] = &telegram.Destination{API: api, ChatID: destination.ChatID}
			}
		case domain.DestinationWord:
			result[id] = &docx.Destination{
				TemplatePath: destination.Template, Folder: destination.Folder,
				StatePath: filepath.Join(server.BaseDir, "data", "word-"+safeID(id)+".json"),
				Network:   destination.Network, Prefix: destination.Prefix,
				Username: destination.Username, Password: destination.Password,
			}
		}
	}
	return result
}

func (server *Server) statusLoop(ctx context.Context) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
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

func (server *Server) fail(title string, err error) {
	server.Logger.Error(title, "error", err)
	server.update(func(status *ServerStatus) {
		status.State = "error"
		status.Title = title
		status.Detail = err.Error()
		status.TransportOK = false
	})
}

func (server *Server) warn(title string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	server.Logger.Warn(title, "error", err)
	server.update(func(status *ServerStatus) {
		status.State = "waiting"
		status.Title = title
		status.Detail = "Очереди сохранены. Повтор будет выполнен автоматически. " + err.Error()
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
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '-' || char == '_' {
			return char
		}
		return '-'
	}, value)
}
