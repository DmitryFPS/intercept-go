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
