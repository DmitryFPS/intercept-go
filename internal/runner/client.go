package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/infrastructure/eavc"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

const (
	maintenanceInterval = 6 * time.Hour
	terminalRetention   = 30 * 24 * time.Hour
)

// ClientStatus содержит только безопасные для экрана и диагностики данные.
type ClientStatus struct {
	State            string              `json:"state"`
	Title            string              `json:"title"`
	Detail           string              `json:"detail"`
	Configured       bool                `json:"configured"`
	ClientID         string              `json:"client_id,omitempty"`
	ClientName       string              `json:"client_name,omitempty"`
	ClientNumber     string              `json:"client_number,omitempty"`
	DatabaseOK       bool                `json:"database_ok"`
	TelegramOK       bool                `json:"telegram_ok"`
	LastDatabaseRead *time.Time          `json:"last_database_read,omitempty"`
	LastUpload       *time.Time          `json:"last_upload,omitempty"`
	LastAcknowledged *time.Time          `json:"last_acknowledged,omitempty"`
	Queue            stateDB.ClientStats `json:"queue"`
	Version          string              `json:"version"`
}

// Client управляет жизненным циклом переносимого приложения.
type Client struct {
	BaseDir string
	Version string
	Logger  *slog.Logger

	mu          sync.RWMutex
	lifecycleMu sync.Mutex
	status      ClientStatus
	workerStop  context.CancelFunc
	workerDone  chan struct{}
	rootContext context.Context
}

func NewClient(baseDir, version string, logger *slog.Logger) *Client {
	return &Client{
		BaseDir: baseDir, Version: version, Logger: logger,
		status: ClientStatus{State: "setup", Title: "Нужны настройки", Detail: "Загрузите файл настроек, который прислал администратор.", Version: version},
	}
}

func (client *Client) Start(ctx context.Context) {
	client.mu.Lock()
	client.rootContext = ctx
	client.mu.Unlock()
	if _, err := os.Stat(client.configPath()); err == nil {
		if err := client.restartWorker(ctx); err != nil {
			client.fail("Ошибка настроек", err)
		}
	}
	go func() {
		<-ctx.Done()
		client.stopWorker()
	}()
}

func (client *Client) Status() ClientStatus {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.status
}

// ImportConfig полностью проверяет файл до замены действующей конфигурации.
func (client *Client) ImportConfig(ctx context.Context, payload []byte) error {
	cfg, err := config.DecodeClient(bytes.NewReader(payload))
	if err != nil {
		return err
	}
	source, err := eavc.Open(ctx, cfg.EAVC.SQLitePath)
	if err != nil {
		return fmt.Errorf("проверка базы EAVC: %w", err)
	}
	source.Close()
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		return err
	}
	me, err := api.GetMe(ctx)
	if err != nil {
		return fmt.Errorf("проверка Telegram-бота: %w", err)
	}
	if !strings.EqualFold(me.Username, strings.TrimPrefix(cfg.Transport.BotUsername, "@")) {
		return errors.New("токен принадлежит другому Telegram-боту")
	}
	if err := os.MkdirAll(filepath.Dir(client.configPath()), 0o700); err != nil {
		return fmt.Errorf("создать папку данных: %w", err)
	}
	canonical, _ := json.Marshal(cfg)
	if err := secrets.Save(client.configPath(), canonical); err != nil {
		return err
	}
	if err := client.restartWorker(client.applicationContext()); err != nil {
		return err
	}
	client.Logger.Info("настройки точки импортированы", "client_id", cfg.ClientID, "client_number", cfg.ClientNumber)
	return nil
}

func (client *Client) applicationContext() context.Context {
	client.mu.RLock()
	defer client.mu.RUnlock()
	if client.rootContext != nil {
		return client.rootContext
	}
	return context.Background()
}

func (client *Client) Diagnostics() ([]byte, error) {
	var output bytes.Buffer
	archive := zip.NewWriter(&output)
	status, _ := json.MarshalIndent(client.Status(), "", "  ")
	part, _ := archive.Create("status.json")
	_, _ = part.Write(status)
	for _, name := range []string{"client.log", "client.log.1", "client.log.2"} {
		path := filepath.Join(client.BaseDir, "logs", name)
		file, err := os.Open(path)
		if err != nil {
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

func (client *Client) restartWorker(parent context.Context) error {
	client.lifecycleMu.Lock()
	defer client.lifecycleMu.Unlock()
	protected, err := secrets.Load(client.configPath())
	if err != nil {
		return err
	}
	cfg, err := config.DecodeClient(bytes.NewReader(protected))
	if err != nil {
		return err
	}
	client.stopWorkerLocked()
	workerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	client.mu.Lock()
	client.workerStop = cancel
	client.workerDone = done
	client.status = ClientStatus{
		State: "starting", Title: "Запуск", Detail: "Проверяем базу EAVC и Telegram.",
		Configured: true, ClientID: cfg.ClientID, ClientName: cfg.ClientName,
		ClientNumber: cfg.ClientNumber, Version: client.Version,
	}
	client.mu.Unlock()
	go func() {
		defer close(done)
		client.run(workerContext, cfg)
	}()
	return nil
}

func (client *Client) stopWorker() {
	client.lifecycleMu.Lock()
	defer client.lifecycleMu.Unlock()
	client.stopWorkerLocked()
}

func (client *Client) stopWorkerLocked() {
	client.mu.Lock()
	cancel, done := client.workerStop, client.workerDone
	client.workerStop = nil
	client.workerDone = nil
	client.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (client *Client) run(ctx context.Context, cfg config.Client) {
	source, err := eavc.Open(ctx, cfg.EAVC.SQLitePath)
	if err != nil {
		client.fail("Ошибка базы EAVC", err)
		return
	}
	defer source.Close()
	db, err := stateDB.OpenState(filepath.Join(client.BaseDir, "data", "client.db"))
	if err != nil {
		client.fail("Ошибка локальной очереди", err)
		return
	}
	defer db.Close()
	queue, err := stateDB.NewClientQueue(ctx, db)
	if err != nil {
		client.fail("Ошибка локальной очереди", err)
		return
	}
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		client.fail("Ошибка Telegram", err)
		return
	}
	if err := api.DeleteWebhook(ctx, false); err != nil {
		client.fail("Ошибка Telegram", fmt.Errorf("отключить webhook: %w", err))
		return
	}
	key, _ := cfg.Key()
	service := &application.ClientService{
		ClientID: cfg.ClientID, Source: source, Queue: queue, State: queue,
		Sender: &telegram.ClientTransport{
			API: api, ChatID: cfg.Transport.ChatID, ThreadID: cfg.Transport.ThreadID,
			ServerBotUsername: cfg.Transport.ServerBotUsername,
		},
		KeyID: cfg.Cryptography.KeyID, Key: key, BatchSize: cfg.Polling.BatchSize,
		CatchUpExisting: cfg.Polling.CatchUpExisting,
		MaxEventAge:     time.Duration(cfg.Polling.MaxEventAgeMinutes) * time.Minute,
		AttemptTimeout:  20 * time.Second,
		Logger:          client.Logger,
		Now:             time.Now,
	}
	client.update(func(status *ClientStatus) {
		status.State = "running"
		status.Title = "Программа работает"
		status.Detail = "EAVC подключён. Новые данные передаются в течение 30 минут."
		status.DatabaseOK = true
		status.TelegramOK = true
	})

	go client.ackLoop(ctx, service, queue)
	go client.clientMaintenanceLoop(ctx, queue)
	ticker := time.NewTicker(time.Duration(cfg.Polling.EAVCSeconds) * time.Second)
	defer ticker.Stop()
	for {
		client.collectAndDispatch(ctx, service, queue)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (client *Client) clientMaintenanceLoop(ctx context.Context, queue *stateDB.ClientQueue) {
	ticker := time.NewTicker(maintenanceInterval)
	defer ticker.Stop()
	for {
		removed, err := queue.PurgeTerminal(ctx, time.Now().UTC().Add(-terminalRetention))
		if err != nil && ctx.Err() == nil {
			client.Logger.Warn("не удалось очистить завершённые события", "error", err)
		} else if removed > 0 {
			if err := queue.Compact(ctx); err != nil && ctx.Err() == nil {
				client.Logger.Warn("не удалось уплотнить базу клиента", "error", err)
			} else {
				client.Logger.Info("очищены завершённые события", "count", removed)
			}
			client.refreshStats(ctx, queue)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (client *Client) collectAndDispatch(ctx context.Context, service *application.ClientService, queue *stateDB.ClientQueue) {
	if _, err := service.CollectOnce(ctx); err != nil {
		client.transientFailure("Не удалось прочитать EAVC", err)
		return
	}
	now := time.Now().UTC()
	client.update(func(status *ClientStatus) {
		status.DatabaseOK = true
		status.LastDatabaseRead = &now
	})
	count, err := service.DispatchOnce(ctx)
	if err != nil {
		client.transientFailure("Ожидает интернет", err)
	} else if count > 0 {
		client.update(func(status *ClientStatus) {
			status.State = "running"
			status.Title = "Программа работает"
			status.Detail = "Новые данные загружены и ожидают подтверждения сервера."
			status.TelegramOK = true
			status.LastUpload = &now
		})
	}
	client.refreshStats(ctx, queue)
}

func (client *Client) ackLoop(ctx context.Context, service *application.ClientService, queue *stateDB.ClientQueue) {
	for ctx.Err() == nil {
		count, err := service.ReceiveAcknowledgements(ctx)
		if err != nil && ctx.Err() == nil {
			client.transientFailure("Нет подтверждения сервера", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if count > 0 {
			now := time.Now().UTC()
			client.update(func(status *ClientStatus) {
				status.State = "running"
				status.Title = "Программа работает"
				status.Detail = "Сервер подтвердил получение данных."
				status.LastAcknowledged = &now
			})
		}
		client.refreshStats(ctx, queue)
	}
}

func (client *Client) refreshStats(ctx context.Context, queue *stateDB.ClientQueue) {
	stats, err := queue.Stats(ctx)
	if err == nil {
		client.update(func(status *ClientStatus) { status.Queue = stats })
	}
}

func (client *Client) fail(title string, err error) {
	client.Logger.Error(title, "error", err)
	client.update(func(status *ClientStatus) {
		status.State = "error"
		status.Title = title
		status.Detail = err.Error()
	})
}

func (client *Client) transientFailure(title string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	client.Logger.Warn(title, "error", err)
	client.update(func(status *ClientStatus) {
		status.State = "waiting"
		status.Title = title
		status.Detail = "Данные сохранены на компьютере. Повтор будет выполнен автоматически. " + err.Error()
		status.TelegramOK = false
	})
}

func (client *Client) update(change func(*ClientStatus)) {
	client.mu.Lock()
	defer client.mu.Unlock()
	change(&client.status)
}

func (client *Client) configPath() string {
	return filepath.Join(client.BaseDir, "data", "client-config.dat")
}
