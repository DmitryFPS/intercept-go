package runner

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"time"

	stateDB "intercept/internal/infrastructure/sqlite"
)

// ClientStatus содержит только безопасные для экрана и диагностики данные.
type ClientStatus struct {
	State              string              `json:"state"`
	Title              string              `json:"title"`
	Detail             string              `json:"detail"`
	Configured         bool                `json:"configured"`
	ClientID           string              `json:"client_id,omitempty"`
	ClientName         string              `json:"client_name,omitempty"`
	ClientNumber       string              `json:"client_number,omitempty"`
	DatabaseOK         bool                `json:"database_ok"`
	TelegramOK         bool                `json:"telegram_ok"`
	LastDatabaseRead   *time.Time          `json:"last_database_read,omitempty"`
	LastUpload         *time.Time          `json:"last_upload,omitempty"`
	LastAcknowledged   *time.Time          `json:"last_acknowledged,omitempty"`
	LastTransportCheck *time.Time          `json:"last_transport_check,omitempty"`
	EAVCPath           string              `json:"eavc_path,omitempty"`
	Queue              stateDB.ClientStats `json:"queue"`
	Version            string              `json:"version"`
}

// Client управляет жизненным циклом переносимого приложения.
type Client struct {
	BaseDir string
	Version string
	Logger  *slog.Logger

	mu             sync.RWMutex
	lifecycleMu    sync.Mutex
	status         ClientStatus
	workerStop     context.CancelFunc
	workerDone     chan struct{}
	rootContext    context.Context
	pendingProbeID string
}

// NewClient собирает переносимое приложение вокруг выбранной рабочей папки.
func NewClient(baseDir, version string, logger *slog.Logger) *Client {
	return &Client{
		BaseDir: baseDir, Version: version, Logger: logger,
		status: ClientStatus{State: "setup", Title: "Нужны настройки", Detail: "Загрузите файл настроек, который прислал администратор.", Version: version},
	}
}

// Start загружает сохранённые настройки и поддерживает рабочий процесс до
// отмены переданного контекста.
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

// Status возвращает безопасный снимок состояния для экрана оператора.
func (client *Client) Status() ClientStatus {
	client.mu.RLock()
	defer client.mu.RUnlock()
	return client.status
}

func (client *Client) applicationContext() context.Context {
	client.mu.RLock()
	defer client.mu.RUnlock()
	if client.rootContext != nil {
		return client.rootContext
	}
	return context.Background()
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
