package runner

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"intercept/internal/domain"
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
	Processing         bool                `json:"processing"`
}

// QueueEventsPage — безопасная страница локальной очереди сообщений.
type QueueEventsPage struct {
	Items     []stateDB.QueueEvent `json:"items"`
	Total     int                  `json:"total"`
	Limit     int                  `json:"limit"`
	Offset    int                  `json:"offset"`
	State     string               `json:"state"`
	Clearable bool                 `json:"clearable"`
}

// QueueEvents показывает оператору сообщения выбранного этапа обработки.
func (client *Client) QueueEvents(ctx context.Context, state string, limit, offset int) (QueueEventsPage, error) {
	queue, closeQueue, err := client.openQueue(ctx)
	if err != nil {
		return QueueEventsPage{}, err
	}
	defer closeQueue()
	items, total, err := queue.EventsPage(ctx, state, limit, offset)
	if err != nil {
		return QueueEventsPage{}, err
	}
	for index := range items {
		items[index].Reason = publicQueueReason(state, items[index].Reason)
	}
	return QueueEventsPage{
		Items: items, Total: total, Limit: limit, Offset: offset, State: state,
		Clearable: state == "acknowledged" || state == "expired",
	}, nil
}

// ClearQueueEvents очищает только завершённые сообщения.
func (client *Client) ClearQueueEvents(ctx context.Context, state string) (int, error) {
	queue, closeQueue, err := client.openQueue(ctx)
	if err != nil {
		return 0, err
	}
	defer closeQueue()
	count, err := queue.ClearTerminal(ctx, state)
	if err != nil {
		return 0, err
	}
	stats, statsErr := queue.Stats(ctx)
	if statsErr == nil {
		client.update(func(status *ClientStatus) { status.Queue = stats })
	}
	return count, nil
}

func (client *Client) openQueue(ctx context.Context) (*stateDB.ClientQueue, func(), error) {
	db, err := stateDB.OpenState(filepath.Join(client.BaseDir, "data", "client.db"))
	if err != nil {
		return nil, func() {}, err
	}
	queue, err := stateDB.NewClientQueue(ctx, db)
	if err != nil {
		db.Close()
		return nil, func() {}, err
	}
	return queue, func() { _ = db.Close() }, nil
}

func publicQueueReason(state, reason string) string {
	if state == "pending" {
		return "Ожидает первой попытки отправки или следующего автоматического повтора."
	}
	if state == "uploaded" {
		return "Передано через Telegram, ожидается подтверждение центра."
	}
	if state == "acknowledged" {
		return "Центр подтвердил получение сообщения."
	}
	switch strings.TrimSpace(reason) {
	case "", domain.FreshnessExpired:
		return "Истёк допустимый срок отправки — 30 минут."
	default:
		return "Сообщение не удалось доставить за допустимое время."
	}
}

// StartProcessing запускает обработку вручную, используя сохранённые настройки.
func (client *Client) StartProcessing() error {
	if _, err := os.Stat(client.configPath()); err != nil {
		return errors.New("сначала загрузите настройки рабочей точки")
	}
	return client.restartWorker(client.applicationContext())
}

// StopProcessing безопасно останавливает обработку, не удаляя настройки и очередь.
func (client *Client) StopProcessing() {
	client.stopWorker()
	client.update(func(status *ClientStatus) {
		status.State = "paused"
		status.Title = "Обработка остановлена"
		status.Detail = "Настройки и очередь сохранены. Для продолжения нажмите «Старт»."
		status.Processing = false
		status.TelegramOK = false
	})
}

// RefreshProcessing перечитывает настройки и немедленно переподключает источники.
func (client *Client) RefreshProcessing() error {
	return client.StartProcessing()
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
	rootStop       context.CancelFunc
	background     sync.WaitGroup
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
	rootContext, rootStop := context.WithCancel(ctx)
	client.mu.Lock()
	client.rootContext = rootContext
	client.rootStop = rootStop
	client.mu.Unlock()
	if _, err := os.Stat(client.configPath()); errors.Is(err, os.ErrNotExist) {
		if installed, installErr := client.installProvisionedConfig(); installErr != nil {
			client.fail(
				"Не удалось применить подготовленные настройки",
				"Передайте администратору архив диагностики.",
				installErr,
			)
		} else if installed {
			client.Logger.Info("первичная настройка выполнена без действий оператора")
		}
	}
	if _, err := os.Stat(client.configPath()); err == nil {
		if err := client.restartWorker(rootContext); err != nil {
			client.fail(
				"Не удалось загрузить настройки",
				"Загрузите файл настроек рабочей точки ещё раз.",
				err,
			)
		}
	}
	client.background.Add(1)
	go func() {
		defer client.background.Done()
		<-rootContext.Done()
		client.stopWorker()
	}()
}

// Close дожидается завершения всех циклов клиента перед обновлением или выходом.
func (client *Client) Close() {
	client.mu.Lock()
	rootStop := client.rootStop
	client.rootStop = nil
	client.mu.Unlock()
	if rootStop != nil {
		rootStop()
	}
	client.stopWorker()
	client.background.Wait()
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

func (client *Client) fail(title, detail string, err error) {
	client.Logger.Error(title, "error", err)
	client.update(func(status *ClientStatus) {
		status.State = "error"
		status.Title = title
		status.Detail = detail
	})
}

func (client *Client) transientFailure(title, detail string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	client.Logger.Warn(title, "error", err)
	client.update(func(status *ClientStatus) {
		status.State = "waiting"
		status.Title = title
		status.Detail = detail
		status.TelegramOK = false
	})
}

func (client *Client) update(change func(*ClientStatus)) {
	client.mu.Lock()
	defer client.mu.Unlock()
	change(&client.status)
}
