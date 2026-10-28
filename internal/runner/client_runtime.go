package runner

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/infrastructure/eavc"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/infrastructure/telegram"
)

type localQueueHandle struct {
	database *sql.DB
	queue    *stateDB.ClientQueue
}

type localQueueOpener func(context.Context, string) (localQueueHandle, error)

var errLocalStorageRetryCycle = errors.New("цикл восстановления локального хранилища завершён")

func openLocalQueueOnce(ctx context.Context, path string) (localQueueHandle, error) {
	database, err := stateDB.OpenState(path)
	if err != nil {
		return localQueueHandle{}, err
	}
	queue, err := stateDB.NewClientQueue(ctx, database)
	if err != nil {
		_ = database.Close()
		return localQueueHandle{}, err
	}
	return localQueueHandle{database: database, queue: queue}, nil
}

func (client *Client) waitForLocalQueue(ctx context.Context, path string) (localQueueHandle, error) {
	delay := client.storageRetry
	if delay <= 0 {
		delay = 5 * time.Second
	}
	cycle := client.storageCycle
	if cycle <= 0 {
		cycle = 2 * time.Minute
	}
	started := time.Now()
	attempts := 0
	for {
		attempts++
		handle, err := client.storageOpener(ctx, path)
		if err == nil {
			return handle, nil
		}
		if stateDB.IsPermanentMigrationError(err) {
			return localQueueHandle{}, err
		}
		client.transientFailure(
			"Локальное хранилище временно недоступно",
			"Данные сохранены. Программа повторит открытие автоматически.",
			err,
		)
		// Даже если первая попытка сама заняла весь короткий цикл, выполняем
		// хотя бы один реальный повтор. Это важно после краткой блокировки SQLite
		// антивирусом или предыдущим процессом во время обновления.
		if attempts >= 2 && time.Since(started) >= cycle {
			return localQueueHandle{}, errLocalStorageRetryCycle
		}
		if !waitForRetry(ctx, delay) {
			return localQueueHandle{}, ctx.Err()
		}
		if delay < time.Minute {
			delay *= 2
			if delay > time.Minute {
				delay = time.Minute
			}
		}
	}
}

// runSupervised перезапускает весь рабочий цикл после затянувшегося временного
// сбоя хранилища. Это заново открывает источник EAVC и SQLite после перезагрузки
// Windows, не требуя от оператора нажимать «Стоп» и «Старт».
func (client *Client) runSupervised(ctx context.Context, cfg config.Client) {
	for {
		runErr := client.run(ctx, cfg)
		if errors.Is(runErr, errLocalStorageRetryCycle) && client.requestProcessRestart() {
			return
		}
		if !shouldRestartClientCycle(ctx.Err(), client.Status().State) {
			return
		}
		if !waitForRetry(ctx, 5*time.Second) {
			return
		}
		client.update(func(status *ClientStatus) {
			status.State = "starting"
			status.Title = "Повторный запуск"
			status.Detail = "Автоматически восстанавливаем локальное хранилище и связь."
		})
	}
}

func shouldRestartClientCycle(ctxErr error, state string) bool {
	return ctxErr == nil && state == "waiting"
}

const (
	maintenanceInterval = 6 * time.Hour
	terminalRetention   = 30 * 24 * time.Hour
)

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
	if _, _, err := client.verifyTrustedIssuer(cfg); err != nil {
		return err
	}
	client.stopWorkerLocked()
	workerContext, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	client.mu.Lock()
	client.workerStop = cancel
	client.workerDone = done
	client.pendingProbeID = ""
	client.status = ClientStatus{
		State: "starting", Title: "Запуск", Detail: "Проверяем базу EAVC и Telegram.",
		Configured: true, ClientID: cfg.ClientID, ClientName: cfg.ClientName,
		ClientNumber: cfg.ClientNumber, Version: client.Version, Processing: true,
	}
	client.mu.Unlock()
	go func() {
		defer close(done)
		client.runSupervised(workerContext, cfg)
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
	client.update(func(status *ClientStatus) { status.Processing = false })
}

func (client *Client) run(ctx context.Context, cfg config.Client) error {
	// Локальная очередь и транспорт открываются до EAVC. Так сохранённые пакеты
	// продолжают уходить в центр даже при холодном запуске без доступной базы EAVC.
	storage, err := client.waitForLocalQueue(ctx, filepath.Join(client.BaseDir, "data", "client.db"))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errLocalStorageRetryCycle) {
			return err
		}
		client.fail(
			"Не удалось открыть локальное хранилище",
			"Схема локального хранилища несовместима с этой версией. Установите актуальное обновление.",
			err,
		)
		return err
	}
	db, queue := storage.database, storage.queue
	defer db.Close()
	api, err := telegram.New(cfg.Transport.BotToken, nil)
	if err != nil {
		client.fail(
			"Неверные настройки Telegram",
			"Загрузите новый файл настроек, созданный в центре управления.",
			err,
		)
		return err
	}
	for {
		if err := api.DeleteWebhook(ctx, false); err == nil {
			break
		} else {
			client.transientFailure(
				"Telegram временно недоступен",
				"Данные сохранены. Программа повторит подключение автоматически.",
				err,
			)
		}
		if !waitForRetry(ctx, 5*time.Second) {
			return ctx.Err()
		}
	}
	key, _ := cfg.Key()
	location, err := time.LoadLocation(cfg.EAVCTimeZone())
	if err != nil {
		client.fail(
			"Неверный часовой пояс",
			"Создайте файл настроек рабочей точки заново.",
			err,
		)
		return err
	}
	transport := &telegram.ClientTransport{
		API: api, ChatID: cfg.Transport.ChatID, ThreadID: cfg.Transport.ThreadID,
		ServerBotID:       cfg.Transport.ServerBotID,
		ServerBotUsername: cfg.Transport.ServerBotUsername,
		Key:               key,
	}
	service := &application.ClientService{
		ClientID: cfg.ClientID, Queue: queue, State: queue, TimeState: queue,
		Sender: transport,
		KeyID:  cfg.Cryptography.KeyID, Key: key, BatchSize: cfg.Polling.BatchSize,
		CatchUpExisting: cfg.Polling.CatchUpExisting,
		MaxEventAge:     time.Duration(cfg.Polling.MaxEventAgeMinutes) * time.Minute,
		SourceLocation:  location,
		AttemptTimeout:  20 * time.Second,
		Logger:          client.Logger,
		Now:             time.Now,
	}
	client.update(func(status *ClientStatus) { status.TelegramOK = true })

	var background sync.WaitGroup
	background.Add(3)
	go func() { defer background.Done(); client.ackLoop(ctx, service, queue) }()
	go func() { defer background.Done(); client.clientMaintenanceLoop(ctx, queue) }()
	go func() { defer background.Done(); client.heartbeatLoop(ctx, cfg.ClientID, transport) }()
	defer background.Wait()
	ticker := time.NewTicker(time.Duration(cfg.Polling.EAVCSeconds) * time.Second)
	defer ticker.Stop()
	var source *eavc.Source
	defer func() {
		if source != nil {
			_ = source.Close()
		}
	}()
	for {
		if source == nil {
			eavcPath, pathErr := client.resolveEAVCPath(ctx, cfg)
			if pathErr != nil {
				client.sourceTransientFailure(
					"Ожидаем базу EAVC",
					"Сохранённая очередь продолжает отправляться; путь EAVC будет проверен снова.",
					pathErr,
				)
			} else {
				opened, openErr := eavc.Open(ctx, eavcPath)
				if openErr != nil {
					client.sourceTransientFailure(
						"База EAVC временно недоступна",
						"Сохранённая очередь продолжает отправляться; подключение будет повторено.",
						openErr,
					)
				} else {
					source = opened
					service.Source = source
					client.update(func(status *ClientStatus) {
						status.State = "running"
						status.Title = "Программа работает"
						status.Detail = "EAVC подключён."
						status.DatabaseOK = true
						status.TelegramOK = true
						status.EAVCPath = eavcPath
					})
				}
			}
		}
		if source == nil {
			client.dispatchSavedQueue(ctx, service, queue)
		} else {
			client.refreshShiftSnapshot(ctx, source, location)
			client.collectAndDispatch(ctx, service, queue)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (client *Client) dispatchSavedQueue(ctx context.Context, service *application.ClientService, queue *stateDB.ClientQueue) {
	now := time.Now().UTC()
	count, err := service.DispatchOnce(ctx)
	if err != nil {
		client.transientFailure(
			"Telegram временно недоступен",
			"Данные сохранены на компьютере. Программа повторит отправку автоматически.",
			err,
		)
	} else {
		client.transportRecovered("Сохранённая очередь передаётся; ожидаем EAVC.")
	}
	if err == nil && count > 0 {
		client.update(func(status *ClientStatus) {
			status.TelegramOK = true
			status.LastUpload = &now
		})
	}
	client.refreshStats(ctx, queue)
}

// heartbeatLoop отправляет редкий служебный пакет, чтобы центр различал
// исправную точку и длительное отсутствие связи даже при отсутствии перехватов.
// Один пакет раз в две минуты значительно ниже ограничений Telegram.
func (client *Client) heartbeatLoop(ctx context.Context, clientID string, transport *telegram.ClientTransport) {
	timer := time.NewTimer(15 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			probeCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			_, err := transport.SendProbe(probeCtx, clientID, client.currentShiftSnapshot())
			cancel()
			if err != nil && ctx.Err() == nil {
				client.Logger.Warn("не удалось отправить контрольный пакет связи", "error", err)
			}
			timer.Reset(2 * time.Minute)
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
		if !errors.Is(err, context.Canceled) {
			client.Logger.Warn("Не удалось прочитать EAVC", "error", err)
			client.update(func(status *ClientStatus) {
				status.State = "waiting"
				status.Title = "Не удалось прочитать EAVC"
				status.Detail = "База EAVC временно недоступна. Сохранённая очередь продолжает отправляться."
				status.DatabaseOK = false
			})
		}
	} else {
		now := time.Now().UTC()
		client.update(func(status *ClientStatus) {
			status.DatabaseOK = true
			status.LastDatabaseRead = &now
			if status.State == "waiting" && status.Title == "Не удалось прочитать EAVC" {
				status.State = "running"
				status.Title = "Программа работает"
				status.Detail = "EAVC подключён."
			}
		})
	}
	now := time.Now().UTC()
	count, err := service.DispatchOnce(ctx)
	if err != nil {
		client.transientFailure(
			"Telegram временно недоступен",
			"Данные сохранены на компьютере. Программа повторит отправку автоматически.",
			err,
		)
	} else {
		client.transportRecovered("EAVC подключён.")
	}
	if err == nil && count > 0 {
		client.update(func(status *ClientStatus) {
			status.State = "running"
			status.Title = "Программа работает"
			status.Detail = "Новые данные загружены и ожидают подтверждения центра."
			status.TelegramOK = true
			status.LastUpload = &now
		})
	}
	client.refreshStats(ctx, queue)
}

func (client *Client) ackLoop(ctx context.Context, service *application.ClientService, queue *stateDB.ClientQueue) {
	for ctx.Err() == nil {
		batch, err := service.ReceiveAcknowledgements(ctx)
		if err != nil && ctx.Err() == nil {
			client.transientFailure(
				"Нет подтверждения центра",
				"Связь временно прервалась.",
				err,
			)
			if !waitForRetry(ctx, 5*time.Second) {
				return
			}
			continue
		}
		client.transportRecovered("EAVC подключён.")
		if batch.Events > 0 || len(batch.Probes) > 0 {
			now := time.Now().UTC()
			client.update(func(status *ClientStatus) {
				if batch.Events > 0 {
					status.State = "running"
					status.Title = "Программа работает"
					status.Detail = "Центр подтвердил получение данных."
					status.LastAcknowledged = &now
				}
				if probeAt, confirmed := batch.Probes[client.pendingProbeID]; client.pendingProbeID != "" && confirmed {
					status.State = "running"
					status.Title = "Программа работает"
					status.LastTransportCheck = &probeAt
					status.Detail = "Двусторонняя связь с центром проверена."
					client.pendingProbeID = ""
				}
			})
		}
		client.refreshStats(ctx, queue)
	}
}

func (client *Client) transportRecovered(detail string) {
	client.update(func(status *ClientStatus) {
		status.TelegramOK = true
		if status.State != "waiting" ||
			(status.Title != "Telegram временно недоступен" && status.Title != "Нет подтверждения центра") {
			return
		}
		status.State = "running"
		status.Title = "Программа работает"
		status.Detail = detail
	})
}

func (client *Client) refreshStats(ctx context.Context, queue *stateDB.ClientQueue) {
	stats, err := queue.Stats(ctx)
	if err == nil {
		client.update(func(status *ClientStatus) { status.Queue = stats })
	}
}
