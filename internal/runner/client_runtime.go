package runner

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
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
	eavcPath, err := client.resolveEAVCPath(ctx, cfg)
	if err != nil {
		client.fail("Не найдена база EAVC", err)
		return
	}
	source, err := eavc.Open(ctx, eavcPath)
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
	location, err := time.LoadLocation(cfg.EAVCTimeZone())
	if err != nil {
		client.fail("Ошибка часового пояса", err)
		return
	}
	service := &application.ClientService{
		ClientID: cfg.ClientID, Source: source, Queue: queue, State: queue,
		Sender: &telegram.ClientTransport{
			API: api, ChatID: cfg.Transport.ChatID, ThreadID: cfg.Transport.ThreadID,
			ServerBotID:       cfg.Transport.ServerBotID,
			ServerBotUsername: cfg.Transport.ServerBotUsername,
		},
		KeyID: cfg.Cryptography.KeyID, Key: key, BatchSize: cfg.Polling.BatchSize,
		CatchUpExisting: cfg.Polling.CatchUpExisting,
		MaxEventAge:     time.Duration(cfg.Polling.MaxEventAgeMinutes) * time.Minute,
		SourceLocation:  location,
		AttemptTimeout:  20 * time.Second,
		Logger:          client.Logger,
		Now:             time.Now,
	}
	client.update(func(status *ClientStatus) {
		status.State = "running"
		status.Title = "Программа работает"
		status.Detail = "EAVC подключён."
		status.DatabaseOK = true
		status.TelegramOK = true
		status.EAVCPath = eavcPath
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
		client.transientFailure(
			"Не удалось прочитать EAVC",
			"База EAVC временно недоступна. Программа повторит чтение автоматически.",
			err,
		)
		return
	}
	now := time.Now().UTC()
	client.update(func(status *ClientStatus) {
		status.DatabaseOK = true
		status.LastDatabaseRead = &now
	})
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
		count, err := service.ReceiveAcknowledgements(ctx)
		if err != nil && ctx.Err() == nil {
			client.transientFailure(
				"Нет подтверждения центра",
				"Связь временно прервалась. Программа повторит проверку автоматически; данные сохранены.",
				err,
			)
			time.Sleep(5 * time.Second)
			continue
		}
		client.transportRecovered("EAVC подключён.")
		if count > 0 {
			now := time.Now().UTC()
			probeID, probeAt, probeExists, _ := service.LastTransportProbe(ctx)
			client.update(func(status *ClientStatus) {
				if client.pendingProbeID != "" && (!probeExists || probeID != client.pendingProbeID) {
					// Обычное подтверждение или старый ответ не завершает новую проверку.
					return
				}
				status.State = "running"
				status.Title = "Программа работает"
				status.Detail = "Центр подтвердил получение данных."
				status.LastAcknowledged = &now
				if probeExists && probeID == client.pendingProbeID {
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
