package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

const (
	eavcCursorName         = "eavc"
	eavcInitializedName    = "eavc_initialized"
	eavcBaselineName       = "eavc_initial_baseline"
	transportOffsetName    = "transport_offset"
	lastTransportProbeName = "last_transport_probe"
	lastTransportProbeID   = "last_transport_probe_id"
)

// ClientService объединяет сценарии чтения EAVC, очереди и транспорта.
type ClientService struct {
	ClientID        string
	Source          EventSource
	Queue           ClientQueue
	State           ClientState
	Sender          TransportSender
	KeyID           string
	Key             []byte
	BatchSize       int
	CatchUpExisting bool
	MaxEventAge     time.Duration
	SourceLocation  *time.Location
	AttemptTimeout  time.Duration
	Logger          *slog.Logger
	Now             func() time.Time
}

// CollectOnce переносит одну порцию EAVC в локальную очередь.
func (service *ClientService) CollectOnce(ctx context.Context) (int, error) {
	if service.Now == nil {
		service.Now = time.Now
	}
	cursor, err := service.State.Cursor(ctx, eavcCursorName)
	if err != nil {
		return 0, err
	}
	_, initialized, err := service.State.Value(ctx, eavcInitializedName)
	if err != nil {
		return 0, err
	}
	if !initialized && !service.CatchUpExisting {
		if headSource, supported := service.Source.(EventSourceHead); supported {
			baseline, err := service.initialBaseline(ctx, headSource)
			if err != nil {
				return 0, err
			}
			if err := service.State.SaveCursor(ctx, eavcCursorName, baseline); err != nil {
				return 0, err
			}
			if err := service.State.SaveValue(ctx, eavcInitializedName, "true"); err != nil {
				return 0, err
			}
			return 0, nil
		}
	}
	items, next, err := service.Source.Scan(ctx, cursor, service.BatchSize)
	if err != nil {
		return 0, err
	}
	if !service.CatchUpExisting {
		baseline, exists, baselineErr := service.savedInitialBaseline(ctx)
		if baselineErr != nil {
			return 0, baselineErr
		}
		if exists {
			filtered := items[:0]
			for _, item := range items {
				if item.Record.UpdatedAt == baseline.UpdatedAt && item.Record.ID <= baseline.ID {
					continue
				}
				filtered = append(filtered, item)
			}
			items = filtered
		}
	}
	if !initialized && !service.CatchUpExisting {
		if err := service.State.SaveCursor(ctx, eavcCursorName, next); err != nil {
			return 0, err
		}
		if len(items) < service.BatchSize {
			if err := service.State.SaveValue(ctx, eavcInitializedName, "true"); err != nil {
				return 0, err
			}
		}
		return 0, nil
	}

	queued := 0
	for _, item := range items {
		event, err := service.newEvent(item)
		if err != nil {
			// Одна повреждённая строка внешней БД не должна навсегда закрывать
			// последующие корректные перехваты. Источник повторно отдаёт граничную
			// секунду, поэтому исправленная оператором запись будет принята позже.
			if service.Logger != nil {
				service.Logger.Warn("запись EAVC отклонена до постановки в очередь",
					"source_id", item.Record.ID, "source_uuid", item.Record.UUID,
					"updated_at", item.Record.UpdatedAt, "error", err,
				)
			}
			continue
		}
		if err := service.Queue.Enqueue(ctx, event); err != nil {
			return queued, err
		}
		if service.Logger != nil {
			level := slog.LevelInfo
			message := "событие EAVC поставлено в очередь"
			if event.FreshnessIssue != "" {
				level = slog.LevelWarn
				message = "событие EAVC не будет отправлено из-за срока"
			}
			service.Logger.Log(ctx, level, message,
				"event_id", event.ID, "source_uuid", event.Source.UUID,
				"source_id", event.Source.ID, "position", event.Position.UnitName,
				"source_at", event.SourceAt, "expires_at", event.ExpiresAt,
				"reason", event.FreshnessIssue,
			)
		}
		queued++
	}
	if err := service.State.SaveCursor(ctx, eavcCursorName, next); err != nil {
		return queued, err
	}
	if !initialized {
		if err := service.State.SaveValue(ctx, eavcInitializedName, "true"); err != nil {
			return queued, err
		}
	}
	return queued, nil
}

func (service *ClientService) savedInitialBaseline(ctx context.Context) (Cursor, bool, error) {
	raw, exists, err := service.State.Value(ctx, eavcBaselineName)
	if err != nil || !exists {
		return Cursor{}, exists, err
	}
	var baseline Cursor
	if err := json.Unmarshal([]byte(raw), &baseline); err != nil {
		return Cursor{}, false, fmt.Errorf("прочитать сохранённую базовую линию EAVC: %w", err)
	}
	return baseline, true, nil
}

func (service *ClientService) initialBaseline(ctx context.Context, source EventSourceHead) (Cursor, error) {
	if baseline, exists, err := service.savedInitialBaseline(ctx); err != nil || exists {
		return baseline, err
	}
	baseline, err := source.Head(ctx)
	if err != nil {
		return Cursor{}, err
	}
	payload, err := json.Marshal(baseline)
	if err != nil {
		return Cursor{}, err
	}
	if err := service.State.SaveValue(ctx, eavcBaselineName, string(payload)); err != nil {
		return Cursor{}, err
	}
	return baseline, nil
}

// DispatchOnce загружает ожидающие события. Подтверждение обрабатывается отдельно.
func (service *ClientService) DispatchOnce(ctx context.Context) (int, error) {
	if service.Now == nil {
		service.Now = time.Now
	}
	now := service.Now().UTC()
	if _, err := service.Queue.ExpireDue(ctx, now); err != nil {
		return 0, err
	}
	// Одновременно выбираются первые события разных подразделений. Внутри одного
	// подразделения более поздний перехват всё равно ждёт подтверждения раннего.
	events, err := service.Queue.Pending(ctx, 4, now)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, queued := range events {
		event := queued.Event
		if event.Expired(now) && queued.State != "uploaded" {
			if err := service.Queue.MarkExpired(ctx, event.ID, now, domain.FreshnessExpired); err != nil {
				return sent, err
			}
			continue
		}
		envelope, err := protocol.Seal(event, service.KeyID, service.Key, now, rand.Reader)
		if err != nil {
			return sent, err
		}
		attemptTimeout := service.AttemptTimeout
		if attemptTimeout <= 0 {
			attemptTimeout = 20 * time.Second
		}
		sendContext, cancel := context.WithTimeout(ctx, attemptTimeout)
		messageID, err := service.Sender.Send(sendContext, envelope)
		cancel()
		if err != nil {
			next := now.Add(retryDelayFor(err, queued.Attempts+1))
			if markErr := service.Queue.MarkRetry(ctx, event.ID, next, err.Error()); markErr != nil {
				return sent, fmt.Errorf("отправка: %v; сохранить повтор: %w", err, markErr)
			}
			if service.Logger != nil {
				service.Logger.Warn("передача события не удалась, назначен повтор",
					"event_id", event.ID, "attempt", queued.Attempts+1,
					"next_attempt_at", next, "error", err,
				)
			}
			continue
		}
		if err := service.Queue.MarkUploaded(ctx, event.ID, messageID); err != nil {
			return sent, err
		}
		if service.Logger != nil {
			service.Logger.Info("событие загружено в транспортную группу",
				"event_id", event.ID, "telegram_message_id", messageID,
				"attempt", queued.Attempts+1,
			)
		}
		sent++
	}
	return sent, nil
}

// AcknowledgementBatch разделяет подтверждения данных и контрольных пакетов.
// Благодаря этому автоматический heartbeat не имитирует доставку перехвата и
// не может затереть ответ на ручную проверку связи.
type AcknowledgementBatch struct {
	Events int
	Probes map[string]time.Time
}

// ReceiveAcknowledgements подтверждает только ответы серверного бота.
func (service *ClientService) ReceiveAcknowledgements(ctx context.Context) (AcknowledgementBatch, error) {
	result := AcknowledgementBatch{Probes: make(map[string]time.Time)}
	offset := int64(0)
	if raw, exists, err := service.State.Value(ctx, transportOffsetName); err != nil {
		return result, err
	} else if exists {
		offset, _ = strconv.ParseInt(raw, 10, 64)
	}
	acknowledgements, next, err := service.Sender.Acknowledgements(ctx, offset)
	if err != nil {
		return result, err
	}
	for _, acknowledgement := range acknowledgements {
		if acknowledgement.Status == "probe" {
			result.Probes[acknowledgement.EventID] = acknowledgement.AcceptedAt
			if err := service.State.SaveValue(ctx, lastTransportProbeName, acknowledgement.AcceptedAt.Format(time.RFC3339Nano)); err != nil {
				return result, err
			}
			if err := service.State.SaveValue(ctx, lastTransportProbeID, acknowledgement.EventID); err != nil {
				return result, err
			}
			continue
		}
		var err error
		if acknowledgement.Status == "expired" {
			err = service.Queue.MarkExpired(ctx, acknowledgement.EventID, acknowledgement.AcceptedAt, domain.FreshnessExpired)
		} else {
			err = service.Queue.MarkAcknowledged(ctx, acknowledgement.EventID, acknowledgement.AcceptedAt)
		}
		if err != nil {
			return result, err
		}
		result.Events++
		if service.Logger != nil {
			service.Logger.Info("получено подтверждение сервера",
				"event_id", acknowledgement.EventID, "status", acknowledgement.Status,
				"accepted_at", acknowledgement.AcceptedAt,
			)
		}
	}
	if next > offset {
		if err := service.State.SaveValue(ctx, transportOffsetName, strconv.FormatInt(next, 10)); err != nil {
			return result, err
		}
	}
	return result, nil
}

// LastTransportProbe возвращает идентификатор и время последней полной
// проверки клиент → сервер → клиент.
func (service *ClientService) LastTransportProbe(ctx context.Context) (string, time.Time, bool, error) {
	raw, exists, err := service.State.Value(ctx, lastTransportProbeName)
	if err != nil || !exists {
		return "", time.Time{}, exists, err
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return "", time.Time{}, false, fmt.Errorf("прочитать время проверки транспорта: %w", err)
	}
	probeID, idExists, err := service.State.Value(ctx, lastTransportProbeID)
	if err != nil {
		return "", time.Time{}, false, err
	}
	return probeID, value, idExists, nil
}

func (service *ClientService) newEvent(item SourceItem) (domain.Event, error) {
	now := service.Now().UTC()
	maxAge := service.MaxEventAge
	if maxAge <= 0 {
		maxAge = 30 * time.Minute
	}
	sourceAt, expiresAt, freshnessIssue := domain.FreshnessInLocation(
		item.Record.UpdatedAt, item.ObservedAt, now, maxAge, service.SourceLocation,
	)
	id, err := protocol.UUIDv7(now, rand.Reader)
	if err != nil {
		return domain.Event{}, err
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		normalizeContent(item.Content), domain.NormalizeUnitName(item.Position.UnitName),
		strings.TrimSpace(item.Position.Frequency), strings.TrimSpace(item.Position.GroupCode),
	}, "\x01")))
	event := domain.Event{
		ID: id, ClientID: service.ClientID, CreatedAt: now, ObservedAt: item.ObservedAt,
		SourceAt: sourceAt, ExpiresAt: expiresAt, FreshnessIssue: freshnessIssue,
		Source: item.Record, Position: item.Position, Mode: item.Mode,
		Content: normalizeContent(item.Content), Blocks: item.Blocks, UpdatedBy: item.UpdatedBy,
		ContentDigest: hex.EncodeToString(digest[:]),
	}
	return event, event.Validate()
}

func normalizeContent(content string) string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.ReplaceAll(content, "\r", "\n")
	return strings.TrimSpace(content)
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return time.Duration(1<<uint(attempt-1)) * 5 * time.Second
}

func retryDelayFor(err error, attempt int) time.Duration {
	delay := retryDelay(attempt)
	var limited interface{ RetryAfter() time.Duration }
	if errors.As(err, &limited) {
		// Секунда запаса исключает повтор на самой границе retry_after.
		if requested := limited.RetryAfter() + time.Second; requested > delay {
			return requested
		}
	}
	return delay
}
