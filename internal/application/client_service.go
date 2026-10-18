package application

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/domain"
	"intercept/internal/protocol"
)

const (
	eavcCursorName      = "eavc"
	eavcInitializedName = "eavc_initialized"
	transportOffsetName = "transport_offset"
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
	AttemptTimeout  time.Duration
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
	items, next, err := service.Source.Scan(ctx, cursor, service.BatchSize)
	if err != nil {
		return 0, err
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
			return queued, err
		}
		if err := service.Queue.Enqueue(ctx, event); err != nil {
			return queued, err
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

// DispatchOnce загружает ожидающие события. Подтверждение обрабатывается отдельно.
func (service *ClientService) DispatchOnce(ctx context.Context) (int, error) {
	if service.Now == nil {
		service.Now = time.Now
	}
	now := service.Now().UTC()
	if _, err := service.Queue.ExpireDue(ctx, now); err != nil {
		return 0, err
	}
	// Одна точка отправляет события строго по порядку. Следующее событие не
	// обгоняет предыдущее, пока оно не подтверждено или не просрочено.
	events, err := service.Queue.Pending(ctx, 1, now)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, queued := range events {
		event := queued.Event
		if event.Expired(now) {
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
			next := now.Add(retryDelay(queued.Attempts + 1))
			if markErr := service.Queue.MarkRetry(ctx, event.ID, next, err.Error()); markErr != nil {
				return sent, fmt.Errorf("отправка: %v; сохранить повтор: %w", err, markErr)
			}
			continue
		}
		if err := service.Queue.MarkUploaded(ctx, event.ID, messageID); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

// ReceiveAcknowledgements подтверждает только ответы серверного бота.
func (service *ClientService) ReceiveAcknowledgements(ctx context.Context) (int, error) {
	offset := int64(0)
	if raw, exists, err := service.State.Value(ctx, transportOffsetName); err != nil {
		return 0, err
	} else if exists {
		offset, _ = strconv.ParseInt(raw, 10, 64)
	}
	acknowledgements, next, err := service.Sender.Acknowledgements(ctx, offset)
	if err != nil {
		return 0, err
	}
	for _, acknowledgement := range acknowledgements {
		var err error
		if acknowledgement.Status == "expired" {
			err = service.Queue.MarkExpired(ctx, acknowledgement.EventID, acknowledgement.AcceptedAt, domain.FreshnessExpired)
		} else {
			err = service.Queue.MarkAcknowledged(ctx, acknowledgement.EventID, acknowledgement.AcceptedAt)
		}
		if err != nil {
			return 0, err
		}
	}
	if next > offset {
		if err := service.State.SaveValue(ctx, transportOffsetName, strconv.FormatInt(next, 10)); err != nil {
			return 0, err
		}
	}
	return len(acknowledgements), nil
}

func (service *ClientService) newEvent(item SourceItem) (domain.Event, error) {
	now := service.Now().UTC()
	maxAge := service.MaxEventAge
	if maxAge <= 0 {
		maxAge = 30 * time.Minute
	}
	sourceAt, expiresAt, freshnessIssue := domain.Freshness(
		item.Record.UpdatedAt, item.ObservedAt, now, maxAge,
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
