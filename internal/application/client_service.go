package application

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

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
	TimeState       SourceTimeStateStore
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

	timeState := service.TimeState
	if timeState == nil {
		if storedByQueue, ok := service.Queue.(SourceTimeStateStore); ok {
			timeState = storedByQueue
		}
	}
	queued := 0
	for _, item := range items {
		if strings.TrimSpace(item.ContractError) != "" {
			if service.Logger != nil {
				service.Logger.Error("нарушена обратная совместимость записи EAVC; запись изолирована",
					"source_id", item.Record.ID, "source_uuid", item.Record.UUID,
					"source_version", item.Record.UpdatedAt, "position", item.Position.UnitName,
					"frequency", item.Position.Frequency, "group_code", item.Position.GroupCode,
					"reason", item.ContractError,
				)
			}
			continue
		}
		if len(item.Blocks) > 0 || len(item.DeletedBlocks) > 0 {
			var stored SourceTimeStateRecord
			var exists bool
			var stateKey string
			if timeState != nil {
				stateKey = sourceTimeStateKey(item)
				var stateErr error
				stored, exists, stateErr = timeState.LoadSourceTimeState(ctx, stateKey)
				if stateErr != nil {
					return queued, stateErr
				}
			}
			originalStored := stored
			if item.ArchiveMetadataAmbiguous {
				var safe bool
				observed, observeErr := observeSourceTimeBlocks(item.Blocks)
				if observeErr == nil && exists && stored.Snapshot != nil &&
					sameArchivedTimeObservations(item, observed, *stored.Snapshot) {
					if sourceTimeSnapshotHasMetadata(*stored.Snapshot) {
						safe = sameArchivedSourceMetadata(*stored.Snapshot, item)
					} else if metadata, ok := service.Queue.(ArchivedTimeMetadataStore); ok {
						mode, catalogUUID, found, recoveryErr := metadata.RecoverArchivedTimeMetadata(ctx, item, *stored.Snapshot)
						if recoveryErr != nil {
							return queued, recoveryErr
						}
						if found {
							snapshot := *stored.Snapshot
							position := item.Position
							position.CatalogUUID = catalogUUID
							snapshot.SourceMode = mode
							snapshot.SourcePosition = &position
							snapshot.SourceContentHash = sourceTimeContentHash(item.Content)
							stored.Snapshot = &snapshot
							stored.Conflict = nil
							safe = true
						}
					}
				}
				if !safe {
					conflict := newSourceTimeConflict(
						"archive_metadata_unverifiable",
						"архивная запись не имеет доказанных данных маршрута",
						item.Record.UpdatedAt, "", 0, nil, len(observed), service.Now().UTC(),
					)
					quarantined := SourceTimeStateRecord{Snapshot: stored.Snapshot, Conflict: conflict}
					if timeState != nil && !sourceTimeStateRecordsEqual(originalStored, quarantined) {
						if err := timeState.SaveSourceTimeState(ctx, stateKey, quarantined); err != nil {
							return queued, err
						}
					}
					if service.Logger != nil {
						service.Logger.Warn("архивная запись EAVC изолирована до доказательства маршрута",
							"source_id", item.Record.ID, "source_uuid", item.Record.UUID,
							"source_version", item.Record.UpdatedAt, "reason", conflict.Reason,
						)
					}
					continue
				}
				stored.Conflict = nil
			}
			prepared := planSourceTimeBlocks(item, stored, exists, service.Now().UTC())
			// Состояние сохраняется до очереди: после аварийного завершения повтор
			// получит те же occurrence-ID. Tombstone остаются в снимке и поэтому
			// удаление также не потеряется при таком повторе.
			if timeState != nil && (!exists || !sourceTimeStateRecordsEqual(originalStored, prepared.Record)) {
				if stateErr := timeState.SaveSourceTimeState(ctx, stateKey, prepared.Record); stateErr != nil {
					return queued, stateErr
				}
			}
			if prepared.Conflict != nil {
				if service.Logger != nil {
					service.Logger.Warn("Конфликт сопоставления временных блоков EAVC",
						"source_id", item.Record.ID, "source_uuid", item.Record.UUID,
						"source_version", item.Record.UpdatedAt, "position", item.Position.UnitName,
						"frequency", item.Position.Frequency, "group_code", item.Position.GroupCode,
						"conflict_kind", prepared.Conflict.Kind, "reason", prepared.Conflict.Reason,
						"canonical_time", prepared.Conflict.CanonicalTime,
						"day_offset", prepared.Conflict.DayOffset,
						"previous_occurrence_ids", strings.Join(prepared.Conflict.PreviousOccurrenceIDs, ","),
						"current_occurrences", prepared.Conflict.CurrentOccurrences,
					)
				}
				continue
			}
			item = prepared.Item
		}
		events, err := service.newEvents(item)
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
		if len(item.Blocks) > 0 || len(item.DeletedBlocks) > 0 {
			legacySourceUUID := strings.TrimSpace(item.Record.UUID)
			if legacySourceUUID == "" {
				legacySourceUUID = fmt.Sprintf("id:%d", item.Record.ID)
			}
			if item.Archived {
				archivedReplay, archiveErr := service.Queue.SeedArchiveBaselinesFromSeenBlocks(
					ctx, legacySourceUUID, strings.TrimSpace(item.Record.UpdatedAt), events,
				)
				if archiveErr != nil {
					return queued, archiveErr
				}
				if archivedReplay {
					if service.Logger != nil {
						service.Logger.Info("архивная копия уже доставленных блоков EAVC подавлена",
							"source_uuid", item.Record.UUID, "source_id", item.Record.ID,
							"source_version", item.Record.UpdatedAt, "content_blocks", len(events),
						)
					}
					continue
				}
			}
		}
		for _, event := range events {
			inserted, err := service.Queue.Enqueue(ctx, event)
			if err != nil {
				return queued, err
			}
			if !inserted {
				continue
			}
			if service.Logger != nil {
				level := slog.LevelInfo
				message := "временной блок EAVC поставлен в очередь"
				if event.FreshnessIssue != "" {
					level = slog.LevelWarn
					message = "временной блок EAVC не будет отправлен из-за срока"
				}
				if event.SourceBlockDeleted {
					message = "удаление временного блока EAVC поставлено в очередь"
				}
				service.Logger.Log(ctx, level, message,
					"event_id", event.ID, "trace_id", event.ID, "source_uuid", event.Source.UUID,
					"source_id", event.Source.ID, "source_block_uuid", event.SourceBlockUUID,
					"source_block_deleted", event.SourceBlockDeleted, "source_version", event.SourceVersion(),
					"position", event.Position.UnitName,
					"source_at", event.SourceAt, "expires_at", event.ExpiresAt,
					"reason", event.FreshnessIssue, "content_kind", eventContentKind(event),
					"content_chars", utf8.RuneCountInString(event.Content),
					"content_blocks", len(event.Blocks), "source_mode", string(event.Mode),
				)
			}
			queued++
		}
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
		// Возможность ACK/4 находится внутри AES-GCM, поэтому нельзя подменить
		// ответ старому Клиенту посредством правки внешнего JSON конверта.
		event.AckTraceV4 = true
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
					"event_id", event.ID, "trace_id", event.ID, "attempt", queued.Attempts+1,
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
				"event_id", event.ID, "trace_id", event.ID,
				"telegram_message_id", messageID, "transport_message_id", messageID,
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
		changed, err := service.Queue.MarkServerDisposition(ctx, acknowledgement.EventID,
			acknowledgement.AcceptedAt, acknowledgement.Status, acknowledgement.TransportMessageID)
		if err != nil {
			return result, err
		}
		if changed {
			result.Events++
		}
		if service.Logger != nil {
			service.Logger.Info("получено подтверждение сервера",
				"event_id", acknowledgement.EventID, "trace_id", acknowledgement.EventID,
				"status", acknowledgement.Status,
				"transport_message_id", acknowledgement.TransportMessageID,
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
	content := normalizeContent(item.Content)
	// EAVC сначала создаёт карточку перехвата, а затем отдельным запросом
	// записывает её текст. При высокой нагрузке клиент может увидеть этот
	// промежуточный пустой черновик. Он не является сообщением и не должен
	// уходить в Telegram, Word и центр; граничная секунда источника будет
	// перечитана, поэтому заполненная версия попадёт в очередь позднее.
	if content == "" {
		return domain.Event{}, errors.New("перехват ещё не содержит текста")
	}
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
	event := domain.Event{
		ID: id, ClientID: service.ClientID, CreatedAt: now, ObservedAt: item.ObservedAt,
		SourceAt: sourceAt, ExpiresAt: expiresAt, FreshnessIssue: freshnessIssue,
		Source: item.Record, Position: item.Position, Mode: item.Mode,
		Content: content, Blocks: item.Blocks, UpdatedBy: item.UpdatedBy,
		ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	return event, event.Validate()
}

// newEvents превращает каждый устойчиво идентифицированный временной блок EAVC
// в самостоятельное событие. Старый источник без таблицы блоков остаётся
// совместимым и передаётся одним событием прежнего формата.
func (service *ClientService) newEvents(item SourceItem) ([]domain.Event, error) {
	if len(item.Blocks) == 0 && len(item.DeletedBlocks) == 0 {
		event, err := service.newEvent(item)
		if err != nil {
			return nil, err
		}
		return []domain.Event{event}, nil
	}
	if !item.logicalTimeBlocks {
		now := time.Now().UTC()
		if service.Now != nil {
			now = service.Now().UTC()
		}
		prepared := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, now)
		if prepared.Conflict != nil {
			return nil, errors.New(prepared.Conflict.Reason)
		}
		item = prepared.Item
	}
	hasActiveBlocks := len(item.Blocks) > 0
	events := make([]domain.Event, 0, len(item.Blocks)+len(item.DeletedBlocks))
	for _, block := range item.Blocks {
		event, err := service.newBlockEvent(item, block, false, hasActiveBlocks)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	for _, block := range item.DeletedBlocks {
		event, err := service.newBlockEvent(item, block, true, hasActiveBlocks)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (service *ClientService) newBlockEvent(
	item SourceItem, block domain.ContentBlock, deleted bool, hasActiveBlocks bool,
) (domain.Event, error) {
	now := service.Now().UTC()
	blockUUID := strings.TrimSpace(block.Identity())
	if blockUUID == "" {
		return domain.Event{}, errors.New("временной блок не имеет устойчивого идентификатора")
	}
	content := normalizeContent(block.Text)
	if !deleted && content == "" {
		return domain.Event{}, errors.New("временной блок ещё не содержит текста")
	}
	version := strings.TrimSpace(block.UpdatedAt)
	if deleted && strings.TrimSpace(block.DeletedAt) != "" {
		version = strings.TrimSpace(block.DeletedAt)
	}
	if version == "" {
		version = item.Record.UpdatedAt
	}
	maxAge := service.MaxEventAge
	if maxAge <= 0 {
		maxAge = 30 * time.Minute
	}
	freshnessTimestamp := strings.TrimSpace(item.Record.UpdatedAt)
	if deleted && strings.TrimSpace(block.DeletedAt) != "" {
		freshnessTimestamp = strings.TrimSpace(block.DeletedAt)
	}
	if freshnessTimestamp == "" {
		freshnessTimestamp = version
	}
	sourceAt, expiresAt, freshnessIssue := domain.FreshnessInLocation(
		freshnessTimestamp, item.ObservedAt, now, maxAge, service.SourceLocation,
	)
	// Срок свежести относится к снимку записи EAVC целиком, но идентичность и
	// дата самостоятельного события относятся к версии конкретного блока.
	// Поэтому редакция соседнего блока не меняет SourceAt и digest этого блока.
	if blockSourceAt, _, _ := domain.FreshnessInLocation(
		version, time.Time{}, now, maxAge, service.SourceLocation,
	); !blockSourceAt.IsZero() {
		sourceAt = blockSourceAt
	}
	if deleted {
		expiresAt = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
		freshnessIssue = ""
		content = ""
		block.Text = ""
		block.Callsigns = nil
	} else {
		// У scoped-события существует один источник истины. Это исключает
		// ситуацию, когда маршрут/дайджест проверяют Content, а Telegram или Word
		// отображают отличающийся Blocks[0].Text.
		block.Text = content
	}
	id, err := protocol.UUIDv7(now, rand.Reader)
	if err != nil {
		return domain.Event{}, err
	}
	event := domain.Event{
		ID: id, ClientID: service.ClientID, CreatedAt: now, ObservedAt: item.ObservedAt,
		SourceAt: sourceAt, ExpiresAt: expiresAt, FreshnessIssue: freshnessIssue,
		Source: item.Record, Position: item.Position, Mode: item.Mode,
		Content: content, Blocks: []domain.ContentBlock{block},
		SourceBlockUUID: blockUUID, SourceBlockDeleted: deleted,
		HasActiveBlocks: hasActiveBlocks,
		UpdatedBy:       block.UpdatedBy, ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	if strings.TrimSpace(event.UpdatedBy) == "" {
		// Старые совместимые схемы EAVC не имели updated_by у блока.
		// Для них сохраняем прежний источник, не требуя изменения EAVC.
		event.UpdatedBy = item.UpdatedBy
	}
	event.ContentDigest = event.CanonicalContentDigest()
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
