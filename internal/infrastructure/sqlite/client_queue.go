package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

const (
	uploadedRetryBaseInterval   = 20 * time.Minute
	uploadedRetryMaxInterval    = 2 * time.Hour
	clientClearedEventRetention = 180 * 24 * time.Hour
	clientClearedEventLimit     = 200000
)

// ClientQueue хранит события независимо от процесса и состояния сети.
type ClientQueue struct {
	db *sql.DB
}

const clientTerminalRecordLimit = 100000

// ClientStats содержит счётчики, безопасные для экрана оператора.
type ClientStats struct {
	Pending      int `json:"pending"`
	Uploaded     int `json:"uploaded"`
	Acknowledged int `json:"acknowledged"`
	Expired      int `json:"expired"`
}

// QueueEvent описывает сообщение очереди для локального экрана точки.
// Токены, ключи и транспортные идентификаторы сюда намеренно не входят.
type QueueEvent struct {
	EventID            string            `json:"event_id"`
	Position           string            `json:"position"`
	SourceAt           time.Time         `json:"source_at"`
	StatusAt           time.Time         `json:"status_at"`
	Reason             string            `json:"reason"`
	Content            string            `json:"content"`
	ContentKind        string            `json:"content_kind"`
	ContentNote        string            `json:"content_note"`
	Mode               domain.SourceMode `json:"mode"`
	SourceRecord       int64             `json:"source_record,omitempty"`
	SourceUUID         string            `json:"source_uuid,omitempty"`
	SourceBlockUUID    string            `json:"source_block_uuid,omitempty"`
	SourceBlockDeleted bool              `json:"source_block_deleted,omitempty"`
	SourceVersion      string            `json:"source_version,omitempty"`
	ContentDigest      string            `json:"content_digest,omitempty"`
	Attempts           int               `json:"attempts"`
	TransportMessageID *int64            `json:"transport_message_id,omitempty"`
	UploadMessageID    *int64            `json:"telegram_upload_message_id,omitempty"`
	CenterMessageID    *int64            `json:"center_transport_message_id,omitempty"`
}

// EventsPage возвращает одну страницу выбранного состояния очереди. Содержимое
// расшифровывается только для локального интерфейса.
func (queue *ClientQueue) EventsPage(ctx context.Context, state string, limit, offset int) ([]QueueEvent, int, error) {
	if limit < 1 || limit > 100 || offset < 0 {
		return nil, 0, errors.New("неверные параметры страницы очереди")
	}
	switch state {
	case "pending", "uploaded", "acknowledged", "expired":
	default:
		return nil, 0, errors.New("неизвестное состояние очереди")
	}
	var total int
	if err := queue.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_events WHERE state=?", state).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("посчитать сообщения очереди: %w", err)
	}
	rows, err := queue.db.QueryContext(ctx, `
		SELECT event_id, payload_json,
		       COALESCE(expired_at, acknowledged_at, uploaded_at, created_at), last_error,
		       attempts, telegram_message_id, center_transport_message_id
		FROM client_events WHERE state=?
		ORDER BY COALESCE(expired_at, acknowledged_at, uploaded_at, created_at) DESC,
		         enqueue_seq DESC, event_id DESC
		LIMIT ? OFFSET ?`, state, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("прочитать сообщения очереди: %w", err)
	}
	defer rows.Close()
	result := make([]QueueEvent, 0, limit)
	for rows.Next() {
		var eventID, statusText, reason string
		var attempts int
		var uploadMessageID, centerMessageID sql.NullInt64
		var protected []byte
		if err := rows.Scan(&eventID, &protected, &statusText, &reason, &attempts, &uploadMessageID, &centerMessageID); err != nil {
			return nil, 0, fmt.Errorf("прочитать сообщение очереди: %w", err)
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			return nil, 0, fmt.Errorf("открыть сообщение очереди: %w", err)
		}
		var event domain.Event
		if err := json.Unmarshal(plain, &event); err != nil {
			return nil, 0, fmt.Errorf("разобрать сообщение очереди: %w", err)
		}
		statusAt, err := time.Parse(time.RFC3339Nano, statusText)
		if err != nil {
			return nil, 0, fmt.Errorf("разобрать время сообщения: %w", err)
		}
		kind, note := clientQueueContentDescription(event)
		transportMessageID := nullableInt64(centerMessageID)
		if transportMessageID == nil {
			transportMessageID = nullableInt64(uploadMessageID)
		}
		result = append(result, QueueEvent{
			EventID: eventID, Position: event.Position.UnitName, SourceAt: event.SourceAt,
			StatusAt: statusAt, Reason: reason, Content: event.Content,
			ContentKind: kind, ContentNote: note, Mode: event.Mode,
			SourceRecord: event.Source.ID, SourceUUID: event.Source.UUID,
			SourceBlockUUID: event.SourceBlockUUID, SourceBlockDeleted: event.SourceBlockDeleted,
			SourceVersion: event.SourceVersion(), ContentDigest: event.ContentDigest,
			Attempts: attempts, TransportMessageID: transportMessageID,
			UploadMessageID: nullableInt64(uploadMessageID), CenterMessageID: nullableInt64(centerMessageID),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("завершить чтение сообщений очереди: %w", err)
	}
	return result, total, nil
}

func nullableInt64(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}

func clientQueueContentDescription(event domain.Event) (string, string) {
	if event.SourceBlockDeleted {
		return "deleted_block", "Временной блок удалён в EAVC. Клиент передал Центру точную команду удалить только связанную публикацию."
	}
	if strings.TrimSpace(event.Content) == "" {
		return "empty_text", "EAVC передал запись без текста. Вложения по текущему контракту EAVC → Клиент не передаются, поэтому определить их тип невозможно."
	}
	return "text", "Текст перехвата, прочитанный Клиентом из EAVC."
}

// ClearTerminal удаляет подтверждённые либо просроченные сообщения по явной
// команде оператора. Активные состояния этим методом удалить невозможно.
func (queue *ClientQueue) ClearTerminal(ctx context.Context, state string) (int, error) {
	if state != "acknowledged" && state != "expired" {
		return 0, errors.New("активную очередь очищать запрещено")
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("начать очистку завершённых сообщений: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO client_cleared_events(source_uuid, source_version, content_digest, cleared_at)
		SELECT source_uuid, source_version, content_digest, ? FROM client_events WHERE state=?`,
		time.Now().UTC().Format(time.RFC3339Nano), state); err != nil {
		return 0, fmt.Errorf("запомнить очищенные сообщения: %w", err)
	}
	result, err := tx.ExecContext(ctx, "DELETE FROM client_events WHERE state=?", state)
	if err != nil {
		return 0, fmt.Errorf("очистить завершённые сообщения: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число очищенных сообщений: %w", err)
	}
	if err := pruneClientClearedEvents(ctx, tx, time.Now().UTC(), clientClearedEventRetention, clientClearedEventLimit); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("завершить очистку сообщений: %w", err)
	}
	return int(count), nil
}

// PurgeTerminal удаляет только давно завершённые события. Активная очередь,
// ожидающая отправки или подтверждения, этим обслуживанием не затрагивается.
func (queue *ClientQueue) PurgeTerminal(ctx context.Context, before time.Time) (int, error) {
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("начать очистку клиента: %w", err)
	}
	defer tx.Rollback()
	cutoff := before.UTC().Format(time.RFC3339Nano)
	clearedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO client_cleared_events(source_uuid, source_version, content_digest, cleared_at)
		SELECT source_uuid, source_version, content_digest, ? FROM client_events
		WHERE (state='acknowledged' AND acknowledged_at IS NOT NULL AND acknowledged_at < ?)
		   OR (state='expired' AND expired_at IS NOT NULL AND expired_at < ?)`,
		clearedAt, cutoff, cutoff); err != nil {
		return 0, fmt.Errorf("запомнить автоматически очищенные события клиента: %w", err)
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM client_events
		WHERE (state='acknowledged' AND acknowledged_at IS NOT NULL AND acknowledged_at < ?)
		   OR (state='expired' AND expired_at IS NOT NULL AND expired_at < ?)`,
		cutoff, cutoff)
	if err != nil {
		return 0, fmt.Errorf("очистить завершённые события клиента: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число очищенных событий клиента: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO client_cleared_events(source_uuid, source_version, content_digest, cleared_at)
		SELECT source_uuid, source_version, content_digest, ? FROM client_events
		WHERE state IN ('acknowledged','expired')
		  AND event_id NOT IN (
			SELECT event_id FROM client_events
			WHERE state IN ('acknowledged','expired')
			ORDER BY COALESCE(acknowledged_at, expired_at, created_at) DESC,
			         enqueue_seq DESC, event_id DESC
			LIMIT ?
		  )`, clearedAt, clientTerminalRecordLimit); err != nil {
		return 0, fmt.Errorf("запомнить ограниченные события клиента: %w", err)
	}
	overflow, err := tx.ExecContext(ctx, `
		DELETE FROM client_events
		WHERE state IN ('acknowledged','expired')
		  AND event_id NOT IN (
			SELECT event_id FROM client_events
			WHERE state IN ('acknowledged','expired')
			ORDER BY COALESCE(acknowledged_at, expired_at, created_at) DESC,
			         enqueue_seq DESC, event_id DESC
			LIMIT ?
		  )`, clientTerminalRecordLimit)
	if err != nil {
		return 0, fmt.Errorf("ограничить завершённые события клиента: %w", err)
	}
	overflowCount, err := overflow.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число ограниченных событий клиента: %w", err)
	}
	if err := pruneClientClearedEvents(ctx, tx, time.Now().UTC(), clientClearedEventRetention, clientClearedEventLimit); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("завершить очистку клиента: %w", err)
	}
	return int(count + overflowCount), nil
}

// pruneClientClearedEvents ограничивает технические метки очистки. Свежие
// метки нужны, чтобы повторное чтение граничной строки EAVC не вернуло её в
// интерфейс, но хранить их бессрочно нет необходимости: курсор чтения к этому
// моменту уже многократно сохранён и продвинут вперёд.
func pruneClientClearedEvents(
	ctx context.Context, tx *sql.Tx, now time.Time, retention time.Duration, limit int,
) error {
	if retention <= 0 || limit < 1 {
		return errors.New("неверные параметры очистки меток клиентской очереди")
	}
	cutoff := now.UTC().Add(-retention).Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx,
		"DELETE FROM client_cleared_events WHERE cleared_at < ?", cutoff,
	); err != nil {
		return fmt.Errorf("удалить устаревшие метки клиентской очереди: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM client_cleared_events
		WHERE rowid NOT IN (
			SELECT rowid FROM client_cleared_events
			ORDER BY cleared_at DESC, rowid DESC
			LIMIT ?
		)`, limit); err != nil {
		return fmt.Errorf("ограничить метки клиентской очереди: %w", err)
	}
	return nil
}

// Compact возвращает SQLite свободные страницы и ограничивает WAL-файл.
func (queue *ClientQueue) Compact(ctx context.Context) error {
	return compact(ctx, queue.db)
}

// Enqueue сохраняет событие до любых сетевых действий и подавляет точный повтор.
func (queue *ClientQueue) Enqueue(ctx context.Context, event domain.Event) (bool, error) {
	if err := event.Validate(); err != nil {
		return false, err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("кодировать событие: %w", err)
	}
	payload, err = protectDatabaseValue(payload)
	if err != nil {
		return false, fmt.Errorf("защитить событие: %w", err)
	}
	sourceKey := event.SourceLineageKey()
	sourceVersion := event.SourceVersion()
	if sourceVersion == "" && !event.SourceAt.IsZero() {
		sourceVersion = event.SourceAt.UTC().Format(time.RFC3339Nano)
	}
	if sourceVersion == "" {
		sourceVersion = event.CreatedAt.UTC().Format(time.RFC3339Nano)
	}
	expiresAt := event.ExpiresAt.UTC()
	if expiresAt.IsZero() {
		expiresAt = event.CreatedAt.UTC().Add(30 * time.Minute)
	}
	state := "pending"
	expiredAt := any(nil)
	lastError := ""
	if event.FreshnessIssue != "" || time.Now().UTC().After(expiresAt) {
		state = "expired"
		expiredAt = time.Now().UTC().Format(time.RFC3339Nano)
		lastError = event.FreshnessIssue
		if lastError == "" {
			lastError = domain.FreshnessExpired
		}
	}
	result, err := queue.db.ExecContext(ctx, `
		INSERT INTO client_events(
			event_id, source_uuid, source_version, stream_key, content_digest, payload_json, state,
			next_attempt_at, expires_at, created_at, expired_at, last_error
		)
		SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		WHERE NOT EXISTS (
			SELECT 1 FROM client_cleared_events
			WHERE source_uuid=? AND source_version=? AND content_digest=?
		)
		AND NOT EXISTS (
			SELECT 1 FROM client_events
			WHERE source_uuid=? AND source_version=? AND content_digest=?
		)
		ON CONFLICT(source_uuid, source_version, content_digest) DO NOTHING`,
		event.ID, sourceKey, sourceVersion, clientStreamKey(event), event.ContentDigest, payload, state,
		event.CreatedAt.UTC().Format(time.RFC3339Nano),
		expiresAt.Format(time.RFC3339Nano),
		event.CreatedAt.UTC().Format(time.RFC3339Nano),
		expiredAt, lastError,
		sourceKey, sourceVersion, event.ContentDigest,
		sourceKey, sourceVersion, event.ContentDigest,
	)
	if err != nil {
		return false, fmt.Errorf("поставить событие в очередь: %w", err)
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("определить результат постановки события в очередь: %w", err)
	}
	return inserted > 0, nil
}

// SeedBlockBaselinesFromLegacy запоминает поблочные отпечатки только если
// цельная версия той же записи уже присутствует в очереди прежней версии или
// в журнале очищенных идентичностей. Проверка и запись выполняются одной
// транзакцией, поэтому параллельный цикл сбора не создаст миграционный дубль.
func (queue *ClientQueue) SeedBlockBaselinesFromLegacy(
	ctx context.Context, sourceUUID, sourceVersion string, events []domain.Event,
) (bool, error) {
	sourceUUID = strings.TrimSpace(sourceUUID)
	sourceVersion = strings.TrimSpace(sourceVersion)
	if sourceUUID == "" || sourceVersion == "" || len(events) == 0 {
		return false, nil
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("начать миграцию поблочных идентичностей: %w", err)
	}
	defer tx.Rollback()
	var known int
	err = tx.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1 FROM client_events WHERE source_uuid=? AND source_version=?
			UNION ALL
			SELECT 1 FROM client_cleared_events WHERE source_uuid=? AND source_version=?
		)`, sourceUUID, sourceVersion, sourceUUID, sourceVersion).Scan(&known)
	if err != nil {
		return false, fmt.Errorf("проверить цельную версию прежнего клиента: %w", err)
	}
	if known == 0 {
		return false, nil
	}
	clearedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range events {
		if strings.TrimSpace(event.SourceBlockUUID) == "" {
			continue
		}
		if err := event.Validate(); err != nil {
			return false, fmt.Errorf("проверить поблочную миграцию: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO client_cleared_events(
				source_uuid, source_version, content_digest, cleared_at
			) VALUES(?, ?, ?, ?)`,
			event.SourceLineageKey(), event.SourceVersion(), event.ContentDigest, clearedAt,
		); err != nil {
			return false, fmt.Errorf("сохранить поблочную идентичность прежней версии: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить миграцию поблочных идентичностей: %w", err)
	}
	return true, nil
}

// SeedArchiveBaselinesFromSeenBlocks защищает от смены идентичности при
// закрытии смены EAVC. Активная запись имеет настоящие UUID временных блоков,
// но EAVC удаляет их при переносе записи в intercept_items_archive. Архивный
// снимок поэтому получает синтетические legacy-UUID и без этой проверки был бы
// принят как новый перехват.
//
// Для логических time: идентичностей каждый архивный блок сверяется с записью
// очереди или очищенной историей. При потере архивом mode/catalog_uuid можно
// также сверить содержимое сохранённого события в очереди. Это сохраняет
// незаписанные события после аварийного завершения между сохранением снимка и
// постановкой в очередь.
// Для старых физических идентичностей действует миграционная проверка:
//   - раньше замечено не меньше самостоятельных UUID блоков, чем есть в архиве;
//   - среди них есть версия, равная последней версии архивной записи.
//
// Если Клиент успел увидеть лишь часть блоков либо пропустил последнюю правку,
// метод возвращает false: архив будет обработан и данные не потеряются.
func (queue *ClientQueue) SeedArchiveBaselinesFromSeenBlocks(
	ctx context.Context, sourceUUID, sourceVersion string, events []domain.Event,
) (bool, error) {
	sourceUUID = strings.TrimSpace(sourceUUID)
	sourceVersion = strings.TrimSpace(sourceVersion)
	if sourceUUID == "" || sourceVersion == "" || len(events) == 0 {
		return false, nil
	}
	expectedBlocks := make(map[string]struct{}, len(events))
	logicalBlocks := true
	for _, event := range events {
		if err := event.Validate(); err != nil {
			return false, fmt.Errorf("проверить архивную идентичность: %w", err)
		}
		blockUUID := strings.ToLower(strings.TrimSpace(event.SourceBlockUUID))
		if blockUUID == "" || event.SourceBlockDeleted {
			return false, nil
		}
		base := strings.TrimSpace(event.Source.UUID)
		if base == "" {
			base = fmt.Sprintf("id:%d", event.Source.ID)
		}
		if !strings.EqualFold(base, sourceUUID) {
			return false, nil
		}
		expectedBlocks[blockUUID] = struct{}{}
		logicalBlocks = logicalBlocks && strings.HasPrefix(blockUUID, "time:")
	}
	if len(expectedBlocks) == 0 {
		return false, nil
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("начать проверку архивного повтора: %w", err)
	}
	defer tx.Rollback()
	if logicalBlocks {
		// A saved time snapshot may survive a crash after only some block events
		// reached the queue. Historical block count cannot prove that each event
		// of the current version was actually queued.
		allSeen := true
		clearedAt := time.Now().UTC().Format(time.RFC3339Nano)
		for _, event := range events {
			lineage, version, digest := event.SourceLineageKey(), event.SourceVersion(), event.ContentDigest
			var seen int
			err := tx.QueryRowContext(ctx, `
				SELECT EXISTS(
					SELECT 1 FROM client_events
					WHERE source_uuid=? AND source_version=? AND content_digest=?
					UNION ALL
					SELECT 1 FROM client_cleared_events
					WHERE source_uuid=? AND source_version=? AND content_digest=?
				)`, lineage, version, digest, lineage, version, digest).Scan(&seen)
			if err != nil {
				return false, fmt.Errorf("сверить архивный блок с историей очереди: %w", err)
			}
			if seen == 0 {
				// Archive rows drop source_mode and catalog_uuid. Their digest can
				// change even when the same logical block was already queued.
				matched, matchErr := archivedBlockMatchesQueuedEvent(ctx, tx, event)
				if matchErr != nil {
					return false, matchErr
				}
				if matched {
					seen = 1
				}
			}
			if seen == 0 {
				allSeen = false
				continue
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT OR IGNORE INTO client_cleared_events(
					source_uuid, source_version, content_digest, cleared_at
				) VALUES(?, ?, ?, ?)`, lineage, version, digest, clearedAt,
			); err != nil {
				return false, fmt.Errorf("сохранить подтверждённый архивный блок: %w", err)
			}
		}
		if err := tx.Commit(); err != nil {
			return false, fmt.Errorf("завершить проверку архивных блоков: %w", err)
		}
		return allSeen, nil
	}
	prefix := sourceUUID + "|block:"
	var knownBlocks, latestVersion int
	err = tx.QueryRowContext(ctx, `
		SELECT COUNT(DISTINCT source_uuid),
		       COALESCE(MAX(CASE WHEN source_version=? THEN 1 ELSE 0 END), 0)
		FROM (
			SELECT source_uuid, source_version FROM client_events
			UNION ALL
			SELECT source_uuid, source_version FROM client_cleared_events
		)
		WHERE substr(source_uuid, 1, length(?))=?`,
		sourceVersion, prefix, prefix,
	).Scan(&knownBlocks, &latestVersion)
	if err != nil {
		return false, fmt.Errorf("сверить архив с историей блоков: %w", err)
	}
	if knownBlocks < len(expectedBlocks) || latestVersion == 0 {
		return false, nil
	}
	clearedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, event := range events {
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO client_cleared_events(
				source_uuid, source_version, content_digest, cleared_at
			) VALUES(?, ?, ?, ?)`,
			event.SourceLineageKey(), event.SourceVersion(), event.ContentDigest, clearedAt,
		); err != nil {
			return false, fmt.Errorf("сохранить идентичность архивного повтора: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить подавление архивного повтора: %w", err)
	}
	return true, nil
}

func archivedBlockMatchesQueuedEvent(ctx context.Context, tx *sql.Tx, archive domain.Event) (bool, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT payload_json FROM client_events WHERE source_uuid=? AND source_version=?`,
		archive.SourceLineageKey(), archive.SourceVersion(),
	)
	if err != nil {
		return false, fmt.Errorf("прочитать историю архивного блока: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var protected []byte
		if err := rows.Scan(&protected); err != nil {
			return false, fmt.Errorf("прочитать событие архивного блока: %w", err)
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			return false, fmt.Errorf("открыть событие архивного блока: %w", err)
		}
		var queued domain.Event
		if err := json.Unmarshal(plain, &queued); err != nil {
			return false, fmt.Errorf("разобрать событие архивного блока: %w", err)
		}
		if sameArchivedQueuedBlock(archive, queued) {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("прочитать историю архивного блока: %w", err)
	}
	return false, nil
}

func sameArchivedQueuedBlock(archive, queued domain.Event) bool {
	if archive.SourceLineageKey() != queued.SourceLineageKey() ||
		archive.SourceVersion() != queued.SourceVersion() ||
		archive.Source.ID != queued.Source.ID ||
		!strings.EqualFold(archive.Source.UUID, queued.Source.UUID) ||
		archive.Source.SessionID != queued.Source.SessionID ||
		archive.Source.SessionUUID != queued.Source.SessionUUID ||
		archive.Position.UnitName != queued.Position.UnitName ||
		archive.Position.Frequency != queued.Position.Frequency ||
		archive.Position.GroupCode != queued.Position.GroupCode ||
		archive.Position.CatalogUUID != "" ||
		(archive.Mode != domain.SourceModeUnknown && archive.Mode != queued.Mode) ||
		archive.SourceBlockDeleted || queued.SourceBlockDeleted ||
		archive.HasActiveBlocks != queued.HasActiveBlocks ||
		archive.Content != queued.Content || archive.UpdatedBy != queued.UpdatedBy ||
		len(archive.Blocks) != 1 || len(queued.Blocks) != 1 {
		return false
	}
	archiveBlock, queuedBlock := archive.Blocks[0], queued.Blocks[0]
	archiveBlock.Sequence, queuedBlock.Sequence = 0, 0
	if len(archiveBlock.Callsigns) == 0 {
		archiveBlock.Callsigns = nil
	}
	if len(queuedBlock.Callsigns) == 0 {
		queuedBlock.Callsigns = nil
	}
	return reflect.DeepEqual(archiveBlock, queuedBlock)
}

// RecoverArchivedTimeMetadata requires an intact queued payload for every
// active block in a legacy time snapshot. Cleared identities cannot prove the
// missing mode and catalog UUID.
func (queue *ClientQueue) RecoverArchivedTimeMetadata(
	ctx context.Context, item application.SourceItem, snapshot application.SourceTimeSnapshot,
) (domain.SourceMode, string, bool, error) {
	entries := make([]application.SourceTimeStateEntry, 0, len(snapshot.Blocks))
	for _, entry := range snapshot.Blocks {
		if entry.Active {
			entries = append(entries, entry)
		}
	}
	if len(entries) == 0 {
		return "", "", false, nil
	}
	var mode domain.SourceMode
	var catalogUUID string
	for _, entry := range entries {
		lineage := strings.TrimSpace(item.Record.UUID)
		if lineage == "" {
			lineage = fmt.Sprintf("id:%d", item.Record.ID)
		}
		lineage += "|block:" + strings.ToLower(strings.TrimSpace(entry.Block.Identity()))
		rows, err := queue.db.QueryContext(ctx, `
			SELECT payload_json FROM client_events WHERE source_uuid=? AND source_version=?`,
			lineage, strings.TrimSpace(entry.Block.UpdatedAt),
		)
		if err != nil {
			return "", "", false, fmt.Errorf("прочитать сохранённые события архивного блока: %w", err)
		}
		found := false
		for rows.Next() {
			var protected []byte
			if err := rows.Scan(&protected); err != nil {
				rows.Close()
				return "", "", false, fmt.Errorf("прочитать сохранённое событие архивного блока: %w", err)
			}
			plain, err := unprotectDatabaseValue(protected)
			if err != nil {
				rows.Close()
				return "", "", false, fmt.Errorf("открыть сохранённое событие архивного блока: %w", err)
			}
			var event domain.Event
			// Защита DPAPI может отказать временно — тогда сканирование
			// повторяется без сдвига курсора. Некорректный JSON уже не может
			// доказать маршрут: изолируем архивный блок через conflict ниже,
			// не удерживая все более новые записи EAVC.
			if err := json.Unmarshal(plain, &event); err != nil {
				continue
			}
			if !queuedEventProvesArchivedMetadata(event, item, entry) {
				continue
			}
			if found && (mode != event.Mode || catalogUUID != event.Position.CatalogUUID) {
				rows.Close()
				return "", "", false, nil
			}
			if mode != "" && (mode != event.Mode || catalogUUID != event.Position.CatalogUUID) {
				rows.Close()
				return "", "", false, nil
			}
			mode, catalogUUID, found = event.Mode, event.Position.CatalogUUID, true
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return "", "", false, fmt.Errorf("прочитать сохранённые события архивного блока: %w", err)
		}
		if err := rows.Close(); err != nil {
			return "", "", false, fmt.Errorf("закрыть чтение архивного блока: %w", err)
		}
		if !found {
			return "", "", false, nil
		}
	}
	return mode, catalogUUID, true, nil
}

func queuedEventProvesArchivedMetadata(
	event domain.Event, item application.SourceItem, entry application.SourceTimeStateEntry,
) bool {
	if event.Source.ID != item.Record.ID ||
		!strings.EqualFold(event.Source.UUID, item.Record.UUID) ||
		(domain.Event{Source: event.Source}).CanonicalSourceVersion() !=
			(domain.Event{Source: item.Record}).CanonicalSourceVersion() ||
		event.Source.SessionID != item.Record.SessionID ||
		event.Source.SessionUUID != item.Record.SessionUUID ||
		event.Position.UnitName != item.Position.UnitName ||
		event.Position.Frequency != item.Position.Frequency ||
		event.Position.GroupCode != item.Position.GroupCode ||
		event.SourceBlockDeleted || len(event.Blocks) != 1 ||
		!strings.EqualFold(event.SourceBlockUUID, entry.Block.Identity()) ||
		event.SourceVersion() != strings.TrimSpace(entry.Block.UpdatedAt) ||
		event.Content != entry.Block.Text || event.Blocks[0].Text != entry.Block.Text ||
		event.UpdatedBy != entry.Block.UpdatedBy ||
		!reflect.DeepEqual(event.Blocks[0].Callsigns, entry.Block.Callsigns) {
		return false
	}
	switch event.Mode {
	case domain.SourceModeNormal, domain.SourceModeDeferred, domain.SourceModeUnknown:
		return true
	default:
		return false
	}
}

// Pending выбирает только головы независимых очередей подразделений.
func (queue *ClientQueue) Pending(ctx context.Context, limit int, now time.Time) ([]application.QueuedEvent, error) {
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("лимит очереди должен быть от 1 до 1000")
	}
	rows, err := queue.db.QueryContext(ctx, `
		SELECT payload_json, attempts, state
		FROM client_events
		WHERE state IN ('pending', 'uploaded')
			AND next_attempt_at <= ? AND (state='uploaded' OR expires_at >= ?)
			AND NOT EXISTS (
				SELECT 1 FROM client_events earlier
				WHERE earlier.state IN ('pending', 'uploaded')
					AND earlier.stream_key = client_events.stream_key
					AND (earlier.created_at < client_events.created_at OR
						(earlier.created_at = client_events.created_at AND
							(earlier.enqueue_seq < client_events.enqueue_seq OR
								(earlier.enqueue_seq = client_events.enqueue_seq AND
									earlier.event_id < client_events.event_id))))
			)
		ORDER BY CASE state WHEN 'pending' THEN 0 ELSE 1 END,
		         created_at, enqueue_seq, event_id
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать очередь: %w", err)
	}
	defer rows.Close()
	var events []application.QueuedEvent
	for rows.Next() {
		var payload []byte
		var queued application.QueuedEvent
		if err := rows.Scan(&payload, &queued.Attempts, &queued.State); err != nil {
			return nil, fmt.Errorf("прочитать событие очереди: %w", err)
		}
		plain, err := unprotectDatabaseValue(payload)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(plain, &queued.Event); err != nil {
			return nil, fmt.Errorf("декодировать событие очереди: %w", err)
		}
		events = append(events, queued)
	}
	return events, rows.Err()
}

// clientStreamKey сохраняет строгий порядок всех редакций одной записи EAVC.
// Название подразделения нельзя использовать: оператор может исправить его, но
// более ранняя версия всё равно должна быть передана первой.
func clientStreamKey(event domain.Event) string {
	if sourceUUID := strings.TrimSpace(event.Source.UUID); sourceUUID != "" {
		return "source:" + strings.ToLower(sourceUUID)
	}
	return fmt.Sprintf("source-id:%d", event.Source.ID)
}

// MarkUploaded запоминает загрузку в транспорт, не считая её приёмом сервером.
func (queue *ClientQueue) MarkUploaded(ctx context.Context, eventID string, telegramMessageID int64) error {
	var attempts int
	if err := queue.db.QueryRowContext(ctx, `SELECT attempts FROM client_events WHERE event_id=?`, eventID).Scan(&attempts); errors.Is(err, sql.ErrNoRows) {
		// Центр мог подтвердить событие, а оператор — очистить завершённую
		// запись до возврата Telegram Send. Для очереди это уже успех.
		return nil
	} else if err != nil {
		return fmt.Errorf("прочитать число транспортных попыток: %w", err)
	}
	now := time.Now().UTC()
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state = CASE WHEN state IN ('acknowledged','expired') THEN state ELSE 'uploaded' END,
			telegram_message_id = CASE WHEN state IN ('acknowledged','expired')
				THEN COALESCE(telegram_message_id, ?) ELSE ? END,
			uploaded_at = COALESCE(uploaded_at, ?),
			last_error = CASE WHEN state='expired' THEN last_error ELSE '' END,
			next_attempt_at = CASE WHEN state IN ('acknowledged','expired') THEN next_attempt_at ELSE ? END,
			attempts = attempts + 1
		WHERE event_id = ? AND state IN ('pending','uploaded','acknowledged','expired')`,
		telegramMessageID, telegramMessageID, now.Format(time.RFC3339Nano),
		now.Add(uploadedRetryDelay(attempts+1)).Format(time.RFC3339Nano), eventID,
	)
	// Центр может подтвердить маленький пакет быстрее, чем Send вернёт
	// управление этому потоку. Поздняя отметка обязана сохранить message_id для
	// трассировки, но не должна откатывать конечное состояние обратно в очередь.
	return terminalTransition(queue.db, ctx, result, err, eventID, "отметить загрузку")
}

// uploadedRetryDelay оставляет центру время разобрать уже принятый Telegram
// пакет. Частые повторы больших пакетов иначе сами вытесняют свежие события из
// транспортной группы. Первая страховочная копия ещё укладывается в 30 минут
// свежести исходного перехвата, дальнейшие повторы разрежаются экспоненциально.
func uploadedRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := uploadedRetryBaseInterval
	for current := 1; current < attempt && delay < uploadedRetryMaxInterval; current++ {
		delay *= 2
	}
	if delay > uploadedRetryMaxInterval {
		return uploadedRetryMaxInterval
	}
	return delay
}

// ExpireDue завершает события, которые уже нельзя впервые передавать.
func (queue *ClientQueue) ExpireDue(ctx context.Context, now time.Time) (int, error) {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state='expired', expired_at=?, last_error=?
		WHERE state = 'pending' AND expires_at < ?`,
		now.UTC().Format(time.RFC3339Nano), domain.FreshnessExpired,
		now.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("завершить просроченные события: %w", err)
	}
	count, err := result.RowsAffected()
	return int(count), err
}

// MarkExpired сохраняет окончательную причину отказа от передачи события.
func (queue *ClientQueue) MarkExpired(ctx context.Context, eventID string, at time.Time, reason string) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state='expired', expired_at=?, last_error=?
		WHERE event_id=? AND state IN ('pending', 'uploaded')`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID)
	return terminalTransition(queue.db, ctx, result, err, eventID, "отметить просроченное событие")
}

// MarkAcknowledged завершает событие только после ответа сервера.
func (queue *ClientQueue) MarkAcknowledged(ctx context.Context, eventID string, at time.Time) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state = 'acknowledged', acknowledged_at = ?, last_error = ''
		WHERE event_id = ? AND state IN ('pending', 'uploaded')`,
		at.UTC().Format(time.RFC3339Nano), eventID,
	)
	return terminalTransition(queue.db, ctx, result, err, eventID, "подтвердить событие")
}

// MarkServerDisposition атомарно сохраняет исход подтверждения и ID пакета,
// реально увиденного Центром. Поздний ACK/4 может дополнить уже завершённую
// запись после ACK/3, но повтор не меняет терминальное состояние и первый ID.
func (queue *ClientQueue) MarkServerDisposition(
	ctx context.Context, eventID string, at time.Time, status string, transportMessageID int64,
) (bool, error) {
	if transportMessageID < 0 {
		return false, errors.New("отрицательный ID транспортного сообщения в подтверждении")
	}
	var result sql.Result
	var err error
	switch status {
	case "accepted":
		result, err = queue.db.ExecContext(ctx, `
			UPDATE client_events SET
				state='acknowledged',
				acknowledged_at=COALESCE(acknowledged_at, ?),
				expired_at=NULL,
				last_error='',
				center_transport_message_id=CASE WHEN state='expired'
					THEN NULLIF(?,0) ELSE COALESCE(center_transport_message_id,NULLIF(?,0)) END
			WHERE event_id=? AND state IN ('pending','uploaded','expired')`,
			at.UTC().Format(time.RFC3339Nano), transportMessageID, transportMessageID, eventID)
	case "expired":
		result, err = queue.db.ExecContext(ctx, `
			UPDATE client_events SET
				state='expired',
				expired_at=COALESCE(expired_at, ?),
				last_error=?,
				center_transport_message_id=COALESCE(center_transport_message_id,NULLIF(?,0))
			WHERE event_id=? AND state IN ('pending','uploaded')`, at.UTC().Format(time.RFC3339Nano), domain.FreshnessExpired,
			transportMessageID, eventID)
	default:
		return false, fmt.Errorf("неизвестный статус подтверждения %q", status)
	}
	if err != nil {
		return false, fmt.Errorf("сохранить подтверждение Центра: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("определить результат подтверждения Центра: %w", err)
	}
	if changed == 1 {
		return true, nil
	}
	if transportMessageID > 0 {
		// Дополнение трассы после ACK/3 не считается новым подтверждением.
		result, err = queue.db.ExecContext(ctx, `
			UPDATE client_events SET center_transport_message_id=?
			WHERE event_id=? AND ((?='accepted' AND state='acknowledged')
				OR (?='expired' AND state='expired'))
			  AND center_transport_message_id IS NULL`, transportMessageID, eventID, status, status)
		if err != nil {
			return false, fmt.Errorf("дополнить трассу подтверждения Центра: %w", err)
		}
		if err := terminalTransition(queue.db, ctx, result, nil, eventID, "дополнить трассу подтверждения Центра"); err != nil {
			return false, err
		}
		return false, nil
	}
	return false, terminalTransition(queue.db, ctx, result, nil, eventID, "проверить повтор подтверждения Центра")
}

// terminalTransition не позволяет старому или повторному подтверждению
// отравить Telegram offset. Уже завершённая либо очищенная запись безопасно
// считается обработанной: изменить активную очередь таким ответом невозможно.
func terminalTransition(
	db *sql.DB, ctx context.Context, result sql.Result, err error, eventID, operation string,
) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: определить число строк: %w", operation, err)
	}
	if count == 1 {
		return nil
	}
	var state string
	err = db.QueryRowContext(ctx, "SELECT state FROM client_events WHERE event_id=?", eventID).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%s: проверить завершённое событие: %w", operation, err)
	}
	if state == "acknowledged" || state == "expired" {
		return nil
	}
	return fmt.Errorf("%s: событие находится в неожиданном состоянии %q", operation, state)
}

// MarkRetry сохраняет ошибку и время следующей разрешённой попытки.
func (queue *ClientQueue) MarkRetry(ctx context.Context, eventID string, next time.Time, reason string) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET attempts = attempts + 1, next_attempt_at = ?, last_error = ?
		WHERE event_id = ? AND state IN ('pending', 'uploaded')`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID,
	)
	// Пока Telegram возвращает тайм-аут, ACK может уже перевести запись в
	// терминальное состояние. Такой результат не является ошибкой очереди.
	return terminalTransition(queue.db, ctx, result, err, eventID, "запланировать повтор")
}

// Cursor возвращает последнюю надёжно обработанную позицию чтения EAVC.
func (queue *ClientQueue) Cursor(ctx context.Context, name string) (application.Cursor, error) {
	var cursor application.Cursor
	err := queue.db.QueryRowContext(ctx,
		"SELECT updated_at, record_id FROM client_cursors WHERE name = ?", name,
	).Scan(&cursor.UpdatedAt, &cursor.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return application.Cursor{}, nil
	}
	if err != nil {
		return application.Cursor{}, fmt.Errorf("прочитать курсор: %w", err)
	}
	return cursor, nil
}

// SaveCursor атомарно сохраняет новую позицию чтения EAVC.
func (queue *ClientQueue) SaveCursor(ctx context.Context, name string, cursor application.Cursor) error {
	_, err := queue.db.ExecContext(ctx, `
		INSERT INTO client_cursors(name, updated_at, record_id) VALUES(?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET updated_at = excluded.updated_at, record_id = excluded.record_id`,
		name, cursor.UpdatedAt, cursor.ID,
	)
	if err != nil {
		return fmt.Errorf("сохранить курсор: %w", err)
	}
	return nil
}

// Value читает небольшой технический параметр клиента.
func (queue *ClientQueue) Value(ctx context.Context, name string) (string, bool, error) {
	var value string
	err := queue.db.QueryRowContext(ctx, "SELECT value FROM client_values WHERE name = ?", name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("прочитать параметр состояния: %w", err)
	}
	return value, true, nil
}

// SaveValue сохраняет небольшой технический параметр клиента.
func (queue *ClientQueue) SaveValue(ctx context.Context, name, value string) error {
	_, err := queue.db.ExecContext(ctx, `
		INSERT INTO client_values(name, value) VALUES(?, ?)
		ON CONFLICT(name) DO UPDATE SET value = excluded.value`, name, value)
	if err != nil {
		return fmt.Errorf("сохранить параметр состояния: %w", err)
	}
	return nil
}

// Stats считает состояния очереди без раскрытия содержимого перехватов.
func (queue *ClientQueue) Stats(ctx context.Context) (ClientStats, error) {
	rows, err := queue.db.QueryContext(ctx, "SELECT state, COUNT(*) FROM client_events GROUP BY state")
	if err != nil {
		return ClientStats{}, fmt.Errorf("прочитать статистику очереди: %w", err)
	}
	defer rows.Close()
	var stats ClientStats
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return ClientStats{}, err
		}
		switch state {
		case "pending":
			stats.Pending = count
		case "uploaded":
			stats.Uploaded = count
		case "acknowledged":
			stats.Acknowledged = count
		case "expired":
			stats.Expired = count
		}
	}
	return stats, rows.Err()
}

func changedExactlyOnce(result sql.Result, err error, operation string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: определить число строк: %w", operation, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: событие не найдено", operation)
	}
	return nil
}
