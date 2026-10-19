package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

// ClientQueue хранит события независимо от процесса и состояния сети.
type ClientQueue struct {
	db *sql.DB
}

// ClientStats содержит счётчики, безопасные для экрана оператора.
type ClientStats struct {
	Pending      int `json:"pending"`
	Uploaded     int `json:"uploaded"`
	Acknowledged int `json:"acknowledged"`
	Expired      int `json:"expired"`
}

// PurgeTerminal удаляет только давно завершённые события. Активная очередь,
// ожидающая отправки или подтверждения, этим обслуживанием не затрагивается.
func (queue *ClientQueue) PurgeTerminal(ctx context.Context, before time.Time) (int, error) {
	result, err := queue.db.ExecContext(ctx, `
		DELETE FROM client_events
		WHERE (state='acknowledged' AND acknowledged_at IS NOT NULL AND acknowledged_at < ?)
		   OR (state='expired' AND expired_at IS NOT NULL AND expired_at < ?)`,
		before.UTC().Format(time.RFC3339Nano), before.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return 0, fmt.Errorf("очистить завершённые события клиента: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число очищенных событий клиента: %w", err)
	}
	return int(count), nil
}

// Compact возвращает SQLite свободные страницы и ограничивает WAL-файл.
func (queue *ClientQueue) Compact(ctx context.Context) error {
	return compact(ctx, queue.db)
}

// NewClientQueue применяет миграции и открывает долговечную очередь точки.
func NewClientQueue(ctx context.Context, db *sql.DB) (*ClientQueue, error) {
	if err := Migrate(ctx, db, []string{
		clientMigration, clientExpiryMigration, clientSourceVersionMigration,
		clientStreamMigration, clientStreamRepairMigration,
	}); err != nil {
		return nil, err
	}
	if err := protectExistingColumn(ctx, db, "client_events", "payload_json"); err != nil {
		return nil, fmt.Errorf("защитить очередь клиента: %w", err)
	}
	if err := backfillClientStreamKeys(ctx, db); err != nil {
		return nil, err
	}
	return &ClientQueue{db: db}, nil
}

func backfillClientStreamKeys(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "SELECT event_id, payload_json FROM client_events WHERE stream_key=''")
	if err != nil {
		return fmt.Errorf("прочитать старые очереди подразделений: %w", err)
	}
	type row struct {
		id      string
		payload []byte
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(&value.id, &value.payload); err != nil {
			rows.Close()
			return err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, value := range values {
		plain, err := unprotectDatabaseValue(value.payload)
		if err != nil {
			return fmt.Errorf("прочитать старое событие %s: %w", value.id, err)
		}
		var event domain.Event
		if err := json.Unmarshal(plain, &event); err != nil {
			return fmt.Errorf("разобрать старое событие %s: %w", value.id, err)
		}
		if _, err := tx.ExecContext(ctx,
			"UPDATE client_events SET stream_key=? WHERE event_id=?",
			clientStreamKey(event), value.id,
		); err != nil {
			return fmt.Errorf("восстановить очередь события %s: %w", value.id, err)
		}
	}
	return tx.Commit()
}

// Enqueue сохраняет событие до любых сетевых действий и подавляет точный повтор.
func (queue *ClientQueue) Enqueue(ctx context.Context, event domain.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("кодировать событие: %w", err)
	}
	payload, err = protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить событие: %w", err)
	}
	sourceKey := strings.TrimSpace(event.Source.UUID)
	if sourceKey == "" {
		sourceKey = "id:" + strconv.FormatInt(event.Source.ID, 10)
	}
	sourceVersion := strings.TrimSpace(event.Source.UpdatedAt)
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
	_, err = queue.db.ExecContext(ctx, `
		INSERT INTO client_events(
			event_id, source_uuid, source_version, stream_key, content_digest, payload_json, state,
			next_attempt_at, expires_at, created_at, expired_at, last_error
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_uuid, source_version, content_digest) DO NOTHING`,
		event.ID, sourceKey, sourceVersion, clientStreamKey(event), event.ContentDigest, payload, state,
		event.CreatedAt.UTC().Format(time.RFC3339Nano),
		expiresAt.Format(time.RFC3339Nano),
		event.CreatedAt.UTC().Format(time.RFC3339Nano),
		expiredAt, lastError,
	)
	if err != nil {
		return fmt.Errorf("поставить событие в очередь: %w", err)
	}
	return nil
}

// Pending выбирает только головы независимых очередей подразделений.
func (queue *ClientQueue) Pending(ctx context.Context, limit int, now time.Time) ([]application.QueuedEvent, error) {
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("лимит очереди должен быть от 1 до 1000")
	}
	rows, err := queue.db.QueryContext(ctx, `
		SELECT payload_json, attempts
		FROM client_events
		WHERE state IN ('pending', 'uploaded')
			AND next_attempt_at <= ? AND expires_at >= ?
			AND NOT EXISTS (
				SELECT 1 FROM client_events earlier
				WHERE earlier.state IN ('pending', 'uploaded')
					AND earlier.stream_key = client_events.stream_key
					AND (earlier.created_at < client_events.created_at OR
						(earlier.created_at = client_events.created_at AND earlier.event_id < client_events.event_id))
			)
		ORDER BY created_at, event_id
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать очередь: %w", err)
	}
	defer rows.Close()
	var events []application.QueuedEvent
	for rows.Next() {
		var payload []byte
		var queued application.QueuedEvent
		if err := rows.Scan(&payload, &queued.Attempts); err != nil {
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

// clientStreamKey ограничивает строгий порядок одним подразделением. Если в
// будущей EAVC появится стабильный каталог, переименование позиции не разделит
// её очередь; в текущей EAVC используется нормализованное точное название.
func clientStreamKey(event domain.Event) string {
	if catalogUUID := strings.TrimSpace(event.Position.CatalogUUID); catalogUUID != "" {
		return "catalog:" + strings.ToLower(catalogUUID)
	}
	return "name:" + domain.NormalizeUnitName(domain.RoutingUnitName(event.Position.UnitName))
}

// MarkUploaded запоминает загрузку в транспорт, не считая её приёмом сервером.
func (queue *ClientQueue) MarkUploaded(ctx context.Context, eventID string, telegramMessageID int64) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state = 'uploaded', telegram_message_id = ?, uploaded_at = ?,
			last_error = '', next_attempt_at = ?, attempts = attempts + 1
		WHERE event_id = ? AND state IN ('pending', 'uploaded')`,
		telegramMessageID, time.Now().UTC().Format(time.RFC3339Nano),
		time.Now().UTC().Add(30*time.Second).Format(time.RFC3339Nano), eventID,
	)
	return changedExactlyOnce(result, err, "отметить загрузку")
}

// ExpireDue завершает события, которые уже нельзя впервые передавать.
func (queue *ClientQueue) ExpireDue(ctx context.Context, now time.Time) (int, error) {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state='expired', expired_at=?, last_error=?
		WHERE state IN ('pending', 'uploaded') AND expires_at < ?`,
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
	return changedExactlyOnce(result, err, "запланировать повтор")
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
