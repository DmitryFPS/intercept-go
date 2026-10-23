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

const uploadedRetryInterval = 2 * time.Minute

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
	EventID  string            `json:"event_id"`
	Position string            `json:"position"`
	SourceAt time.Time         `json:"source_at"`
	StatusAt time.Time         `json:"status_at"`
	Reason   string            `json:"reason"`
	Content  string            `json:"content"`
	Mode     domain.SourceMode `json:"mode"`
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
		       COALESCE(expired_at, acknowledged_at, uploaded_at, created_at), last_error
		FROM client_events WHERE state=?
		ORDER BY COALESCE(expired_at, acknowledged_at, uploaded_at, created_at) DESC, event_id DESC
		LIMIT ? OFFSET ?`, state, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("прочитать сообщения очереди: %w", err)
	}
	defer rows.Close()
	result := make([]QueueEvent, 0, limit)
	for rows.Next() {
		var eventID, statusText, reason string
		var protected []byte
		if err := rows.Scan(&eventID, &protected, &statusText, &reason); err != nil {
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
		result = append(result, QueueEvent{
			EventID: eventID, Position: event.Position.UnitName, SourceAt: event.SourceAt,
			StatusAt: statusAt, Reason: reason, Content: event.Content, Mode: event.Mode,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("завершить чтение сообщений очереди: %w", err)
	}
	return result, total, nil
}

// ClearTerminal удаляет подтверждённые либо просроченные сообщения по явной
// команде оператора. Активные состояния этим методом удалить невозможно.
func (queue *ClientQueue) ClearTerminal(ctx context.Context, state string) (int, error) {
	if state != "acknowledged" && state != "expired" {
		return 0, errors.New("активную очередь очищать запрещено")
	}
	result, err := queue.db.ExecContext(ctx, "DELETE FROM client_events WHERE state=?", state)
	if err != nil {
		return 0, fmt.Errorf("очистить завершённые сообщения: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число очищенных сообщений: %w", err)
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
	result, err := tx.ExecContext(ctx, `
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
	overflow, err := tx.ExecContext(ctx, `
		DELETE FROM client_events
		WHERE state IN ('acknowledged','expired')
		  AND event_id NOT IN (
			SELECT event_id FROM client_events
			WHERE state IN ('acknowledged','expired')
			ORDER BY COALESCE(acknowledged_at, expired_at, created_at) DESC, event_id DESC
			LIMIT ?
		  )`, clientTerminalRecordLimit)
	if err != nil {
		return 0, fmt.Errorf("ограничить завершённые события клиента: %w", err)
	}
	overflowCount, err := overflow.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("определить число ограниченных событий клиента: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("завершить очистку клиента: %w", err)
	}
	return int(count + overflowCount), nil
}

// Compact возвращает SQLite свободные страницы и ограничивает WAL-файл.
func (queue *ClientQueue) Compact(ctx context.Context) error {
	return compact(ctx, queue.db)
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
		ORDER BY CASE state WHEN 'pending' THEN 0 ELSE 1 END, created_at, event_id
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
		time.Now().UTC().Add(uploadedRetryInterval).Format(time.RFC3339Nano), eventID,
	)
	// Центр может подтвердить маленький пакет быстрее, чем Send вернёт
	// управление этому потоку. Поздняя промежуточная отметка загрузки не должна
	// откатывать конечное состояние и не является ошибкой.
	return terminalTransition(queue.db, ctx, result, err, eventID, "отметить загрузку")
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
