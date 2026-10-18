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

const clientMigration = `
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	UNIQUE(source_uuid, content_digest)
);
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
CREATE TABLE client_cursors (
	name TEXT PRIMARY KEY,
	updated_at TEXT NOT NULL,
	record_id INTEGER NOT NULL
);
CREATE TABLE client_values (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
);`

const clientExpiryMigration = `
ALTER TABLE client_events RENAME TO client_events_v1;
DROP INDEX client_events_pending_idx;
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	expired_at TEXT,
	UNIQUE(source_uuid, content_digest)
);
INSERT INTO client_events(
	event_id, source_uuid, content_digest, payload_json, state, attempts,
	next_attempt_at, expires_at, telegram_message_id, last_error, created_at,
	uploaded_at, acknowledged_at
)
SELECT event_id, source_uuid, content_digest, payload_json, state, attempts,
	next_attempt_at, created_at, telegram_message_id, last_error, created_at,
	uploaded_at, acknowledged_at
FROM client_events_v1;
DROP TABLE client_events_v1;
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
`

const clientSourceVersionMigration = `
ALTER TABLE client_events RENAME TO client_events_v2;
DROP INDEX client_events_pending_idx;
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	source_version TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	expired_at TEXT,
	UNIQUE(source_uuid, source_version, content_digest)
);
INSERT INTO client_events(
	event_id, source_uuid, source_version, content_digest, payload_json, state,
	attempts, next_attempt_at, expires_at, telegram_message_id, last_error,
	created_at, uploaded_at, acknowledged_at, expired_at
)
SELECT event_id, source_uuid, created_at, content_digest, payload_json, state,
	attempts, next_attempt_at, expires_at, telegram_message_id, last_error,
	created_at, uploaded_at, acknowledged_at, expired_at
FROM client_events_v2;
DROP TABLE client_events_v2;
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
`

// ClientQueue хранит события независимо от процесса и состояния сети.
type ClientQueue struct {
	db *sql.DB
}

type ClientStats struct {
	Pending      int `json:"pending"`
	Uploaded     int `json:"uploaded"`
	Acknowledged int `json:"acknowledged"`
	Expired      int `json:"expired"`
}

func NewClientQueue(ctx context.Context, db *sql.DB) (*ClientQueue, error) {
	if err := Migrate(ctx, db, []string{clientMigration, clientExpiryMigration, clientSourceVersionMigration}); err != nil {
		return nil, err
	}
	return &ClientQueue{db: db}, nil
}

func (queue *ClientQueue) Enqueue(ctx context.Context, event domain.Event) error {
	if err := event.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("кодировать событие: %w", err)
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
			event_id, source_uuid, source_version, content_digest, payload_json, state,
			next_attempt_at, expires_at, created_at, expired_at, last_error
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_uuid, source_version, content_digest) DO NOTHING`,
		event.ID, sourceKey, sourceVersion, event.ContentDigest, payload, state,
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
		if err := json.Unmarshal(payload, &queued.Event); err != nil {
			return nil, fmt.Errorf("декодировать событие очереди: %w", err)
		}
		events = append(events, queued)
	}
	return events, rows.Err()
}

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

func (queue *ClientQueue) MarkExpired(ctx context.Context, eventID string, at time.Time, reason string) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state='expired', expired_at=?, last_error=?
		WHERE event_id=? AND state IN ('pending', 'uploaded')`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID)
	return changedExactlyOnce(result, err, "отметить просроченное событие")
}

func (queue *ClientQueue) MarkAcknowledged(ctx context.Context, eventID string, at time.Time) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET state = 'acknowledged', acknowledged_at = ?, last_error = ''
		WHERE event_id = ? AND state IN ('pending', 'uploaded')`,
		at.UTC().Format(time.RFC3339Nano), eventID,
	)
	if err == nil {
		count, countErr := result.RowsAffected()
		if countErr == nil && count == 0 {
			var exists int
			if queryErr := queue.db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM client_events WHERE event_id = ? AND state = 'acknowledged'", eventID,
			).Scan(&exists); queryErr == nil && exists == 1 {
				return nil
			}
		}
	}
	return changedExactlyOnce(result, err, "подтвердить событие")
}

func (queue *ClientQueue) MarkRetry(ctx context.Context, eventID string, next time.Time, reason string) error {
	result, err := queue.db.ExecContext(ctx, `
		UPDATE client_events
		SET attempts = attempts + 1, next_attempt_at = ?, last_error = ?
		WHERE event_id = ? AND state IN ('pending', 'uploaded')`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID,
	)
	return changedExactlyOnce(result, err, "запланировать повтор")
}

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

func (queue *ClientQueue) SaveValue(ctx context.Context, name, value string) error {
	_, err := queue.db.ExecContext(ctx, `
		INSERT INTO client_values(name, value) VALUES(?, ?)
		ON CONFLICT(name) DO UPDATE SET value = excluded.value`, name, value)
	if err != nil {
		return fmt.Errorf("сохранить параметр состояния: %w", err)
	}
	return nil
}

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
