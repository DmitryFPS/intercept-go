package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

const serverMigration = `
CREATE TABLE server_clients (
	client_id TEXT PRIMARY KEY,
	client_number TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL,
	sender_bot_id INTEGER NOT NULL,
	sender_bot_username TEXT NOT NULL,
	chat_id INTEGER NOT NULL,
	thread_id INTEGER NOT NULL,
	key_id TEXT NOT NULL,
	key_bytes BLOB NOT NULL,
	active INTEGER NOT NULL CHECK(active IN (0, 1)),
	updated_at TEXT NOT NULL
);
CREATE TABLE route_versions (
	version INTEGER PRIMARY KEY,
	payload_json BLOB NOT NULL,
	author TEXT NOT NULL,
	created_at TEXT NOT NULL,
	active INTEGER NOT NULL CHECK(active IN (0, 1))
);
CREATE UNIQUE INDEX one_active_route_version ON route_versions(active) WHERE active = 1;
CREATE TABLE received_events (
	event_id TEXT PRIMARY KEY,
	client_id TEXT NOT NULL,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	rule_version INTEGER NOT NULL,
	received_at TEXT NOT NULL,
	FOREIGN KEY(client_id) REFERENCES server_clients(client_id)
);
CREATE TABLE delivery_jobs (
	event_id TEXT NOT NULL,
	destination_id TEXT NOT NULL,
	delivery_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'delivered')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	delivered_at TEXT,
	PRIMARY KEY(event_id, destination_id),
	FOREIGN KEY(event_id) REFERENCES received_events(event_id) ON DELETE CASCADE
);
CREATE INDEX delivery_jobs_pending_idx ON delivery_jobs(state, next_attempt_at);
CREATE TABLE server_values (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE rejected_packets (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	sender_bot_id INTEGER NOT NULL,
	sender_bot_username TEXT NOT NULL,
	chat_id INTEGER NOT NULL,
	thread_id INTEGER NOT NULL,
	reason TEXT NOT NULL,
	payload BLOB NOT NULL,
	received_at TEXT NOT NULL
);
`

const serverDeliveryOrderMigration = `
ALTER TABLE delivery_jobs RENAME TO delivery_jobs_v1;
DROP INDEX delivery_jobs_pending_idx;
CREATE TABLE delivery_jobs (
	event_id TEXT NOT NULL,
	destination_id TEXT NOT NULL,
	delivery_json BLOB NOT NULL,
	stream_key TEXT NOT NULL,
	source_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'delivered', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	delivered_at TEXT,
	expired_at TEXT,
	PRIMARY KEY(event_id, destination_id),
	FOREIGN KEY(event_id) REFERENCES received_events(event_id) ON DELETE CASCADE
);
INSERT INTO delivery_jobs(
	event_id, destination_id, delivery_json, stream_key, source_at, expires_at,
	state, attempts, next_attempt_at, last_error, delivered_at
)
SELECT j.event_id, j.destination_id, j.delivery_json, j.destination_id,
	e.received_at, e.received_at, j.state, j.attempts, j.next_attempt_at,
	j.last_error, j.delivered_at
FROM delivery_jobs_v1 j
JOIN received_events e ON e.event_id = j.event_id;
DROP TABLE delivery_jobs_v1;
CREATE INDEX delivery_jobs_pending_idx
	ON delivery_jobs(state, stream_key, source_at, event_id, next_attempt_at);
`

// ServerStore реализует реестр, версии правил, входные события и очереди.
type ServerStore struct {
	db *sql.DB
}

type ServerStats struct {
	Clients   int `json:"clients"`
	Events    int `json:"events"`
	Pending   int `json:"pending"`
	Delivered int `json:"delivered"`
	Expired   int `json:"expired"`
	Rejected  int `json:"rejected"`
}

// RuleVersion содержит журнал публикаций без копирования всей матрицы в интерфейс.
type RuleVersion struct {
	Version   int64     `json:"version"`
	Author    string    `json:"author"`
	CreatedAt time.Time `json:"created_at"`
	Active    bool      `json:"active"`
}

func NewServerStore(ctx context.Context, db *sql.DB) (*ServerStore, error) {
	if err := Migrate(ctx, db, []string{serverMigration, serverDeliveryOrderMigration}); err != nil {
		return nil, err
	}
	return &ServerStore{db: db}, nil
}

// Close освобождает файл базы после штатной остановки сервера.
func (store *ServerStore) Close() error {
	return store.db.Close()
}

func (store *ServerStore) Client(ctx context.Context, id string) (application.RegisteredClient, error) {
	var client application.RegisteredClient
	var active int
	err := store.db.QueryRowContext(ctx, `
		SELECT client_id, client_number, name, sender_bot_id, sender_bot_username, chat_id,
			thread_id, key_id, key_bytes, active
		FROM server_clients WHERE client_id = ?`, id).Scan(
		&client.ID, &client.Number, &client.Name, &client.SenderBotID, &client.SenderBotUsername,
		&client.ChatID, &client.ThreadID, &client.KeyID, &client.Key, &active,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return application.RegisteredClient{}, errors.New("точка не найдена")
	}
	if err != nil {
		return application.RegisteredClient{}, fmt.Errorf("прочитать точку: %w", err)
	}
	client.Active = active == 1
	return client, nil
}

func (store *ServerStore) SaveClient(ctx context.Context, client application.RegisteredClient) error {
	if client.ID == "" || client.Number == "" || client.Name == "" || client.SenderBotID == 0 || client.ChatID == 0 || client.ThreadID <= 0 {
		return errors.New("запись точки заполнена не полностью")
	}
	if len(client.Key) != 32 || client.KeyID == "" {
		return errors.New("ключ точки некорректен")
	}
	active := 0
	if client.Active {
		active = 1
	}
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO server_clients(
			client_id, client_number, name, sender_bot_id, sender_bot_username, chat_id,
			thread_id, key_id, key_bytes, active, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(client_id) DO UPDATE SET
			client_number=excluded.client_number, name=excluded.name, sender_bot_id=excluded.sender_bot_id,
			sender_bot_username=excluded.sender_bot_username, chat_id=excluded.chat_id,
			thread_id=excluded.thread_id, key_id=excluded.key_id,
			key_bytes=excluded.key_bytes, active=excluded.active, updated_at=excluded.updated_at`,
		client.ID, client.Number, client.Name, client.SenderBotID, client.SenderBotUsername,
		client.ChatID, client.ThreadID, client.KeyID, client.Key, active,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("сохранить точку: %w", err)
	}
	return nil
}

func (store *ServerStore) Clients(ctx context.Context) ([]application.RegisteredClient, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT client_id, client_number, name, sender_bot_id, sender_bot_username,
			chat_id, thread_id, key_id, active
		FROM server_clients ORDER BY client_number, name`)
	if err != nil {
		return nil, fmt.Errorf("прочитать точки: %w", err)
	}
	defer rows.Close()
	var clients []application.RegisteredClient
	for rows.Next() {
		var client application.RegisteredClient
		var active int
		if err := rows.Scan(
			&client.ID, &client.Number, &client.Name, &client.SenderBotID,
			&client.SenderBotUsername, &client.ChatID, &client.ThreadID,
			&client.KeyID, &active,
		); err != nil {
			return nil, err
		}
		client.Active = active == 1
		clients = append(clients, client)
	}
	return clients, rows.Err()
}

func (store *ServerStore) Value(ctx context.Context, name string) (string, bool, error) {
	var value string
	err := store.db.QueryRowContext(ctx, "SELECT value FROM server_values WHERE name=?", name).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func (store *ServerStore) SaveValue(ctx context.Context, name, value string) error {
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO server_values(name,value) VALUES(?,?)
		ON CONFLICT(name) DO UPDATE SET value=excluded.value`, name, value)
	return err
}

func (store *ServerStore) Quarantine(ctx context.Context, packet application.IncomingPacket, reason string) error {
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id, sender_bot_username, chat_id, thread_id, reason, payload, received_at
		) VALUES(?,?,?,?,?,?,?)`, packet.SenderBotID, packet.SenderBotUsername,
		packet.ChatID, packet.ThreadID, reason, packet.Payload, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("сохранить пакет в карантин: %w", err)
	}
	return nil
}

func (store *ServerStore) Stats(ctx context.Context) (ServerStats, error) {
	var stats ServerStats
	queries := []struct {
		query  string
		target *int
	}{
		{"SELECT COUNT(*) FROM server_clients WHERE active=1", &stats.Clients},
		{"SELECT COUNT(*) FROM received_events", &stats.Events},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='pending'", &stats.Pending},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='delivered'", &stats.Delivered},
		{"SELECT COUNT(*) FROM delivery_jobs WHERE state='expired'", &stats.Expired},
		{"SELECT COUNT(*) FROM rejected_packets", &stats.Rejected},
	}
	for _, item := range queries {
		if err := store.db.QueryRowContext(ctx, item.query).Scan(item.target); err != nil {
			return ServerStats{}, err
		}
	}
	return stats, nil
}

func (store *ServerStore) ActiveRules(ctx context.Context) (domain.RuleSet, error) {
	var payload []byte
	err := store.db.QueryRowContext(ctx,
		"SELECT payload_json FROM route_versions WHERE active = 1",
	).Scan(&payload)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.RuleSet{}, errors.New("маршруты ещё не опубликованы")
	}
	if err != nil {
		return domain.RuleSet{}, fmt.Errorf("прочитать маршруты: %w", err)
	}
	var rules domain.RuleSet
	if err := json.Unmarshal(payload, &rules); err != nil {
		return domain.RuleSet{}, errors.New("опубликованные маршруты повреждены")
	}
	if err := rules.Validate(); err != nil {
		return domain.RuleSet{}, fmt.Errorf("опубликованные маршруты некорректны: %w", err)
	}
	return rules, nil
}

func (store *ServerStore) RuleHistory(ctx context.Context) ([]RuleVersion, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT version, author, created_at, active
		FROM route_versions ORDER BY version DESC`)
	if err != nil {
		return nil, fmt.Errorf("прочитать историю маршрутов: %w", err)
	}
	defer rows.Close()
	var result []RuleVersion
	for rows.Next() {
		var item RuleVersion
		var createdAt string
		var active int
		if err := rows.Scan(&item.Version, &item.Author, &createdAt, &active); err != nil {
			return nil, fmt.Errorf("прочитать версию маршрутов: %w", err)
		}
		item.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		item.Active = active == 1
		result = append(result, item)
	}
	return result, rows.Err()
}

func (store *ServerStore) RulesByVersion(ctx context.Context, version int64) (domain.RuleSet, error) {
	var payload []byte
	if err := store.db.QueryRowContext(ctx,
		"SELECT payload_json FROM route_versions WHERE version = ?", version,
	).Scan(&payload); errors.Is(err, sql.ErrNoRows) {
		return domain.RuleSet{}, errors.New("версия маршрутов не найдена")
	} else if err != nil {
		return domain.RuleSet{}, fmt.Errorf("прочитать версию маршрутов: %w", err)
	}
	var rules domain.RuleSet
	if err := json.Unmarshal(payload, &rules); err != nil {
		return domain.RuleSet{}, errors.New("версия маршрутов повреждена")
	}
	return rules, nil
}

func (store *ServerStore) NextRuleVersion(ctx context.Context) (int64, error) {
	var version int64
	if err := store.db.QueryRowContext(ctx,
		"SELECT COALESCE(MAX(version), 0) + 1 FROM route_versions",
	).Scan(&version); err != nil {
		return 0, fmt.Errorf("определить следующую версию маршрутов: %w", err)
	}
	return version, nil
}

func (store *ServerStore) PublishRules(ctx context.Context, rules domain.RuleSet, author string) error {
	if err := rules.Validate(); err != nil {
		return err
	}
	payload, err := json.Marshal(rules)
	if err != nil {
		return fmt.Errorf("кодировать маршруты: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать публикацию маршрутов: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "UPDATE route_versions SET active = 0 WHERE active = 1"); err != nil {
		return fmt.Errorf("отключить предыдущие маршруты: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO route_versions(version, payload_json, author, created_at, active)
		VALUES(?, ?, ?, ?, 1)`, rules.Version, payload, author,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("опубликовать маршруты: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("завершить публикацию маршрутов: %w", err)
	}
	return nil
}

func (store *ServerStore) Accept(ctx context.Context, event domain.Event, plan domain.DeliveryPlan) (bool, error) {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("кодировать событие: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("начать приём события: %w", err)
	}
	defer tx.Rollback()
	var existingDigest string
	err = tx.QueryRowContext(ctx,
		"SELECT content_digest FROM received_events WHERE event_id = ?", event.ID,
	).Scan(&existingDigest)
	if err == nil {
		if existingDigest != event.ContentDigest {
			return false, errors.New("конфликт: UUID события уже принят с другим содержимым")
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("проверить повтор события: %w", err)
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO received_events(
			event_id, client_id, source_uuid, content_digest, payload_json, rule_version, received_at
		) VALUES(?, ?, ?, ?, ?, ?, ?)`, event.ID, event.ClientID, event.Source.UUID,
		event.ContentDigest, eventPayload, plan.RuleSetVersion, now); err != nil {
		return false, fmt.Errorf("сохранить входное событие: %w", err)
	}
	for _, delivery := range plan.Deliveries {
		deliveryPayload, err := json.Marshal(delivery)
		if err != nil {
			return false, fmt.Errorf("кодировать задание доставки: %w", err)
		}
		sourceAt := event.SourceAt.UTC()
		if sourceAt.IsZero() {
			sourceAt = event.CreatedAt.UTC()
		}
		expiresAt := event.ExpiresAt.UTC()
		if expiresAt.IsZero() {
			expiresAt = sourceAt.Add(30 * time.Minute)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_jobs(
				event_id, destination_id, delivery_json, stream_key, source_at,
				expires_at, state, next_attempt_at
			) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)`, event.ID, delivery.Destination.ID,
			deliveryPayload, deliveryStreamKey(delivery), sourceAt.Format(time.RFC3339Nano),
			expiresAt.Format(time.RFC3339Nano), now); err != nil {
			return false, fmt.Errorf("создать задание %s: %w", delivery.Destination.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить приём события: %w", err)
	}
	return true, nil
}

func (store *ServerStore) PendingDeliveries(ctx context.Context, limit int, now time.Time) ([]application.DeliveryJob, error) {
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("лимит заданий должен быть от 1 до 1000")
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET state='expired', expired_at=?, last_error=?
		WHERE state='pending' AND expires_at < ?`,
		now.UTC().Format(time.RFC3339Nano), domain.FreshnessExpired,
		now.UTC().Format(time.RFC3339Nano)); err != nil {
		return nil, fmt.Errorf("завершить просроченные доставки: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT e.payload_json, j.delivery_json, j.attempts
		FROM delivery_jobs j
		JOIN received_events e ON e.event_id = j.event_id
		WHERE j.state = 'pending' AND j.next_attempt_at <= ? AND j.expires_at >= ?
			AND NOT EXISTS (
				SELECT 1 FROM delivery_jobs earlier
				WHERE earlier.state = 'pending' AND earlier.stream_key = j.stream_key
					AND (earlier.source_at < j.source_at OR
						(earlier.source_at = j.source_at AND earlier.event_id < j.event_id))
			)
		ORDER BY j.source_at, j.event_id, j.destination_id
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать очередь доставки: %w", err)
	}
	defer rows.Close()
	var jobs []application.DeliveryJob
	for rows.Next() {
		var eventPayload, deliveryPayload []byte
		var job application.DeliveryJob
		if err := rows.Scan(&eventPayload, &deliveryPayload, &job.Attempts); err != nil {
			return nil, fmt.Errorf("прочитать задание доставки: %w", err)
		}
		if err := json.Unmarshal(eventPayload, &job.Event); err != nil {
			return nil, errors.New("сохранённое событие повреждено")
		}
		if err := json.Unmarshal(deliveryPayload, &job.Delivery); err != nil {
			return nil, errors.New("сохранённое задание повреждено")
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (store *ServerStore) MarkDelivered(ctx context.Context, eventID, destinationID string, at time.Time) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='delivered', delivered_at=?, last_error=''
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), eventID, destinationID)
	return changedExactlyOnce(result, err, "подтвердить внешнюю доставку")
}

func (store *ServerStore) MarkDeliveryExpired(
	ctx context.Context, eventID, destinationID string, at time.Time, reason string,
) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='expired', expired_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "завершить просроченную доставку")
}

func (store *ServerStore) MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET attempts=attempts+1, next_attempt_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "запланировать внешнюю доставку")
}

func deliveryStreamKey(delivery domain.PlannedDelivery) string {
	if delivery.Destination.Kind == domain.DestinationTelegram {
		return fmt.Sprintf("%s:topic:%d", delivery.Destination.ID, delivery.Decision.ThreadID)
	}
	return delivery.Destination.ID
}
