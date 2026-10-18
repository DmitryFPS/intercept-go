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
`

// ServerStore реализует реестр, версии правил, входные события и очереди.
type ServerStore struct {
	db *sql.DB
}

func NewServerStore(ctx context.Context, db *sql.DB) (*ServerStore, error) {
	if err := Migrate(ctx, db, []string{serverMigration}); err != nil {
		return nil, err
	}
	return &ServerStore{db: db}, nil
}

func (store *ServerStore) Client(ctx context.Context, id string) (application.RegisteredClient, error) {
	var client application.RegisteredClient
	var active int
	err := store.db.QueryRowContext(ctx, `
		SELECT client_id, name, sender_bot_id, sender_bot_username, chat_id,
			thread_id, key_id, key_bytes, active
		FROM server_clients WHERE client_id = ?`, id).Scan(
		&client.ID, &client.Name, &client.SenderBotID, &client.SenderBotUsername,
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
	if client.ID == "" || client.Name == "" || client.SenderBotID == 0 || client.ChatID == 0 || client.ThreadID <= 0 {
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
			client_id, name, sender_bot_id, sender_bot_username, chat_id,
			thread_id, key_id, key_bytes, active, updated_at
		) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(client_id) DO UPDATE SET
			name=excluded.name, sender_bot_id=excluded.sender_bot_id,
			sender_bot_username=excluded.sender_bot_username, chat_id=excluded.chat_id,
			thread_id=excluded.thread_id, key_id=excluded.key_id,
			key_bytes=excluded.key_bytes, active=excluded.active, updated_at=excluded.updated_at`,
		client.ID, client.Name, client.SenderBotID, client.SenderBotUsername,
		client.ChatID, client.ThreadID, client.KeyID, client.Key, active,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("сохранить точку: %w", err)
	}
	return nil
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
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
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_jobs(
				event_id, destination_id, delivery_json, state, next_attempt_at
			) VALUES(?, ?, ?, 'pending', ?)`, event.ID, delivery.Destination.ID,
			deliveryPayload, now); err != nil {
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
	rows, err := store.db.QueryContext(ctx, `
		SELECT e.payload_json, j.delivery_json, j.attempts
		FROM delivery_jobs j
		JOIN received_events e ON e.event_id = j.event_id
		WHERE j.state = 'pending' AND j.next_attempt_at <= ?
		ORDER BY j.next_attempt_at, j.event_id, j.destination_id
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
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

func (store *ServerStore) MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET attempts=attempts+1, next_attempt_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "запланировать внешнюю доставку")
}
