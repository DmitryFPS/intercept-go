package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
)

const (
	rejectedPacketLimit = 1000
	rejectedBytesLimit  = 64 * 1024 * 1024
)

// ServerStore объединяет постоянные хранилища сервера в одной транзакционной БД.
type ServerStore struct {
	db *sql.DB
}

// NewServerStore применяет миграции и защищает чувствительные столбцы.
func NewServerStore(ctx context.Context, db *sql.DB) (*ServerStore, error) {
	if err := Migrate(ctx, db, []string{
		serverMigration, serverDeliveryOrderMigration, serverStreamRepairMigration,
		serverObservabilityMigration,
	}); err != nil {
		return nil, err
	}
	for _, item := range []struct{ table, column string }{
		{"server_clients", "key_bytes"},
		{"received_events", "payload_json"},
		{"delivery_jobs", "delivery_json"},
		{"rejected_packets", "payload"},
	} {
		if err := protectExistingColumn(ctx, db, item.table, item.column); err != nil {
			return nil, fmt.Errorf("защитить хранилище сервера: %w", err)
		}
	}
	if err := backfillServerStreamKeys(ctx, db); err != nil {
		return nil, err
	}
	if err := repairIncidentTimes(ctx, db); err != nil {
		return nil, err
	}
	return &ServerStore{db: db}, nil
}

// repairIncidentTimes исправляет записи старых версий, в которых задержанный
// Telegram-пакет мог ошибочно поставить восстановление раньше начала сбоя.
func repairIncidentTimes(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		UPDATE client_connection_incidents
		SET recovered_at=CASE
			WHEN EXISTS(
				SELECT 1 FROM client_health h
				WHERE h.client_id=client_connection_incidents.client_id
				  AND julianday(h.last_seen_at)>=julianday(client_connection_incidents.started_at)
			) THEN (
			(SELECT h.last_seen_at FROM client_health h
			 WHERE h.client_id=client_connection_incidents.client_id
			   AND julianday(h.last_seen_at)>=julianday(client_connection_incidents.started_at))
			)
			WHEN id=(
				SELECT latest.id FROM client_connection_incidents latest
				WHERE latest.client_id=client_connection_incidents.client_id
				ORDER BY julianday(latest.started_at) DESC, latest.id DESC LIMIT 1
			) THEN NULL
			ELSE started_at
		END
		WHERE recovered_at IS NOT NULL
		  AND julianday(recovered_at)<julianday(started_at)`)
	if err != nil {
		return fmt.Errorf("исправить время истории связи: %w", err)
	}
	return nil
}

// Close освобождает файл базы после штатной остановки сервера.
func (store *ServerStore) Close() error {
	return store.db.Close()
}

// Client возвращает одну зарегистрированную точку вместе с расшифрованным ключом.
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
	client.Key, err = unprotectDatabaseValue(client.Key)
	if err != nil {
		return application.RegisteredClient{}, fmt.Errorf("прочитать ключ точки: %w", err)
	}
	client.Active = active == 1
	return client, nil
}

// SaveClient создаёт или безопасно обновляет регистрацию одной точки.
func (store *ServerStore) SaveClient(ctx context.Context, client application.RegisteredClient) error {
	if client.ID == "" || client.Number == "" || client.Name == "" ||
		client.SenderBotID == 0 || client.ChatID == 0 || client.ThreadID <= 0 {
		return errors.New("запись точки заполнена не полностью")
	}
	if len(client.Key) != 32 || client.KeyID == "" {
		return errors.New("ключ точки некорректен")
	}
	active := 0
	if client.Active {
		active = 1
	}
	protectedKey, err := protectDatabaseValue(client.Key)
	if err != nil {
		return fmt.Errorf("защитить ключ точки: %w", err)
	}
	_, err = store.db.ExecContext(ctx, `
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
		client.ChatID, client.ThreadID, client.KeyID, protectedKey, active,
		time.Now().UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("сохранить точку: %w", err)
	}
	return nil
}

// AuthorizeTransport проверяет отправителя по Telegram-метаданным до скачивания файла.
func (store *ServerStore) AuthorizeTransport(
	ctx context.Context, senderBotID int64, senderBotUsername string, chatID, threadID int64,
) (bool, error) {
	var registeredUsername string
	err := store.db.QueryRowContext(ctx, `
		SELECT sender_bot_username FROM server_clients
		WHERE sender_bot_id=? AND chat_id=? AND thread_id=? AND active=1`,
		senderBotID, chatID, threadID,
	).Scan(&registeredUsername)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("проверить транспортного отправителя: %w", err)
	}
	return normalizeTelegramUsername(registeredUsername) == normalizeTelegramUsername(senderBotUsername), nil
}

func normalizeTelegramUsername(value string) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(value), "@"))
}

// Clients возвращает список точек без извлечения их ключей из хранилища.
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

// Value читает небольшой технический параметр сервера.
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

// SaveValue сохраняет небольшой технический параметр сервера.
func (store *ServerStore) SaveValue(ctx context.Context, name, value string) error {
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO server_values(name,value) VALUES(?,?)
		ON CONFLICT(name) DO UPDATE SET value=excluded.value`, name, value)
	return err
}

// Quarantine сохраняет отклонённый пакет в защищённом ограниченном хранилище.
func (store *ServerStore) Quarantine(ctx context.Context, packet application.IncomingPacket, reason string) error {
	protectedPayload, err := protectDatabaseValue(packet.Payload)
	if err != nil {
		return fmt.Errorf("защитить пакет карантина: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать запись карантина: %w", err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id, sender_bot_username, chat_id, thread_id, reason, payload, received_at
		) VALUES(?,?,?,?,?,?,?)`, packet.SenderBotID, packet.SenderBotUsername,
		packet.ChatID, packet.ThreadID, reason, protectedPayload, time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("сохранить пакет в карантин: %w", err)
	}
	if err := trimQuarantine(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("завершить запись карантина: %w", err)
	}
	return nil
}

func trimQuarantine(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, LENGTH(payload) FROM rejected_packets ORDER BY id DESC`)
	if err != nil {
		return fmt.Errorf("проверить объём карантина: %w", err)
	}
	var count int
	var totalBytes, deleteAtAndBelow int64
	for rows.Next() {
		var id, size int64
		if err := rows.Scan(&id, &size); err != nil {
			rows.Close()
			return err
		}
		count++
		if count > rejectedPacketLimit || totalBytes+size > rejectedBytesLimit {
			deleteAtAndBelow = id
			break
		}
		totalBytes += size
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if deleteAtAndBelow == 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM rejected_packets WHERE id<=?", deleteAtAndBelow); err != nil {
		return fmt.Errorf("ограничить карантин: %w", err)
	}
	return nil
}
