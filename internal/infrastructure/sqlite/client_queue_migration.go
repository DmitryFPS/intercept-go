package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"intercept/internal/domain"
)

// NewClientQueue применяет миграции и открывает долговечную очередь точки.
func NewClientQueue(ctx context.Context, db *sql.DB) (*ClientQueue, error) {
	if err := Migrate(ctx, db, []string{
		clientMigration, clientExpiryMigration, clientSourceVersionMigration,
		clientStreamMigration, clientStreamRepairMigration, clientSourceStreamMigration,
		clientClearedEventsMigration,
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

// backfillClientStreamKeys восстанавливает новые ключи порядка у данных,
// которые были созданы ранней тестовой версией клиента.
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
