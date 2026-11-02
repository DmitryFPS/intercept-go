package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"intercept/internal/domain"
)

// NewClientQueue применяет миграции и открывает долговечную очередь точки.
func NewClientQueue(ctx context.Context, db *sql.DB) (*ClientQueue, error) {
	if err := Migrate(ctx, db, clientMigrationPlan()); err != nil {
		return nil, err
	}
	if err := protectExistingColumn(ctx, db, "client_events", "payload_json"); err != nil {
		return nil, fmt.Errorf("защитить очередь клиента: %w", err)
	}
	if err := backfillClientStreamKeys(ctx, db); err != nil {
		return nil, err
	}
	if err := cleanupRC88LegacyBlockArtifacts(ctx, db); err != nil {
		return nil, err
	}
	return &ClientQueue{db: db}, nil
}

func clientMigrationPlan() []string {
	return []string{
		clientMigration, clientExpiryMigration, clientSourceVersionMigration,
		clientStreamMigration, clientStreamRepairMigration, clientSourceStreamMigration,
		clientClearedEventsMigration, clientTimeStateMigration, clientTimeStateRetentionMigration,
		clientConfirmedTransportMigration, clientEnqueueSequenceMigration,
	}
}

// cleanupRC88LegacyBlockArtifacts удаляет только ложные локальные записи,
// которые rc88 мог создать сразу после перехода с цельной модели rc87.
// Их поблочная идентичность сначала сохраняется, поэтому граничное перечитывание
// EAVC не вернёт запись обратно. Реально отправлявшиеся события не затрагиваются.
func cleanupRC88LegacyBlockArtifacts(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT event_id, source_uuid, source_version, content_digest, payload_json
		FROM client_events
		WHERE state='expired' AND attempts=0 AND telegram_message_id IS NULL
		  AND instr(source_uuid, '|block:') > 0`)
	if err != nil {
		return fmt.Errorf("прочитать миграционные остатки rc88: %w", err)
	}
	type candidate struct {
		id, sourceUUID, sourceVersion, digest string
		payload                               []byte
	}
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.id, &value.sourceUUID, &value.sourceVersion, &value.digest, &value.payload); err != nil {
			rows.Close()
			return fmt.Errorf("прочитать миграционный остаток rc88: %w", err)
		}
		candidates = append(candidates, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(candidates) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	clearedAt := time.Now().UTC().Format(time.RFC3339Nano)
	for _, value := range candidates {
		plain, err := unprotectDatabaseValue(value.payload)
		if err != nil {
			return fmt.Errorf("прочитать миграционный остаток %s: %w", value.id, err)
		}
		var event domain.Event
		if err := json.Unmarshal(plain, &event); err != nil {
			return fmt.Errorf("разобрать миграционный остаток %s: %w", value.id, err)
		}
		if event.SourceBlockDeleted || event.SourceBlockUUID == "" || event.FreshnessIssue == "" {
			continue
		}
		legacySourceUUID := event.Source.UUID
		if legacySourceUUID == "" {
			legacySourceUUID = fmt.Sprintf("id:%d", event.Source.ID)
		}
		var known int
		if err := tx.QueryRowContext(ctx, `
			SELECT EXISTS(
				SELECT 1 FROM client_events WHERE source_uuid=? AND source_version=?
				UNION ALL
				SELECT 1 FROM client_cleared_events WHERE source_uuid=? AND source_version=?
			)`, legacySourceUUID, event.Source.UpdatedAt, legacySourceUUID, event.Source.UpdatedAt).Scan(&known); err != nil {
			return fmt.Errorf("проверить исходную версию остатка %s: %w", value.id, err)
		}
		if known == 0 {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO client_cleared_events(
				source_uuid, source_version, content_digest, cleared_at
			) VALUES(?, ?, ?, ?)`, value.sourceUUID, value.sourceVersion, value.digest, clearedAt); err != nil {
			return fmt.Errorf("запомнить миграционный остаток %s: %w", value.id, err)
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM client_events WHERE event_id=?", value.id); err != nil {
			return fmt.Errorf("удалить миграционный остаток %s: %w", value.id, err)
		}
	}
	return tx.Commit()
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
