package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
)

const (
	maxClientTimeStateBytes   = 16 << 20
	maxClientTimeStateRecords = 200_000
	clientTimeStateRetention  = 180 * 24 * time.Hour
)

func (queue *ClientQueue) LoadSourceTimeState(
	ctx context.Context, sourceKey string,
) (application.SourceTimeStateRecord, bool, error) {
	sourceKey = strings.TrimSpace(sourceKey)
	if sourceKey == "" {
		return application.SourceTimeStateRecord{}, false, errors.New("пустой ключ состояния временных блоков EAVC")
	}
	var protected []byte
	err := queue.db.QueryRowContext(ctx,
		"SELECT payload FROM client_time_state WHERE source_key=?", sourceKey,
	).Scan(&protected)
	if errors.Is(err, sql.ErrNoRows) {
		return application.SourceTimeStateRecord{}, false, nil
	}
	if err != nil {
		return application.SourceTimeStateRecord{}, false, fmt.Errorf("прочитать состояние временных блоков EAVC: %w", err)
	}
	plain, err := unprotectDatabaseValue(protected)
	if err != nil {
		return application.SourceTimeStateRecord{}, false, fmt.Errorf("открыть состояние временных блоков EAVC: %w", err)
	}
	var record application.SourceTimeStateRecord
	if err := json.Unmarshal(plain, &record); err != nil {
		return application.SourceTimeStateRecord{}, false, fmt.Errorf(
			"%w: разобрать сохранённое состояние: %v", application.ErrSourceTimeStateInvalid, err,
		)
	}
	if err := validateSourceTimeState(record); err != nil {
		return record, true, fmt.Errorf("%w: %v", application.ErrSourceTimeStateInvalid, err)
	}
	return record, true, nil
}

func (queue *ClientQueue) SaveSourceTimeState(
	ctx context.Context, sourceKey string, record application.SourceTimeStateRecord,
) error {
	sourceKey = strings.TrimSpace(sourceKey)
	if sourceKey == "" {
		return errors.New("пустой ключ состояния временных блоков EAVC")
	}
	if err := validateSourceTimeState(record); err != nil {
		return fmt.Errorf("%w: %v", application.ErrSourceTimeStateInvalid, err)
	}
	plain, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("кодировать состояние временных блоков EAVC: %w", err)
	}
	if len(plain) > maxClientTimeStateBytes {
		return fmt.Errorf("состояние временных блоков EAVC превышает предел %d байт", maxClientTimeStateBytes)
	}
	protected, err := protectDatabaseValue(plain)
	if err != nil {
		return fmt.Errorf("защитить состояние временных блоков EAVC: %w", err)
	}
	tx, err := queue.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать сохранение состояния временных блоков EAVC: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	if _, err = tx.ExecContext(ctx, "DELETE FROM client_time_state WHERE updated_at < ?",
		now.Add(-clientTimeStateRetention).Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("очистить устаревшее состояние временных блоков EAVC: %w", err)
	}
	var existing int
	if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_time_state WHERE source_key=?", sourceKey).Scan(&existing); err != nil {
		return fmt.Errorf("проверить состояние временных блоков EAVC: %w", err)
	}
	if existing == 0 {
		var count int
		if err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_time_state").Scan(&count); err != nil {
			return fmt.Errorf("посчитать состояния временных блоков EAVC: %w", err)
		}
		if count >= maxClientTimeStateRecords {
			remove := count - maxClientTimeStateRecords + 1
			if _, err = tx.ExecContext(ctx, `DELETE FROM client_time_state WHERE source_key IN (
				SELECT source_key FROM client_time_state ORDER BY updated_at,source_key LIMIT ?
			)`, remove); err != nil {
				return fmt.Errorf("ограничить историю временных блоков EAVC: %w", err)
			}
		}
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO client_time_state(source_key, payload, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(source_key) DO UPDATE SET
			payload=excluded.payload, updated_at=excluded.updated_at`,
		sourceKey, protected, now.Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("сохранить состояние временных блоков EAVC: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать состояние временных блоков EAVC: %w", err)
	}
	return nil
}

func validateSourceTimeState(record application.SourceTimeStateRecord) error {
	if record.Snapshot == nil && record.Conflict == nil {
		return errors.New("пустое состояние временных блоков EAVC")
	}
	if record.Snapshot == nil {
		return nil
	}
	if len(record.Snapshot.Blocks) > 10_000 {
		return fmt.Errorf("слишком много временных блоков в состоянии EAVC: %d", len(record.Snapshot.Blocks))
	}
	seen := make(map[string]bool, len(record.Snapshot.Blocks))
	for _, entry := range record.Snapshot.Blocks {
		occurrence := strings.ToLower(strings.TrimSpace(entry.OccurrenceID))
		identity := strings.ToLower(strings.TrimSpace(entry.Block.UUID))
		if occurrence == "" || identity == "" || strings.TrimSpace(entry.CanonicalTime) == "" {
			return errors.New("состояние EAVC содержит временной блок без логической идентичности")
		}
		if seen[identity] {
			return fmt.Errorf("состояние EAVC содержит повтор логического UUID %q", entry.Block.UUID)
		}
		seen[identity] = true
	}
	return nil
}
