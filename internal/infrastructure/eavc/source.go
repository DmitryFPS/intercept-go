package eavc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"

	_ "modernc.org/sqlite"
)

var requiredColumns = []string{
	"id", "session_id", "session_uuid", "unit_name", "frequency",
	"group_code", "uuid", "content", "updated_by", "updated_at",
}

// Source читает EAVC только через SQLite mode=ro и PRAGMA query_only.
type Source struct {
	db             *sql.DB
	hasCatalogUUID bool
	hasSourceMode  bool
}

// Open проверяет файл и схему до запуска фонового чтения.
func Open(ctx context.Context, path string) (*Source, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("получить полный путь EAVC: %w", err)
	}
	dsn := "file:" + filepath.ToSlash(abs) +
		"?mode=ro&_pragma=query_only(1)&_pragma=busy_timeout(10000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("открыть EAVC: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("прочитать EAVC: %w", err)
	}
	columns, err := tableColumns(ctx, db, "intercept_items")
	if err != nil {
		db.Close()
		return nil, err
	}
	var missing []string
	for _, column := range requiredColumns {
		if !columns[column] {
			missing = append(missing, column)
		}
	}
	if len(missing) != 0 {
		db.Close()
		sort.Strings(missing)
		return nil, fmt.Errorf("схема EAVC не поддерживается, отсутствуют поля: %s", strings.Join(missing, ", "))
	}
	return &Source{
		db: db, hasCatalogUUID: columns["catalog_uuid"], hasSourceMode: columns["source_mode"],
	}, nil
}

func (source *Source) Close() error {
	return source.db.Close()
}

// Scan читает изменения в устойчивом порядке updated_at + id.
func (source *Source) Scan(ctx context.Context, after application.Cursor, limit int) ([]application.SourceItem, application.Cursor, error) {
	if limit <= 0 || limit > 1000 {
		return nil, after, errors.New("лимит чтения EAVC должен быть от 1 до 1000")
	}
	catalogExpression := "''"
	if source.hasCatalogUUID {
		catalogExpression = "COALESCE(catalog_uuid, '')"
	}
	modeExpression := "'unknown'"
	if source.hasSourceMode {
		modeExpression = "COALESCE(source_mode, 'unknown')"
	}
	query := fmt.Sprintf(`
		SELECT id, session_id, session_uuid, unit_name, frequency, group_code,
			uuid, content, updated_by, updated_at, %s, %s
		FROM intercept_items
		WHERE updated_at > ? OR (updated_at = ? AND id > ?)
		ORDER BY updated_at, id
		LIMIT ?`, catalogExpression, modeExpression)
	rows, err := source.db.QueryContext(ctx, query, after.UpdatedAt, after.UpdatedAt, after.ID, limit)
	if err != nil {
		return nil, after, fmt.Errorf("прочитать изменения EAVC: %w", err)
	}
	defer rows.Close()

	cursor := after
	var items []application.SourceItem
	for rows.Next() {
		var item application.SourceItem
		var mode string
		var updatedAt string
		var sessionID sql.NullInt64
		var sessionUUID, unitName, frequency, groupCode, sourceUUID sql.NullString
		var content, updatedBy, catalogUUID sql.NullString
		var id int64
		if err := rows.Scan(
			&id, &sessionID, &sessionUUID, &unitName, &frequency, &groupCode,
			&sourceUUID, &content, &updatedBy, &updatedAt, &catalogUUID, &mode,
		); err != nil {
			return nil, after, fmt.Errorf("декодировать запись EAVC: %w", err)
		}
		item.Record = domain.SourceRecord{
			ID: id, UUID: sourceUUID.String, SessionID: sessionID.Int64,
			SessionUUID: sessionUUID.String, UpdatedAt: updatedAt,
		}
		item.Position = domain.Position{
			CatalogUUID: catalogUUID.String, UnitName: unitName.String,
			Frequency: frequency.String, GroupCode: groupCode.String,
		}
		item.Mode = parseMode(mode)
		item.Content = content.String
		item.UpdatedBy = updatedBy.String
		item.ObservedAt = time.Now().UTC()
		items = append(items, item)
		cursor = application.Cursor{UpdatedAt: updatedAt, ID: id}
	}
	if err := rows.Err(); err != nil {
		return nil, after, fmt.Errorf("прочитать записи EAVC: %w", err)
	}
	return items, cursor, nil
}

func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info("+table+")")
	if err != nil {
		return nil, fmt.Errorf("прочитать схему %s: %w", table, err)
	}
	defer rows.Close()
	columns := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, fmt.Errorf("прочитать поле %s: %w", table, err)
		}
		columns[strings.ToLower(name)] = true
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("таблица %s не найдена", table)
	}
	return columns, rows.Err()
}

func parseMode(value string) domain.SourceMode {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "normal", "обычный":
		return domain.SourceModeNormal
	case "deferred", "отложенный":
		return domain.SourceModeDeferred
	default:
		return domain.SourceModeUnknown
	}
}
