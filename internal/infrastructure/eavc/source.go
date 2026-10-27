package eavc

import (
	"context"
	"crypto/sha256"
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

var requiredBlockColumns = []string{
	"uuid", "item_uuid", "item_id", "time_key", "day_offset", "ord",
	"header", "body", "text_hash", "updated_at", "deleted_at",
}

// maxBoundaryReplay ограничивает повторное чтение записей с одной секундой
// updated_at. До этого ошибочная или искусственно плотная база могла вернуть
// неограниченный объём, обходя BatchSize. Переполнение не замалчивается:
// оператор получает явную ошибку, а курсор не сдвигается.
const maxBoundaryReplay = 1000

// Source читает EAVC только через SQLite mode=ro и PRAGMA query_only.
type Source struct {
	db              *sql.DB
	hasCatalogUUID  bool
	hasSourceMode   bool
	hasCallsigns    bool
	callsignColumns map[string]bool
	hasArchive      bool
}

func (source *Source) allItemsQuery() string {
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
			uuid, content, updated_by, updated_at, %s AS catalog_uuid, %s AS source_mode,
			0 AS is_archive
		FROM intercept_items`, catalogExpression, modeExpression)
	if source.hasArchive {
		query += ` UNION ALL
		SELECT id, session_id, session_uuid, unit_name, frequency, group_code,
			uuid, content, updated_by, updated_at, '' AS catalog_uuid, 'unknown' AS source_mode,
			1 AS is_archive
		FROM intercept_items_archive`
	}
	return query
}

// Head возвращает неизменяемую верхнюю границу для безопасного первого запуска.
func (source *Source) Head(ctx context.Context) (application.Cursor, error) {
	query := fmt.Sprintf(`
		SELECT updated_at, id FROM (%s)
		ORDER BY julianday(updated_at) DESC, id DESC LIMIT 1`, source.allItemsQuery())
	var cursor application.Cursor
	err := source.db.QueryRowContext(ctx, query).Scan(&cursor.UpdatedAt, &cursor.ID)
	if errors.Is(err, sql.ErrNoRows) {
		return application.Cursor{}, nil
	}
	if err != nil {
		return application.Cursor{}, fmt.Errorf("определить верхнюю границу EAVC: %w", err)
	}
	return cursor, nil
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
	hasBlocks, err := hasTable(ctx, db, "intercept_blocks")
	if err != nil {
		db.Close()
		return nil, err
	}
	if !hasBlocks {
		db.Close()
		return nil, errors.New("схема EAVC не поддерживается: отсутствует таблица intercept_blocks")
	}
	blockColumns, err := tableColumns(ctx, db, "intercept_blocks")
	if err != nil {
		db.Close()
		return nil, err
	}
	missing = missing[:0]
	for _, column := range requiredBlockColumns {
		if !blockColumns[column] {
			missing = append(missing, column)
		}
	}
	if len(missing) != 0 {
		db.Close()
		sort.Strings(missing)
		return nil, fmt.Errorf("схема блоков EAVC не поддерживается, отсутствуют поля: %s", strings.Join(missing, ", "))
	}
	hasCallsigns, err := hasTable(ctx, db, "intercept_callsigns")
	if err != nil {
		db.Close()
		return nil, err
	}
	callsignColumns := map[string]bool{}
	if hasCallsigns {
		callsignColumns, err = tableColumns(ctx, db, "intercept_callsigns")
		if err != nil {
			db.Close()
			return nil, err
		}
		if !callsignColumns["code"] || !callsignColumns["label"] {
			db.Close()
			return nil, errors.New("схема позывных EAVC не поддерживается: отсутствуют code или label")
		}
	}
	hasArchive, err := hasTable(ctx, db, "intercept_items_archive")
	if err != nil {
		db.Close()
		return nil, err
	}
	if hasArchive {
		archiveColumns, archiveErr := tableColumns(ctx, db, "intercept_items_archive")
		if archiveErr != nil {
			db.Close()
			return nil, archiveErr
		}
		for _, column := range requiredColumns {
			if !archiveColumns[column] {
				db.Close()
				return nil, fmt.Errorf("схема архива EAVC не поддерживается, отсутствует поле: %s", column)
			}
		}
	}
	return &Source{
		db: db, hasCatalogUUID: columns["catalog_uuid"], hasSourceMode: columns["source_mode"],
		hasCallsigns: hasCallsigns, callsignColumns: callsignColumns, hasArchive: hasArchive,
	}, nil
}

// Close освобождает соединение с базой EAVC, не изменяя её файл.
func (source *Source) Close() error {
	return source.db.Close()
}

// Scan читает изменения в устойчивом порядке updated_at + id.
func (source *Source) Scan(ctx context.Context, after application.Cursor, limit int) ([]application.SourceItem, application.Cursor, error) {
	if limit <= 0 || limit > 1000 {
		return nil, after, errors.New("лимит чтения EAVC должен быть от 1 до 1000")
	}
	allItemsQuery := source.allItemsQuery()
	query := fmt.Sprintf(`
		WITH all_items AS (%s),
		newer AS (
			SELECT * FROM all_items
			WHERE ? = '' OR julianday(updated_at) > julianday(?)
			ORDER BY julianday(updated_at), id
			LIMIT ?
		),
		boundary AS (
			SELECT * FROM all_items
			WHERE ? <> '' AND julianday(updated_at) = julianday(?)
			ORDER BY id DESC
			LIMIT ?
		)
		SELECT id, session_id, session_uuid, unit_name, frequency, group_code,
			uuid, content, updated_by, updated_at, catalog_uuid, source_mode, is_archive, is_boundary
		FROM (
			SELECT *, 0 AS is_boundary FROM newer
			UNION ALL
			SELECT *, 1 AS is_boundary FROM boundary
		)
		ORDER BY julianday(updated_at), id
		`, allItemsQuery)
	rows, err := source.db.QueryContext(
		ctx, query, after.UpdatedAt, after.UpdatedAt, limit,
		after.UpdatedAt, after.UpdatedAt, maxBoundaryReplay+1,
	)
	if err != nil {
		return nil, after, fmt.Errorf("прочитать изменения EAVC: %w", err)
	}
	defer rows.Close()

	cursor := after
	var items []application.SourceItem
	var archived []bool
	boundaryCount := 0
	for rows.Next() {
		var item application.SourceItem
		var mode string
		var updatedAt string
		var sessionID sql.NullInt64
		var sessionUUID, unitName, frequency, groupCode, sourceUUID sql.NullString
		var content, updatedBy, catalogUUID sql.NullString
		var id int64
		var isArchive, isBoundary bool
		if err := rows.Scan(
			&id, &sessionID, &sessionUUID, &unitName, &frequency, &groupCode,
			&sourceUUID, &content, &updatedBy, &updatedAt, &catalogUUID, &mode, &isArchive, &isBoundary,
		); err != nil {
			return nil, after, fmt.Errorf("декодировать запись EAVC: %w", err)
		}
		if isBoundary {
			boundaryCount++
			if boundaryCount > maxBoundaryReplay {
				return nil, after, fmt.Errorf(
					"в EAVC более %d записей с одной отметкой времени %q; чтение остановлено без сдвига курсора",
					maxBoundaryReplay, after.UpdatedAt,
				)
			}
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
		if domain.IsDeferredUnitName(unitName.String) {
			item.Mode = domain.SourceModeDeferred
		}
		item.Content = content.String
		item.Archived = isArchive
		item.UpdatedBy = updatedBy.String
		item.ObservedAt = time.Now().UTC()
		items = append(items, item)
		archived = append(archived, isArchive)
		cursor = application.Cursor{UpdatedAt: updatedAt, ID: id}
	}
	if err := rows.Err(); err != nil {
		return nil, after, fmt.Errorf("прочитать записи EAVC: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, after, fmt.Errorf("закрыть чтение EAVC: %w", err)
	}
	for index := range items {
		if archived[index] {
			items[index].Blocks = legacyArchiveBlocks(items[index])
		} else {
			blocks, deletedBlocks, err := source.readBlocks(ctx, items[index])
			if err != nil {
				return nil, after, err
			}
			items[index].Blocks = blocks
			items[index].DeletedBlocks = deletedBlocks
		}
		if source.hasCallsigns {
			if err := source.enrichCallsigns(ctx, &items[index]); err != nil {
				return nil, after, err
			}
		}
	}
	return items, cursor, nil
}

func (source *Source) readBlocks(ctx context.Context, item application.SourceItem) ([]domain.ContentBlock, []domain.ContentBlock, error) {
	rows, err := source.db.QueryContext(ctx, `
		SELECT uuid, item_uuid, item_id, time_key, day_offset, ord,
			header, body, text_hash, updated_at, deleted_at
		FROM intercept_blocks
		WHERE item_id=?
		ORDER BY (deleted_at IS NOT NULL), day_offset, (time_key IS NULL), time_key, ord`, item.Record.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("прочитать блоки EAVC для записи %d: %w", item.Record.ID, err)
	}
	defer rows.Close()
	blocks := make([]domain.ContentBlock, 0)
	deletedBlocks := make([]domain.ContentBlock, 0)
	texts := make([]string, 0)
	for rows.Next() {
		var uuid, itemUUID, header, body, storedHash, updatedAt string
		var deletedAt sql.NullString
		var itemID int64
		var timeKey sql.NullInt64
		var dayOffset, order int
		if err := rows.Scan(&uuid, &itemUUID, &itemID, &timeKey, &dayOffset, &order,
			&header, &body, &storedHash, &updatedAt, &deletedAt); err != nil {
			return nil, nil, fmt.Errorf("декодировать блок EAVC для записи %d: %w", item.Record.ID, err)
		}
		if strings.TrimSpace(uuid) == "" || itemID != item.Record.ID ||
			(strings.TrimSpace(item.Record.UUID) != "" && itemUUID != item.Record.UUID) {
			return nil, nil, fmt.Errorf("нарушен контракт EAVC: блок записи %d имеет неверную связь с исходным перехватом", item.Record.ID)
		}
		text := strings.TrimSpace(strings.TrimSpace(header) + "\n" + strings.TrimSpace(body))
		digest := sha256.Sum256([]byte(text))
		hash := fmt.Sprintf("%x", digest[:])
		if storedHash != "" && !strings.EqualFold(storedHash, hash) {
			return nil, nil, fmt.Errorf("нарушен контракт EAVC: хеш блока %s не соответствует его тексту", uuid)
		}
		key := strings.TrimSpace(header)
		if key == "" {
			key = "без времени"
		}
		block := domain.ContentBlock{
			UUID: uuid, Key: key, Sequence: order, Text: text, Hash: hash,
			UpdatedAt: updatedAt, DeletedAt: deletedAt.String,
		}
		if deletedAt.Valid && strings.TrimSpace(deletedAt.String) != "" {
			deletedBlocks = append(deletedBlocks, block)
		} else {
			block.Sequence = len(blocks)
			blocks = append(blocks, block)
			texts = append(texts, text)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("прочитать блоки EAVC для записи %d: %w", item.Record.ID, err)
	}
	if normalizeSnapshot(strings.Join(texts, "\n")) != normalizeSnapshot(item.Content) {
		return nil, nil, fmt.Errorf("нарушен контракт EAVC: текст записи %d и таблица intercept_blocks временно расходятся", item.Record.ID)
	}
	return blocks, deletedBlocks, nil
}

func legacyArchiveBlocks(item application.SourceItem) []domain.ContentBlock {
	blocks := domain.SplitContentBlocks(item.Content)
	for index := range blocks {
		seed := fmt.Sprintf("%s|%d|%d|%s", item.Record.UUID, item.Record.ID, index, blocks[index].Hash)
		digest := sha256.Sum256([]byte(seed))
		blocks[index].UUID = fmt.Sprintf("legacy-%x", digest[:16])
		blocks[index].UpdatedAt = item.Record.UpdatedAt
	}
	return blocks
}

func normalizeSnapshot(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	lines := strings.Split(value, "\n")
	compact := make([]string, 0, len(lines))
	empty := false
	for _, line := range lines {
		line = strings.TrimRight(line, " \t")
		if strings.TrimSpace(line) == "" {
			if len(compact) == 0 || empty {
				continue
			}
			empty = true
			compact = append(compact, "")
			continue
		}
		empty = false
		compact = append(compact, line)
	}
	for len(compact) > 0 && compact[len(compact)-1] == "" {
		compact = compact[:len(compact)-1]
	}
	return strings.Join(compact, "\n")
}

func (source *Source) enrichCallsigns(ctx context.Context, item *application.SourceItem) error {
	codes := domain.ExtractCorrespondentCodes(item.Content)
	if len(codes) == 0 {
		return nil
	}
	callsigns := make(map[string]domain.Callsign)
	lookup := func(where string, args []any, missing []string) error {
		if len(missing) == 0 {
			return nil
		}
		placeholders := strings.TrimRight(strings.Repeat("?,", len(missing)), ",")
		numberExpression := "0"
		if source.callsignColumns["id"] {
			numberExpression = "id"
		}
		tagExpression := "''"
		if source.callsignColumns["tag"] {
			tagExpression = "tag"
		}
		descriptionExpression := "''"
		if source.callsignColumns["tag_desc"] {
			descriptionExpression = "tag_desc"
		}
		query := "SELECT " + numberExpression + ", code, label, " + tagExpression + ", " + descriptionExpression +
			" FROM intercept_callsigns WHERE " + where +
			" AND code IN (" + placeholders + ")"
		args = append(args, stringsToAny(missing)...)
		rows, err := source.db.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("прочитать позывные EAVC: %w", err)
		}
		for rows.Next() {
			var number sql.NullInt64
			var code, label, tag, description sql.NullString
			if err := rows.Scan(&number, &code, &label, &tag, &description); err != nil {
				rows.Close()
				return fmt.Errorf("декодировать позывной EAVC: %w", err)
			}
			key := domain.NormalizeCorrespondentCode(code.String)
			if key != "" && strings.TrimSpace(label.String) != "" {
				if _, exists := callsigns[key]; !exists {
					callsigns[key] = domain.Callsign{
						Number: number.Int64, Code: code.String, Label: label.String,
						Tag: tag.String, Description: description.String,
					}
				}
			}
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil {
			return rowErr
		}
		return closeErr
	}
	missing := func() []string {
		result := make([]string, 0)
		for _, code := range codes {
			if _, exists := callsigns[domain.NormalizeCorrespondentCode(code)]; !exists {
				result = append(result, code)
			}
		}
		return result
	}
	frequency := strings.TrimSpace(item.Position.Frequency)
	group := strings.TrimSpace(item.Position.GroupCode)
	if frequency != "" && group != "" {
		if err := lookup("frequency = ? AND group_code = ?", []any{frequency, group}, missing()); err != nil {
			return err
		}
	}
	unit := strings.TrimSpace(item.Position.UnitName)
	if unit != "" {
		if err := lookup("unit_name = ?", []any{unit}, missing()); err != nil {
			return err
		}
	}
	if err := lookup("1 = 1", nil, missing()); err != nil {
		return err
	}
	for blockIndex := range item.Blocks {
		for _, code := range domain.ExtractCorrespondentCodes(item.Blocks[blockIndex].Text) {
			callsign := callsigns[domain.NormalizeCorrespondentCode(code)]
			callsign.Code = code
			if strings.TrimSpace(callsign.Label) == "" {
				callsign.Label = "н/у"
			}
			item.Blocks[blockIndex].Callsigns = append(
				item.Blocks[blockIndex].Callsigns, callsign,
			)
		}
	}
	return nil
}

func stringsToAny(values []string) []any {
	result := make([]any, len(values))
	for index := range values {
		result[index] = values[index]
	}
	return result
}

func hasTable(ctx context.Context, db *sql.DB, table string) (bool, error) {
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table,
	).Scan(&count); err != nil {
		return false, fmt.Errorf("проверить таблицу %s: %w", table, err)
	}
	return count == 1, nil
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
