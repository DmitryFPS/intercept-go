package eavc

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"intercept/internal/application"

	_ "modernc.org/sqlite"
)

// Клиент читает нужные поля из действующей и расширенной базы EAVC, не изменяя саму базу.
func TestSourceReadsCurrentAndFutureSchemaReadOnly(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE intercept_items (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT,
			catalog_uuid TEXT, source_mode TEXT
		);
		CREATE TABLE intercept_callsigns (
			frequency TEXT, group_code TEXT, unit_name TEXT, code TEXT, label TEXT
		);
		INSERT INTO intercept_callsigns VALUES('100.0', 'A', '1 мб 155 омбр', '123', 'Лаки');
		INSERT INTO intercept_items VALUES(
			7, 2, 'session', '1 мб 155 омбр', '100.0', 'A', 'source-7',
			'12.00\n-Данные. (123)', 'Иванов', '2026-10-18T10:00:00Z', 'catalog-1', 'deferred'
		);`)
	if err != nil {
		t.Fatalf("fixture: %v", err)
	}
	db.Close()

	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer source.Close()
	items, cursor, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(items) != 1 || items[0].Position.CatalogUUID != "catalog-1" || string(items[0].Mode) != "deferred" {
		t.Fatalf("неожиданные данные: %#v", items)
	}
	if cursor.ID != 7 {
		t.Fatalf("неожиданный курсор: %#v", cursor)
	}
	if len(items[0].Blocks) != 1 || len(items[0].Blocks[0].Callsigns) != 1 ||
		items[0].Blocks[0].Callsigns[0].Label != "Лаки" {
		t.Fatalf("позывные и блоки не прочитаны: %#v", items[0].Blocks)
	}
	if _, err := source.db.Exec("DELETE FROM intercept_items"); err == nil {
		t.Fatal("источник EAVC должен быть открыт только для чтения")
	}
}

// Номер карточки и описание позывного нужны для точного оформления Telegram и Word.
func TestSourceReadsCallsignNumberAndDescription(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`
		CREATE TABLE intercept_items (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT
		);
		CREATE TABLE intercept_callsigns (
			id INTEGER PRIMARY KEY, frequency TEXT, group_code TEXT, unit_name TEXT,
			code TEXT, label TEXT, tag TEXT, tag_desc TEXT
		);
		INSERT INTO intercept_callsigns VALUES(
			6, '159.3000', '18902', 'Гольназ', '52325', 'Торба', 'Старший',
			'Старший группы Гольназ, мужской голос, украинская речь'
		);
		INSERT INTO intercept_items VALUES(
			1, 1, 'session', 'Гольназ', '159.3000', '18902', 'source-1',
			'17.30\n-Монако Торбе на связь. (52325)', 'Иванов', '2026-10-20 17:34:00'
		);`)
	db.Close()
	if err != nil {
		t.Fatal(err)
	}

	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	items, _, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].Blocks) != 1 || len(items[0].Blocks[0].Callsigns) != 1 {
		t.Fatalf("позывной не прочитан: %#v", items)
	}
	callsign := items[0].Blocks[0].Callsigns[0]
	if callsign.Number != 6 || callsign.Label != "Торба" || callsign.Tag != "Старший" ||
		callsign.Description != "Старший группы Гольназ, мужской голос, украинская речь" {
		t.Fatalf("метаданные позывного потеряны: %#v", callsign)
	}
}

// Название с конечной пометкой «отложенный режим» распознаётся как обычный отложенный перехват.
func TestSourceInfersDeferredModeFromUnitName(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE intercept_items (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT
		);
		INSERT INTO intercept_items VALUES(
			1, 1, 'session', '2 об 3 обр ОсН НГ (отложенный режим)',
			'100.0', 'A', 'source-1', '12.00\n-Текст', 'Иванов',
			'2026-10-18T10:00:00Z'
		);`)
	if err != nil {
		db.Close()
		t.Fatalf("fixture: %v", err)
	}
	db.Close()

	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	defer source.Close()
	items, _, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(items) != 1 || items[0].Mode != "deferred" {
		t.Fatalf("отложенный режим не распознан: %#v", items)
	}
	if items[0].Position.UnitName != "2 об 3 обр ОсН НГ (отложенный режим)" {
		t.Fatalf("название изменено: %q", items[0].Position.UnitName)
	}
}

// Курсор одинаково понимает ISO-время и формат SQLite CURRENT_TIMESTAMP.
// Иначе после начала новой смены новые строки могли навсегда остаться за курсором.
func TestSourceCursorComparesMixedTimestampFormatsAsTime(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE intercept_items (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT
		);
		INSERT INTO intercept_items VALUES(
			1, 1, 'session-1', 'Первая', '100.0', 'A', 'source-1',
			'12.00\n-Первая запись', 'Иванов', '2026-10-18T10:00:00Z'
		);`)
	if err != nil {
		db.Close()
		t.Fatalf("fixture: %v", err)
	}
	db.Close()

	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	items, cursor, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil || len(items) != 1 {
		source.Close()
		t.Fatalf("first scan: items=%d cursor=%#v err=%v", len(items), cursor, err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen fixture: %v", err)
	}
	_, err = db.Exec(`INSERT INTO intercept_items VALUES(
		2, 2, 'session-2', 'Вторая', '101.0', 'B', 'source-2',
		'12.01\n-Новая запись', 'Петров', '2026-10-18 10:01:00'
	)`)
	db.Close()
	if err != nil {
		t.Fatalf("append fixture: %v", err)
	}

	source, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen source: %v", err)
	}
	defer source.Close()
	items, next, err := source.Scan(context.Background(), cursor, 10)
	if err != nil {
		t.Fatalf("second scan: %v", err)
	}
	if len(items) != 1 || items[0].Record.ID != 2 {
		t.Fatalf("новая строка SQLite пропущена: %#v", items)
	}
	if next.UpdatedAt != "2026-10-18 10:01:00" || next.ID != 2 {
		t.Fatalf("неожиданный курсор: %#v", next)
	}
}

// Закрытая смена переносится EAVC в архив. Клиент обязан дочитать её после
// запуска, даже если не работал в момент архивирования.
func TestSourceReadsUnseenArchivedItems(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open fixture: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE intercept_items (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT
		);
		CREATE TABLE intercept_items_archive (
			id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
			unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
			content TEXT, updated_by TEXT, updated_at TEXT
		);
		INSERT INTO intercept_items VALUES(
			10, 1, 'session-1', 'Первая', '100.0', 'A', 'source-10',
			'12.00\n-Первая запись', 'Иванов', '2026-10-18T10:00:00Z'
		);`)
	if err != nil {
		db.Close()
		t.Fatalf("fixture: %v", err)
	}
	db.Close()

	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	_, cursor, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		source.Close()
		t.Fatalf("first scan: %v", err)
	}
	if err := source.Close(); err != nil {
		t.Fatalf("close source: %v", err)
	}

	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("reopen fixture: %v", err)
	}
	_, err = db.Exec(`
		DELETE FROM intercept_items;
		INSERT INTO intercept_items_archive VALUES(
			11, 2, 'session-2', 'Архивная', '101.0', 'B', 'source-11',
			'12.01\n-Не прочитано до закрытия', 'Петров', '2026-10-18 10:01:00'
		);`)
	db.Close()
	if err != nil {
		t.Fatalf("archive fixture: %v", err)
	}

	source, err = Open(context.Background(), path)
	if err != nil {
		t.Fatalf("reopen source: %v", err)
	}
	defer source.Close()
	items, next, err := source.Scan(context.Background(), cursor, 10)
	if err != nil {
		t.Fatalf("archive scan: %v", err)
	}
	if len(items) != 1 || items[0].Record.ID != 11 || items[0].Position.UnitName != "Архивная" {
		t.Fatalf("архивная строка пропущена: %#v", items)
	}
	if next.ID != 11 {
		t.Fatalf("неожиданный курсор: %#v", next)
	}
}
