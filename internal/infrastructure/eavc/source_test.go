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
