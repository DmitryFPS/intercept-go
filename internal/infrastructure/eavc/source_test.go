package eavc

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"intercept/internal/application"

	_ "modernc.org/sqlite"
)

var fixtureTimeLine = regexp.MustCompile(`^\d{1,2}\.\d{2}$`)

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
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
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
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
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
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
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
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
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
	syncTestBlocks(t, db)
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
	if len(items) != 2 || items[len(items)-1].Record.ID != 2 {
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
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
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

// SQLite в EAVC хранит время с точностью до секунды. Повторное чтение всей
// граничной секунды не даёт потерять вторую правку той же строки.
func TestSourceReadsSecondEditWithSameTimestamp(t *testing.T) {
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
		INSERT INTO intercept_items VALUES(
			1, 1, 'session', 'Позиция', '100.0', 'A', 'source-1',
			'10.00\n-Первая редакция', 'Иванов', '2026-10-23 10:00:00'
		);`)
	if err != nil {
		t.Fatal(err)
	}
	createTestBlockSchema(t, db)
	syncTestBlocks(t, db)
	db.Close()
	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	_, cursor, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	source.Close()
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec("UPDATE intercept_items SET content=? WHERE id=1", "10.00\n-Вторая редакция")
	if err != nil {
		t.Fatal(err)
	}
	syncTestBlocks(t, db)
	db.Close()
	source, err = Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	items, _, err := source.Scan(context.Background(), cursor, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Content != "10.00\n-Вторая редакция" {
		t.Fatalf("быстрая редакция потеряна: %#v", items)
	}
}

func TestSourceReadsStableUUIDForEveryEAVCBlock(t *testing.T) {
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
		CREATE TABLE intercept_blocks (
			uuid TEXT PRIMARY KEY, item_uuid TEXT NOT NULL, item_id INTEGER NOT NULL,
			time_key INTEGER, day_offset INTEGER NOT NULL, ord INTEGER NOT NULL,
			header TEXT NOT NULL, body TEXT NOT NULL, text_hash TEXT NOT NULL,
			updated_at TEXT, deleted_at TEXT
		);
		INSERT INTO intercept_items VALUES(
			1,1,'session','Позиция','100','A','item-1',
			'10.00' || char(10) || '-Первый' || char(10) || '10.05' || char(10) || '-Второй',
			'Оператор','2026-10-27 10:06:00'
		);
		INSERT INTO intercept_blocks VALUES
			('block-a','item-1',1,600,0,0,'10.00','-Первый','','2026-10-27 10:00:00',NULL),
			('block-b','item-1',1,605,0,1,'10.05','-Второй','','2026-10-27 10:05:00',NULL);
	`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	items, _, err := source.Scan(context.Background(), application.Cursor{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || len(items[0].Blocks) != 2 ||
		items[0].Blocks[0].UUID != "block-a" || items[0].Blocks[1].UUID != "block-b" {
		t.Fatalf("стабильные UUID блоков не прочитаны: %#v", items)
	}
}

func TestSourceRejectsInconsistentItemAndBlockSnapshot(t *testing.T) {
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
		CREATE TABLE intercept_blocks (
			uuid TEXT PRIMARY KEY, item_uuid TEXT NOT NULL, item_id INTEGER NOT NULL,
			time_key INTEGER, day_offset INTEGER NOT NULL, ord INTEGER NOT NULL,
			header TEXT NOT NULL, body TEXT NOT NULL, text_hash TEXT NOT NULL,
			updated_at TEXT, deleted_at TEXT
		);
		INSERT INTO intercept_items VALUES(
			1,1,'session','Позиция','100','A','item-1',
			'10.00' || char(10) || '-Новая редакция','Оператор','2026-10-27 10:06:00'
		);
		INSERT INTO intercept_blocks VALUES(
			'block-a','item-1',1,600,0,0,'10.00','-Старая редакция','','2026-10-27 10:00:00',NULL
		);
	`)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	_, _, err = source.Scan(context.Background(), application.Cursor{}, 10)
	if err == nil || !strings.Contains(err.Error(), "временно расходятся") {
		t.Fatalf("рассогласованный снимок ушёл в очередь: %v", err)
	}
}

// Вся граничная секунда перечитывается целиком: EAVC хранит updated_at с
// секундной точностью, поэтому ограничение этой части запроса могло навсегда
// пропустить середину большой одновременной пачки.
func TestSourceDoesNotSkipLargeSameTimestampBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE intercept_items (
		id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
		unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
		content TEXT, updated_by TEXT, updated_at TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	createTestBlockSchema(t, db)
	for id := 1; id <= 300; id++ {
		if _, err = db.Exec(`INSERT INTO intercept_items VALUES(?,?,?,?,?,?,?,?,?,?)`,
			id, id, fmt.Sprintf("session-%d", id), "Позиция", "100.0", "A",
			fmt.Sprintf("source-%d", id), "10.00\n-Текст", "Оператор", "2026-10-23 10:00:00"); err != nil {
			t.Fatal(err)
		}
	}
	syncTestBlocks(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	first, cursor, err := source.Scan(context.Background(), application.Cursor{}, 100)
	if err != nil || len(first) != 100 {
		t.Fatalf("первая пачка: len=%d cursor=%+v err=%v", len(first), cursor, err)
	}
	second, _, err := source.Scan(context.Background(), cursor, 100)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[int64]bool, len(second))
	for _, item := range second {
		seen[item.Record.ID] = true
	}
	for id := int64(101); id <= 300; id++ {
		if !seen[id] {
			t.Fatalf("запись %d из граничной секунды пропущена", id)
		}
	}
}

func TestSourceRejectsUnboundedSameTimestampBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "main.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`CREATE TABLE intercept_items (
		id INTEGER PRIMARY KEY, session_id INTEGER, session_uuid TEXT,
		unit_name TEXT, frequency TEXT, group_code TEXT, uuid TEXT,
		content TEXT, updated_by TEXT, updated_at TEXT
	)`); err != nil {
		t.Fatal(err)
	}
	createTestBlockSchema(t, db)
	statement, err := db.Prepare(`INSERT INTO intercept_items VALUES(?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= maxBoundaryReplay+1; id++ {
		if _, err = statement.Exec(
			id, id, fmt.Sprintf("session-%d", id), "Позиция", "100.0", "A",
			fmt.Sprintf("source-%d", id), "10.00\n-Текст", "Оператор", "2026-10-23 10:00:00",
		); err != nil {
			statement.Close()
			t.Fatal(err)
		}
	}
	syncTestBlocks(t, db)
	if err := statement.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	source, err := Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	_, _, err = source.Scan(context.Background(), application.Cursor{
		UpdatedAt: "2026-10-23 10:00:00", ID: 1,
	}, 100)
	if err == nil || !strings.Contains(err.Error(), "без сдвига курсора") {
		t.Fatalf("неограниченная граничная секунда не отклонена: %v", err)
	}
}

func createTestBlockSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS intercept_blocks (
		uuid TEXT PRIMARY KEY, item_uuid TEXT NOT NULL, item_id INTEGER NOT NULL,
		time_key INTEGER, day_offset INTEGER NOT NULL DEFAULT 0,
		ord INTEGER NOT NULL DEFAULT 0, header TEXT NOT NULL DEFAULT '',
		body TEXT NOT NULL DEFAULT '', text_hash TEXT NOT NULL DEFAULT '',
		updated_by TEXT NOT NULL DEFAULT '', created_at TEXT,
		updated_at TEXT, deleted_at TEXT
	)`)
	if err != nil {
		t.Fatalf("create block schema: %v", err)
	}
}

func syncTestBlocks(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.Query(`SELECT id, uuid, content, updated_by, updated_at FROM intercept_items`)
	if err != nil {
		t.Fatalf("read fixture items: %v", err)
	}
	type fixtureItem struct {
		id                             int64
		uuid, content, author, updated string
	}
	var items []fixtureItem
	for rows.Next() {
		var item fixtureItem
		if err := rows.Scan(&item.id, &item.uuid, &item.content, &item.author, &item.updated); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		items = append(items, item)
	}
	rows.Close()
	for _, item := range items {
		lines := strings.SplitN(item.content, "\n", 2)
		header := ""
		body := item.content
		if len(lines) > 0 && fixtureTimeLine.MatchString(strings.TrimSpace(lines[0])) {
			header = strings.TrimSpace(lines[0])
			body = ""
			if len(lines) == 2 {
				body = lines[1]
			}
		}
		_, err := db.Exec(`INSERT INTO intercept_blocks(
			uuid,item_uuid,item_id,time_key,day_offset,ord,header,body,text_hash,updated_by,updated_at,deleted_at
		) VALUES(?,?,?,?,0,0,?,?,?, ?,?,NULL)
		ON CONFLICT(uuid) DO UPDATE SET header=excluded.header,body=excluded.body,
			updated_by=excluded.updated_by,updated_at=excluded.updated_at,deleted_at=NULL`,
			"block-"+item.uuid, item.uuid, item.id, 0, header, body, "", item.author, item.updated)
		if err != nil {
			t.Fatalf("sync fixture block: %v", err)
		}
	}
}
