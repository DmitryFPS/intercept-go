package sqlite

const clientMigration = `
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	UNIQUE(source_uuid, content_digest)
);
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
CREATE TABLE client_cursors (
	name TEXT PRIMARY KEY,
	updated_at TEXT NOT NULL,
	record_id INTEGER NOT NULL
);
CREATE TABLE client_values (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
);`

const clientExpiryMigration = `
ALTER TABLE client_events RENAME TO client_events_v1;
DROP INDEX client_events_pending_idx;
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	expired_at TEXT,
	UNIQUE(source_uuid, content_digest)
);
INSERT INTO client_events(
	event_id, source_uuid, content_digest, payload_json, state, attempts,
	next_attempt_at, expires_at, telegram_message_id, last_error, created_at,
	uploaded_at, acknowledged_at
)
SELECT event_id, source_uuid, content_digest, payload_json, state, attempts,
	next_attempt_at, strftime('%Y-%m-%dT%H:%M:%fZ', datetime(created_at, '+30 minutes')),
	telegram_message_id, last_error, created_at,
	uploaded_at, acknowledged_at
FROM client_events_v1;
DROP TABLE client_events_v1;
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
`

const clientSourceVersionMigration = `
ALTER TABLE client_events RENAME TO client_events_v2;
DROP INDEX client_events_pending_idx;
CREATE TABLE client_events (
	event_id TEXT PRIMARY KEY,
	source_uuid TEXT NOT NULL,
	source_version TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'uploaded', 'acknowledged', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	telegram_message_id INTEGER,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	uploaded_at TEXT,
	acknowledged_at TEXT,
	expired_at TEXT,
	UNIQUE(source_uuid, source_version, content_digest)
);
INSERT INTO client_events(
	event_id, source_uuid, source_version, content_digest, payload_json, state,
	attempts, next_attempt_at, expires_at, telegram_message_id, last_error,
	created_at, uploaded_at, acknowledged_at, expired_at
)
SELECT event_id, source_uuid, created_at, content_digest, payload_json, state,
	attempts, next_attempt_at, expires_at, telegram_message_id, last_error,
	created_at, uploaded_at, acknowledged_at, expired_at
FROM client_events_v2;
DROP TABLE client_events_v2;
CREATE INDEX client_events_pending_idx ON client_events(state, next_attempt_at, created_at);
`

const clientStreamMigration = `
ALTER TABLE client_events ADD COLUMN stream_key TEXT NOT NULL DEFAULT '';
UPDATE client_events SET stream_key=source_uuid WHERE stream_key='';
DROP INDEX client_events_pending_idx;
CREATE INDEX client_events_pending_idx
	ON client_events(state, stream_key, next_attempt_at, created_at, event_id);
`

// Ранняя версия миграции использовала UUID исходной записи и могла разделить
// одно подразделение на несколько очередей. Значение восстанавливается из
// сохранённого события после применения SQL-миграций.
const clientStreamRepairMigration = `
UPDATE client_events SET stream_key='';
`

// Ранняя схема группировала очередь по названию подразделения. При его
// исправлении две версии одной записи могли обогнать друг друга. Миграция запускает
// повторный расчёт по стабильному UUID/ID исходной записи.
const clientSourceStreamMigration = `
UPDATE client_events SET stream_key='';
`

// Очищенная оператором завершённая версия не должна снова появляться при
// повторном чтении граничной строки EAVC. Здесь хранится только техническая
// идентичность версии, без текста перехвата и транспортных данных.
const clientClearedEventsMigration = `
CREATE TABLE client_cleared_events (
	source_uuid TEXT NOT NULL,
	source_version TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	cleared_at TEXT NOT NULL,
	PRIMARY KEY(source_uuid, source_version, content_digest)
);
CREATE INDEX client_cleared_events_time_idx ON client_cleared_events(cleared_at);
`

// Локальный occurrence-ID переживает перезапуск, смену физического UUID EAVC
// и добавление/удаление служебной решётки.
const clientTimeStateMigration = `
CREATE TABLE client_time_state (
	source_key TEXT PRIMARY KEY,
	payload BLOB NOT NULL,
	updated_at TEXT NOT NULL
);
`

const clientTimeStateRetentionMigration = `
CREATE INDEX client_time_state_updated_idx ON client_time_state(updated_at);
`
