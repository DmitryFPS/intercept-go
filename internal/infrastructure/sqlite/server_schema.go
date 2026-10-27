package sqlite

const serverMigration = `
CREATE TABLE server_clients (
	client_id TEXT PRIMARY KEY,
	client_number TEXT NOT NULL UNIQUE,
	name TEXT NOT NULL,
	sender_bot_id INTEGER NOT NULL,
	sender_bot_username TEXT NOT NULL,
	chat_id INTEGER NOT NULL,
	thread_id INTEGER NOT NULL,
	key_id TEXT NOT NULL,
	key_bytes BLOB NOT NULL,
	active INTEGER NOT NULL CHECK(active IN (0, 1)),
	updated_at TEXT NOT NULL
);
CREATE TABLE route_versions (
	version INTEGER PRIMARY KEY,
	payload_json BLOB NOT NULL,
	author TEXT NOT NULL,
	created_at TEXT NOT NULL,
	active INTEGER NOT NULL CHECK(active IN (0, 1))
);
CREATE UNIQUE INDEX one_active_route_version ON route_versions(active) WHERE active = 1;
CREATE TABLE received_events (
	event_id TEXT PRIMARY KEY,
	client_id TEXT NOT NULL,
	source_uuid TEXT NOT NULL,
	content_digest TEXT NOT NULL,
	payload_json BLOB NOT NULL,
	rule_version INTEGER NOT NULL,
	received_at TEXT NOT NULL,
	FOREIGN KEY(client_id) REFERENCES server_clients(client_id)
);
CREATE TABLE delivery_jobs (
	event_id TEXT NOT NULL,
	destination_id TEXT NOT NULL,
	delivery_json BLOB NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'delivered')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	delivered_at TEXT,
	PRIMARY KEY(event_id, destination_id),
	FOREIGN KEY(event_id) REFERENCES received_events(event_id) ON DELETE CASCADE
);
CREATE INDEX delivery_jobs_pending_idx ON delivery_jobs(state, next_attempt_at);
CREATE TABLE server_values (
	name TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE rejected_packets (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	sender_bot_id INTEGER NOT NULL,
	sender_bot_username TEXT NOT NULL,
	chat_id INTEGER NOT NULL,
	thread_id INTEGER NOT NULL,
	reason TEXT NOT NULL,
	payload BLOB NOT NULL,
	received_at TEXT NOT NULL
);
`

const serverDeliveryOrderMigration = `
ALTER TABLE delivery_jobs RENAME TO delivery_jobs_v1;
DROP INDEX delivery_jobs_pending_idx;
CREATE TABLE delivery_jobs (
	event_id TEXT NOT NULL,
	destination_id TEXT NOT NULL,
	delivery_json BLOB NOT NULL,
	stream_key TEXT NOT NULL,
	source_at TEXT NOT NULL,
	expires_at TEXT NOT NULL,
	state TEXT NOT NULL CHECK(state IN ('pending', 'delivered', 'expired')),
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	delivered_at TEXT,
	expired_at TEXT,
	PRIMARY KEY(event_id, destination_id),
	FOREIGN KEY(event_id) REFERENCES received_events(event_id) ON DELETE CASCADE
);
INSERT INTO delivery_jobs(
	event_id, destination_id, delivery_json, stream_key, source_at, expires_at,
	state, attempts, next_attempt_at, last_error, delivered_at
)
SELECT j.event_id, j.destination_id, j.delivery_json, j.destination_id,
	e.received_at, strftime('%Y-%m-%dT%H:%M:%fZ', datetime(e.received_at, '+30 minutes')),
	j.state, j.attempts, j.next_attempt_at,
	j.last_error, j.delivered_at
FROM delivery_jobs_v1 j
JOIN received_events e ON e.event_id = j.event_id;
DROP TABLE delivery_jobs_v1;
CREATE INDEX delivery_jobs_pending_idx
	ON delivery_jobs(state, stream_key, source_at, event_id, next_attempt_at);
`

// Старые задания получали одну общую очередь на Word-направление. Ключи
// пересчитываются из сохранённых событий после расшифрования столбцов.
const serverStreamRepairMigration = `
UPDATE delivery_jobs SET stream_key='';
`

// Наблюдаемость хранится отдельно от очереди доставки. Очистка истории
// никогда не должна удалить незавершённое задание или запись дедупликации.
const serverObservabilityMigration = `
CREATE TABLE client_health (
	client_id TEXT PRIMARY KEY,
	status TEXT NOT NULL CHECK(status IN ('online', 'offline')),
	last_seen_at TEXT NOT NULL,
	last_probe_at TEXT,
	last_event_at TEXT,
	last_reason TEXT NOT NULL DEFAULT '',
	FOREIGN KEY(client_id) REFERENCES server_clients(client_id) ON DELETE CASCADE
);
CREATE TABLE client_connection_incidents (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	client_id TEXT NOT NULL,
	started_at TEXT NOT NULL,
	recovered_at TEXT,
	reason TEXT NOT NULL,
	FOREIGN KEY(client_id) REFERENCES server_clients(client_id) ON DELETE CASCADE
);
CREATE INDEX client_connection_incidents_idx
	ON client_connection_incidents(client_id, started_at DESC);
CREATE TABLE transport_cleanup (
	message_id INTEGER PRIMARY KEY,
	created_at TEXT NOT NULL,
	next_attempt_at TEXT NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX transport_cleanup_due_idx ON transport_cleanup(next_attempt_at, message_id);
`

const serverIntegrityMigration = `
CREATE UNIQUE INDEX unique_active_sender_bot
	ON server_clients(sender_bot_id) WHERE active=1;
UPDATE client_connection_incidents
	SET started_at=recovered_at
	WHERE recovered_at IS NOT NULL AND julianday(started_at)>julianday(recovered_at);
`

const serverHistoryIndexesMigration = `
CREATE INDEX received_events_history_idx
	ON received_events(received_at DESC, client_id);
CREATE INDEX delivery_jobs_delivered_history_idx
	ON delivery_jobs(state, delivered_at DESC, destination_id);
CREATE INDEX delivery_jobs_expired_history_idx
	ON delivery_jobs(state, expired_at DESC, destination_id);
CREATE INDEX rejected_packets_history_idx
	ON rejected_packets(received_at DESC);
CREATE INDEX client_connection_incidents_time_idx
	ON client_connection_incidents(started_at DESC);
`

// Результат доставки хранит фактические ID конечных Telegram-сообщений.
// Старые строки остаются допустимыми: у них результат пуст, потому что прежняя
// версия Центра ещё не сохраняла эту информацию.
const serverDeliveryResultMigration = `
ALTER TABLE delivery_jobs ADD COLUMN result_json BLOB NOT NULL DEFAULT X'';
`

// Журнал транспорта отделён от рабочих счётчиков и очередей. Повтор одного
// Telegram update обновляет ту же строку, а не создаёт бесконечную историю.
const serverTransportAuditMigration = `
CREATE TABLE transport_audit (
	chat_id INTEGER NOT NULL,
	message_id INTEGER NOT NULL,
	update_id INTEGER NOT NULL,
	thread_id INTEGER NOT NULL,
	sender_bot_id INTEGER NOT NULL,
	sender_bot_username TEXT NOT NULL,
	message_kind TEXT NOT NULL,
	outcome TEXT NOT NULL,
	reason TEXT NOT NULL DEFAULT '',
	event_id TEXT NOT NULL DEFAULT '',
	client_id TEXT NOT NULL DEFAULT '',
	counted INTEGER NOT NULL DEFAULT 0 CHECK(counted IN (0,1)),
	state_rank INTEGER NOT NULL DEFAULT 0,
	seen_count INTEGER NOT NULL DEFAULT 1,
	sent_at TEXT NOT NULL,
	first_seen_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	PRIMARY KEY(chat_id, message_id)
);
CREATE INDEX transport_audit_time_idx ON transport_audit(last_seen_at DESC);
CREATE INDEX transport_audit_event_idx ON transport_audit(event_id) WHERE event_id<>'';
`

// Входной Telegram-пакет сначала фиксируется локально вместе со смещением.
// Поэтому исчезновение сообщения из Telegram или перезапуск Центра после
// скачивания уже не могут потерять пакет. Содержимое защищается так же, как
// принятые события и карантин.
const serverTransportInboxMigration = `
CREATE TABLE transport_inbox (
	update_id INTEGER PRIMARY KEY,
	chat_id INTEGER NOT NULL,
	message_id INTEGER NOT NULL,
	packet_json BLOB NOT NULL,
	attempts INTEGER NOT NULL DEFAULT 0,
	next_attempt_at TEXT NOT NULL,
	last_error TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	UNIQUE(chat_id, message_id)
);
CREATE INDEX transport_inbox_due_idx
	ON transport_inbox(next_attempt_at, update_id);
`
