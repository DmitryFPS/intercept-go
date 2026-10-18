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
