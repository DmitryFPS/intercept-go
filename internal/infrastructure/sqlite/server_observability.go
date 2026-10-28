package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

const maxHistorySearchRows = 10000

const (
	clientOfflineReason = "Нет контрольного пакета или данных более 5 минут"
	historyPrefix       = "history_clear_"
)

// ClientHealth — безопасный снимок качества связи с рабочей точкой.
type ClientHealth struct {
	ClientID           string               `json:"client_id"`
	Number             string               `json:"number"`
	Name               string               `json:"name"`
	Status             string               `json:"status"`
	Quality            string               `json:"quality"`
	LastSeenAt         *time.Time           `json:"last_seen_at,omitempty"`
	LastProbeAt        *time.Time           `json:"last_probe_at,omitempty"`
	LastEventAt        *time.Time           `json:"last_event_at,omitempty"`
	DelaySeconds       int64                `json:"delay_seconds"`
	CurrentReason      string               `json:"current_reason,omitempty"`
	ShiftReadState     string               `json:"shift_read_state,omitempty"`
	ShiftStatusReason  string               `json:"shift_status_reason,omitempty"`
	LastShiftProbeAt   *time.Time           `json:"last_shift_probe_at,omitempty"`
	LastShiftSuccessAt *time.Time           `json:"last_shift_success_at,omitempty"`
	Shifts             []ClientShiftSession `json:"shifts,omitempty"`
}

// ClientShiftSession is one deduplicated EAVC shift lifecycle. Repeated
// connection probes only refresh LastSeenAt and never create another row.
type ClientShiftSession struct {
	ClientID        string     `json:"client_id"`
	ClientName      string     `json:"client_name,omitempty"`
	PositionName    string     `json:"position_name"`
	SessionUUID     string     `json:"session_uuid"`
	SourceSessionID int64      `json:"source_session_id"`
	OpenedAt        time.Time  `json:"opened_at"`
	ClosedAt        *time.Time `json:"closed_at,omitempty"`
	FirstSeenAt     time.Time  `json:"first_seen_at"`
	LastSeenAt      time.Time  `json:"last_seen_at"`
	Current         bool       `json:"current"`
}

// ConnectionIncident фиксирует период отсутствия связи и её восстановление.
type ConnectionIncident struct {
	ID          int64      `json:"id"`
	ClientID    string     `json:"client_id"`
	ClientName  string     `json:"client_name"`
	StartedAt   time.Time  `json:"started_at"`
	RecoveredAt *time.Time `json:"recovered_at,omitempty"`
	Reason      string     `json:"reason"`
}

// ServerHistoryItem — одна строка истории для защищённого локального интерфейса.
// Текст раскрывается только уже авторизованному локальному оператору. Для
// карантина содержимое намеренно недоступно: отклонённый пакет мог не пройти
// проверку подписи и его нельзя выдавать за достоверный перехват.
type ServerHistoryItem struct {
	ID                 string                             `json:"id"`
	ClientID           string                             `json:"client_id,omitempty"`
	ClientName         string                             `json:"client_name,omitempty"`
	UnitName           string                             `json:"unit_name,omitempty"`
	Frequency          string                             `json:"frequency,omitempty"`
	GroupCode          string                             `json:"group_code,omitempty"`
	DestinationID      string                             `json:"destination_id,omitempty"`
	Destination        string                             `json:"destination,omitempty"`
	State              string                             `json:"state"`
	OccurredAt         time.Time                          `json:"occurred_at"`
	SourceAt           *time.Time                         `json:"source_at,omitempty"`
	SourceRecord       int64                              `json:"source_record,omitempty"`
	SourceUUID         string                             `json:"source_uuid,omitempty"`
	SourceVersion      string                             `json:"source_version,omitempty"`
	ContentDigest      string                             `json:"content_digest,omitempty"`
	SourceMode         string                             `json:"source_mode,omitempty"`
	NextAttemptAt      *time.Time                         `json:"next_attempt_at,omitempty"`
	Attempts           int                                `json:"attempts,omitempty"`
	Reason             string                             `json:"reason,omitempty"`
	UpdatedBy          string                             `json:"updated_by,omitempty"`
	Content            string                             `json:"content,omitempty"`
	ContentReady       bool                               `json:"content_ready"`
	ContentKind        string                             `json:"content_kind"`
	ContentNote        string                             `json:"content_note,omitempty"`
	TelegramChat       string                             `json:"telegram_chat_id,omitempty"`
	TelegramTopic      int64                              `json:"telegram_thread_id,omitempty"`
	TelegramIDs        []int64                            `json:"telegram_message_ids,omitempty"`
	TelegramURLs       []string                           `json:"telegram_message_urls,omitempty"`
	BlockUUIDs         []string                           `json:"block_uuids,omitempty"`
	TelegramBlocks     []application.DeliveryBlockReceipt `json:"telegram_blocks,omitempty"`
	TransportUpdateID  int64                              `json:"transport_update_id,omitempty"`
	TransportMessageID int64                              `json:"transport_message_id,omitempty"`
	TransportKind      string                             `json:"transport_kind,omitempty"`
	TransportOutcome   string                             `json:"transport_outcome,omitempty"`
	TransportSeen      int                                `json:"transport_seen,omitempty"`
	Counted            bool                               `json:"counted"`
}

// ServerHistoryPage объединяет текущую связь и постраничную историю.
type ServerHistoryPage struct {
	Kind         string               `json:"kind"`
	Items        []ServerHistoryItem  `json:"items"`
	Clients      []ClientHealth       `json:"clients,omitempty"`
	Incidents    []ConnectionIncident `json:"incidents,omitempty"`
	ShiftHistory []ClientShiftSession `json:"shift_history,omitempty"`
	ShiftWarning string               `json:"shift_warning,omitempty"`
	// InternalWarnings передаются runner только для эксплуатационного журнала
	// и намеренно не раскрываются в локальном API.
	InternalWarnings []string `json:"-"`
	Total            int      `json:"total"`
	Limit            int      `json:"limit"`
	Offset           int      `json:"offset"`
	Clearable        bool     `json:"clearable"`
	RetentionDays    int      `json:"retention_days"`
}

// HistoryFilter ограничивает историю операторскими признаками. Поиск по тексту
// выполняется только после расшифровки в памяти: защищённое содержимое не
// переносится в открытые индексы SQLite.
type HistoryFilter struct {
	Query         string
	ClientID      string
	DestinationID string
	Quality       string
	From          *time.Time
	To            *time.Time
}

// RecordClientSignalByTransport отмечает только пакет уже авторизованного бота.
func (store *ServerStore) RecordClientSignalByTransport(
	ctx context.Context, senderBotID int64, chatID, threadID int64, probe bool, at time.Time,
) error {
	var clientID string
	err := store.db.QueryRowContext(ctx, `
		SELECT client_id FROM server_clients
		WHERE sender_bot_id=? AND chat_id=? AND thread_id=? AND active=1`,
		senderBotID, chatID, threadID,
	).Scan(&clientID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("найти точку для контроля связи: %w", err)
	}
	return store.recordClientSignal(ctx, clientID, probe, at.UTC())
}

func (store *ServerStore) recordClientSignal(ctx context.Context, clientID string, probe bool, at time.Time) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var status, lastSeen string
	err = tx.QueryRowContext(ctx,
		"SELECT status,last_seen_at FROM client_health WHERE client_id=?", clientID,
	).Scan(&status, &lastSeen)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil {
		previous, parseErr := time.Parse(time.RFC3339Nano, lastSeen)
		if parseErr == nil && !at.After(previous) {
			// Telegram может вернуть несколько сообщений не в порядке создания.
			// Старый пакет не должен ухудшать свежесть уже принятого сигнала.
			return tx.Commit()
		}
		if parseErr == nil && at.Sub(previous) > 5*time.Minute && status != "offline" {
			started := previous.Add(5 * time.Minute)
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO client_connection_incidents(client_id,started_at,recovered_at,reason)
				VALUES(?,?,?,?)`, clientID, stamp(started), stamp(at), clientOfflineReason); err != nil {
				return err
			}
		}
	}
	if status == "offline" {
		var startedRaw string
		lookupErr := tx.QueryRowContext(ctx, `
			SELECT started_at FROM client_connection_incidents
			WHERE client_id=? AND recovered_at IS NULL
			ORDER BY started_at DESC LIMIT 1`, clientID).Scan(&startedRaw)
		if lookupErr != nil && !errors.Is(lookupErr, sql.ErrNoRows) {
			return lookupErr
		}
		if lookupErr == nil {
			started, parseErr := time.Parse(time.RFC3339Nano, startedRaw)
			if parseErr == nil && !at.After(started) {
				// Пакет был отправлен ещё до начала зафиксированного сбоя и лишь
				// задержался в Telegram. Он не доказывает восстановление связи.
				return tx.Commit()
			}
		}
	}
	probeAt, eventAt := any(nil), any(nil)
	if probe {
		probeAt = stamp(at)
	} else {
		eventAt = stamp(at)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO client_health(client_id,status,last_seen_at,last_probe_at,last_event_at,last_reason)
		VALUES(?, 'online', ?, ?, ?, '')
		ON CONFLICT(client_id) DO UPDATE SET
			status='online', last_seen_at=excluded.last_seen_at,
			last_probe_at=COALESCE(excluded.last_probe_at,client_health.last_probe_at),
			last_event_at=COALESCE(excluded.last_event_at,client_health.last_event_at),
			last_reason=''`, clientID, stamp(at), probeAt, eventAt); err != nil {
		return err
	}
	if status == "offline" {
		if _, err := tx.ExecContext(ctx, `
			UPDATE client_connection_incidents SET recovered_at=?
			WHERE id=(SELECT id FROM client_connection_incidents
				WHERE client_id=? AND recovered_at IS NULL AND julianday(started_at)<julianday(?)
				ORDER BY started_at DESC LIMIT 1)`,
			stamp(at), clientID, stamp(at)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// EvaluateClientHealth открывает один инцидент после пяти минут тишины.
func (store *ServerStore) EvaluateClientHealth(ctx context.Context, now time.Time, timeout time.Duration) error {
	if timeout <= 0 {
		return errors.New("порог отсутствия связи должен быть положительным")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT client_id,last_seen_at FROM client_health
		WHERE status='online' AND last_seen_at < ?`, stamp(now.UTC().Add(-timeout)))
	if err != nil {
		return err
	}
	defer rows.Close()
	type staleClient struct{ id, lastSeen string }
	var stale []staleClient
	for rows.Next() {
		var item staleClient
		if err := rows.Scan(&item.id, &item.lastSeen); err != nil {
			return err
		}
		stale = append(stale, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range stale {
		lastSeen, err := time.Parse(time.RFC3339Nano, item.lastSeen)
		if err != nil {
			continue
		}
		tx, err := store.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE client_health SET status='offline',last_reason=?
			WHERE client_id=? AND status='online' AND last_seen_at=?`,
			clientOfflineReason, item.id, item.lastSeen)
		if err != nil {
			tx.Rollback()
			return err
		}
		changed, _ := result.RowsAffected()
		if changed == 1 {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO client_connection_incidents(client_id,started_at,reason)
				VALUES(?,?,?)`, item.id, stamp(lastSeen.Add(timeout)), clientOfflineReason); err != nil {
				tx.Rollback()
				return err
			}
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// HistoryPage возвращает только служебные признаки и никогда не раскрывает текст перехвата.
func (store *ServerStore) HistoryPage(ctx context.Context, kind string, limit, offset int, now time.Time, filters ...HistoryFilter) (ServerHistoryPage, error) {
	if limit <= 0 || limit > 100 || offset < 0 {
		return ServerHistoryPage{}, errors.New("некорректные параметры страницы")
	}
	page := ServerHistoryPage{Kind: kind, Limit: limit, Offset: offset, RetentionDays: 30, Clearable: kind != "pending"}
	filter := HistoryFilter{}
	if len(filters) > 0 {
		filter = filters[0]
	}
	if kind == "clients" {
		clients, warnings, err := store.clientHealth(ctx, now.UTC())
		if err != nil {
			return ServerHistoryPage{}, err
		}
		clients = filterClientHealth(clients, filter)
		incidents, total, err := store.connectionIncidents(ctx, limit, offset, filter)
		if err != nil {
			return ServerHistoryPage{}, err
		}
		shiftHistory, err := store.shiftHistory(ctx, filter, 100)
		if err != nil {
			warnings = append(warnings, fmt.Errorf("прочитать историю смен: %w", err))
			shiftHistory = nil
		}
		page.Clients, page.Incidents, page.ShiftHistory, page.Total = clients, incidents, shiftHistory, total
		if len(warnings) > 0 {
			page.ShiftWarning = "История смен временно доступна не полностью. Связь и обработка перехватов продолжаются."
			for _, warning := range warnings {
				page.InternalWarnings = append(page.InternalWarnings, warning.Error())
			}
		}
		page.RetentionDays = 90
		return page, nil
	}
	if kind == "rejected" {
		page.RetentionDays = 90
		items, total, err := store.rejectedHistory(ctx, limit, offset, filter)
		page.Items, page.Total = items, total
		return page, err
	}
	if kind == "transport" {
		page.RetentionDays = 90
		items, total, err := store.transportHistory(ctx, limit, offset, filter)
		page.Items, page.Total = items, total
		return page, err
	}
	if kind != "received" && kind != "pending" && kind != "delivered" && kind != "expired" {
		return ServerHistoryPage{}, errors.New("неизвестный раздел истории")
	}
	items, total, err := store.eventHistory(ctx, kind, limit, offset, filter)
	page.Items, page.Total = items, total
	return page, err
}

func (store *ServerStore) clientHealth(ctx context.Context, now time.Time) ([]ClientHealth, []error, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT c.client_id,c.client_number,c.name,h.status,h.last_seen_at,h.last_probe_at,h.last_event_at,h.last_reason
		FROM server_clients c LEFT JOIN client_health h ON h.client_id=c.client_id
		WHERE c.active=1 ORDER BY c.client_number,c.name`)
	if err != nil {
		return nil, nil, err
	}
	var result []ClientHealth
	for rows.Next() {
		var item ClientHealth
		var status, lastSeen, lastProbe, lastEvent, reason sql.NullString
		if err := rows.Scan(&item.ClientID, &item.Number, &item.Name, &status, &lastSeen, &lastProbe, &lastEvent, &reason); err != nil {
			rows.Close()
			return nil, nil, err
		}
		item.Status, item.Quality = "unknown", "unknown"
		if status.Valid {
			item.Status = status.String
		}
		item.LastSeenAt = parseNullableTime(lastSeen)
		item.LastProbeAt = parseNullableTime(lastProbe)
		item.LastEventAt = parseNullableTime(lastEvent)
		item.CurrentReason = reason.String
		if item.LastSeenAt != nil {
			item.DelaySeconds = max64(0, int64(now.Sub(*item.LastSeenAt).Seconds()))
			switch {
			case item.Status == "offline":
				item.Quality = "offline"
			case item.DelaySeconds <= 150:
				item.Quality = "good"
			default:
				item.Quality = "degraded"
			}
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}

	warnings := make([]error, 0)
	for index := range result {
		item := &result[index]
		var shiftState, shiftProbe, shiftSuccess sql.NullString
		syncErr := store.db.QueryRowContext(ctx, `
			SELECT read_state,last_probe_at,last_success_at
			FROM client_shift_sync WHERE client_id=?`, item.ClientID,
		).Scan(&shiftState, &shiftProbe, &shiftSuccess)
		if syncErr != nil && !errors.Is(syncErr, sql.ErrNoRows) {
			item.ShiftReadState = "error"
			item.ShiftStatusReason = "Состояние смен временно недоступно; обработка перехватов продолжается"
			warnings = append(warnings, fmt.Errorf("прочитать состояние смен точки %s: %w", item.ClientID, syncErr))
			continue
		}
		item.ShiftReadState = shiftState.String
		item.LastShiftProbeAt = parseNullableTime(shiftProbe)
		item.LastShiftSuccessAt = parseNullableTime(shiftSuccess)
		shifts, shiftErr := store.currentClientShifts(ctx, item.ClientID)
		if shiftErr != nil {
			item.ShiftReadState = "error"
			item.ShiftStatusReason = "Состояние смен временно недоступно; обработка перехватов продолжается"
			warnings = append(warnings, fmt.Errorf("прочитать смены точки %s: %w", item.ClientID, shiftErr))
			continue
		}
		item.Shifts = shifts
	}
	return result, warnings, nil
}

func filterClientHealth(clients []ClientHealth, filter HistoryFilter) []ClientHealth {
	query := strings.ToLower(strings.TrimSpace(filter.Query))
	result := make([]ClientHealth, 0, len(clients))
	for _, client := range clients {
		if filter.ClientID != "" && client.ClientID != filter.ClientID {
			continue
		}
		if filter.Quality != "" && client.Quality != filter.Quality {
			continue
		}
		shiftTerms := make([]string, 0, len(client.Shifts)*2)
		for _, shift := range client.Shifts {
			shiftTerms = append(shiftTerms, shift.PositionName, shift.SessionUUID)
		}
		searchable := strings.ToLower(strings.Join(append([]string{client.Number, client.Name, client.CurrentReason}, shiftTerms...), " "))
		if query != "" && !strings.Contains(searchable, query) {
			continue
		}
		result = append(result, client)
	}
	return result
}

func (store *ServerStore) connectionIncidents(ctx context.Context, limit, offset int, filter HistoryFilter) ([]ConnectionIncident, int, error) {
	cutoff := store.historyCutoff(ctx, "clients")
	where := []string{"i.started_at>?"}
	args := []any{cutoff}
	appendHistoryTimeFilter(&where, &args, "i.started_at", filter)
	if filter.ClientID != "" {
		where = append(where, "i.client_id=?")
		args = append(args, filter.ClientID)
	}
	clause := " WHERE " + strings.Join(where, " AND ")
	searchQuery := strings.ToLower(strings.TrimSpace(filter.Query))
	total := 0
	if searchQuery == "" {
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM client_connection_incidents i JOIN server_clients c ON c.client_id=i.client_id"+clause, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	query := `
		SELECT i.id,i.client_id,c.name,i.started_at,i.recovered_at,i.reason
		FROM client_connection_incidents i JOIN server_clients c ON c.client_id=i.client_id ` + clause + ` ORDER BY i.started_at DESC`
	queryArgs := args
	if searchQuery == "" {
		query += " LIMIT ? OFFSET ?"
		queryArgs = append(queryArgs, limit, offset)
	} else {
		query += " LIMIT ?"
		queryArgs = append(queryArgs, maxHistorySearchRows+1)
	}
	rows, err := store.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []ConnectionIncident
	scanned := 0
	for rows.Next() {
		scanned++
		if scanned > maxHistorySearchRows {
			return nil, 0, errors.New("поиск охватывает слишком много записей; выберите рабочую точку или уточните запрос")
		}
		var item ConnectionIncident
		var started string
		var recovered sql.NullString
		if err := rows.Scan(&item.ID, &item.ClientID, &item.ClientName, &started, &recovered, &item.Reason); err != nil {
			return nil, 0, err
		}
		item.StartedAt, _ = time.Parse(time.RFC3339Nano, started)
		item.RecoveredAt = parseNullableTime(recovered)
		if searchQuery != "" {
			searchable := strings.ToLower(strings.Join([]string{item.ClientID, item.ClientName, item.Reason}, " "))
			if !strings.Contains(searchable, searchQuery) {
				continue
			}
			total++
			if total <= offset || len(result) >= limit {
				continue
			}
		}
		result = append(result, item)
	}
	return result, total, rows.Err()
}

func (store *ServerStore) eventHistory(ctx context.Context, kind string, limit, offset int, filter HistoryFilter) ([]ServerHistoryItem, int, error) {
	cutoff := store.historyCutoff(ctx, kind)
	stateClause, timeColumn := "", "e.received_at"
	if kind != "received" {
		stateClause = " AND j.state='" + kind + "'"
		switch kind {
		case "delivered":
			timeColumn = "j.delivered_at"
		case "expired":
			timeColumn = "j.expired_at"
		case "pending":
			timeColumn = "e.received_at"
		}
	}
	join := ""
	if kind != "received" {
		join = " JOIN delivery_jobs j ON j.event_id=e.event_id"
	}
	conditions := []string{timeColumn + ">?"}
	args := []any{cutoff}
	if stateClause != "" {
		conditions = append(conditions, strings.TrimPrefix(stateClause, " AND "))
	}
	appendHistoryTimeFilter(&conditions, &args, timeColumn, filter)
	if filter.ClientID != "" {
		conditions = append(conditions, "e.client_id=?")
		args = append(args, filter.ClientID)
	}
	if filter.DestinationID != "" && kind != "received" {
		conditions = append(conditions, "j.destination_id=?")
		args = append(args, filter.DestinationID)
	}
	where := " WHERE " + strings.Join(conditions, " AND ")
	searchQuery := strings.ToLower(strings.TrimSpace(filter.Query))
	total := 0
	if searchQuery == "" {
		countQuery := "SELECT COUNT(*) FROM received_events e JOIN server_clients c ON c.client_id=e.client_id" + join + where
		if err := store.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	columns := "e.event_id,e.client_id,c.name,e.payload_json,'' AS destination_id,NULL AS delivery_json,NULL AS result_json,'received' AS state,e.received_at,0 AS attempts,NULL AS next_attempt_at,'' AS last_error"
	if kind != "received" {
		columns = "e.event_id,e.client_id,c.name,e.payload_json,j.destination_id,j.delivery_json,j.result_json,j.state," + timeColumn + ",j.attempts,j.next_attempt_at,j.last_error"
	}
	query := "SELECT " + columns + " FROM received_events e JOIN server_clients c ON c.client_id=e.client_id" + join + where + " ORDER BY " + timeColumn + " DESC"
	queryArgs := args
	if searchQuery == "" {
		query += " LIMIT ? OFFSET ?"
		queryArgs = append(queryArgs, limit, offset)
	} else {
		query += " LIMIT ?"
		queryArgs = append(queryArgs, maxHistorySearchRows+1)
	}
	rows, err := store.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var scannedItems []ServerHistoryItem
	scanned := 0
	for rows.Next() {
		scanned++
		if scanned > maxHistorySearchRows {
			return nil, 0, errors.New("поиск охватывает слишком много записей; выберите рабочую точку, получателя или уточните запрос")
		}
		item, scanErr := scanServerHistoryItem(rows)
		if scanErr != nil {
			return nil, 0, scanErr
		}
		scannedItems = append(scannedItems, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	// Освобождаем SELECT истории до пакетного запроса связей. Это важно и для
	// SQLite с единственным доступным соединением, и для предсказуемой памяти.
	if err := rows.Close(); err != nil {
		return nil, 0, err
	}
	if err := store.attachTransportTraces(ctx, scannedItems); err != nil {
		return nil, 0, err
	}
	result := make([]ServerHistoryItem, 0, min(limit, len(scannedItems)))
	for _, item := range scannedItems {
		if searchQuery != "" {
			if !serverHistoryItemMatches(item, searchQuery) {
				continue
			}
			total++
			if total <= offset || len(result) >= limit {
				continue
			}
		}
		result = append(result, item)
	}
	return result, total, nil
}

func scanServerHistoryItem(rows *sql.Rows) (ServerHistoryItem, error) {
	var item ServerHistoryItem
	var eventPayload, deliveryPayload, resultPayload []byte
	var occurred string
	var next sql.NullString
	if err := rows.Scan(&item.ID, &item.ClientID, &item.ClientName, &eventPayload,
		&item.DestinationID, &deliveryPayload, &resultPayload, &item.State, &occurred, &item.Attempts, &next, &item.Reason); err != nil {
		return ServerHistoryItem{}, err
	}
	parsedOccurredAt, err := time.Parse(time.RFC3339Nano, occurred)
	if err != nil {
		return ServerHistoryItem{}, fmt.Errorf("история %s: неверное время события %q: %w", item.ID, occurred, err)
	}
	item.OccurredAt = parsedOccurredAt
	item.NextAttemptAt, err = parseNullableTimeStrict(next)
	if err != nil {
		return ServerHistoryItem{}, fmt.Errorf("история %s: неверное время следующей попытки: %w", item.ID, err)
	}
	plain, err := unprotectDatabaseValue(eventPayload)
	if err != nil {
		return ServerHistoryItem{}, fmt.Errorf("история %s: не удалось расшифровать событие: %w", item.ID, err)
	}
	var event domain.Event
	if err := json.Unmarshal(plain, &event); err != nil {
		return ServerHistoryItem{}, fmt.Errorf("история %s: повреждён JSON события: %w", item.ID, err)
	}
	item.UnitName = event.Position.UnitName
	item.Frequency = event.Position.Frequency
	item.GroupCode = event.Position.GroupCode
	item.SourceRecord = event.Source.ID
	item.SourceUUID = event.Source.UUID
	item.SourceVersion = event.SourceVersion()
	item.ContentDigest = event.ContentDigest
	item.SourceMode = string(event.Mode)
	item.UpdatedBy = event.UpdatedBy
	item.Content = event.Content
	for _, block := range event.Blocks {
		item.BlockUUIDs = append(item.BlockUUIDs, block.Identity())
	}
	item.ContentReady = true
	item.ContentKind = "text"
	if event.SourceBlockDeleted {
		item.ContentKind = "deleted_block"
		item.ContentNote = "Временной блок удалён в EAVC. Центр удаляет только связанную с ним публикацию и не затрагивает соседние перехваты."
	} else if strings.TrimSpace(event.Content) == "" {
		item.ContentKind = "empty_text"
		item.ContentNote = "Клиент передал перехват без текста. Протокол rc69 не передаёт вложения, поэтому фото, аудио, видео или иной файл в этом событии отсутствуют."
	}
	sourceAt := event.SourceAt
	if sourceAt.IsZero() {
		sourceAt = event.ObservedAt
	}
	if !sourceAt.IsZero() {
		item.SourceAt = &sourceAt
	}
	if len(deliveryPayload) > 0 {
		plain, err := unprotectDatabaseValue(deliveryPayload)
		if err != nil {
			return ServerHistoryItem{}, fmt.Errorf("история %s/%s: не удалось расшифровать доставку: %w", item.ID, item.DestinationID, err)
		}
		var delivery domain.PlannedDelivery
		if err := json.Unmarshal(plain, &delivery); err != nil {
			return ServerHistoryItem{}, fmt.Errorf("история %s/%s: повреждён JSON доставки: %w", item.ID, item.DestinationID, err)
		}
		item.Destination = delivery.Destination.Name
		if delivery.Destination.Kind == domain.DestinationTelegram {
			item.TelegramTopic = delivery.Decision.ThreadID
		}
	}
	if len(resultPayload) > 0 {
		plain, err := unprotectDatabaseValue(resultPayload)
		if err != nil {
			return ServerHistoryItem{}, fmt.Errorf("история %s/%s: не удалось расшифровать результат доставки: %w", item.ID, item.DestinationID, err)
		}
		if len(plain) > 0 {
			var receipt application.DeliveryReceipt
			if err := json.Unmarshal(plain, &receipt); err != nil {
				return ServerHistoryItem{}, fmt.Errorf("история %s/%s: повреждён результат доставки: %w", item.ID, item.DestinationID, err)
			}
			if receipt.Kind == "telegram" {
				item.TelegramChat = strconv.FormatInt(receipt.ChatID, 10)
				item.TelegramTopic = receipt.ThreadID
				item.TelegramIDs = append([]int64(nil), receipt.MessageIDs...)
				item.TelegramBlocks = append([]application.DeliveryBlockReceipt(nil), receipt.Blocks...)
				for _, messageID := range receipt.MessageIDs {
					if link := telegramMessageURL(receipt.ChatID, messageID); link != "" {
						item.TelegramURLs = append(item.TelegramURLs, link)
					}
				}
			}
		}
	}
	return item, nil
}

func telegramMessageURL(chatID, messageID int64) string {
	chat := strconv.FormatInt(chatID, 10)
	if messageID <= 0 || !strings.HasPrefix(chat, "-100") || len(chat) <= 4 {
		return ""
	}
	return "https://t.me/c/" + strings.TrimPrefix(chat, "-100") + "/" + strconv.FormatInt(messageID, 10)
}

func serverHistoryItemMatches(item ServerHistoryItem, query string) bool {
	searchable := strings.ToLower(strings.Join([]string{
		item.ID, item.ClientID, item.ClientName, item.UnitName,
		item.Frequency, item.GroupCode, item.DestinationID, item.Destination,
		item.Reason, item.UpdatedBy, item.Content,
		item.TelegramChat, fmt.Sprint(item.TelegramTopic), fmt.Sprint(item.TelegramIDs),
		fmt.Sprint(item.BlockUUIDs), fmt.Sprint(item.TelegramBlocks),
		item.SourceUUID, item.SourceVersion, item.ContentDigest,
		fmt.Sprint(item.TransportMessageID), fmt.Sprint(item.TransportUpdateID),
		fmt.Sprintf("%d", item.SourceRecord),
	}, " "))
	return strings.Contains(searchable, query)
}

func (store *ServerStore) rejectedHistory(ctx context.Context, limit, offset int, filter HistoryFilter) ([]ServerHistoryItem, int, error) {
	cutoff := store.historyCutoff(ctx, "rejected")
	where := []string{"received_at>?"}
	args := []any{cutoff}
	appendHistoryTimeFilter(&where, &args, "received_at", filter)
	clause := " WHERE " + strings.Join(where, " AND ")
	searchQuery := strings.ToLower(strings.TrimSpace(filter.Query))
	total := 0
	if searchQuery == "" {
		if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rejected_packets"+clause, args...).Scan(&total); err != nil {
			return nil, 0, err
		}
	}
	query := `
		SELECT id,sender_bot_username,thread_id,reason,received_at FROM rejected_packets ` + clause + ` ORDER BY received_at DESC`
	queryArgs := args
	if searchQuery == "" {
		query += " LIMIT ? OFFSET ?"
		queryArgs = append(queryArgs, limit, offset)
	} else {
		query += " LIMIT ?"
		queryArgs = append(queryArgs, maxHistorySearchRows+1)
	}
	rows, err := store.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var result []ServerHistoryItem
	scanned := 0
	for rows.Next() {
		scanned++
		if scanned > maxHistorySearchRows {
			return nil, 0, errors.New("поиск охватывает слишком много записей; уточните запрос")
		}
		var id int64
		var sender string
		var thread int64
		var occurred string
		var item ServerHistoryItem
		if err := rows.Scan(&id, &sender, &thread, &item.Reason, &occurred); err != nil {
			return nil, 0, err
		}
		item.ID = fmt.Sprintf("%d", id)
		item.ClientName = strings.TrimPrefix(sender, "@")
		item.Destination = fmt.Sprintf("Тема %d", thread)
		item.State = "rejected"
		item.ContentKind = "unverified"
		item.ContentNote = "Содержимое недоступно: пакет отклонён до подтверждения подлинности и безопасной расшифровки."
		item.OccurredAt, _ = time.Parse(time.RFC3339Nano, occurred)
		if searchQuery != "" {
			searchable := strings.ToLower(strings.Join([]string{item.ID, item.ClientName, item.Destination, item.Reason}, " "))
			if !strings.Contains(searchable, searchQuery) {
				continue
			}
			total++
			if total <= offset || len(result) >= limit {
				continue
			}
		}
		result = append(result, item)
	}
	return result, total, rows.Err()
}

func appendHistoryTimeFilter(where *[]string, args *[]any, column string, filter HistoryFilter) {
	if filter.From != nil {
		*where = append(*where, column+">=?")
		*args = append(*args, stamp(filter.From.UTC()))
	}
	if filter.To != nil {
		*where = append(*where, column+"<=?")
		*args = append(*args, stamp(filter.To.UTC()))
	}
}

// ClearHistory скрывает завершённую историю с экрана, не трогая очередь и дедупликацию.
func (store *ServerStore) ClearHistory(ctx context.Context, kind string, now time.Time) error {
	if kind == "pending" {
		return errors.New("ожидающие задания очищать нельзя: они должны быть доставлены или просрочены")
	}
	if kind != "clients" && kind != "received" && kind != "delivered" && kind != "expired" && kind != "rejected" && kind != "transport" {
		return errors.New("неизвестный раздел истории")
	}
	return store.SaveValue(ctx, historyPrefix+kind, stamp(now.UTC()))
}

func (store *ServerStore) historyCutoff(ctx context.Context, kind string) string {
	if value, exists, err := store.Value(ctx, historyPrefix+kind); err == nil && exists {
		return value
	}
	return "0001-01-01T00:00:00Z"
}

const (
	transportDeletionMaxAttempts = 120
	transportDeletionMaxAge      = 24 * time.Hour
)

// QueueTransportDeletion сохраняет неудавшееся удаление служебного сообщения.
// Повтор ограничен числом попыток и временем, чтобы недоступный навсегда ID не
// создавал бесконечную техническую очередь. dropped сообщает об исчерпании лимита.
func (store *ServerStore) QueueTransportDeletion(ctx context.Context, messageID int64, now time.Time) (dropped bool, err error) {
	if messageID <= 0 {
		return false, errors.New("не задано транспортное сообщение")
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO transport_cleanup(message_id,created_at,next_attempt_at,attempts)
		VALUES(?,?,?,1)
		ON CONFLICT(message_id) DO UPDATE SET
			next_attempt_at=excluded.next_attempt_at,
			attempts=transport_cleanup.attempts+1`,
		messageID, stamp(now), stamp(now.Add(time.Minute))); err != nil {
		return false, err
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM transport_cleanup
		WHERE message_id=? AND (attempts>=? OR created_at<?)`,
		messageID, transportDeletionMaxAttempts, stamp(now.Add(-transportDeletionMaxAge)))
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return count > 0, nil
}

// DueTransportDeletions возвращает ограниченную очередь повторного удаления.
func (store *ServerStore) DueTransportDeletions(ctx context.Context, now time.Time, limit int) ([]int64, error) {
	if limit <= 0 || limit > 100 {
		return nil, errors.New("некорректный размер очереди удаления")
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT message_id FROM transport_cleanup
		WHERE next_attempt_at<=? ORDER BY next_attempt_at,message_id LIMIT ?`, stamp(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	return result, rows.Err()
}

func (store *ServerStore) CompleteTransportDeletion(ctx context.Context, messageID int64) error {
	_, err := store.db.ExecContext(ctx, "DELETE FROM transport_cleanup WHERE message_id=?", messageID)
	return err
}

func parseNullableTime(value sql.NullString) *time.Time {
	parsed, _ := parseNullableTimeStrict(value)
	return parsed
}

func parseNullableTimeStrict(value sql.NullString) (*time.Time, error) {
	if !value.Valid || value.String == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func stamp(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func max64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
