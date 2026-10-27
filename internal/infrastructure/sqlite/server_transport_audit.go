package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
)

const serverTransportAuditLimit = 100000

// RecordTransportObservation сохраняет наблюдаемость отдельно от рабочих
// очередей. Более ранняя стадия повторного update не может затереть уже
// установленный окончательный результат.
func (store *ServerStore) RecordTransportObservation(ctx context.Context, item application.TransportObservation) error {
	if item.ChatID == 0 || item.MessageID <= 0 || strings.TrimSpace(item.Outcome) == "" {
		return errors.New("неполные метаданные транспортного сообщения")
	}
	if item.ObservedAt.IsZero() {
		item.ObservedAt = time.Now().UTC()
	}
	if item.SentAt.IsZero() {
		item.SentAt = item.ObservedAt
	}
	if item.Rank < 0 {
		item.Rank = 0
	}
	_, err := store.db.ExecContext(ctx, `
		INSERT INTO transport_audit(
			chat_id,message_id,update_id,thread_id,sender_bot_id,sender_bot_username,
			message_kind,outcome,reason,event_id,client_id,counted,state_rank,seen_count,
			sent_at,first_seen_at,last_seen_at
		) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)
		ON CONFLICT(chat_id,message_id) DO UPDATE SET
			update_id=MAX(transport_audit.update_id,excluded.update_id),
			thread_id=excluded.thread_id,
			sender_bot_id=excluded.sender_bot_id,
			sender_bot_username=excluded.sender_bot_username,
			message_kind=CASE WHEN excluded.state_rank>=transport_audit.state_rank THEN excluded.message_kind ELSE transport_audit.message_kind END,
			outcome=CASE WHEN excluded.state_rank>=transport_audit.state_rank THEN excluded.outcome ELSE transport_audit.outcome END,
			reason=CASE WHEN excluded.state_rank>=transport_audit.state_rank THEN excluded.reason ELSE transport_audit.reason END,
			event_id=CASE WHEN excluded.event_id<>'' THEN excluded.event_id ELSE transport_audit.event_id END,
			client_id=CASE WHEN excluded.client_id<>'' THEN excluded.client_id ELSE transport_audit.client_id END,
			counted=MAX(transport_audit.counted,excluded.counted),
			state_rank=MAX(transport_audit.state_rank,excluded.state_rank),
			seen_count=transport_audit.seen_count+1,
			last_seen_at=excluded.last_seen_at`,
		item.ChatID, item.MessageID, item.UpdateID, item.ThreadID,
		item.SenderBotID, strings.TrimSpace(item.SenderBotUsername),
		item.MessageKind, item.Outcome, item.Reason, strings.TrimSpace(item.EventID),
		strings.TrimSpace(item.ClientID), boolInt(item.Counted), item.Rank,
		stamp(item.SentAt.UTC()), stamp(item.ObservedAt.UTC()), stamp(item.ObservedAt.UTC()),
	)
	if err != nil {
		return fmt.Errorf("записать транспортное наблюдение: %w", err)
	}
	return nil
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

// PurgeTransportAudit ограничивает исключительно диагностический журнал.
// Ошибка этой операции не должна затрагивать рабочие события.
func (store *ServerStore) PurgeTransportAudit(ctx context.Context, before time.Time) (int, error) {
	result, err := store.db.ExecContext(ctx, `
		DELETE FROM transport_audit
		WHERE last_seen_at < ?
		   OR (chat_id,message_id) NOT IN (
			SELECT chat_id,message_id FROM transport_audit
			ORDER BY last_seen_at DESC,update_id DESC LIMIT ?
		   )`, stamp(before.UTC()), serverTransportAuditLimit)
	if err != nil {
		return 0, fmt.Errorf("очистить журнал транспорта: %w", err)
	}
	count, err := result.RowsAffected()
	return int(count), err
}

// attachTransportTraces дополняет всю страницу истории одним пакетным запросом.
// JSON-массив вместо IN(?,?,...) не упирается в лимит параметров SQLite даже
// при защищённом поиске по maxHistorySearchRows строкам.
func (store *ServerStore) attachTransportTraces(ctx context.Context, items []ServerHistoryItem) error {
	ids := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for index := range items {
		id := strings.TrimSpace(items[index].ID)
		if id == "" {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return nil
	}
	payload, err := json.Marshal(ids)
	if err != nil {
		return fmt.Errorf("подготовить транспортные связи событий: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `
		WITH ranked AS (
			SELECT t.event_id,t.update_id,t.message_id,t.message_kind,t.outcome,
				t.seen_count,t.counted,
				ROW_NUMBER() OVER (
					PARTITION BY t.event_id
					ORDER BY t.state_rank DESC,t.last_seen_at DESC
				) AS row_number
			FROM transport_audit t
			JOIN json_each(?) requested ON requested.value=t.event_id
		)
		SELECT event_id,update_id,message_id,message_kind,outcome,seen_count,counted
		FROM ranked WHERE row_number=1`, string(payload))
	if err != nil {
		return fmt.Errorf("прочитать транспортную связь события: %w", err)
	}
	defer rows.Close()
	type trace struct {
		updateID, messageID int64
		kind, outcome       string
		seen, counted       int
	}
	traces := make(map[string]trace, len(ids))
	for rows.Next() {
		var eventID string
		var item trace
		if err := rows.Scan(&eventID, &item.updateID, &item.messageID, &item.kind,
			&item.outcome, &item.seen, &item.counted); err != nil {
			return fmt.Errorf("прочитать транспортную связь события: %w", err)
		}
		traces[eventID] = item
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("прочитать транспортные связи событий: %w", err)
	}
	for index := range items {
		trace, exists := traces[items[index].ID]
		if !exists {
			continue
		}
		items[index].TransportUpdateID = trace.updateID
		items[index].TransportMessageID = trace.messageID
		items[index].TransportKind = trace.kind
		items[index].TransportOutcome = trace.outcome
		items[index].TransportSeen = trace.seen
		items[index].Counted = trace.counted == 1
	}
	return nil
}

func (store *ServerStore) transportHistory(
	ctx context.Context, limit, offset int, filter HistoryFilter,
) ([]ServerHistoryItem, int, error) {
	conditions := []string{"t.last_seen_at>?"}
	args := []any{store.historyCutoff(ctx, "transport")}
	appendHistoryTimeFilter(&conditions, &args, "t.last_seen_at", filter)
	if filter.ClientID != "" {
		conditions = append(conditions, "t.client_id=?")
		args = append(args, filter.ClientID)
	}
	if query := strings.ToLower(strings.TrimSpace(filter.Query)); query != "" {
		pattern := "%" + query + "%"
		conditions = append(conditions, `(lower(t.event_id) LIKE ? OR lower(t.outcome) LIKE ? OR
			lower(t.reason) LIKE ? OR lower(t.message_kind) LIKE ? OR
			CAST(t.message_id AS TEXT) LIKE ? OR CAST(t.update_id AS TEXT) LIKE ? OR
			lower(t.sender_bot_username) LIKE ?)`)
		for index := 0; index < 7; index++ {
			args = append(args, pattern)
		}
	}
	where := " WHERE " + strings.Join(conditions, " AND ")
	var total int
	if err := store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_audit t"+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	query := `SELECT t.update_id,t.message_id,t.chat_id,t.thread_id,t.sender_bot_username,
		t.message_kind,t.outcome,t.reason,t.event_id,t.client_id,COALESCE(c.name,''),
		t.counted,t.seen_count,t.last_seen_at
		FROM transport_audit t LEFT JOIN server_clients c ON c.client_id=t.client_id` + where +
		" ORDER BY t.last_seen_at DESC,t.update_id DESC LIMIT ? OFFSET ?"
	queryArgs := append(append([]any(nil), args...), limit, offset)
	rows, err := store.db.QueryContext(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	items := make([]ServerHistoryItem, 0, limit)
	for rows.Next() {
		var item ServerHistoryItem
		var sender, eventID, clientID, occurred string
		var chatID int64
		var counted int
		if err := rows.Scan(&item.TransportUpdateID, &item.TransportMessageID, &chatID,
			&item.TelegramTopic, &sender, &item.TransportKind, &item.TransportOutcome,
			&item.Reason, &eventID, &clientID, &item.ClientName, &counted,
			&item.TransportSeen, &occurred); err != nil {
			return nil, 0, err
		}
		item.ID = eventID
		item.TelegramChat = fmt.Sprintf("%d", chatID)
		if item.ID == "" {
			item.ID = fmt.Sprintf("transport:%s:%d", item.TelegramChat, item.TransportMessageID)
		}
		item.ClientID = clientID
		if item.ClientName == "" {
			item.ClientName = sender
		}
		item.State = "transport"
		item.Counted = counted == 1
		item.ContentKind = "transport"
		item.ContentNote = item.Reason
		item.OccurredAt, err = time.Parse(time.RFC3339Nano, occurred)
		if err != nil {
			return nil, 0, err
		}
		items = append(items, item)
	}
	return items, total, rows.Err()
}
