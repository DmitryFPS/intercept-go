package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
)

type ShiftChangeSummary struct {
	Opened       int
	Closed       int
	IgnoredOlder bool
	Conflict     bool
}

type storedShiftSession struct {
	position string
	closedAt sql.NullString
}

// RecordClientShiftSnapshot applies a signed, bounded snapshot idempotently.
// The probe timestamp is the monotonic barrier: an older Telegram packet can
// improve neither the current shift nor its history.
func (store *ServerStore) RecordClientShiftSnapshot(
	ctx context.Context, clientID string, probeAt time.Time, snapshot application.ShiftSnapshot,
) (ShiftChangeSummary, error) {
	var summary ShiftChangeSummary
	snapshot = application.NormalizeShiftSnapshot(snapshot)
	if err := application.ValidateShiftSnapshot(snapshot); err != nil {
		return summary, fmt.Errorf("проверить снимок смен точки: %w", err)
	}
	probeAt = probeAt.UTC()
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return summary, err
	}
	defer tx.Rollback()

	var previousProbe string
	err = tx.QueryRowContext(ctx,
		"SELECT last_probe_at FROM client_shift_sync WHERE client_id=?", clientID,
	).Scan(&previousProbe)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return summary, err
	}
	if err == nil {
		previous, parseErr := time.Parse(time.RFC3339Nano, previousProbe)
		if parseErr == nil && !probeAt.After(previous) {
			summary.IgnoredOlder = true
			return summary, tx.Commit()
		}
	}
	if snapshot.ReadState == application.ShiftReadUnavailable {
		if err := upsertShiftSync(ctx, tx, clientID, "unavailable", probeAt, false); err != nil {
			return summary, err
		}
		return summary, tx.Commit()
	}

	incoming := append(append([]application.ShiftSession(nil), snapshot.Active...), snapshot.LastClosed...)
	existing := make(map[string]storedShiftSession, len(incoming))
	for _, session := range incoming {
		var row storedShiftSession
		queryErr := tx.QueryRowContext(ctx, `
			SELECT position_name,closed_at FROM client_shift_sessions
			WHERE client_id=? AND session_uuid=?`, clientID, session.SessionUUID,
		).Scan(&row.position, &row.closedAt)
		if queryErr != nil && !errors.Is(queryErr, sql.ErrNoRows) {
			return summary, queryErr
		}
		if queryErr == nil {
			existing[session.SessionUUID] = row
			if row.position != session.PositionName || (session.ClosedAt == nil && row.closedAt.Valid) {
				summary.Conflict = true
			}
		}
	}
	if summary.Conflict {
		if err := upsertShiftSync(ctx, tx, clientID, "conflict", probeAt, false); err != nil {
			return summary, err
		}
		return summary, tx.Commit()
	}

	if _, err := tx.ExecContext(ctx,
		"UPDATE client_shift_sessions SET is_current=0 WHERE client_id=? AND is_current=1", clientID,
	); err != nil {
		return summary, err
	}
	now := time.Now().UTC()
	for _, session := range snapshot.LastClosed {
		row, exists := existing[session.SessionUUID]
		if !exists {
			summary.Opened++
		}
		if !exists || !row.closedAt.Valid {
			summary.Closed++
		}
		if err := upsertShiftSession(ctx, tx, clientID, session, now, probeAt); err != nil {
			return summary, err
		}
	}
	for _, session := range snapshot.Active {
		if _, exists := existing[session.SessionUUID]; !exists {
			summary.Opened++
		}
		if err := upsertShiftSession(ctx, tx, clientID, session, now, probeAt); err != nil {
			return summary, err
		}
	}
	currentByPosition := make(map[string]string, len(snapshot.Active)+len(snapshot.LastClosed))
	for _, session := range snapshot.LastClosed {
		currentByPosition[session.PositionName] = session.SessionUUID
	}
	for _, session := range snapshot.Active {
		currentByPosition[session.PositionName] = session.SessionUUID
	}
	for _, sessionUUID := range currentByPosition {
		if _, err := tx.ExecContext(ctx, `
			UPDATE client_shift_sessions SET is_current=1
			WHERE client_id=? AND session_uuid=?`, clientID, sessionUUID); err != nil {
			return summary, err
		}
	}
	if err := upsertShiftSync(ctx, tx, clientID, "ok", probeAt, true); err != nil {
		return summary, err
	}
	return summary, tx.Commit()
}

func upsertShiftSync(ctx context.Context, tx *sql.Tx, clientID, state string, probeAt time.Time, success bool) error {
	var successAt any
	if success {
		successAt = stamp(probeAt)
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO client_shift_sync(client_id,read_state,last_probe_at,last_success_at)
		VALUES(?,?,?,?)
		ON CONFLICT(client_id) DO UPDATE SET
			read_state=excluded.read_state,
			last_probe_at=excluded.last_probe_at,
			last_success_at=COALESCE(excluded.last_success_at,client_shift_sync.last_success_at)`,
		clientID, state, stamp(probeAt), successAt)
	return err
}

func upsertShiftSession(
	ctx context.Context, tx *sql.Tx, clientID string, session application.ShiftSession,
	seenAt, probeAt time.Time,
) error {
	var closedAt any
	if session.ClosedAt != nil {
		closedAt = stamp(session.ClosedAt.UTC())
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO client_shift_sessions(
			client_id,position_name,session_uuid,source_session_id,opened_at,closed_at,
			first_seen_at,last_seen_at,last_probe_at,is_current
		) VALUES(?,?,?,?,?,?,?,?,?,0)
		ON CONFLICT(client_id,session_uuid) DO UPDATE SET
			source_session_id=excluded.source_session_id,
			opened_at=excluded.opened_at,
			closed_at=COALESCE(excluded.closed_at,client_shift_sessions.closed_at),
			last_seen_at=excluded.last_seen_at,
			last_probe_at=excluded.last_probe_at`,
		clientID, session.PositionName, session.SessionUUID, session.SessionID,
		stamp(session.OpenedAt.UTC()), closedAt, stamp(seenAt), stamp(seenAt), stamp(probeAt))
	return err
}

func (store *ServerStore) currentClientShifts(ctx context.Context, clientID string) ([]ClientShiftSession, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT s.client_id,c.name,s.position_name,s.session_uuid,s.source_session_id,
			s.opened_at,s.closed_at,s.first_seen_at,s.last_seen_at,s.is_current
		FROM client_shift_sessions s JOIN server_clients c ON c.client_id=s.client_id
		WHERE s.client_id=? AND s.is_current=1
		ORDER BY s.position_name,s.opened_at DESC`, clientID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanShiftSessions(rows)
}

// recentClientShifts selects the current open shift and the latest closed
// shift for every client independently. A global LIMIT would let a busy client
// hide another client's still-open shift in the dashboard.
func (store *ServerStore) recentClientShifts(ctx context.Context) ([]ClientShiftSession, error) {
	rows, err := store.db.QueryContext(ctx, `
		WITH categorized AS (
			SELECT s.client_id,c.name AS client_name,s.position_name,s.session_uuid,
				s.source_session_id,s.opened_at,s.closed_at,s.first_seen_at,
				s.last_seen_at,s.is_current,
				CASE
					WHEN s.is_current=1 AND s.closed_at IS NULL THEN 0
					WHEN s.closed_at IS NOT NULL THEN 1
					ELSE 2 END AS shift_kind
			FROM client_shift_sessions s
			JOIN server_clients c ON c.client_id=s.client_id
		), ranked AS (
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY client_id,shift_kind
					ORDER BY CASE WHEN shift_kind=0 THEN opened_at ELSE closed_at END DESC,
						session_uuid
				) AS row_number
			FROM categorized
		)
		SELECT client_id,client_name,position_name,session_uuid,source_session_id,
			opened_at,closed_at,first_seen_at,last_seen_at,is_current
		FROM ranked WHERE shift_kind IN (0,1) AND row_number=1
		ORDER BY client_id,shift_kind`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanShiftSessions(rows)
}

func (store *ServerStore) shiftHistory(ctx context.Context, filter HistoryFilter, limit int) ([]ClientShiftSession, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	where := []string{"1=1"}
	args := make([]any, 0, 2)
	if filter.ClientID != "" {
		where = append(where, "s.client_id=?")
		args = append(args, filter.ClientID)
	}
	query := `
		SELECT s.client_id,c.name,s.position_name,s.session_uuid,s.source_session_id,
			s.opened_at,s.closed_at,s.first_seen_at,s.last_seen_at,s.is_current
		FROM client_shift_sessions s JOIN server_clients c ON c.client_id=s.client_id
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY s.opened_at DESC,s.session_uuid
		LIMIT ?`
	args = append(args, limit)
	rows, err := store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items, err := scanShiftSessions(rows)
	if err != nil {
		return nil, err
	}
	search := strings.ToLower(strings.TrimSpace(filter.Query))
	if search == "" {
		return items, nil
	}
	filtered := items[:0]
	for _, item := range items {
		haystack := strings.ToLower(strings.Join([]string{
			item.ClientID, item.ClientName, item.PositionName, item.SessionUUID,
		}, " "))
		if strings.Contains(haystack, search) {
			filtered = append(filtered, item)
		}
	}
	return filtered, nil
}

type shiftRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanShiftSessions(rows shiftRows) ([]ClientShiftSession, error) {
	result := make([]ClientShiftSession, 0)
	for rows.Next() {
		var item ClientShiftSession
		var opened, firstSeen, lastSeen string
		var closed sql.NullString
		var current int
		if err := rows.Scan(
			&item.ClientID, &item.ClientName, &item.PositionName, &item.SessionUUID,
			&item.SourceSessionID, &opened, &closed, &firstSeen, &lastSeen, &current,
		); err != nil {
			return nil, err
		}
		var err error
		item.OpenedAt, err = time.Parse(time.RFC3339Nano, opened)
		if err != nil {
			return nil, fmt.Errorf("прочитать время открытия смены: %w", err)
		}
		item.ClosedAt = parseNullableTime(closed)
		item.FirstSeenAt, err = time.Parse(time.RFC3339Nano, firstSeen)
		if err != nil {
			return nil, fmt.Errorf("прочитать время первого сигнала смены: %w", err)
		}
		item.LastSeenAt, err = time.Parse(time.RFC3339Nano, lastSeen)
		if err != nil {
			return nil, fmt.Errorf("прочитать время последнего сигнала смены: %w", err)
		}
		item.Current = current == 1
		result = append(result, item)
	}
	return result, rows.Err()
}
