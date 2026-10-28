package application

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

const MaxShiftSessionsPerSnapshot = 100

const (
	ShiftReadOK          = "ok"
	ShiftReadUnavailable = "unavailable"
)

// ShiftSession is a read-only EAVC shift identity. SessionUUID is the stable
// identity; the local numeric ID is diagnostic only and is never used for
// deduplication across working points.
type ShiftSession struct {
	SessionID    int64      `json:"session_id"`
	SessionUUID  string     `json:"session_uuid"`
	PositionName string     `json:"position_name"`
	OpenedAt     time.Time  `json:"opened_at"`
	ClosedAt     *time.Time `json:"closed_at,omitempty"`
}

// ShiftSnapshot carries only the current open shift and the latest closed
// shift for every EAVC position. This bounded snapshot is enough to recover a
// missed close/open transition without replaying an unbounded history.
type ShiftSnapshot struct {
	ReadState  string         `json:"read_state"`
	Active     []ShiftSession `json:"active,omitempty"`
	LastClosed []ShiftSession `json:"last_closed,omitempty"`
}

func UnavailableShiftSnapshot() ShiftSnapshot {
	return ShiftSnapshot{ReadState: ShiftReadUnavailable}
}

func EmptyShiftSnapshot() ShiftSnapshot {
	return ShiftSnapshot{ReadState: ShiftReadOK}
}

// NormalizeShiftSnapshot returns deterministic slice ordering for signatures,
// comparisons and logs. It deliberately does not alter source values.
func NormalizeShiftSnapshot(snapshot ShiftSnapshot) ShiftSnapshot {
	normalized := snapshot
	normalized.ReadState = strings.TrimSpace(snapshot.ReadState)
	normalized.Active = append([]ShiftSession(nil), snapshot.Active...)
	normalized.LastClosed = append([]ShiftSession(nil), snapshot.LastClosed...)
	less := func(left, right ShiftSession) bool {
		if left.PositionName != right.PositionName {
			return left.PositionName < right.PositionName
		}
		if !left.OpenedAt.Equal(right.OpenedAt) {
			return left.OpenedAt.Before(right.OpenedAt)
		}
		return left.SessionUUID < right.SessionUUID
	}
	sort.Slice(normalized.Active, func(i, j int) bool { return less(normalized.Active[i], normalized.Active[j]) })
	sort.Slice(normalized.LastClosed, func(i, j int) bool { return less(normalized.LastClosed[i], normalized.LastClosed[j]) })
	return normalized
}

func ValidateShiftSnapshot(snapshot ShiftSnapshot) error {
	if snapshot.ReadState != ShiftReadOK && snapshot.ReadState != ShiftReadUnavailable {
		return errors.New("неизвестное состояние чтения смены EAVC")
	}
	if len(snapshot.Active) > MaxShiftSessionsPerSnapshot || len(snapshot.LastClosed) > MaxShiftSessionsPerSnapshot {
		return fmt.Errorf("снимок смен EAVC превышает предел %d записей", MaxShiftSessionsPerSnapshot)
	}
	if snapshot.ReadState == ShiftReadUnavailable {
		if len(snapshot.Active) != 0 || len(snapshot.LastClosed) != 0 {
			return errors.New("недоступный снимок смен EAVC не должен содержать данные")
		}
		return nil
	}
	seenUUID := make(map[string]string, len(snapshot.Active)+len(snapshot.LastClosed))
	activePositions := make(map[string]struct{}, len(snapshot.Active))
	for _, session := range snapshot.Active {
		if session.ClosedAt != nil {
			return errors.New("открытая смена EAVC содержит время закрытия")
		}
		if err := validateShiftSession(session); err != nil {
			return err
		}
		if _, exists := activePositions[session.PositionName]; exists {
			return fmt.Errorf("для позиции %q передано несколько открытых смен", session.PositionName)
		}
		activePositions[session.PositionName] = struct{}{}
		seenUUID[session.SessionUUID] = "open"
	}
	closedPositions := make(map[string]struct{}, len(snapshot.LastClosed))
	for _, session := range snapshot.LastClosed {
		if session.ClosedAt == nil {
			return errors.New("закрытая смена EAVC не содержит время закрытия")
		}
		if err := validateShiftSession(session); err != nil {
			return err
		}
		if _, exists := closedPositions[session.PositionName]; exists {
			return fmt.Errorf("для позиции %q передано несколько последних закрытых смен", session.PositionName)
		}
		closedPositions[session.PositionName] = struct{}{}
		if previous, exists := seenUUID[session.SessionUUID]; exists {
			return fmt.Errorf("смена %q одновременно передана как %s и закрытая", session.SessionUUID, previous)
		}
		seenUUID[session.SessionUUID] = "closed"
	}
	return nil
}

func validateShiftSession(session ShiftSession) error {
	if session.SessionID <= 0 {
		return errors.New("смена EAVC содержит некорректный локальный ID")
	}
	uuid := strings.TrimSpace(session.SessionUUID)
	if uuid == "" || len(uuid) > 128 || strings.ContainsAny(uuid, "\x00\r\n\t") {
		return errors.New("смена EAVC содержит некорректный UUID")
	}
	if utf8.RuneCountInString(session.PositionName) > 200 || strings.ContainsRune(session.PositionName, '\x00') {
		return errors.New("название позиции смены EAVC недопустимо")
	}
	if session.OpenedAt.IsZero() {
		return errors.New("смена EAVC не содержит время открытия")
	}
	if session.ClosedAt != nil && session.ClosedAt.Before(session.OpenedAt) {
		return errors.New("смена EAVC закрыта раньше открытия")
	}
	return nil
}
