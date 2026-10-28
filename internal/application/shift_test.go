package application

import (
	"testing"
	"time"
)

func TestValidateShiftSnapshotAcceptsOpenAndLastClosedPerPosition(t *testing.T) {
	t.Parallel()
	opened := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	closed := opened.Add(4 * time.Hour)
	snapshot := ShiftSnapshot{
		ReadState: ShiftReadOK,
		Active: []ShiftSession{{
			SessionID: 2, SessionUUID: "session-open", PositionName: "Позиция 1", OpenedAt: closed,
		}},
		LastClosed: []ShiftSession{{
			SessionID: 1, SessionUUID: "session-closed", PositionName: "Позиция 1",
			OpenedAt: opened, ClosedAt: &closed,
		}},
	}
	if err := ValidateShiftSnapshot(snapshot); err != nil {
		t.Fatalf("корректный снимок отклонён: %v", err)
	}
}

func TestValidateShiftSnapshotRejectsAmbiguousLifecycle(t *testing.T) {
	t.Parallel()
	opened := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	closed := opened.Add(time.Hour)
	session := ShiftSession{
		SessionID: 1, SessionUUID: "same-session", PositionName: "Позиция", OpenedAt: opened,
	}
	closedSession := session
	closedSession.ClosedAt = &closed
	if err := ValidateShiftSnapshot(ShiftSnapshot{
		ReadState: ShiftReadOK, Active: []ShiftSession{session}, LastClosed: []ShiftSession{closedSession},
	}); err == nil {
		t.Fatal("одна смена одновременно принята открытой и закрытой")
	}
}

func TestUnavailableShiftSnapshotCannotCarryStaleRows(t *testing.T) {
	t.Parallel()
	if err := ValidateShiftSnapshot(ShiftSnapshot{
		ReadState: ShiftReadUnavailable,
		Active:    []ShiftSession{{SessionID: 1, SessionUUID: "stale", OpenedAt: time.Now().UTC()}},
	}); err == nil {
		t.Fatal("недоступный снимок сохранил непроверенные данные")
	}
}
