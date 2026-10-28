package sqlite

import (
	"testing"
	"time"

	"intercept/internal/application"
)

func TestShiftSnapshotsAreDeduplicatedAndOrderedAcrossRestartSafeStorage(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	base := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	firstClosedAt := base.Add(4 * time.Hour)
	first := application.ShiftSession{
		SessionID: 1, SessionUUID: "shift-1", PositionName: "Позиция 1",
		OpenedAt: base, ClosedAt: &firstClosedAt,
	}
	second := application.ShiftSession{
		SessionID: 2, SessionUUID: "shift-2", PositionName: "Позиция 1",
		OpenedAt: firstClosedAt,
	}
	snapshot := application.ShiftSnapshot{
		ReadState: application.ShiftReadOK,
		Active:    []application.ShiftSession{second}, LastClosed: []application.ShiftSession{first},
	}
	change, err := store.RecordClientShiftSnapshot(ctx, "textile", base.Add(5*time.Hour), snapshot)
	if err != nil || change.Opened != 2 || change.Closed != 1 {
		t.Fatalf("первый снимок сохранён неверно: %+v err=%v", change, err)
	}
	change, err = store.RecordClientShiftSnapshot(ctx, "textile", base.Add(5*time.Hour+2*time.Minute), snapshot)
	if err != nil || change.Opened != 0 || change.Closed != 0 {
		t.Fatalf("повторный probe создал дубли: %+v err=%v", change, err)
	}
	page, err := store.HistoryPage(ctx, "clients", 30, 0, base.Add(5*time.Hour+2*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if len(page.ShiftHistory) != 2 || len(page.Clients) != 1 || len(page.Clients[0].Shifts) != 1 ||
		page.Clients[0].Shifts[0].SessionUUID != "shift-2" || page.Clients[0].Shifts[0].ClosedAt != nil {
		t.Fatalf("текущее состояние и история смен неверны: %+v", page)
	}

	older := application.ShiftSnapshot{
		ReadState: application.ShiftReadOK,
		Active: []application.ShiftSession{{
			SessionID: 99, SessionUUID: "late-old", PositionName: "Позиция 1", OpenedAt: base.Add(time.Hour),
		}},
	}
	change, err = store.RecordClientShiftSnapshot(ctx, "textile", base.Add(5*time.Hour+time.Minute), older)
	if err != nil || !change.IgnoredOlder {
		t.Fatalf("запоздавший probe не отброшен: %+v err=%v", change, err)
	}

	if _, err := store.RecordClientShiftSnapshot(
		ctx, "textile", base.Add(5*time.Hour+3*time.Minute), application.UnavailableShiftSnapshot(),
	); err != nil {
		t.Fatal(err)
	}
	page, err = store.HistoryPage(ctx, "clients", 30, 0, base.Add(5*time.Hour+3*time.Minute))
	if err != nil || page.Clients[0].ShiftReadState != "unavailable" ||
		len(page.Clients[0].Shifts) != 1 || page.Clients[0].Shifts[0].SessionUUID != "shift-2" {
		t.Fatalf("недоступность EAVC затёрла последнее состояние: %+v err=%v", page.Clients[0], err)
	}

	secondClosedAt := base.Add(8 * time.Hour)
	second.ClosedAt = &secondClosedAt
	third := application.ShiftSession{
		SessionID: 3, SessionUUID: "shift-3", PositionName: "Позиция 1", OpenedAt: secondClosedAt,
	}
	change, err = store.RecordClientShiftSnapshot(ctx, "textile", base.Add(8*time.Hour+time.Minute), application.ShiftSnapshot{
		ReadState: application.ShiftReadOK,
		Active:    []application.ShiftSession{third}, LastClosed: []application.ShiftSession{second},
	})
	if err != nil || change.Opened != 1 || change.Closed != 1 {
		t.Fatalf("переход закрыть/открыть сохранён неверно: %+v err=%v", change, err)
	}
	page, err = store.HistoryPage(ctx, "clients", 30, 0, base.Add(8*time.Hour+time.Minute))
	if err != nil || len(page.ShiftHistory) != 3 || page.Clients[0].Shifts[0].SessionUUID != "shift-3" {
		t.Fatalf("новая смена не стала текущей без дублей: %+v err=%v", page, err)
	}
}

func TestClosedShiftCannotBeReopenedByNewerConflictingSnapshot(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	base := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	closedAt := base.Add(time.Hour)
	closed := application.ShiftSession{
		SessionID: 1, SessionUUID: "closed-shift", PositionName: "Позиция",
		OpenedAt: base, ClosedAt: &closedAt,
	}
	if _, err := store.RecordClientShiftSnapshot(ctx, "textile", closedAt, application.ShiftSnapshot{
		ReadState: application.ShiftReadOK, LastClosed: []application.ShiftSession{closed},
	}); err != nil {
		t.Fatal(err)
	}
	reopened := closed
	reopened.ClosedAt = nil
	change, err := store.RecordClientShiftSnapshot(ctx, "textile", closedAt.Add(time.Minute), application.ShiftSnapshot{
		ReadState: application.ShiftReadOK, Active: []application.ShiftSession{reopened},
	})
	if err != nil || !change.Conflict {
		t.Fatalf("повторное открытие закрытой смены не изолировано: %+v err=%v", change, err)
	}
	page, err := store.HistoryPage(ctx, "clients", 30, 0, closedAt.Add(time.Minute))
	if err != nil || page.Clients[0].ShiftReadState != "conflict" || page.Clients[0].Shifts[0].ClosedAt == nil {
		t.Fatalf("противоречивый снимок откатил историю: %+v err=%v", page, err)
	}
}

func TestBrokenShiftRowsDoNotBreakClientHealthPage(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	openedAt := time.Date(2026, 10, 28, 6, 0, 0, 0, time.UTC)
	if _, err := store.RecordClientShiftSnapshot(ctx, "textile", openedAt.Add(time.Minute), application.ShiftSnapshot{
		ReadState: application.ShiftReadOK,
		Active: []application.ShiftSession{{
			SessionID: 1, SessionUUID: "broken-shift", PositionName: "Позиция", OpenedAt: openedAt,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE client_shift_sessions SET opened_at='повреждённое время'
		WHERE client_id='textile' AND session_uuid='broken-shift'`); err != nil {
		t.Fatal(err)
	}

	page, err := store.HistoryPage(ctx, "clients", 30, 0, openedAt.Add(2*time.Minute))
	if err != nil {
		t.Fatalf("ошибка данных смены закрыла всю карточку рабочих точек: %v", err)
	}
	if len(page.Clients) != 1 || page.Clients[0].ShiftReadState != "error" || page.ShiftWarning == "" || len(page.InternalWarnings) == 0 {
		t.Fatalf("повреждение смены не изолировано и не объяснено: %+v", page)
	}
}

func TestMissingShiftTablesDoNotHideConnectionHealth(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	if _, err := store.db.ExecContext(ctx, `
		DROP TABLE client_shift_sessions;
		DROP TABLE client_shift_sync;`); err != nil {
		t.Fatal(err)
	}

	page, err := store.HistoryPage(ctx, "clients", 30, 0, time.Now().UTC())
	if err != nil {
		t.Fatalf("ошибка вспомогательных таблиц смен закрыла состояние связи: %v", err)
	}
	if len(page.Clients) != 1 || page.Clients[0].ShiftReadState != "error" || page.ShiftWarning == "" || len(page.InternalWarnings) < 2 {
		t.Fatalf("отсутствующие таблицы смен не изолированы: %+v", page)
	}
}
