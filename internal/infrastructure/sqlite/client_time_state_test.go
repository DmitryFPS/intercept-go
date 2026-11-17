package sqlite

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestClientTimeStateSurvivesReopenAndStaysProtected(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "state.db")
	ctx := context.Background()
	record := application.SourceTimeStateRecord{Snapshot: &application.SourceTimeSnapshot{
		SourceID: 7, SourceUUID: "source-7", SourceVersion: "2026-10-28 10:10:00",
		Blocks: []application.SourceTimeStateEntry{{
			OccurrenceID: "occurrence-a", CanonicalTime: "10:10", DayOffset: 0,
			PhysicalUUID: "physical-a", Active: true,
			Block: domain.ContentBlock{
				UUID: "time:d0:1010:occurrence-a", Key: "10:10", Text: "10:10\n-Секретный текст",
			},
		}},
	}}

	db, err := OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.SaveSourceTimeState(ctx, "uuid:source-7", record); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	if err := db.QueryRowContext(ctx,
		"SELECT payload FROM client_time_state WHERE source_key='uuid:source-7'",
	).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(payload, []byte("Секретный текст")) || !bytes.HasPrefix(payload, protectedPrefix) {
		t.Fatalf("состояние временных блоков осталось открытым: %q", payload)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = OpenState(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err = NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	loaded, exists, err := queue.LoadSourceTimeState(ctx, "uuid:source-7")
	if err != nil || !exists || loaded.Snapshot == nil || len(loaded.Snapshot.Blocks) != 1 {
		t.Fatalf("состояние не восстановлено: exists=%v record=%#v err=%v", exists, loaded, err)
	}
	if got := loaded.Snapshot.Blocks[0]; got.OccurrenceID != "occurrence-a" ||
		got.Block.UUID != "time:d0:1010:occurrence-a" || got.PhysicalUUID != "physical-a" {
		t.Fatalf("идентичность occurrence изменилась после перезапуска: %#v", got)
	}
}

func TestClientTimeStateRemovesExpiredCacheRows(t *testing.T) {
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO client_time_state(source_key,payload,updated_at) VALUES(?,?,?)`,
		"uuid:expired", []byte("obsolete"), time.Now().UTC().Add(-clientTimeStateRetention-time.Hour).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	record := application.SourceTimeStateRecord{Snapshot: &application.SourceTimeSnapshot{
		SourceID: 8, SourceUUID: "source-8", SourceVersion: "2026-10-28 10:20:00",
		Blocks: []application.SourceTimeStateEntry{{
			OccurrenceID: "occurrence-b", CanonicalTime: "10:20", Active: true,
			Block: domain.ContentBlock{UUID: "time:d0:1020:occurrence-b", Key: "10:20", Text: "10:20\n-Текст"},
		}},
	}}
	if err := queue.SaveSourceTimeState(ctx, "uuid:source-8", record); err != nil {
		t.Fatal(err)
	}
	var expired int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM client_time_state WHERE source_key='uuid:expired'`).Scan(&expired); err != nil || expired != 0 {
		t.Fatalf("устаревшее техническое состояние не очищено: count=%d err=%v", expired, err)
	}
}

func TestClientTimeStateValidationReturnsTypedError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := domain.ContentBlock{UUID: "time:d0:0017:same", Text: "00.17\n-Проверка"}
	record := application.SourceTimeStateRecord{Snapshot: &application.SourceTimeSnapshot{
		SourceID: 17, SourceUUID: "source-17", SourceVersion: "2026-11-16 23:53:21",
		Blocks: []application.SourceTimeStateEntry{
			{OccurrenceID: "occurrence-a", CanonicalTime: "00:17", Active: true, Block: duplicate},
			{OccurrenceID: "occurrence-b", CanonicalTime: "00:17", Active: false, Block: duplicate},
		},
	}}
	if err := queue.SaveSourceTimeState(ctx, "uuid:source-17", record); !errors.Is(err, application.ErrSourceTimeStateInvalid) {
		t.Fatalf("ошибка валидатора не классифицирована: %v", err)
	}
}

func TestClientTimeStateInvalidJSONReturnsTypedSourceError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	queue, err := NewClientQueue(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	protected, err := protectDatabaseValue([]byte("{"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO client_time_state(source_key,payload,updated_at) VALUES(?,?,?)`,
		"uuid:source-invalid-json", protected, time.Now().UTC().Format(time.RFC3339Nano),
	); err != nil {
		t.Fatal(err)
	}
	if _, _, err := queue.LoadSourceTimeState(ctx, "uuid:source-invalid-json"); !errors.Is(err, application.ErrSourceTimeStateInvalid) {
		t.Fatalf("повреждённый JSON не классифицирован как ошибка одной записи: %v", err)
	}
}
