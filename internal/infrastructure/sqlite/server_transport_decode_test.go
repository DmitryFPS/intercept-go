package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
)

func newTransportDecodeTestStore(t *testing.T) (context.Context, *sql.DB, *ServerStore, time.Time) {
	t.Helper()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, db, store, time.Now().UTC().Truncate(time.Second).Add(time.Second)
}

func transportDecodeState(t *testing.T, ctx context.Context, db *sql.DB, updateID int64) (int, int, time.Time, string, []byte) {
	t.Helper()
	var attempts, failures int
	var next, reason string
	var protected []byte
	if err := db.QueryRowContext(ctx, `
		SELECT attempts,decode_failures,next_attempt_at,last_error,packet_json
		FROM transport_inbox WHERE update_id=?`, updateID,
	).Scan(&attempts, &failures, &next, &reason, &protected); err != nil {
		t.Fatal(err)
	}
	nextAt, err := time.Parse(time.RFC3339Nano, next)
	if err != nil {
		t.Fatal(err)
	}
	return attempts, failures, nextAt, reason, protected
}

func transportDecodeCounts(t *testing.T, ctx context.Context, db *sql.DB) (int, int) {
	t.Helper()
	var rejected, cleanup int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rejected_packets").Scan(&rejected); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_cleanup").Scan(&cleanup); err != nil {
		t.Fatal(err)
	}
	return rejected, cleanup
}

func TestPendingTransportPacketsRetriesTransientDPAPIAndKeepsIndependentPacket(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newTransportDecodeTestStore(t)
	packets := []application.IncomingPacket{
		{UpdateID: 21, ChatID: -1001, MessageID: 121, Payload: []byte("first")},
		{UpdateID: 22, ChatID: -1001, MessageID: 122, Payload: []byte("second")},
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 23, packets); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, original := transportDecodeState(t, ctx, db, 21)
	failedOnce := false
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, original) && !failedOnce {
			failedOnce = true
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	firstAt := base.Add(2 * time.Second)
	items, err := store.PendingTransportPackets(ctx, 25, firstAt)
	if err != nil || len(items) != 1 || items[0].Packet.UpdateID != 22 {
		t.Fatalf("временный сбой DPAPI заблокировал независимый пакет: items=%#v err=%v", items, err)
	}
	attempts, failures, retryAt, reason, stillProtected := transportDecodeState(t, ctx, db, 21)
	if attempts != 0 || failures != 1 || !retryAt.After(firstAt) || !strings.Contains(reason, "The data is invalid") {
		t.Fatalf("пакет не отложен для повторного чтения: attempts=%d failures=%d next=%v reason=%q", attempts, failures, retryAt, reason)
	}
	if !bytes.Equal(stillProtected, original) {
		t.Fatal("после ошибки DPAPI изменён зашифрованный пакет")
	}
	if rejected, cleanup := transportDecodeCounts(t, ctx, db); rejected != 0 || cleanup != 0 {
		t.Fatalf("временная ошибка DPAPI привела к карантину или удалению из Telegram: rejected=%d cleanup=%d", rejected, cleanup)
	}
	if offset, exists, err := store.Value(ctx, "telegram_offset"); err != nil || !exists || offset != "23" {
		t.Fatalf("потеряно подтверждённое смещение Telegram: offset=%q exists=%v err=%v", offset, exists, err)
	}
	items, err = store.PendingTransportPackets(ctx, 25, retryAt.Add(-time.Nanosecond))
	if err != nil || len(items) != 1 || items[0].Packet.UpdateID != 22 {
		t.Fatalf("пакет выдан до назначенного повтора: items=%#v err=%v", items, err)
	}
	items, err = store.PendingTransportPackets(ctx, 25, retryAt)
	if err != nil || len(items) != 2 {
		t.Fatalf("пакет не восстановился после временной ошибки: items=%#v err=%v", items, err)
	}
	seen := map[int64]bool{}
	for _, item := range items {
		if seen[item.Packet.UpdateID] {
			t.Fatalf("после повтора пришёл дубликат: items=%#v", items)
		}
		seen[item.Packet.UpdateID] = true
	}
	if !seen[21] || !seen[22] {
		t.Fatalf("пакет не восстановился после временной ошибки: items=%#v err=%v", items, err)
	}
	attempts, failures, _, reason, stillProtected = transportDecodeState(t, ctx, db, 21)
	if attempts != 0 || failures != 0 || reason != "" || !bytes.Equal(stillProtected, original) {
		t.Fatalf("состояние ошибки не сброшено после успешного чтения: attempts=%d failures=%d reason=%q", attempts, failures, reason)
	}
}

func TestPendingTransportPacketsKeepsRepeatedDPAPIFailureAcrossRestart(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newTransportDecodeTestStore(t)
	packet := application.IncomingPacket{UpdateID: 31, ChatID: -1001, MessageID: 131, Payload: []byte("preserve")}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 32, []application.IncomingPacket{packet}); err != nil {
		t.Fatal(err)
	}
	_, _, _, _, original := transportDecodeState(t, ctx, db, packet.UpdateID)
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, original) {
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	attemptAt := base.Add(2 * time.Second)
	for failure := 1; failure <= 6; failure++ {
		items, err := store.PendingTransportPackets(ctx, 25, attemptAt)
		if err != nil || len(items) != 0 {
			t.Fatalf("попытка %d вернула нечитаемый пакет: items=%#v err=%v", failure, items, err)
		}
		attempts, failures, nextAt, reason, stillProtected := transportDecodeState(t, ctx, db, packet.UpdateID)
		if attempts != 0 || failures != failure || !nextAt.After(attemptAt) || !strings.Contains(reason, "The data is invalid") || !bytes.Equal(stillProtected, original) {
			t.Fatalf("попытка %d потеряла пакет или не назначила повтор: attempts=%d failures=%d next=%v reason=%q", failure, attempts, failures, nextAt, reason)
		}
		if rejected, cleanup := transportDecodeCounts(t, ctx, db); rejected != 0 || cleanup != 0 {
			t.Fatalf("попытка %d очистила транспортный пакет: rejected=%d cleanup=%d", failure, rejected, cleanup)
		}
		attemptAt = nextAt
	}
	restarted, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatalf("перезапуск хранилища после повторных ошибок: %v", err)
	}
	items, err := restarted.PendingTransportPackets(ctx, 25, attemptAt.Add(-time.Nanosecond))
	if err != nil || len(items) != 0 {
		t.Fatalf("перезапуск нарушил задержку повтора: items=%#v err=%v", items, err)
	}
	items, err = restarted.PendingTransportPackets(ctx, 25, attemptAt)
	if err != nil || len(items) != 1 || items[0].Packet.UpdateID != packet.UpdateID || string(items[0].Packet.Payload) != "preserve" {
		t.Fatalf("пакет не восстановлен после перезапуска: items=%#v err=%v", items, err)
	}
	_, failures, _, reason, _ := transportDecodeState(t, ctx, db, packet.UpdateID)
	if failures != 0 || reason != "" {
		t.Fatalf("после успешного чтения осталась ошибка: failures=%d reason=%q", failures, reason)
	}
}

func TestPendingTransportPacketsQuarantinesDecodedStructuralCorruption(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newTransportDecodeTestStore(t)
	packets := []application.IncomingPacket{
		{UpdateID: 41, ChatID: -1001, MessageID: 141, Payload: []byte("first")},
		{UpdateID: 42, ChatID: -1001, MessageID: 142, Payload: []byte("second")},
	}
	if err := store.PersistTransportBatch(ctx, "telegram_offset", 43, packets); err != nil {
		t.Fatal(err)
	}
	malformed, err := protectDatabaseValue([]byte("{"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE transport_inbox SET packet_json=? WHERE update_id=41", malformed); err != nil {
		t.Fatal(err)
	}
	items, err := store.PendingTransportPackets(ctx, 25, base.Add(2*time.Second))
	if err != nil || len(items) != 1 || items[0].Packet.UpdateID != 42 {
		t.Fatalf("структурная порча заблокировала следующий пакет: items=%#v err=%v", items, err)
	}
	var remaining int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_inbox WHERE update_id=41").Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	rejected, cleanup := transportDecodeCounts(t, ctx, db)
	if remaining != 0 || rejected != 1 || cleanup != 1 {
		t.Fatalf("структурная порча не изолирована атомарно: inbox=%d rejected=%d cleanup=%d", remaining, rejected, cleanup)
	}
}
