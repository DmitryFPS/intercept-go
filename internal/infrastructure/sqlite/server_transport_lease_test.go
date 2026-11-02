package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"intercept/internal/application"
)

func openTransportLeaseTestStore(t *testing.T) (context.Context, *ServerStore) {
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
	return ctx, store
}

func persistLeasePackets(t *testing.T, ctx context.Context, store *ServerStore, packets ...application.IncomingPacket) {
	t.Helper()
	var next int64
	for _, packet := range packets {
		if packet.UpdateID >= next {
			next = packet.UpdateID + 1
		}
	}
	if err := store.PersistTransportBatch(ctx, "lease_test_offset", next, packets); err != nil {
		t.Fatal(err)
	}
}

func leasePacket(updateID, senderBotID int64) application.IncomingPacket {
	return application.IncomingPacket{
		UpdateID: updateID, SenderBotID: senderBotID, ChatID: -1001,
		MessageID: 1000 + updateID, Payload: []byte("packet"),
	}
}

func TestClaimTransportPacketsConcurrentWorkersNeverShareRow(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101))
	now := time.Now().UTC()

	start := make(chan struct{})
	results := make(chan []TransportInboxItem, 2)
	errorsCh := make(chan error, 2)
	var workers sync.WaitGroup
	for range 2 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			items, err := store.ClaimTransportPackets(ctx, 1, now, time.Minute)
			results <- items
			errorsCh <- err
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	claimed := 0
	for items := range results {
		claimed += len(items)
	}
	if claimed != 1 {
		t.Fatalf("одна строка выдана %d worker вместо одного", claimed)
	}
}

func TestClaimTransportPacketsLeaseExpiresAfterCrash(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101))
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 10, base, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("первая аренда: items=%#v err=%v", first, err)
	}
	active, err := store.ClaimTransportPackets(ctx, 10, base.Add(time.Minute-time.Nanosecond), time.Minute)
	if err != nil || len(active) != 0 {
		t.Fatalf("активная аренда выдана повторно: items=%#v err=%v", active, err)
	}
	recovered, err := store.ClaimTransportPackets(ctx, 10, base.Add(time.Minute), time.Minute)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("истёкшая аренда не восстановилась: items=%#v err=%v", recovered, err)
	}
	if recovered[0].LeaseToken == first[0].LeaseToken {
		t.Fatal("повторная выдача не получила новый token")
	}
}

func TestReleaseTransportPacketLeaseMakesPacketImmediatelyClaimable(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101))
	var originalNextAttempt string
	if err := store.db.QueryRowContext(ctx,
		"SELECT next_attempt_at FROM transport_inbox WHERE update_id=1").Scan(&originalNextAttempt); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 1, base, time.Hour)
	if err != nil || len(first) != 1 {
		t.Fatalf("первая аренда: items=%#v err=%v", first, err)
	}
	if err := store.ReleaseTransportPacketLease(ctx, first[0].Packet.UpdateID, "foreign"); !errors.Is(err, ErrTransportLeaseLost) {
		t.Fatalf("чужой token освободил аренду: %v", err)
	}
	if err := store.ReleaseTransportPacketLease(ctx, first[0].Packet.UpdateID, first[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimTransportPackets(ctx, 1, base.Add(time.Nanosecond), time.Hour)
	if err != nil || len(second) != 1 {
		t.Fatalf("освобождённый пакет не выдан сразу: items=%#v err=%v", second, err)
	}
	if second[0].LeaseToken == first[0].LeaseToken {
		t.Fatal("повторная аренда получила прежний token")
	}
	var attempts int
	var nextAttempt string
	if err := store.db.QueryRowContext(ctx, `
		SELECT attempts,next_attempt_at FROM transport_inbox WHERE update_id=1`).
		Scan(&attempts, &nextAttempt); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || nextAttempt != originalNextAttempt {
		t.Fatalf("release изменил retry-состояние: attempts=%d next=%q", attempts, nextAttempt)
	}
}

func TestClaimTransportPacketsKeepsOneActiveHeadPerSender(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store,
		leasePacket(1, 101), leasePacket(2, 101),
		leasePacket(3, 202), leasePacket(4, 202),
	)
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 10, base, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 || first[0].Packet.UpdateID != 1 || first[1].Packet.UpdateID != 3 {
		t.Fatalf("выданы не головы двух независимых точек: %#v", first)
	}
	blocked, err := store.ClaimTransportPackets(ctx, 10, base.Add(time.Second), time.Minute)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("следующие пакеты активных полос выданы преждевременно: %#v err=%v", blocked, err)
	}
	if err := store.CompleteTransportPacketLease(ctx, first[0].Packet.UpdateID,
		first[0].Packet.ChatID, first[0].Packet.MessageID, first[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	next, err := store.ClaimTransportPackets(ctx, 10, base.Add(2*time.Second), time.Minute)
	if err != nil || len(next) != 1 || next[0].Packet.UpdateID != 2 {
		t.Fatalf("освободившаяся точка не продолжила независимо: %#v err=%v", next, err)
	}
}

func TestClaimTransportPacketsDoesNotOvertakeDeferredLaneHead(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101), leasePacket(2, 101))
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 1, base, time.Minute)
	if err != nil || len(first) != 1 || first[0].Packet.UpdateID != 1 {
		t.Fatalf("первая голова полосы: %#v err=%v", first, err)
	}
	retryAt := base.Add(5 * time.Minute)
	if err := store.MarkTransportRetryLease(
		ctx, first[0].Packet.UpdateID, retryAt, "временная ошибка", first[0].LeaseToken,
	); err != nil {
		t.Fatal(err)
	}
	blocked, err := store.ClaimTransportPackets(ctx, 1, base.Add(time.Minute), time.Minute)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("второй пакет обогнал отложенную голову: %#v err=%v", blocked, err)
	}
	retried, err := store.ClaimTransportPackets(ctx, 1, retryAt, time.Minute)
	if err != nil || len(retried) != 1 || retried[0].Packet.UpdateID != 1 {
		t.Fatalf("после срока выдана не прежняя голова: %#v err=%v", retried, err)
	}
	if err := store.CompleteTransportPacketLease(
		ctx, retried[0].Packet.UpdateID, retried[0].Packet.ChatID,
		retried[0].Packet.MessageID, retried[0].LeaseToken,
	); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimTransportPackets(ctx, 1, retryAt.Add(time.Second), time.Minute)
	if err != nil || len(second) != 1 || second[0].Packet.UpdateID != 2 {
		t.Fatalf("второй пакет не продолжил полосу после головы: %#v err=%v", second, err)
	}
}

func TestClaimTransportPacketsKeepsLegacyUnknownSendersInOneLane(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101), leasePacket(2, 101))
	if _, err := store.db.ExecContext(ctx, "UPDATE transport_inbox SET sender_bot_id=0"); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 1, base, time.Minute)
	if err != nil || len(first) != 1 || first[0].Packet.UpdateID != 1 {
		t.Fatalf("legacy-голова: %#v err=%v", first, err)
	}
	blocked, err := store.ClaimTransportPackets(ctx, 1, base.Add(time.Second), time.Minute)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("вторая legacy-строка вышла из общей полосы: %#v err=%v", blocked, err)
	}
	var storedSender int64
	if err := store.db.QueryRowContext(ctx,
		"SELECT sender_bot_id FROM transport_inbox WHERE update_id=1").Scan(&storedSender); err != nil {
		t.Fatal(err)
	}
	if storedSender != 0 {
		t.Fatalf("активная legacy-строка сменила полосу во время обработки: %d", storedSender)
	}
}

func TestRenewTransportPacketLeaseExtendsExclusiveOwnership(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	persistLeasePackets(t, ctx, store, leasePacket(1, 101))
	base := time.Now().UTC()
	first, err := store.ClaimTransportPackets(ctx, 1, base, time.Minute)
	if err != nil || len(first) != 1 {
		t.Fatalf("первая аренда: %#v err=%v", first, err)
	}
	renewedUntil, err := store.RenewTransportPacketLease(
		ctx, first[0].Packet.UpdateID, first[0].LeaseToken, base.Add(30*time.Second), 2*time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if want := base.Add(150 * time.Second); !renewedUntil.Equal(want) {
		t.Fatalf("срок продлён до %s вместо %s", renewedUntil, want)
	}
	blocked, err := store.ClaimTransportPackets(ctx, 1, base.Add(time.Minute), time.Minute)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("пакет повторно выдан после старого срока: %#v err=%v", blocked, err)
	}
	recovered, err := store.ClaimTransportPackets(ctx, 1, renewedUntil, time.Minute)
	if err != nil || len(recovered) != 1 {
		t.Fatalf("пакет не восстановился после продлённого срока: %#v err=%v", recovered, err)
	}
}

func TestClaimTransportPacketsReadsLegacyUnknownSenderWithoutChangingLane(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	packet := leasePacket(1, 101)
	persistLeasePackets(t, ctx, store, packet)
	if _, err := store.db.ExecContext(ctx,
		"UPDATE transport_inbox SET sender_bot_id=0 WHERE update_id=?", packet.UpdateID); err != nil {
		t.Fatal(err)
	}
	items, err := store.ClaimTransportPackets(ctx, 1, time.Now().UTC(), time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("legacy-пакет не выдан: %#v err=%v", items, err)
	}
	var senderBotID int64
	if err := store.db.QueryRowContext(ctx,
		"SELECT sender_bot_id FROM transport_inbox WHERE update_id=?", packet.UpdateID).Scan(&senderBotID); err != nil {
		t.Fatal(err)
	}
	if senderBotID != 0 {
		t.Fatalf("legacy-пакет сменил полосу во время обработки: %d", senderBotID)
	}
	if items[0].Packet.SenderBotID != packet.SenderBotID {
		t.Fatalf("sender_bot_id не прочитан из защищённого пакета: %d", items[0].Packet.SenderBotID)
	}
}

func TestBackfillTransportSenderBotIDsFallsBackAtomicallyOnUnreadableLegacyRow(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	first := leasePacket(1, 101)
	second := leasePacket(2, 202)
	persistLeasePackets(t, ctx, store, first, second)
	// Имитируем прерванный старый backfill: ранняя строка ещё legacy и
	// нечитаема, а более поздняя уже получила отдельную полосу.
	if _, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox
		SET sender_bot_id=0,packet_json=? WHERE update_id=?`, []byte("broken"), first.UpdateID); err != nil {
		t.Fatal(err)
	}
	if err := backfillTransportSenderBotIDs(ctx, store.db); err != nil {
		t.Fatal(err)
	}
	var nonLegacy int
	if err := store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM transport_inbox WHERE sender_bot_id<>0").Scan(&nonLegacy); err != nil {
		t.Fatal(err)
	}
	if nonLegacy != 0 {
		t.Fatalf("частичный backfill оставил параллельные полосы: %d", nonLegacy)
	}
}

func TestTransportLeaseRejectsForeignCompletionRetryAndQuarantine(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	packet := leasePacket(1, 101)
	persistLeasePackets(t, ctx, store, packet)
	items, err := store.ClaimTransportPackets(ctx, 1, time.Now().UTC(), time.Minute)
	if err != nil || len(items) != 1 {
		t.Fatalf("аренда: %#v %v", items, err)
	}
	if err := store.CompleteTransportPacketLease(
		ctx, packet.UpdateID, packet.ChatID, packet.MessageID, "foreign",
	); !errors.Is(err, ErrTransportLeaseLost) {
		t.Fatalf("чужое завершение не отклонено: %v", err)
	}
	if err := store.MarkTransportRetryLease(ctx, packet.UpdateID, time.Now().Add(time.Minute), "retry", "foreign"); !errors.Is(err, ErrTransportLeaseLost) {
		t.Fatalf("чужой retry не отклонён: %v", err)
	}
	if err := store.QuarantineTransportPacketLease(ctx, packet, "invalid", "foreign"); !errors.Is(err, ErrTransportLeaseLost) {
		t.Fatalf("чужой карантин не отклонён: %v", err)
	}
	var inbox, cleanup, rejected, attempts int
	_ = store.db.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(MAX(attempts),0) FROM transport_inbox").Scan(&inbox, &attempts)
	_ = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM transport_cleanup_scoped").Scan(&cleanup)
	_ = store.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM rejected_packets").Scan(&rejected)
	if inbox != 1 || attempts != 0 || cleanup != 0 || rejected != 0 {
		t.Fatalf("чужой token изменил данные: inbox=%d attempts=%d cleanup=%d rejected=%d",
			inbox, attempts, cleanup, rejected)
	}
	if err := store.CompleteTransportPacketLease(
		ctx, packet.UpdateID, packet.ChatID, packet.MessageID, items[0].LeaseToken,
	); err != nil {
		t.Fatalf("владелец не смог завершить пакет: %v", err)
	}
}

func TestServerStoreMigratesRc125TransportInboxWithoutBreakingRollback(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rc125 := []string{
		serverMigration, serverDeliveryOrderMigration, serverStreamRepairMigration,
		serverObservabilityMigration, serverIntegrityMigration, serverHistoryIndexesMigration,
		serverDeliveryResultMigration, serverTransportAuditMigration, serverTransportInboxMigration,
		serverRevisionBarrierMigration, serverShiftStateMigration, serverDeliveryDecodeRetryMigration,
	}
	if err := Migrate(ctx, db, rc125); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	packets := make([]application.IncomingPacket, 0, transportSenderBackfillBatchSize*2+1)
	for index := 0; index < transportSenderBackfillBatchSize*2+1; index++ {
		packet := leasePacket(int64(7+index), int64(707+index))
		plain, err := json.Marshal(packet)
		if err != nil {
			t.Fatal(err)
		}
		protected, err := protectDatabaseValue(plain)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO transport_inbox(
				update_id,chat_id,message_id,packet_json,attempts,next_attempt_at,last_error,created_at,decode_failures
			) VALUES(?,?,?,?,0,?,'',?,0)`,
			packet.UpdateID, packet.ChatID, packet.MessageID, protected, now, now); err != nil {
			t.Fatal(err)
		}
		packets = append(packets, packet)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO transport_cleanup(message_id,created_at,next_attempt_at,attempts)
		VALUES(909,?,?,1)`, now, now); err != nil {
		t.Fatal(err)
	}
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatalf("обновление базы rc125: %v", err)
	}
	for _, packet := range packets {
		var senderBotID int64
		var leaseToken, leaseUntil string
		if err := db.QueryRowContext(ctx, `
			SELECT sender_bot_id,lease_token,lease_until FROM transport_inbox WHERE update_id=?`,
			packet.UpdateID).Scan(&senderBotID, &leaseToken, &leaseUntil); err != nil {
			t.Fatal(err)
		}
		if senderBotID != packet.SenderBotID || leaseToken != "" || leaseUntil != "" {
			t.Fatalf("старая строка %d мигрировала неверно: sender=%d token=%q until=%q",
				packet.UpdateID, senderBotID, leaseToken, leaseUntil)
		}
	}
	items, err := store.ClaimTransportPackets(ctx, 1, time.Now().UTC(), time.Minute)
	if err != nil || len(items) != 1 || items[0].Packet.SenderBotID != packets[0].SenderBotID {
		t.Fatalf("старая строка не арендуется после обновления: %#v err=%v", items, err)
	}
	var legacyCleanupCount int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM transport_cleanup WHERE message_id=909").Scan(&legacyCleanupCount); err != nil {
		t.Fatal(err)
	}
	if legacyCleanupCount != 0 {
		t.Fatalf("неизвестная очистка rc125 осталась доступна старому runtime: %d", legacyCleanupCount)
	}
	due, err := store.DueTransportDeletions(ctx, packets[0].ChatID, time.Now().UTC().Add(time.Minute), 10)
	if err != nil || len(due) != 0 {
		t.Fatalf("очистка rc125 может удалить сообщение текущего чата без scope: ids=%v err=%v", due, err)
	}
	if err := store.EnqueueTransportDeletion(ctx, packets[0].ChatID, 910); err != nil {
		t.Fatal(err)
	}
	var scopedCleanupCount int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM transport_cleanup_scoped WHERE message_id=910").Scan(&scopedCleanupCount); err != nil {
		t.Fatal(err)
	}
	if scopedCleanupCount != 1 {
		t.Fatalf("scoped-очистка новой версии не сохранена: %d", scopedCleanupCount)
	}
	// Симуляция запуска мигратора rc125 после работы новой версии. Лишние
	// колонки и индекс не повышают версию, поэтому ErrMigrationTooNew нет.
	if err := Migrate(ctx, db, rc125); err != nil {
		t.Fatalf("rc125 не смог повторно открыть обновлённую базу: %v", err)
	}
	var newest int
	if err := db.QueryRowContext(ctx, "SELECT MAX(version) FROM schema_migrations").Scan(&newest); err != nil {
		t.Fatal(err)
	}
	if newest != len(rc125) {
		t.Fatalf("добавочная схема ошибочно повысила версию: %d", newest)
	}
	// Реальный rc125 читает только старую transport_cleanup. Даже при pending
	// scoped-удалении новой версии его запрос должен вернуть пустую очередь.
	var rc125VisibleCleanup int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM transport_cleanup WHERE next_attempt_at<=?", stamp(time.Now().UTC().Add(time.Hour))).
		Scan(&rc125VisibleCleanup); err != nil {
		t.Fatal(err)
	}
	if rc125VisibleCleanup != 0 {
		t.Fatalf("rc125 увидел scoped-очистку новой версии: %d", rc125VisibleCleanup)
	}
}

func TestEnqueueTransportDeletionIsIdempotentAndDoesNotCountAttempt(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	if err := store.EnqueueTransportDeletion(ctx, -1001, 77); err != nil {
		t.Fatal(err)
	}
	var created, due string
	var attempts int
	if err := store.db.QueryRowContext(ctx, `
		SELECT created_at,next_attempt_at,attempts FROM transport_cleanup_scoped WHERE chat_id=-1001 AND message_id=77`).
		Scan(&created, &due, &attempts); err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueTransportDeletion(ctx, -1001, 77); err != nil {
		t.Fatal(err)
	}
	var createdAgain, dueAgain string
	if err := store.db.QueryRowContext(ctx, `
		SELECT created_at,next_attempt_at,attempts FROM transport_cleanup_scoped WHERE chat_id=-1001 AND message_id=77`).
		Scan(&createdAgain, &dueAgain, &attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 || createdAgain != created || dueAgain != due {
		t.Fatalf("повторная постановка изменила очередь: attempts=%d created=%q/%q due=%q/%q",
			attempts, created, createdAgain, due, dueAgain)
	}
}

func TestTransportDeletionIsScopedToTelegramChat(t *testing.T) {
	t.Parallel()
	ctx, store := openTransportLeaseTestStore(t)
	now := time.Now().UTC()
	const messageID int64 = 77
	if err := store.EnqueueTransportDeletion(ctx, -1001, messageID); err != nil {
		t.Fatal(err)
	}
	wrongChat, err := store.DueTransportDeletions(ctx, -2002, now.Add(time.Minute), 10)
	if err != nil || len(wrongChat) != 0 {
		t.Fatalf("очистка старого чата попала в новый: ids=%v err=%v", wrongChat, err)
	}
	// Telegram message_id может совпасть в другой группе. Обе области должны
	// храниться независимо и завершаться только своим подтверждением.
	if err := store.EnqueueTransportDeletion(ctx, -2002, messageID); err != nil {
		t.Fatal(err)
	}
	oldChat, err := store.DueTransportDeletions(ctx, -1001, now.Add(time.Minute), 10)
	if err != nil || len(oldChat) != 1 || oldChat[0] != messageID {
		t.Fatalf("очистка старого чата потеряна: ids=%v err=%v", oldChat, err)
	}
	newChat, err := store.DueTransportDeletions(ctx, -2002, now.Add(time.Minute), 10)
	if err != nil || len(newChat) != 1 || newChat[0] != messageID {
		t.Fatalf("очистка нового чата потеряна: ids=%v err=%v", newChat, err)
	}
	if err := store.CompleteTransportDeletion(ctx, -1001, messageID); err != nil {
		t.Fatal(err)
	}
	stillDue, err := store.DueTransportDeletions(ctx, -2002, now.Add(time.Minute), 10)
	if err != nil || len(stillDue) != 1 {
		t.Fatalf("чужой чат завершил очистку: ids=%v err=%v", stillDue, err)
	}
	if err := store.CompleteTransportDeletion(ctx, -2002, messageID); err != nil {
		t.Fatal(err)
	}
}
