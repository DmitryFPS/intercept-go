package sqlite

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
)

const transportInboxErrorLimit = 2000

// Пакет до шифрования может содержать 4 MiB. Небольшая пачка удерживает память
// обновления rc125 в предсказуемых пределах даже при большой офлайн-очереди.
const transportSenderBackfillBatchSize = 4

var ErrTransportLeaseLost = errors.New("аренда входного пакета утрачена")

// TransportInboxItem — пакет, который уже не зависит от времени хранения в
// Telegram и ожидает проверки/подтверждения внутри Центра.
type TransportInboxItem struct {
	Packet     application.IncomingPacket
	Attempts   int
	LeaseToken string
	LeaseUntil time.Time
}

// ensureTransportInboxLeaseSchema добавляет только обратно совместимую схему.
// Изменение намеренно не записывается новой версией schema_migrations: rc125
// игнорирует добавочные колонки и отдельную scoped-очередь очистки.
func ensureTransportInboxLeaseSchema(ctx context.Context, db *sql.DB) error {
	columns, err := transportInboxColumns(ctx, db)
	if err != nil {
		return err
	}
	for _, item := range []struct {
		name string
		ddl  string
	}{
		{"sender_bot_id", "ALTER TABLE transport_inbox ADD COLUMN sender_bot_id INTEGER NOT NULL DEFAULT 0"},
		{"lease_token", "ALTER TABLE transport_inbox ADD COLUMN lease_token TEXT NOT NULL DEFAULT ''"},
		{"lease_until", "ALTER TABLE transport_inbox ADD COLUMN lease_until TEXT NOT NULL DEFAULT ''"},
	} {
		if columns[item.name] {
			continue
		}
		if _, err := db.ExecContext(ctx, item.ddl); err != nil {
			// Два одновременно стартовавших процесса могли увидеть старую схему.
			// Повторная проверка отличает этот безопасный случай от реальной ошибки.
			current, checkErr := transportInboxColumns(ctx, db)
			if checkErr != nil || !current[item.name] {
				return fmt.Errorf("добавить поле %s во входную очередь: %w", item.name, err)
			}
		}
		columns[item.name] = true
	}
	if _, err := db.ExecContext(ctx, `
		CREATE INDEX IF NOT EXISTS transport_inbox_sender_claim_idx
		ON transport_inbox(sender_bot_id, lease_token, julianday(lease_until), julianday(next_attempt_at), update_id)`); err != nil {
		return fmt.Errorf("создать индекс аренды входной очереди: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS transport_cleanup_scoped (
			chat_id INTEGER NOT NULL,
			message_id INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			next_attempt_at TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY(chat_id, message_id)
		);
		CREATE INDEX IF NOT EXISTS transport_cleanup_scoped_due_idx
		ON transport_cleanup_scoped(chat_id, next_attempt_at, message_id)`); err != nil {
		return fmt.Errorf("создать совместимую очередь очистки по чату: %w", err)
	}
	// rc125 читает старую таблицу без chat_id. Неизвестную область старых
	// записей безопаснее оставить неудалённой в Telegram, чем при откате
	// применить совпавший message_id к другой транспортной группе.
	if _, err := db.ExecContext(ctx, "DELETE FROM transport_cleanup"); err != nil {
		return fmt.Errorf("обезвредить старую очередь очистки перед возможным откатом: %w", err)
	}
	return nil
}

func transportInboxColumns(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, "PRAGMA table_info(transport_inbox)")
	if err != nil {
		return nil, fmt.Errorf("проверить схему transport_inbox: %w", err)
	}
	defer rows.Close()
	result := make(map[string]bool)
	for rows.Next() {
		var id, notNull, primaryKey int
		var name, kind string
		var defaultValue any
		if err := rows.Scan(&id, &name, &kind, &notNull, &defaultValue, &primaryKey); err != nil {
			return nil, err
		}
		result[name] = true
	}
	return result, rows.Err()
}

// backfillTransportSenderBotIDs восстанавливает ключ последовательной полосы
// для пакетов, которые уже находились в rc125 перед обновлением. Повреждённое
// или временно недоступное значение остаётся в безопасной общей полосе 0 и
// будет обработано обычной логикой повтора/карантина.
func backfillTransportSenderBotIDs(ctx context.Context, db *sql.DB) error {
	type stored struct {
		updateID int64
		payload  []byte
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать восстановление отправителей старой входной очереди: %w", err)
	}
	defer tx.Rollback()
	var hasLegacy int
	if err := tx.QueryRowContext(ctx,
		"SELECT EXISTS(SELECT 1 FROM transport_inbox WHERE sender_bot_id=0)").Scan(&hasLegacy); err != nil {
		return fmt.Errorf("проверить старую входную очередь: %w", err)
	}
	if hasLegacy == 0 {
		return nil
	}
	// Предыдущий запуск мог успеть заполнить только часть строк. Сначала
	// возвращаем весь pending backlog в одну полосу, затем атомарно назначаем
	// полосы заново. При одной нечитаемой строке вся очередь остаётся полосой 0.
	if _, err := tx.ExecContext(ctx, "UPDATE transport_inbox SET sender_bot_id=0"); err != nil {
		return fmt.Errorf("объединить старую входную очередь перед восстановлением: %w", err)
	}
	var cursor int64
	hasCursor := false
	fallbackToSingleLane := false

backfillBatches:
	for {
		query := `
			SELECT update_id,packet_json FROM transport_inbox
			WHERE sender_bot_id=0 ORDER BY update_id LIMIT ?`
		args := []any{transportSenderBackfillBatchSize}
		if hasCursor {
			query = `
				SELECT update_id,packet_json FROM transport_inbox
				WHERE sender_bot_id=0 AND update_id>? ORDER BY update_id LIMIT ?`
			args = []any{cursor, transportSenderBackfillBatchSize}
		}
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("прочитать отправителей старой входной очереди: %w", err)
		}
		values := make([]stored, 0, transportSenderBackfillBatchSize)
		for rows.Next() {
			var value stored
			if err := rows.Scan(&value.updateID, &value.payload); err != nil {
				rows.Close()
				return err
			}
			values = append(values, value)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("прочитать пачку отправителей старой входной очереди: %w", err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if len(values) == 0 {
			break
		}
		cursor, hasCursor = values[len(values)-1].updateID, true
		for _, value := range values {
			plain, err := unprotectDatabaseValue(value.payload)
			if err != nil {
				fallbackToSingleLane = true
				break backfillBatches
			}
			var packet application.IncomingPacket
			if err := json.Unmarshal(plain, &packet); err != nil || packet.UpdateID != value.updateID || packet.SenderBotID == 0 {
				fallbackToSingleLane = true
				break backfillBatches
			}
			if _, err := tx.ExecContext(ctx, `
				UPDATE transport_inbox SET sender_bot_id=?
				WHERE update_id=? AND sender_bot_id=0`, packet.SenderBotID, value.updateID); err != nil {
				return fmt.Errorf("восстановить отправителя пакета %d: %w", value.updateID, err)
			}
		}
	}
	if fallbackToSingleLane {
		if _, err := tx.ExecContext(ctx, "UPDATE transport_inbox SET sender_bot_id=0"); err != nil {
			return fmt.Errorf("сохранить безопасную общую полосу старой очереди: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать восстановление отправителей старой очереди: %w", err)
	}
	return nil
}

// PersistTransportBatch атомарно сохраняет все скачанные пакеты и только
// после этого продвигает Telegram offset. При любой ошибке не фиксируется ни
// один пакет и смещение остаётся прежним.
func (store *ServerStore) PersistTransportBatch(
	ctx context.Context, offsetName string, nextOffset int64, packets []application.IncomingPacket,
) error {
	if strings.TrimSpace(offsetName) == "" || nextOffset < 0 {
		return errors.New("некорректное смещение входного транспорта")
	}
	type protectedPacket struct {
		packet  application.IncomingPacket
		payload []byte
	}
	prepared := make([]protectedPacket, 0, len(packets))
	for _, packet := range packets {
		if packet.UpdateID < 0 || packet.ChatID == 0 || packet.MessageID <= 0 || len(packet.Payload) == 0 {
			return fmt.Errorf("некорректный входной пакет update_id=%d", packet.UpdateID)
		}
		if packet.UpdateID >= nextOffset {
			return fmt.Errorf("пакет update_id=%d выходит за безопасное смещение %d", packet.UpdateID, nextOffset)
		}
		plain, err := json.Marshal(packet)
		if err != nil {
			return fmt.Errorf("сериализовать входной пакет: %w", err)
		}
		protected, err := protectDatabaseValue(plain)
		if err != nil {
			return fmt.Errorf("защитить входной пакет: %w", err)
		}
		prepared = append(prepared, protectedPacket{packet: packet, payload: protected})
	}

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать сохранение входного транспорта: %w", err)
	}
	defer tx.Rollback()
	var currentRaw string
	err = tx.QueryRowContext(ctx, "SELECT value FROM server_values WHERE name=?", offsetName).Scan(&currentRaw)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("прочитать смещение входного транспорта: %w", err)
	}
	if err == nil {
		current, parseErr := strconv.ParseInt(currentRaw, 10, 64)
		if parseErr != nil {
			return fmt.Errorf("сохранённое смещение входного транспорта повреждено: %w", parseErr)
		}
		if nextOffset < current {
			return fmt.Errorf("отказ от отката смещения входного транспорта с %d до %d", current, nextOffset)
		}
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, item := range prepared {
		_, err = tx.ExecContext(ctx, `
			INSERT OR IGNORE INTO transport_inbox(
				update_id,chat_id,message_id,packet_json,next_attempt_at,created_at,sender_bot_id
			) VALUES(?,?,?,?,?,?,?)`,
			item.packet.UpdateID, item.packet.ChatID, item.packet.MessageID,
			item.payload, now, now, item.packet.SenderBotID,
		)
		if err != nil {
			return fmt.Errorf("сохранить входной пакет %d: %w", item.packet.UpdateID, err)
		}
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO server_values(name,value) VALUES(?,?)
		ON CONFLICT(name) DO UPDATE SET value=excluded.value`, offsetName, strconv.FormatInt(nextOffset, 10)); err != nil {
		return fmt.Errorf("сохранить смещение входного транспорта: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать входной транспорт: %w", err)
	}
	return nil
}

// ClaimTransportPackets атомарно резервирует максимум одну готовую запись на
// sender_bot_id. Пока аренда полосы активна, следующая запись той же точки не
// выдаётся другому worker; независимые точки могут обрабатываться параллельно.
func (store *ServerStore) ClaimTransportPackets(
	ctx context.Context, limit int, now time.Time, leaseDuration time.Duration,
) ([]TransportInboxItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	if leaseDuration <= 0 {
		return nil, errors.New("срок аренды входного пакета должен быть положительным")
	}
	token, err := newTransportLeaseToken()
	if err != nil {
		return nil, err
	}
	now = now.UTC()
	leaseUntil := now.Add(leaseDuration)
	nowStamp := now.Format(time.RFC3339Nano)
	nowKey := sqliteNanoTimeKey(now)

	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("начать аренду входных пакетов: %w", err)
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `
		WITH lane_heads AS (
			SELECT i.update_id,i.sender_bot_id,i.next_attempt_at,i.lease_token,i.lease_until,
				ROW_NUMBER() OVER (
					PARTITION BY i.sender_bot_id
					ORDER BY i.update_id
				) AS lane_rank
			FROM transport_inbox i
		), ready AS (
			SELECT head.update_id,head.next_attempt_at
			FROM lane_heads head
			WHERE head.lane_rank=1
				AND julianday(head.next_attempt_at)<=julianday(?)
				AND `+sqliteNanoTimeExpr("head.next_attempt_at")+`<=?
				AND (head.lease_token='' OR `+sqliteNanoTimeExpr("head.lease_until")+`<=?)
				AND NOT EXISTS(
					SELECT 1 FROM transport_inbox active
					WHERE active.sender_bot_id=head.sender_bot_id
						AND active.lease_token<>''
						AND `+sqliteNanoTimeExpr("active.lease_until")+`>?
				)
		)
		SELECT update_id FROM ready
		ORDER BY julianday(next_attempt_at),`+sqliteNanoTimeExpr("next_attempt_at")+`,update_id
		LIMIT ?`, nowStamp, nowKey, nowKey, nowKey, limit)
	if err != nil {
		return nil, fmt.Errorf("выбрать входные пакеты для аренды: %w", err)
	}
	var updateIDs []int64
	for rows.Next() {
		var updateID int64
		if err := rows.Scan(&updateID); err != nil {
			rows.Close()
			return nil, err
		}
		updateIDs = append(updateIDs, updateID)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if len(updateIDs) == 0 {
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("завершить пустую аренду входных пакетов: %w", err)
		}
		return nil, nil
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(updateIDs)), ",")
	args := make([]any, 0, len(updateIDs)+3)
	args = append(args, token, leaseUntil.Format(time.RFC3339Nano))
	for _, updateID := range updateIDs {
		args = append(args, updateID)
	}
	args = append(args, nowKey)
	result, err := tx.ExecContext(ctx, `
		UPDATE transport_inbox SET lease_token=?,lease_until=?
		WHERE update_id IN (`+placeholders+`)
			AND (lease_token='' OR `+sqliteNanoTimeExpr("lease_until")+`<=?)`, args...)
	if err != nil {
		return nil, fmt.Errorf("закрепить аренду входных пакетов: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if changed < 0 || changed > int64(len(updateIDs)) {
		return nil, fmt.Errorf("некорректно закреплено %d из %d входных пакетов", changed, len(updateIDs))
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("зафиксировать аренду входных пакетов: %w", err)
	}
	items, err := store.claimedTransportPackets(ctx, token, leaseUntil)
	if err != nil {
		// Аренда уже зафиксирована. Ошибка чтения или отмена исходного context
		// не должна удерживать полосу до истечения lease после штатной остановки.
		store.releaseTransportClaim(token)
		return nil, err
	}
	return items, nil
}

func (store *ServerStore) releaseTransportClaim(token string) {
	if strings.TrimSpace(token) == "" {
		return
	}
	releaseContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := store.db.ExecContext(releaseContext, `
		UPDATE transport_inbox SET lease_token='',lease_until='' WHERE lease_token=?`, token); err != nil && store.logger != nil {
		store.logger.Warn("не удалось освободить аренду после ошибки чтения входной очереди", "error", err)
	}
}

func newTransportLeaseToken() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("создать token аренды входного пакета: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func (store *ServerStore) claimedTransportPackets(
	ctx context.Context, token string, leaseUntil time.Time,
) ([]TransportInboxItem, error) {
	rows, err := store.db.QueryContext(ctx, `
		SELECT update_id,chat_id,message_id,sender_bot_id,packet_json,attempts,decode_failures
		FROM transport_inbox WHERE lease_token=?
		ORDER BY julianday(next_attempt_at),`+sqliteNanoTimeExpr("next_attempt_at")+`,update_id`, token)
	if err != nil {
		return nil, fmt.Errorf("прочитать арендованные входные пакеты: %w", err)
	}
	type storedPacket struct {
		updateID, chatID, messageID, senderBotID int64
		protected                                []byte
		attempts, decodeFailures                 int
	}
	var stored []storedPacket
	for rows.Next() {
		var value storedPacket
		if err := rows.Scan(&value.updateID, &value.chatID, &value.messageID, &value.senderBotID,
			&value.protected, &value.attempts, &value.decodeFailures); err != nil {
			rows.Close()
			return nil, err
		}
		stored = append(stored, value)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	items := make([]TransportInboxItem, 0, len(stored))
	for _, value := range stored {
		plain, decodeErr := store.unprotectPendingValue(value.protected)
		if decodeErr != nil && bytes.HasPrefix(value.protected, protectedPrefix) {
			if err := store.retryUnreadableTransportLease(ctx, value.updateID, value.decodeFailures,
				time.Now().UTC(), decodeErr, token); err != nil {
				return nil, err
			}
			continue
		}
		var packet application.IncomingPacket
		if decodeErr == nil {
			decodeErr = json.Unmarshal(plain, &packet)
		}
		if decodeErr == nil && (packet.UpdateID != value.updateID || packet.ChatID != value.chatID ||
			packet.MessageID != value.messageID ||
			(value.senderBotID != 0 && packet.SenderBotID != value.senderBotID)) {
			decodeErr = errors.New("идентификаторы пакета не совпадают с локальной очередью")
		}
		if decodeErr != nil {
			reason := "повреждён локальный входной пакет: " + decodeErr.Error()
			if err := store.quarantineCorruptTransportRowLease(ctx, value.updateID, value.chatID,
				value.messageID, value.protected, reason, token); err != nil {
				return nil, err
			}
			continue
		}
		if value.decodeFailures > 0 {
			result, err := store.db.ExecContext(ctx, `
				UPDATE transport_inbox SET decode_failures=0,last_error=''
				WHERE update_id=? AND lease_token=? AND decode_failures>0`, value.updateID, token)
			if err != nil {
				return nil, fmt.Errorf("сбросить ошибки чтения арендованного пакета: %w", err)
			}
			if changed, _ := result.RowsAffected(); changed > 0 && store.logger != nil {
				store.logger.Info("Восстановлено чтение входного пакета", "update_id", value.updateID,
					"message_id", value.messageID, "decode_failures", value.decodeFailures)
			}
		}
		items = append(items, TransportInboxItem{
			Packet: packet, Attempts: value.attempts, LeaseToken: token, LeaseUntil: leaseUntil,
		})
	}
	return items, nil
}

// PendingTransportPackets читает независимые готовые элементы. Пакет с
// отложенным повтором не удерживает более новые пакеты.
func (store *ServerStore) PendingTransportPackets(
	ctx context.Context, limit int, now time.Time,
) ([]TransportInboxItem, error) {
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT update_id,chat_id,message_id,packet_json,attempts,decode_failures FROM transport_inbox
		WHERE julianday(next_attempt_at)<=julianday(?)
			AND `+sqliteNanoTimeExpr("next_attempt_at")+` <= ?
			AND (lease_token='' OR `+sqliteNanoTimeExpr("lease_until")+` <= ?)
		ORDER BY julianday(next_attempt_at),update_id LIMIT ?`,
		now.UTC().Format(time.RFC3339Nano), sqliteNanoTimeKey(now), sqliteNanoTimeKey(now), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать локальную входную очередь: %w", err)
	}
	type storedPacket struct {
		updateID, chatID, messageID int64
		protected                   []byte
		attempts, decodeFailures    int
	}
	stored := make([]storedPacket, 0, limit)
	for rows.Next() {
		var value storedPacket
		if err := rows.Scan(&value.updateID, &value.chatID, &value.messageID, &value.protected, &value.attempts, &value.decodeFailures); err != nil {
			rows.Close()
			return nil, err
		}
		stored = append(stored, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	items := make([]TransportInboxItem, 0, len(stored))
	for _, value := range stored {
		plain, decodeErr := store.unprotectPendingValue(value.protected)
		if decodeErr != nil && bytes.HasPrefix(value.protected, protectedPrefix) {
			if err := store.retryUnreadableTransport(ctx, value.updateID, value.decodeFailures, now, decodeErr); err != nil {
				return nil, err
			}
			continue
		}
		var packet application.IncomingPacket
		if decodeErr == nil {
			decodeErr = json.Unmarshal(plain, &packet)
		}
		if decodeErr == nil && (packet.UpdateID != value.updateID || packet.ChatID != value.chatID || packet.MessageID != value.messageID) {
			decodeErr = errors.New("идентификаторы пакета не совпадают с локальной очередью")
		}
		if decodeErr != nil {
			reason := "повреждён локальный входной пакет: " + decodeErr.Error()
			if err := store.quarantineCorruptTransportRow(ctx, value.updateID, value.chatID, value.messageID, value.protected, reason); err != nil {
				return nil, err
			}
			continue
		}
		if value.decodeFailures > 0 {
			result, err := store.db.ExecContext(ctx, `
				UPDATE transport_inbox SET decode_failures=0,last_error=''
				WHERE update_id=? AND decode_failures>0`, value.updateID)
			if err != nil {
				return nil, fmt.Errorf("сбросить ошибки чтения входного пакета: %w", err)
			}
			if changed, _ := result.RowsAffected(); changed > 0 && store.logger != nil {
				store.logger.Info("Восстановлено чтение входного пакета", "update_id", value.updateID,
					"message_id", value.messageID, "decode_failures", value.decodeFailures)
			}
		}
		items = append(items, TransportInboxItem{Packet: packet, Attempts: value.attempts})
	}
	return items, nil
}

// Входной пакет уже сохранён локально, поэтому временный сбой DPAPI не должен
// удалять ни его, ни транспортное сообщение Telegram. Другие пакеты продолжают
// обрабатываться, а этот повторяется с ограниченной частотой до восстановления.
func (store *ServerStore) retryUnreadableTransport(
	ctx context.Context, updateID int64, previousFailures int, now time.Time, decodeErr error,
) error {
	failures := previousFailures + 1
	delay := deliveryDecodeBackoff(failures)
	if failures > 5 {
		delay = 2 * time.Minute
	}
	reason := fmt.Sprintf("Ошибка расшифрования входного пакета (попытка %d): %v", failures, decodeErr)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox SET decode_failures=decode_failures+1,next_attempt_at=?,last_error=?
		WHERE update_id=? AND decode_failures=?`,
		now.Add(delay).UTC().Format(time.RFC3339Nano), reason, updateID, previousFailures)
	if err != nil {
		return fmt.Errorf("повторить чтение входного пакета: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed > 0 && store.logger != nil && (failures == 1 || failures%20 == 0) {
		store.logger.Warn("Временная ошибка чтения входного пакета", "update_id", updateID,
			"decode_failures", failures, "retry_after", delay.String(), "error", decodeErr)
	}
	return nil
}

func (store *ServerStore) retryUnreadableTransportLease(
	ctx context.Context, updateID int64, previousFailures int, now time.Time, decodeErr error, leaseToken string,
) error {
	failures := previousFailures + 1
	delay := deliveryDecodeBackoff(failures)
	if failures > 5 {
		delay = 2 * time.Minute
	}
	reason := fmt.Sprintf("Ошибка расшифрования входного пакета (попытка %d): %v", failures, decodeErr)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox
		SET decode_failures=decode_failures+1,next_attempt_at=?,last_error=?,lease_token='',lease_until=''
		WHERE update_id=? AND decode_failures=? AND lease_token=?`,
		now.Add(delay).UTC().Format(time.RFC3339Nano), reason, updateID, previousFailures, leaseToken)
	if err != nil {
		return fmt.Errorf("повторить чтение арендованного входного пакета: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	if store.logger != nil && (failures == 1 || failures%20 == 0) {
		store.logger.Warn("Временная ошибка чтения входного пакета", "update_id", updateID,
			"decode_failures", failures, "retry_after", delay.String(), "error", decodeErr)
	}
	return nil
}

func (store *ServerStore) quarantineCorruptTransportRow(
	ctx context.Context, updateID, chatID, messageID int64, protected []byte, reason string,
) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	quarantinePayload, err := protectDatabaseValue(protected)
	if err != nil {
		return fmt.Errorf("защитить повреждённый входной пакет для карантина: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать изоляцию повреждённого входного пакета: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id,sender_bot_username,chat_id,thread_id,reason,payload,received_at
		) VALUES(0,'',?,0,?,?,?)`, chatID, reason, quarantinePayload,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("сохранить повреждённый входной пакет в карантин: %w", err)
	}
	if err = trimQuarantine(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM transport_inbox WHERE update_id=?", updateID); err != nil {
		return fmt.Errorf("удалить повреждённый входной пакет из рабочей очереди: %w", err)
	}
	if messageID > 0 {
		if err = queueTransportDeletionTx(ctx, tx, chatID, messageID); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать изоляцию повреждённого входного пакета: %w", err)
	}
	return nil
}

func (store *ServerStore) quarantineCorruptTransportRowLease(
	ctx context.Context, updateID, chatID, messageID int64, protected []byte, reason, leaseToken string,
) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	quarantinePayload, err := protectDatabaseValue(protected)
	if err != nil {
		return fmt.Errorf("защитить повреждённый входной пакет для карантина: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать изоляцию арендованного входного пакета: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id,sender_bot_username,chat_id,thread_id,reason,payload,received_at
		) VALUES(0,'',?,0,?,?,?)`, chatID, reason, quarantinePayload,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("сохранить повреждённый входной пакет в карантин: %w", err)
	}
	if err = trimQuarantine(ctx, tx); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx,
		"DELETE FROM transport_inbox WHERE update_id=? AND lease_token=?", updateID, leaseToken)
	if err != nil {
		return fmt.Errorf("удалить повреждённый арендованный пакет: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	if messageID > 0 {
		if err = queueTransportDeletionTx(ctx, tx, chatID, messageID); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать изоляцию арендованного входного пакета: %w", err)
	}
	return nil
}

func (store *ServerStore) MarkTransportRetry(
	ctx context.Context, updateID int64, next time.Time, reason string,
) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox
		SET attempts=attempts+1,next_attempt_at=?,last_error=? WHERE update_id=?`,
		next.UTC().Format(time.RFC3339Nano), reason, updateID)
	if err != nil {
		return fmt.Errorf("отложить входной пакет: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("входной пакет %d не найден", updateID)
	}
	return nil
}

// MarkTransportRetryLease переносит повтор и одновременно освобождает полосу.
// Чужой или уже заменённый token не может изменить пакет нового worker.
func (store *ServerStore) MarkTransportRetryLease(
	ctx context.Context, updateID int64, next time.Time, reason, leaseToken string,
) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > transportInboxErrorLimit {
		reason = reason[:transportInboxErrorLimit]
	}
	if strings.TrimSpace(leaseToken) == "" {
		return fmt.Errorf("%w: пустой token", ErrTransportLeaseLost)
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox
		SET attempts=attempts+1,next_attempt_at=?,last_error=?,lease_token='',lease_until=''
		WHERE update_id=? AND lease_token=?`,
		next.UTC().Format(time.RFC3339Nano), reason, updateID, leaseToken)
	if err != nil {
		return fmt.Errorf("отложить арендованный входной пакет: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	return nil
}

// RenewTransportPacketLease продлевает аренду длительной обработки. Продлить
// запись после её повторной выдачи другому worker невозможно.
func (store *ServerStore) RenewTransportPacketLease(
	ctx context.Context, updateID int64, leaseToken string, now time.Time, leaseDuration time.Duration,
) (time.Time, error) {
	if strings.TrimSpace(leaseToken) == "" || leaseDuration <= 0 {
		return time.Time{}, fmt.Errorf("%w: некорректное продление", ErrTransportLeaseLost)
	}
	until := now.UTC().Add(leaseDuration)
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox SET lease_until=? WHERE update_id=? AND lease_token=?`,
		until.Format(time.RFC3339Nano), updateID, leaseToken)
	if err != nil {
		return time.Time{}, fmt.Errorf("продлить аренду входного пакета: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return time.Time{}, err
	}
	if changed != 1 {
		return time.Time{}, fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	return until, nil
}

// ReleaseTransportPacketLease освобождает пакет при штатной остановке worker,
// не считая отмену ошибкой обработки и не откладывая следующую попытку.
func (store *ServerStore) ReleaseTransportPacketLease(
	ctx context.Context, updateID int64, leaseToken string,
) error {
	if strings.TrimSpace(leaseToken) == "" {
		return fmt.Errorf("%w: пустой token", ErrTransportLeaseLost)
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE transport_inbox SET lease_token='',lease_until=''
		WHERE update_id=? AND lease_token=?`, updateID, leaseToken)
	if err != nil {
		return fmt.Errorf("освободить аренду входного пакета: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	return nil
}

func (store *ServerStore) CompleteTransportPacket(ctx context.Context, updateID, chatID, messageID int64) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать завершение входного пакета: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM transport_inbox WHERE update_id=?", updateID); err != nil {
		return fmt.Errorf("завершить входной пакет: %w", err)
	}
	if err = queueTransportDeletionTx(ctx, tx, chatID, messageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать завершение входного пакета: %w", err)
	}
	return nil
}

// CompleteTransportPacketLease атомарно завершает только пакет текущего
// владельца и ставит Telegram-файл в долговечную очередь очистки.
func (store *ServerStore) CompleteTransportPacketLease(
	ctx context.Context, updateID, chatID, messageID int64, leaseToken string,
) error {
	if strings.TrimSpace(leaseToken) == "" {
		return fmt.Errorf("%w: пустой token", ErrTransportLeaseLost)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать завершение арендованного пакета: %w", err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		DELETE FROM transport_inbox WHERE update_id=? AND message_id=? AND lease_token=?`,
		updateID, messageID, leaseToken)
	if err != nil {
		return fmt.Errorf("завершить арендованный входной пакет: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, updateID)
	}
	if err = queueTransportDeletionTx(ctx, tx, chatID, messageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать завершение арендованного пакета: %w", err)
	}
	return nil
}

// QuarantineTransportPacket не оставляет окно между записью причины и
// удалением из входной очереди. После перезапуска один и тот же пакет не
// размножается в карантине.
func (store *ServerStore) QuarantineTransportPacket(
	ctx context.Context, packet application.IncomingPacket, reason string,
) error {
	protectedPayload, err := protectDatabaseValue(packet.Payload)
	if err != nil {
		return fmt.Errorf("защитить пакет карантина: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать перенос входного пакета в карантин: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id,sender_bot_username,chat_id,thread_id,reason,payload,received_at
		) VALUES(?,?,?,?,?,?,?)`, packet.SenderBotID, packet.SenderBotUsername,
		packet.ChatID, packet.ThreadID, reason, protectedPayload,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("сохранить пакет в карантин: %w", err)
	}
	if err = trimQuarantine(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM transport_inbox WHERE update_id=?", packet.UpdateID); err != nil {
		return fmt.Errorf("завершить входной пакет после карантина: %w", err)
	}
	if err = queueTransportDeletionTx(ctx, tx, packet.ChatID, packet.MessageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать карантин входного пакета: %w", err)
	}
	return nil
}

// QuarantineTransportPacketLease переносит пакет в карантин только при
// совпадении аренды. Ошибка token откатывает и запись rejected_packets.
func (store *ServerStore) QuarantineTransportPacketLease(
	ctx context.Context, packet application.IncomingPacket, reason, leaseToken string,
) error {
	if strings.TrimSpace(leaseToken) == "" {
		return fmt.Errorf("%w: пустой token", ErrTransportLeaseLost)
	}
	protectedPayload, err := protectDatabaseValue(packet.Payload)
	if err != nil {
		return fmt.Errorf("защитить пакет карантина: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать перенос арендованного пакета в карантин: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO rejected_packets(
			sender_bot_id,sender_bot_username,chat_id,thread_id,reason,payload,received_at
		) VALUES(?,?,?,?,?,?,?)`, packet.SenderBotID, packet.SenderBotUsername,
		packet.ChatID, packet.ThreadID, reason, protectedPayload,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("сохранить арендованный пакет в карантин: %w", err)
	}
	if err = trimQuarantine(ctx, tx); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		DELETE FROM transport_inbox
		WHERE update_id=? AND message_id=? AND lease_token=?`,
		packet.UpdateID, packet.MessageID, leaseToken)
	if err != nil {
		return fmt.Errorf("завершить арендованный пакет после карантина: %w", err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return fmt.Errorf("%w: update_id=%d", ErrTransportLeaseLost, packet.UpdateID)
	}
	if err = queueTransportDeletionTx(ctx, tx, packet.ChatID, packet.MessageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать карантин арендованного пакета: %w", err)
	}
	return nil
}

func queueTransportDeletionTx(ctx context.Context, tx *sql.Tx, chatID, messageID int64) error {
	if chatID == 0 || messageID <= 0 {
		return errors.New("не заданы чат или транспортное сообщение для очистки")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO transport_cleanup_scoped(chat_id,message_id,created_at,next_attempt_at,attempts)
		VALUES(?,?,?,?,0)
		ON CONFLICT(chat_id,message_id) DO NOTHING`, chatID, messageID, now, now)
	if err != nil {
		return fmt.Errorf("поставить транспортное сообщение в очередь очистки: %w", err)
	}
	return nil
}
