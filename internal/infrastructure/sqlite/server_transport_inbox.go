package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"intercept/internal/application"
)

const transportInboxErrorLimit = 2000

// TransportInboxItem — пакет, который уже не зависит от времени хранения в
// Telegram и ожидает проверки/подтверждения внутри Центра.
type TransportInboxItem struct {
	Packet   application.IncomingPacket
	Attempts int
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
				update_id,chat_id,message_id,packet_json,next_attempt_at,created_at
			) VALUES(?,?,?,?,?,?)`,
			item.packet.UpdateID, item.packet.ChatID, item.packet.MessageID,
			item.payload, now, now,
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
		ORDER BY julianday(next_attempt_at),update_id LIMIT ?`,
		now.UTC().Format(time.RFC3339Nano), sqliteNanoTimeKey(now), limit)
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
		if err = queueTransportDeletionTx(ctx, tx, messageID); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать изоляцию повреждённого входного пакета: %w", err)
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

func (store *ServerStore) CompleteTransportPacket(ctx context.Context, updateID, messageID int64) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать завершение входного пакета: %w", err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM transport_inbox WHERE update_id=?", updateID); err != nil {
		return fmt.Errorf("завершить входной пакет: %w", err)
	}
	if err = queueTransportDeletionTx(ctx, tx, messageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать завершение входного пакета: %w", err)
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
	if err = queueTransportDeletionTx(ctx, tx, packet.MessageID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать карантин входного пакета: %w", err)
	}
	return nil
}

func queueTransportDeletionTx(ctx context.Context, tx *sql.Tx, messageID int64) error {
	if messageID <= 0 {
		return errors.New("не задано транспортное сообщение для очистки")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := tx.ExecContext(ctx, `
		INSERT INTO transport_cleanup(message_id,created_at,next_attempt_at,attempts)
		VALUES(?,?,?,0)
		ON CONFLICT(message_id) DO NOTHING`, messageID, now, now)
	if err != nil {
		return fmt.Errorf("поставить транспортное сообщение в очередь очистки: %w", err)
	}
	return nil
}
