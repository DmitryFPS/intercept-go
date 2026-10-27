package sqlite

import (
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
		SELECT packet_json,attempts FROM transport_inbox
		WHERE next_attempt_at<=?
		ORDER BY next_attempt_at,update_id LIMIT ?`, now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать локальную входную очередь: %w", err)
	}
	defer rows.Close()
	items := make([]TransportInboxItem, 0, limit)
	for rows.Next() {
		var protected []byte
		var item TransportInboxItem
		if err := rows.Scan(&protected, &item.Attempts); err != nil {
			return nil, err
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			return nil, fmt.Errorf("расшифровать локальный входной пакет: %w", err)
		}
		if err := json.Unmarshal(plain, &item.Packet); err != nil {
			return nil, fmt.Errorf("прочитать локальный входной пакет: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
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
