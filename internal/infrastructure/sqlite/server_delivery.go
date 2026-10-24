package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

// KnownEvent проверяет, был ли пакет уже надёжно принят. Совпавший UUID с
// другим содержимым считается конфликтом, а не повторной доставкой.
func (store *ServerStore) KnownEvent(ctx context.Context, eventID, contentDigest string) (bool, error) {
	var existingDigest string
	err := store.db.QueryRowContext(ctx,
		"SELECT content_digest FROM received_events WHERE event_id = ?", eventID,
	).Scan(&existingDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("проверить принятое событие: %w", err)
	}
	if existingDigest != contentDigest {
		return false, errors.New("конфликт: UUID события уже принят с другим содержимым")
	}
	return true, nil
}

func backfillServerStreamKeys(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, `
		SELECT j.event_id, j.destination_id, e.payload_json, j.delivery_json
		FROM delivery_jobs j JOIN received_events e ON e.event_id=j.event_id
		WHERE j.stream_key=''`)
	if err != nil {
		return fmt.Errorf("прочитать старые очереди доставки: %w", err)
	}
	type row struct {
		eventID, destinationID        string
		eventPayload, deliveryPayload []byte
	}
	var values []row
	for rows.Next() {
		var value row
		if err := rows.Scan(&value.eventID, &value.destinationID, &value.eventPayload, &value.deliveryPayload); err != nil {
			rows.Close()
			return err
		}
		values = append(values, value)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if len(values) == 0 {
		return nil
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, value := range values {
		eventPlain, err := unprotectDatabaseValue(value.eventPayload)
		if err != nil {
			return err
		}
		deliveryPlain, err := unprotectDatabaseValue(value.deliveryPayload)
		if err != nil {
			return err
		}
		var event domain.Event
		var delivery domain.PlannedDelivery
		if err := json.Unmarshal(eventPlain, &event); err != nil {
			return fmt.Errorf("разобрать старое событие %s: %w", value.eventID, err)
		}
		if err := json.Unmarshal(deliveryPlain, &delivery); err != nil {
			return fmt.Errorf("разобрать старое задание %s: %w", value.eventID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE delivery_jobs SET stream_key=? WHERE event_id=? AND destination_id=?`,
			deliveryStreamKey(event, delivery), value.eventID, value.destinationID,
		); err != nil {
			return fmt.Errorf("восстановить очередь задания %s: %w", value.eventID, err)
		}
	}
	return tx.Commit()
}

// Accept атомарно сохраняет событие и все задания, рассчитанные маршрутизатором.
func (store *ServerStore) Accept(ctx context.Context, event domain.Event, plan domain.DeliveryPlan) (bool, error) {
	eventPayload, err := json.Marshal(event)
	if err != nil {
		return false, fmt.Errorf("кодировать событие: %w", err)
	}
	eventPayload, err = protectDatabaseValue(eventPayload)
	if err != nil {
		return false, fmt.Errorf("защитить событие: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("начать приём события: %w", err)
	}
	defer tx.Rollback()
	var existingDigest string
	err = tx.QueryRowContext(ctx,
		"SELECT content_digest FROM received_events WHERE event_id = ?", event.ID,
	).Scan(&existingDigest)
	if err == nil {
		if existingDigest != event.ContentDigest {
			return false, errors.New("конфликт: UUID события уже принят с другим содержимым")
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("проверить повтор события: %w", err)
	}
	plan, err = addRemovedRouteCleanup(ctx, tx, event, plan)
	if err != nil {
		return false, err
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO received_events(
			event_id, client_id, source_uuid, content_digest, payload_json, rule_version, received_at
		) VALUES(?, ?, ?, ?, ?, ?, ?)`, event.ID, event.ClientID, event.Source.UUID,
		event.ContentDigest, eventPayload, plan.RuleSetVersion, now); err != nil {
		return false, fmt.Errorf("сохранить входное событие: %w", err)
	}
	for _, delivery := range plan.Deliveries {
		deliveryPayload, err := json.Marshal(delivery)
		if err != nil {
			return false, fmt.Errorf("кодировать задание доставки: %w", err)
		}
		deliveryPayload, err = protectDatabaseValue(deliveryPayload)
		if err != nil {
			return false, fmt.Errorf("защитить задание доставки: %w", err)
		}
		sourceAt := event.SourceAt.UTC()
		if sourceAt.IsZero() {
			sourceAt = event.CreatedAt.UTC()
		}
		expiresAt := event.ExpiresAt.UTC()
		if expiresAt.IsZero() {
			expiresAt = sourceAt.Add(30 * time.Minute)
		}
		// Корректирующая очистка удаляет уже опубликованный результат и не
		// может просрочиться как обычная первая доставка. Иначе после длительного
		// сбоя старый Telegram/DOCX остался бы в запрещённом маршруте.
		if delivery.RuleLevel == "cleanup" {
			expiresAt = time.Date(9999, time.December, 31, 23, 59, 59, 0, time.UTC)
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_jobs(
				event_id, destination_id, delivery_json, stream_key, source_at,
				expires_at, state, next_attempt_at
			) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)`, event.ID, delivery.Destination.ID,
			deliveryPayload, deliveryStreamKey(event, delivery), sourceAt.Format(time.RFC3339Nano),
			expiresAt.Format(time.RFC3339Nano), now); err != nil {
			return false, fmt.Errorf("создать задание %s: %w", delivery.Destination.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить приём события: %w", err)
	}
	return true, nil
}

// addRemovedRouteCleanup добавляет задания удаления для направлений, которые
// были у предыдущей версии той же записи EAVC, но исчезли после исправления
// подразделения или маршрута. Так переход корректной позиции в ошибочную не
// оставляет старый DOCX или сообщение в прежней теме.
func addRemovedRouteCleanup(
	ctx context.Context, tx *sql.Tx, event domain.Event, plan domain.DeliveryPlan,
) (domain.DeliveryPlan, error) {
	sourceUUID := strings.TrimSpace(event.Source.UUID)
	if sourceUUID == "" {
		return plan, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT j.delivery_json
		FROM delivery_jobs j
		JOIN received_events e ON e.event_id=j.event_id
		WHERE e.client_id=? AND e.source_uuid=?
			AND e.rowid=(
				SELECT MAX(previous.rowid) FROM received_events previous
				WHERE previous.client_id=? AND previous.source_uuid=?
			)
		ORDER BY j.destination_id`, event.ClientID, sourceUUID, event.ClientID, sourceUUID)
	if err != nil {
		return plan, fmt.Errorf("прочитать предыдущий маршрут записи: %w", err)
	}
	defer rows.Close()
	current := make(map[string]bool, len(plan.Deliveries))
	for _, delivery := range plan.Deliveries {
		current[delivery.Destination.ID] = true
	}
	for rows.Next() {
		var protected []byte
		if err := rows.Scan(&protected); err != nil {
			return plan, fmt.Errorf("прочитать предыдущее направление: %w", err)
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			return plan, err
		}
		var previous domain.PlannedDelivery
		if err := json.Unmarshal(plain, &previous); err != nil {
			return plan, errors.New("предыдущее задание доставки повреждено")
		}
		if current[previous.Destination.ID] {
			continue
		}
		previous.Decision = domain.RouteDecision{Action: domain.RouteSkip}
		previous.RuleLevel = "cleanup"
		plan.Deliveries = append(plan.Deliveries, previous)
		current[previous.Destination.ID] = true
	}
	if err := rows.Err(); err != nil {
		return plan, fmt.Errorf("прочитать предыдущий маршрут записи: %w", err)
	}
	return plan, nil
}

// PendingDeliveries выбирает головы независимых очередей конечных получателей.
func (store *ServerStore) PendingDeliveries(ctx context.Context, limit int, now time.Time) ([]application.DeliveryJob, error) {
	if limit <= 0 || limit > 1000 {
		return nil, errors.New("лимит заданий должен быть от 1 до 1000")
	}
	if _, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET state='expired', expired_at=?, last_error=?
		WHERE state='pending' AND expires_at < ?`,
		now.UTC().Format(time.RFC3339Nano), domain.FreshnessExpired,
		now.UTC().Format(time.RFC3339Nano)); err != nil {
		return nil, fmt.Errorf("завершить просроченные доставки: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT e.payload_json, j.delivery_json, j.attempts
		FROM delivery_jobs j
		JOIN received_events e ON e.event_id = j.event_id
		WHERE j.state = 'pending' AND j.next_attempt_at <= ? AND j.expires_at >= ?
			AND NOT EXISTS (
				SELECT 1
				FROM delivery_jobs earlier
				JOIN received_events earlier_event ON earlier_event.event_id = earlier.event_id
				WHERE earlier.state = 'pending'
					AND earlier_event.rowid < e.rowid
					AND (
						earlier.stream_key = j.stream_key
						OR (
							earlier.destination_id = j.destination_id
							AND earlier_event.client_id = e.client_id
							AND earlier_event.source_uuid = e.source_uuid
						)
					)
			)
		ORDER BY e.rowid, j.destination_id
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), now.UTC().Format(time.RFC3339Nano), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать очередь доставки: %w", err)
	}
	defer rows.Close()
	var jobs []application.DeliveryJob
	for rows.Next() {
		var eventPayload, deliveryPayload []byte
		var job application.DeliveryJob
		if err := rows.Scan(&eventPayload, &deliveryPayload, &job.Attempts); err != nil {
			return nil, fmt.Errorf("прочитать задание доставки: %w", err)
		}
		eventPlain, err := unprotectDatabaseValue(eventPayload)
		if err != nil {
			return nil, err
		}
		deliveryPlain, err := unprotectDatabaseValue(deliveryPayload)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(eventPlain, &job.Event); err != nil {
			return nil, errors.New("сохранённое событие повреждено")
		}
		if err := json.Unmarshal(deliveryPlain, &job.Delivery); err != nil {
			return nil, errors.New("сохранённое задание повреждено")
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// MarkDelivered окончательно фиксирует успешную доставку одному получателю.
func (store *ServerStore) MarkDelivered(ctx context.Context, eventID, destinationID string, at time.Time) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='delivered', delivered_at=?, last_error=''
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), eventID, destinationID)
	return changedExactlyOnce(result, err, "подтвердить внешнюю доставку")
}

// MarkDeliveryExpired завершает направление после истечения серверного окна.
func (store *ServerStore) MarkDeliveryExpired(
	ctx context.Context, eventID, destinationID string, at time.Time, reason string,
) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='expired', expired_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "завершить просроченную доставку")
}

// MarkDeliveryRetry сохраняет ошибку отдельного направления и время повтора.
func (store *ServerStore) MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET attempts=attempts+1, next_attempt_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "запланировать внешнюю доставку")
}

// PendingDestinationCount сообщает, есть ли у получателя задания, созданные
// ещё для его текущих реквизитов. Пока они не завершены, менять адрес
// получателя или удалять его нельзя: иначе пакет уйдёт не по тому маршруту.
func (store *ServerStore) PendingDestinationCount(ctx context.Context, destinationID string) (int, error) {
	var count int
	if err := store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM delivery_jobs WHERE destination_id=? AND state='pending'",
		destinationID,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("посчитать очередь получателя: %w", err)
	}
	return count, nil
}

// DestinationDeliveryCount нужен для защиты идентичности получателя. После первой
// доставки один ID нельзя переназначать другому Telegram-чату или Word-папке:
// иначе ранее созданные результаты станут неуправляемыми.
func (store *ServerStore) DestinationDeliveryCount(ctx context.Context, destinationID string) (int, error) {
	var count int
	if err := store.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM delivery_jobs WHERE destination_id=?",
		destinationID,
	).Scan(&count); err != nil {
		return 0, fmt.Errorf("посчитать историю получателя: %w", err)
	}
	return count, nil
}

func deliveryStreamKey(event domain.Event, delivery domain.PlannedDelivery) string {
	if delivery.Destination.Kind == domain.DestinationTelegram {
		return fmt.Sprintf("%s:topic:%d", delivery.Destination.ID, delivery.Decision.ThreadID)
	}
	position := "name:" + domain.NormalizeUnitName(domain.RoutingUnitName(event.Position.UnitName))
	if catalogUUID := strings.TrimSpace(event.Position.CatalogUUID); catalogUUID != "" {
		position = "catalog:" + strings.ToLower(catalogUUID)
	}
	return delivery.Destination.ID + ":position:" + position
}
