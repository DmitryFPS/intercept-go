package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

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
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO delivery_jobs(
				event_id, destination_id, delivery_json, stream_key, source_at,
				expires_at, state, next_attempt_at
			) VALUES(?, ?, ?, ?, ?, ?, 'pending', ?)`, event.ID, delivery.Destination.ID,
			deliveryPayload, deliveryStreamKey(delivery), sourceAt.Format(time.RFC3339Nano),
			expiresAt.Format(time.RFC3339Nano), now); err != nil {
			return false, fmt.Errorf("создать задание %s: %w", delivery.Destination.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить приём события: %w", err)
	}
	return true, nil
}

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
				SELECT 1 FROM delivery_jobs earlier
				WHERE earlier.state = 'pending' AND earlier.stream_key = j.stream_key
					AND (earlier.source_at < j.source_at OR
						(earlier.source_at = j.source_at AND earlier.event_id < j.event_id))
			)
		ORDER BY j.source_at, j.event_id, j.destination_id
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

func (store *ServerStore) MarkDelivered(ctx context.Context, eventID, destinationID string, at time.Time) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='delivered', delivered_at=?, last_error=''
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), eventID, destinationID)
	return changedExactlyOnce(result, err, "подтвердить внешнюю доставку")
}

func (store *ServerStore) MarkDeliveryExpired(
	ctx context.Context, eventID, destinationID string, at time.Time, reason string,
) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='expired', expired_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "завершить просроченную доставку")
}

func (store *ServerStore) MarkDeliveryRetry(ctx context.Context, eventID, destinationID string, next time.Time, reason string) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET attempts=attempts+1, next_attempt_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		next.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "запланировать внешнюю доставку")
}

func deliveryStreamKey(delivery domain.PlannedDelivery) string {
	if delivery.Destination.Kind == domain.DestinationTelegram {
		return fmt.Sprintf("%s:topic:%d", delivery.Destination.ID, delivery.Decision.ThreadID)
	}
	return delivery.Destination.ID
}
