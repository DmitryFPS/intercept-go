package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"intercept/internal/domain"
)

type sourceRevisionBarrier struct {
	ClientID      string
	SourceUUID    string
	SourceVersion string
	SourceEventAt time.Time
	ContentDigest string
	EventID       string
	Route         []domain.PlannedDelivery
}

// backfillSourceRevisionBarriers выполняется один раз после миграции rc90.
// История намеренно читается по rowid ASC: при полностью одинаковой ревизии
// действует first-wins, а более высокая исходная ревизия заменяет старую вне
// зависимости от порядка прихода транспортных сообщений.
func backfillSourceRevisionBarriers(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("начать миграцию барьеров ревизий: %w", err)
	}
	defer tx.Rollback()
	var existing int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM source_revision_barriers").Scan(&existing); err != nil {
		return fmt.Errorf("проверить барьеры ревизий: %w", err)
	}
	if existing > 0 {
		return tx.Commit()
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT client_id, source_uuid, payload_json
		FROM received_events ORDER BY rowid ASC`)
	if err != nil {
		return fmt.Errorf("прочитать историю для барьеров ревизий: %w", err)
	}
	barriers := make(map[string]sourceRevisionBarrier)
	for rows.Next() {
		var clientID, sourceUUID string
		var protected []byte
		if err := rows.Scan(&clientID, &sourceUUID, &protected); err != nil {
			rows.Close()
			return fmt.Errorf("прочитать событие для барьера ревизий: %w", err)
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			rows.Close()
			return fmt.Errorf("расшифровать событие для барьера ревизий: %w", err)
		}
		var event domain.Event
		if err := json.Unmarshal(plain, &event); err != nil {
			rows.Close()
			return errors.New("история события повреждена при миграции барьеров")
		}
		// Старые версии ещё не защищали монотонный барьер от ошибочных часов.
		// Такая запись остаётся в истории для диагностики, но не получает права
		// навсегда остановить последующие корректные редакции.
		if err := domain.ValidateRevisionClock(event, time.Now().UTC()); err != nil {
			continue
		}
		key := clientID + "\x00" + sourceUUID
		current, exists := barriers[key]
		if exists && domain.CompareSourceRevision(
			event.CanonicalSourceVersion(), event.CreatedAt, current.SourceVersion, current.SourceEventAt,
		) <= 0 {
			continue
		}
		barriers[key] = sourceRevisionBarrier{
			ClientID: clientID, SourceUUID: sourceUUID,
			SourceVersion: event.CanonicalSourceVersion(), SourceEventAt: event.CreatedAt,
			ContentDigest: event.ContentDigest, EventID: event.ID,
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("перебрать историю для барьеров ревизий: %w", err)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for key, barrier := range barriers {
		route, err := loadBarrierRouteFromLegacyJobs(ctx, tx, barrier.EventID)
		if err != nil {
			return err
		}
		barrier.Route = route
		barriers[key] = barrier
		if err := saveSourceRevisionBarrier(ctx, tx, barrier); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("зафиксировать миграцию барьеров ревизий: %w", err)
	}
	return nil
}

func loadSourceRevisionBarrier(
	ctx context.Context, tx *sql.Tx, clientID, sourceUUID string,
) (sourceRevisionBarrier, bool, error) {
	var barrier sourceRevisionBarrier
	var createdAt string
	var protectedRoute []byte
	err := tx.QueryRowContext(ctx, `
		SELECT source_version, source_event_at, content_digest, event_id, route_json
		FROM source_revision_barriers WHERE client_id=? AND source_uuid=?`,
		clientID, sourceUUID,
	).Scan(
		&barrier.SourceVersion, &createdAt, &barrier.ContentDigest, &barrier.EventID, &protectedRoute,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return barrier, false, nil
	}
	if err != nil {
		return barrier, false, fmt.Errorf("прочитать барьер ревизии: %w", err)
	}
	barrier.ClientID = clientID
	barrier.SourceUUID = sourceUUID
	barrier.SourceEventAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return barrier, false, errors.New("время барьера ревизии повреждено")
	}
	plainRoute, err := unprotectDatabaseValue(protectedRoute)
	if err != nil {
		return barrier, false, fmt.Errorf("прочитать маршрут барьера ревизии: %w", err)
	}
	if err := json.Unmarshal(plainRoute, &barrier.Route); err != nil {
		return barrier, false, errors.New("маршрут барьера ревизии повреждён")
	}
	return barrier, true, nil
}

func saveSourceRevisionBarrier(ctx context.Context, tx *sql.Tx, barrier sourceRevisionBarrier) error {
	routeJSON, err := json.Marshal(barrier.Route)
	if err != nil {
		return fmt.Errorf("подготовить маршрут барьера ревизии: %w", err)
	}
	protectedRoute, err := protectDatabaseValue(routeJSON)
	if err != nil {
		return fmt.Errorf("защитить маршрут барьера ревизии: %w", err)
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO source_revision_barriers(
			client_id, source_uuid, source_version, source_event_at, content_digest, event_id, route_json
		) VALUES(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(client_id, source_uuid) DO UPDATE SET
			source_version=excluded.source_version,
			source_event_at=excluded.source_event_at,
			content_digest=excluded.content_digest,
			event_id=excluded.event_id,
			route_json=excluded.route_json`,
		barrier.ClientID, barrier.SourceUUID, barrier.SourceVersion,
		barrier.SourceEventAt.UTC().Format(time.RFC3339Nano),
		barrier.ContentDigest, barrier.EventID, protectedRoute,
	)
	if err != nil {
		return fmt.Errorf("сохранить барьер ревизии: %w", err)
	}
	return nil
}

func loadBarrierRouteFromLegacyJobs(
	ctx context.Context, tx *sql.Tx, eventID string,
) ([]domain.PlannedDelivery, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT delivery_json FROM delivery_jobs
		WHERE event_id=? ORDER BY destination_id`, eventID)
	if err != nil {
		return nil, fmt.Errorf("прочитать маршрут старого барьера ревизии: %w", err)
	}
	defer rows.Close()
	var route []domain.PlannedDelivery
	for rows.Next() {
		var protected []byte
		if err := rows.Scan(&protected); err != nil {
			return nil, err
		}
		plain, err := unprotectDatabaseValue(protected)
		if err != nil {
			return nil, err
		}
		var delivery domain.PlannedDelivery
		if err := json.Unmarshal(plain, &delivery); err != nil {
			return nil, errors.New("старый маршрут барьера ревизии повреждён")
		}
		if delivery.Decision.Action != domain.RouteSkip {
			route = append(route, delivery)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return route, nil
}
