package sqlite

import (
	"bytes"
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

func (store *ServerStore) unprotectPendingValue(value []byte) ([]byte, error) {
	if store.pendingUnprotect != nil {
		return store.pendingUnprotect(value)
	}
	return unprotectDatabaseValue(value)
}

// Барьеры не удаляются вместе с журналом: иначе старый пакет смог бы снова
// создать публикацию. Жёсткий предел превращает бесконечный рост от
// скомпрометированной точки в явный карантин, не ослабляя replay-защиту.
const maxSourceLineagesPerClient = 200000

// KnownEvent проверяет, был ли пакет уже надёжно принят. Совпавший UUID с
// другим содержимым считается конфликтом, а не повторной доставкой.
func (store *ServerStore) KnownEvent(ctx context.Context, eventID, clientID, contentDigest string) (bool, error) {
	var existingClientID, existingDigest string
	err := store.db.QueryRowContext(ctx,
		"SELECT client_id,content_digest FROM received_events WHERE event_id = ?", eventID,
	).Scan(&existingClientID, &existingDigest)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("проверить принятое событие: %w", err)
	}
	if existingClientID != clientID || existingDigest != contentDigest {
		return false, application.ErrEventIDConflict
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
	var existingClientID, existingDigest string
	err = tx.QueryRowContext(ctx,
		"SELECT client_id,content_digest FROM received_events WHERE event_id = ?", event.ID,
	).Scan(&existingClientID, &existingDigest)
	if err == nil {
		if existingClientID != event.ClientID || existingDigest != event.ContentDigest {
			return false, application.ErrEventIDConflict
		}
		return false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("проверить повтор события: %w", err)
	}
	// Telegram может доставить служебные пакеты не в порядке их создания.
	// Барьер сравнивает каноническую исходную ревизию, а CreatedAt разрешает
	// несколько реальных правок в одну секунду. Полностью совпавшее состояние
	// только продвигает барьер и повторно не создаёт задания доставки.
	barrier, hasBarrier, err := loadSourceRevisionBarrier(
		ctx, tx, event.ClientID, event.SourceLineageKey(),
	)
	if err != nil {
		return false, err
	}
	if hasBarrier {
		sameDigest := strings.EqualFold(
			strings.TrimSpace(event.ContentDigest), strings.TrimSpace(barrier.ContentDigest),
		)
		order := domain.CompareSourceRevision(
			event.CanonicalSourceVersion(), event.CreatedAt, barrier.SourceVersion, barrier.SourceEventAt,
		)
		if order < 0 {
			return false, nil
		}
		if sameDigest {
			if order == 0 {
				return false, nil
			}
			barrier.SourceVersion = event.CanonicalSourceVersion()
			barrier.SourceEventAt = event.CreatedAt
			if err := saveSourceRevisionBarrier(ctx, tx, barrier); err != nil {
				return false, err
			}
			if err := tx.Commit(); err != nil {
				return false, fmt.Errorf("продвинуть барьер повторного события: %w", err)
			}
			return false, nil
		}
		if order == 0 {
			return false, application.ErrSourceRevisionConflict
		}
	} else {
		var lineages int
		if err := tx.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM source_revision_barriers WHERE client_id=?`,
			event.ClientID,
		).Scan(&lineages); err != nil {
			return false, fmt.Errorf("проверить предел линий источника: %w", err)
		}
		limit := store.maxSourceLineages
		if limit <= 0 {
			limit = maxSourceLineagesPerClient
		}
		if lineages >= limit {
			return false, application.ErrSourceLineageLimit
		}
	}
	// Сохраняем тему прежней публикации, только если тот же Telegram-получатель
	// и его основной маршрут остаются действующими. Снятие получателя создаёт
	// задание очистки, а явное изменение маршрута вступает в силу.
	positionKey := event.PositionKey()
	currentRedirected := false
	for index := range plan.Deliveries {
		plan.Deliveries[index].SourcePositionKey = positionKey
		currentRedirected = currentRedirected || plan.Deliveries[index].TopicRedirected
	}
	// Правка уже принятого перехвата с тем же исходным названием не должна
	// переезжать в другой маршрут из-за последующей правки/отмены привязки.
	// Изменение самого названия в EAVC, напротив, рассчитывается заново.
	if hasBarrier && len(barrier.Route) > 0 && barrier.Route[0].RuleLevel == "alias" {
		previousPositionKey, err := sourcePositionKeyForBarrier(ctx, tx, barrier)
		if err != nil {
			return false, err
		}
		currentNameKey := "name:" + domain.NormalizeUnitName(domain.RoutingUnitName(event.Position.UnitName))
		if previousPositionKey == positionKey && barrier.Route[0].SourceUnitNameKey == currentNameKey {
			plan.Deliveries = append([]domain.PlannedDelivery(nil), barrier.Route...)
			for index := range plan.Deliveries {
				plan.Deliveries[index].SourcePositionKey = positionKey
			}
			currentRedirected = false
			for _, delivery := range plan.Deliveries {
				currentRedirected = currentRedirected || delivery.TopicRedirected
			}
		}
	}
	previousRedirected := false
	for _, delivery := range barrier.Route {
		previousRedirected = previousRedirected || delivery.TopicRedirected
	}
	if hasBarrier && (currentRedirected || previousRedirected) {
		previousPositionKey, err := sourcePositionKeyForBarrier(ctx, tx, barrier)
		if err != nil {
			return false, err
		}
		if previousPositionKey == positionKey {
			previousTelegram := make(map[string]domain.PlannedDelivery)
			for _, delivery := range barrier.Route {
				if delivery.Destination.Kind == domain.DestinationTelegram && delivery.Decision.Action != domain.RouteSkip {
					previousTelegram[delivery.Destination.ID] = delivery
				}
			}
			for index, current := range plan.Deliveries {
				if current.Destination.Kind != domain.DestinationTelegram ||
					!current.Destination.Enabled || current.Decision.Action == domain.RouteSkip {
					continue
				}
				previous, exists := previousTelegram[current.Destination.ID]
				if !exists || !(current.TopicRedirected || previous.TopicRedirected) {
					continue
				}
				base := telegramBaseDecision(current)
				previousBase := telegramBaseDecision(previous)
				if base.Action != previousBase.Action || base.ThreadID != previousBase.ThreadID {
					continue
				}
				current.Decision.ThreadID = previous.Decision.ThreadID
				current.TopicRedirected = true
				current.BaseDecision = &base
				plan.Deliveries[index] = current
			}
		}
	}
	// В барьере хранится именно активный маршрут этой редакции. Задания cleanup,
	// добавленные ниже, не должны становиться маршрутом следующей версии.
	activeRoute := append([]domain.PlannedDelivery(nil), plan.Deliveries...)
	plan, err = addRemovedRouteCleanup(event, plan, barrier, hasBarrier)
	if err != nil {
		return false, err
	}
	nowTime := time.Now().UTC()
	now := nowTime.Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO received_events(
			event_id, client_id, source_uuid, content_digest, payload_json, rule_version, received_at
		) VALUES(?, ?, ?, ?, ?, ?, ?)`, event.ID, event.ClientID, event.SourceLineageKey(),
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
	if err := saveSourceRevisionBarrier(ctx, tx, sourceRevisionBarrier{
		ClientID: event.ClientID, SourceUUID: event.SourceLineageKey(),
		SourceVersion: event.CanonicalSourceVersion(), SourceEventAt: event.CreatedAt,
		ContentDigest: event.ContentDigest, EventID: event.ID, Route: activeRoute,
	}); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("завершить приём события: %w", err)
	}
	return true, nil
}

func telegramBaseDecision(delivery domain.PlannedDelivery) domain.RouteDecision {
	if delivery.BaseDecision != nil {
		return *delivery.BaseDecision
	}
	return delivery.Decision
}

func sourcePositionKeyForBarrier(
	ctx context.Context, tx *sql.Tx, barrier sourceRevisionBarrier,
) (string, error) {
	for _, delivery := range barrier.Route {
		if delivery.SourcePositionKey != "" {
			return delivery.SourcePositionKey, nil
		}
	}
	// Старый барьер не содержал позицию. Читаем её из принятого события,
	// пока оно есть в истории; после очистки истории не делаем предположений.
	var protected []byte
	err := tx.QueryRowContext(ctx,
		"SELECT payload_json FROM received_events WHERE event_id=?", barrier.EventID,
	).Scan(&protected)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("прочитать предыдущую позицию перехвата: %w", err)
	}
	plain, err := unprotectDatabaseValue(protected)
	if err != nil {
		return "", fmt.Errorf("расшифровать предыдущую позицию перехвата: %w", err)
	}
	var previous domain.Event
	if err := json.Unmarshal(plain, &previous); err != nil {
		return "", errors.New("предыдущая позиция перехвата повреждена")
	}
	return previous.PositionKey(), nil
}

// addRemovedRouteCleanup добавляет задания удаления для направлений, которые
// были у предыдущей версии той же записи EAVC, но исчезли после исправления
// подразделения или маршрута. Так переход корректной позиции в ошибочную не
// оставляет старый DOCX или сообщение в прежней теме.
func addRemovedRouteCleanup(
	event domain.Event, plan domain.DeliveryPlan, previous sourceRevisionBarrier, hasPrevious bool,
) (domain.DeliveryPlan, error) {
	sourceUUID := event.SourceLineageKey()
	if sourceUUID == "" || !hasPrevious {
		return plan, nil
	}
	current := make(map[string]bool, len(plan.Deliveries))
	for _, delivery := range plan.Deliveries {
		current[delivery.Destination.ID] = true
	}
	for _, delivery := range previous.Route {
		if current[delivery.Destination.ID] {
			continue
		}
		delivery.Decision = domain.RouteDecision{Action: domain.RouteSkip}
		delivery.RuleLevel = "cleanup"
		plan.Deliveries = append(plan.Deliveries, delivery)
		current[delivery.Destination.ID] = true
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
		SET state='expired', expired_at=?,
			last_error=CASE WHEN decode_failures>0
				THEN 'Срок доставки истёк после ошибок расшифрования: ' || last_error
				ELSE ? END
		WHERE state='pending' AND julianday(expires_at) <= julianday(?)
			AND `+sqliteNanoTimeExpr("expires_at")+` < ?`,
		now.UTC().Format(time.RFC3339Nano), domain.FreshnessExpired,
		now.UTC().Format(time.RFC3339Nano), sqliteNanoTimeKey(now)); err != nil {
		return nil, fmt.Errorf("завершить просроченные доставки: %w", err)
	}
	rows, err := store.db.QueryContext(ctx, `
		SELECT j.event_id, j.destination_id, e.payload_json, j.delivery_json,
			j.attempts, j.decode_failures, j.expires_at
		FROM delivery_jobs j
		JOIN received_events e ON e.event_id = j.event_id
		WHERE j.state = 'pending' AND julianday(j.next_attempt_at) <= julianday(?)
			AND `+sqliteNanoTimeExpr("j.next_attempt_at")+` <= ?
			AND julianday(j.expires_at) >= julianday(?)
			AND `+sqliteNanoTimeExpr("j.expires_at")+` >= ?
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
		LIMIT ?`, now.UTC().Format(time.RFC3339Nano), sqliteNanoTimeKey(now),
		now.UTC().Format(time.RFC3339Nano), sqliteNanoTimeKey(now), limit)
	if err != nil {
		return nil, fmt.Errorf("прочитать очередь доставки: %w", err)
	}
	type storedDelivery struct {
		eventID, destinationID        string
		expiresAt                     string
		eventPayload, deliveryPayload []byte
		attempts, decodeFailures      int
	}
	stored := make([]storedDelivery, 0, limit)
	for rows.Next() {
		var value storedDelivery
		if err := rows.Scan(&value.eventID, &value.destinationID, &value.eventPayload,
			&value.deliveryPayload, &value.attempts, &value.decodeFailures, &value.expiresAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("прочитать задание доставки: %w", err)
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
	jobs := make([]application.DeliveryJob, 0, len(stored))
	for _, value := range stored {
		var job application.DeliveryJob
		job.Attempts = value.attempts
		eventPlain, decodeErr := store.unprotectPendingValue(value.eventPayload)
		eventCorrupt := decodeErr != nil
		if decodeErr != nil && bytes.HasPrefix(value.eventPayload, protectedPrefix) {
			if err := store.retryUnreadableDelivery(ctx, value.eventID, value.destinationID,
				value.decodeFailures, value.expiresAt, now, "события", decodeErr); err != nil {
				return nil, err
			}
			continue
		}
		if decodeErr == nil {
			decodeErr = json.Unmarshal(eventPlain, &job.Event)
			eventCorrupt = decodeErr != nil
		}
		if decodeErr == nil {
			var deliveryPlain []byte
			deliveryPlain, decodeErr = store.unprotectPendingValue(value.deliveryPayload)
			if decodeErr != nil && bytes.HasPrefix(value.deliveryPayload, protectedPrefix) {
				if err := store.retryUnreadableDelivery(ctx, value.eventID, value.destinationID,
					value.decodeFailures, value.expiresAt, now, "маршрута", decodeErr); err != nil {
					return nil, err
				}
				continue
			}
			if decodeErr == nil {
				decodeErr = json.Unmarshal(deliveryPlain, &job.Delivery)
			}
		}
		if decodeErr == nil && (job.Event.ID != value.eventID || job.Delivery.Destination.ID != value.destinationID) {
			decodeErr = errors.New("идентификаторы задания не совпадают с локальной очередью")
		}
		if decodeErr != nil {
			reason := "повреждено локальное задание доставки: " + decodeErr.Error()
			if err := store.quarantineCorruptDelivery(ctx, value.eventID, value.destinationID, eventCorrupt, now, reason); err != nil {
				return nil, err
			}
			continue
		}
		if value.decodeFailures > 0 {
			result, err := store.db.ExecContext(ctx, `
				UPDATE delivery_jobs SET decode_failures=0,last_error=''
				WHERE event_id=? AND destination_id=? AND state='pending' AND decode_failures>0`,
				value.eventID, value.destinationID)
			if err != nil {
				return nil, fmt.Errorf("сбросить ошибки чтения задания: %w", err)
			}
			if changed, _ := result.RowsAffected(); changed > 0 && store.logger != nil {
				store.logger.Info("Восстановлено чтение задания доставки", "event_id", value.eventID,
					"destination_id", value.destinationID, "decode_failures", value.decodeFailures)
			}
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

// Ошибка DPAPI ещё не доказывает порчу сохранённых данных: Центр может
// прочитать тот же BLOB при следующем проходе. Повторы продолжаются до
// существующего срока доставки, но другие независимые потоки не задерживаются.
func (store *ServerStore) retryUnreadableDelivery(
	ctx context.Context, eventID, destinationID string, previousFailures int, expiresAt string, now time.Time,
	phase string, decodeErr error,
) error {
	failures := previousFailures + 1
	// Корректирующее удаление нельзя завершать по таймеру: иначе ошибочная
	// публикация навсегда останется снаружи. После длительного сбоя нужен
	// сигнал оператору, но само задание и порядок его потока сохраняются.
	if strings.HasPrefix(expiresAt, "9999-") && failures >= 60 && failures%60 == 0 && store.logger != nil {
		store.logger.Error("Требует вмешательства: бессрочное задание не расшифровывается",
			"event_id", eventID, "destination_id", destinationID,
			"decode_failures", failures, "error", decodeErr)
	}
	reason := fmt.Sprintf("Ошибка расшифрования %s (попытка %d): %v", phase, failures, decodeErr)
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	delay := deliveryDecodeBackoff(failures)
	if strings.HasPrefix(expiresAt, "9999-") && failures > 5 {
		delay = 2 * time.Minute
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET decode_failures=decode_failures+1,next_attempt_at=?,last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending' AND decode_failures=?`,
		now.Add(delay).UTC().Format(time.RFC3339Nano), reason,
		eventID, destinationID, previousFailures)
	if err != nil {
		return fmt.Errorf("повторить чтение задания доставки: %w", err)
	}
	if changed, _ := result.RowsAffected(); changed > 0 && store.logger != nil && (failures == 1 || failures%20 == 0) {
		store.logger.Warn("Временная ошибка чтения задания доставки", "event_id", eventID,
			"destination_id", destinationID, "phase", phase, "decode_failures", failures,
			"retry_after", delay.String(), "error", decodeErr)
	}
	return nil
}

func deliveryDecodeBackoff(failures int) time.Duration {
	switch failures {
	case 1:
		return 2 * time.Second
	case 2:
		return 4 * time.Second
	case 3:
		return 8 * time.Second
	case 4:
		return 16 * time.Second
	default:
		return 30 * time.Second
	}
}

func (store *ServerStore) quarantineCorruptDelivery(
	ctx context.Context, eventID, destinationID string, entireEvent bool, at time.Time, reason string,
) error {
	reason = strings.TrimSpace(reason)
	if len(reason) > 2000 {
		reason = reason[:2000]
	}
	query := `UPDATE delivery_jobs SET state='expired',expired_at=?,last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`
	args := []any{at.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID}
	if entireEvent {
		query = `UPDATE delivery_jobs SET state='expired',expired_at=?,last_error=?
			WHERE event_id=? AND state='pending'`
		args = []any{at.UTC().Format(time.RFC3339Nano), reason, eventID}
	}
	if _, err := store.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("изолировать повреждённое задание доставки: %w", err)
	}
	return nil
}

// DeliveryJobByID загружает ровно одно ранее сохранённое задание для явного
// операторского действия. Метод не меняет состояние очереди.
func (store *ServerStore) DeliveryJobByID(ctx context.Context, eventID, destinationID string) (application.DeliveryJob, error) {
	var eventPayload, deliveryPayload []byte
	var job application.DeliveryJob
	err := store.db.QueryRowContext(ctx, `
		SELECT e.payload_json, j.delivery_json, j.attempts
		FROM delivery_jobs j JOIN received_events e ON e.event_id=j.event_id
		WHERE j.event_id=? AND j.destination_id=?`, eventID, destinationID,
	).Scan(&eventPayload, &deliveryPayload, &job.Attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return job, errors.New("доставка не найдена")
	}
	if err != nil {
		return job, fmt.Errorf("прочитать доставку: %w", err)
	}
	eventPlain, err := unprotectDatabaseValue(eventPayload)
	if err != nil {
		return job, err
	}
	deliveryPlain, err := unprotectDatabaseValue(deliveryPayload)
	if err != nil {
		return job, err
	}
	if err := json.Unmarshal(eventPlain, &job.Event); err != nil {
		return job, errors.New("сохранённое событие повреждено")
	}
	if err := json.Unmarshal(deliveryPlain, &job.Delivery); err != nil {
		return job, errors.New("сохранённое задание повреждено")
	}
	return job, nil
}

// CurrentDeliveryJobForRepublish связывает историческую карточку с последней
// принятой редакцией того же блока через устойчивый барьер ревизий. Старый
// текст карточки никогда не становится новой публикацией Telegram.
func (store *ServerStore) CurrentDeliveryJobForRepublish(
	ctx context.Context, eventID, destinationID string,
) (application.DeliveryJob, error) {
	var currentEventID string
	err := store.db.QueryRowContext(ctx, `
		SELECT current.event_id
		FROM delivery_jobs selected_job
		JOIN received_events selected ON selected.event_id=selected_job.event_id
		JOIN source_revision_barriers current
			ON current.client_id=selected.client_id AND current.source_uuid=selected.source_uuid
		WHERE selected_job.event_id=? AND selected_job.destination_id=?`,
		eventID, destinationID,
	).Scan(&currentEventID)
	if errors.Is(err, sql.ErrNoRows) {
		return application.DeliveryJob{}, errors.New("выбранная доставка или актуальная редакция перехвата не найдена")
	}
	if err != nil {
		return application.DeliveryJob{}, fmt.Errorf("найти актуальную редакцию перехвата: %w", err)
	}
	job, err := store.DeliveryJobByID(ctx, currentEventID, destinationID)
	if err != nil {
		return application.DeliveryJob{}, fmt.Errorf("актуальная редакция больше не направляется выбранному получателю: %w", err)
	}
	if job.Event.ID != currentEventID || job.Delivery.Destination.ID != destinationID {
		return application.DeliveryJob{}, errors.New("актуальное задание доставки повреждено")
	}
	return job, nil
}

// ReplaceDeliveryReceipt сохраняет результат явной повторной публикации, не
// возвращая историческое задание в общую очередь.
func (store *ServerStore) ReplaceDeliveryReceipt(ctx context.Context, eventID, destinationID string, at time.Time, receipt application.DeliveryReceipt) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("кодировать результат доставки: %w", err)
	}
	protected, err := protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить результат доставки: %w", err)
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET state='delivered', delivered_at=?, expired_at=NULL,
			last_error='', result_json=? WHERE event_id=? AND destination_id=?`,
		at.UTC().Format(time.RFC3339Nano), protected, eventID, destinationID)
	if err != nil {
		return fmt.Errorf("сохранить повторную публикацию: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil || count != 1 {
		return errors.New("доставка для повторной публикации не найдена")
	}
	return nil
}

// MarkDelivered окончательно фиксирует успешную доставку одному получателю.
func (store *ServerStore) MarkDelivered(ctx context.Context, eventID, destinationID string, at time.Time) error {
	return store.markDelivered(ctx, eventID, destinationID, at, nil)
}

// MarkDeliveredWithReceipt атомарно сохраняет доставку вместе с фактическим
// внешним ID. Эти данные защищаются так же, как событие и план доставки.
func (store *ServerStore) MarkDeliveredWithReceipt(
	ctx context.Context, eventID, destinationID string, at time.Time, receipt application.DeliveryReceipt,
) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("кодировать результат доставки: %w", err)
	}
	protected, err := protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить результат доставки: %w", err)
	}
	return store.markDelivered(ctx, eventID, destinationID, at, protected)
}

// MarkDeliveredOnAliasFallback сохраняет фактическую публикацию и маршрут
// следующих редакций одной транзакцией. После сбоя между этими действиями
// новая редакция могла бы снова выбрать удалённую тему и создать дубль.
func (store *ServerStore) MarkDeliveredOnAliasFallback(
	ctx context.Context, eventID, destinationID string, at time.Time,
	receipt application.DeliveryReceipt, fallback domain.PlannedDelivery,
) error {
	if fallback.RuleLevel != "alias" || fallback.Destination.Kind != domain.DestinationTelegram ||
		fallback.Decision.Action == domain.RouteSkip || fallback.Decision.ThreadID != receipt.ThreadID ||
		receipt.Kind != "telegram" || receipt.ChatID == 0 {
		return errors.New("резервная квитанция не соответствует маршруту General")
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("кодировать резервную квитанцию: %w", err)
	}
	protected, err := protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить резервную квитанцию: %w", err)
	}
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var clientID, sourceUUID string
	if err := tx.QueryRowContext(ctx, `SELECT e.client_id,e.source_uuid FROM received_events e
		JOIN delivery_jobs j ON j.event_id=e.event_id WHERE e.event_id=? AND j.destination_id=? AND j.state='pending'`,
		eventID, destinationID).Scan(&clientID, &sourceUUID); err != nil {
		return fmt.Errorf("найти исходную линию резервной доставки: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE delivery_jobs SET state='delivered', delivered_at=?, last_error='', result_json=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), protected, eventID, destinationID)
	if err := changedExactlyOnce(result, err, "подтвердить резервную доставку"); err != nil {
		return err
	}
	barrier, exists, err := loadSourceRevisionBarrier(ctx, tx, clientID, sourceUUID)
	if err != nil {
		return err
	}
	if exists && barrier.EventID == eventID {
		replaced := false
		for index, delivery := range barrier.Route {
			if delivery.Destination.ID != destinationID {
				continue
			}
			barrier.Route[index] = fallback
			replaced = true
			break
		}
		if !replaced {
			return errors.New("исходный маршрут не найден в барьере редакций")
		}
		if err := saveSourceRevisionBarrier(ctx, tx, barrier); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SavePartialDeliveryReceipt сохраняет уже известные внешние ID и признаки
// неопределённого исхода, но оставляет задание незавершённым. Последующее
// MarkDeliveryRetry или MarkDeliveryFailed не очищает эту квитанцию.
func (store *ServerStore) SavePartialDeliveryReceipt(
	ctx context.Context, eventID, destinationID string, receipt application.DeliveryReceipt,
) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("кодировать частичный результат доставки: %w", err)
	}
	protected, err := protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить частичный результат доставки: %w", err)
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET result_json=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		protected, eventID, destinationID)
	return changedExactlyOnce(result, err, "сохранить частичный результат доставки")
}

// SaveManualDeliveryReceipt refreshes the gateway's durable state for an
// operator action, including jobs that have already expired. It does not change
// the delivery state or enqueue a retry.
func (store *ServerStore) SaveManualDeliveryReceipt(
	ctx context.Context, eventID, destinationID string, receipt application.DeliveryReceipt,
) error {
	payload, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("кодировать состояние ручной публикации: %w", err)
	}
	protected, err := protectDatabaseValue(payload)
	if err != nil {
		return fmt.Errorf("защитить состояние ручной публикации: %w", err)
	}
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs SET result_json=?
		WHERE event_id=? AND destination_id=?`,
		protected, eventID, destinationID)
	return changedExactlyOnce(result, err, "сохранить состояние ручной публикации")
}

func (store *ServerStore) markDelivered(
	ctx context.Context, eventID, destinationID string, at time.Time, receipt []byte,
) error {
	query := `UPDATE delivery_jobs SET state='delivered', delivered_at=?, last_error=''`
	args := []any{at.UTC().Format(time.RFC3339Nano)}
	if receipt != nil {
		query += `, result_json=?`
		args = append(args, receipt)
	}
	query += ` WHERE event_id=? AND destination_id=? AND state='pending'`
	args = append(args, eventID, destinationID)
	result, err := store.db.ExecContext(ctx, `
		`+query, args...)
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

// MarkDeliveryFailed завершает предпринятую, но объективно неисправимую
// доставку. Для совместимости истории она хранится среди недоставленных за
// разрешённое окно, но причина и фактическая попытка сохраняются отдельно.
func (store *ServerStore) MarkDeliveryFailed(
	ctx context.Context, eventID, destinationID string, at time.Time, reason string,
) error {
	result, err := store.db.ExecContext(ctx, `
		UPDATE delivery_jobs
		SET state='expired', attempts=attempts+1, expired_at=?, last_error=?
		WHERE event_id=? AND destination_id=? AND state='pending'`,
		at.UTC().Format(time.RFC3339Nano), reason, eventID, destinationID)
	return changedExactlyOnce(result, err, "завершить неисправимую доставку")
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
