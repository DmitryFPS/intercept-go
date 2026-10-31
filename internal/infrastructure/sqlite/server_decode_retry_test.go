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
	"intercept/internal/domain"
)

func newDecodeRetryTestStore(t *testing.T) (context.Context, *sql.DB, *ServerStore, time.Time) {
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
	if err := store.SaveClient(ctx, application.RegisteredClient{
		ID: "textile", Number: "001", Name: "Точка", SenderBotID: 12,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 10,
		KeyID: "key-1", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	return ctx, db, store, time.Now().UTC().Truncate(time.Second).Add(time.Second)
}

func acceptDecodeRetryEvent(t *testing.T, ctx context.Context, store *ServerStore, id string, version int, at time.Time, destinations ...string) {
	t.Helper()
	event := serverTestEvent()
	event.ID = id
	event.Source.UUID = "same-source-block"
	event.CreatedAt = at.Add(time.Duration(version) * time.Second)
	event.Source.UpdatedAt = event.CreatedAt.Format("2006-01-02 15:04:05")
	event.SourceAt = at
	event.ExpiresAt = at.Add(time.Hour)
	event.Content = "содержимое " + id
	event.ContentDigest = "digest-" + id
	rules := serverTestRules()
	plan := domain.DeliveryPlan{EventID: id, RuleSetVersion: 1}
	for _, destinationID := range destinations {
		plan.Deliveries = append(plan.Deliveries, domain.PlannedDelivery{
			Destination: rules.Destinations[destinationID],
			Decision:    domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 2},
		})
	}
	accepted, err := store.Accept(ctx, event, plan)
	if err != nil || !accepted {
		t.Fatalf("принять %s: accepted=%v err=%v", id, accepted, err)
	}
}

func decodeRetryState(t *testing.T, ctx context.Context, db *sql.DB, eventID, destinationID string) (string, int, int, time.Time, string) {
	t.Helper()
	var state, next, reason string
	var attempts, failures int
	if err := db.QueryRowContext(ctx, `
		SELECT state,attempts,decode_failures,next_attempt_at,last_error
		FROM delivery_jobs WHERE event_id=? AND destination_id=?`, eventID, destinationID,
	).Scan(&state, &attempts, &failures, &next, &reason); err != nil {
		t.Fatal(err)
	}
	nextAt, err := time.Parse(time.RFC3339Nano, next)
	if err != nil {
		t.Fatal(err)
	}
	return state, attempts, failures, nextAt, reason
}

func TestPendingDeliveriesRetriesOneDPAPIFailureWithoutLosingRoute(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "original", 0, base, "a", "w")
	var routeBlob []byte
	if err := db.QueryRowContext(ctx,
		"SELECT delivery_json FROM delivery_jobs WHERE event_id='original' AND destination_id='a'",
	).Scan(&routeBlob); err != nil {
		t.Fatal(err)
	}
	failedOnce := false
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, routeBlob) && !failedOnce {
			failedOnce = true
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	firstAttempt := base.Add(2 * time.Second)
	jobs, err := store.PendingDeliveries(ctx, 10, firstAttempt)
	if err != nil || len(jobs) != 1 || jobs[0].Delivery.Destination.ID != "w" {
		t.Fatalf("сбой одного маршрута затронул независимую доставку: jobs=%#v err=%v", jobs, err)
	}
	state, attempts, failures, nextAt, reason := decodeRetryState(t, ctx, db, "original", "a")
	if state != "pending" || attempts != 0 || failures != 1 || !nextAt.Equal(firstAttempt.Add(2*time.Second)) || !strings.Contains(reason, "попытка 1") {
		t.Fatalf("временная ошибка потеряла маршрут: state=%q attempts=%d failures=%d next=%v reason=%q", state, attempts, failures, nextAt, reason)
	}
	if err := store.MarkDelivered(ctx, "original", "w", firstAttempt); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, firstAttempt.Add(time.Second))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("маршрут вышел раньше повтора: jobs=%#v err=%v", jobs, err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, nextAt)
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "original" || jobs[0].Delivery.Destination.ID != "a" {
		t.Fatalf("маршрут не восстановлен после временной ошибки: jobs=%#v err=%v", jobs, err)
	}
	state, attempts, failures, _, reason = decodeRetryState(t, ctx, db, "original", "a")
	if state != "pending" || attempts != 0 || failures != 0 || reason != "" {
		t.Fatalf("ошибка чтения не сброшена после успеха: state=%q attempts=%d failures=%d reason=%q", state, attempts, failures, reason)
	}
	if err := store.MarkDelivered(ctx, "original", "a", nextAt); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, nextAt)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("после доставки остались повторные задания: jobs=%#v err=%v", jobs, err)
	}
}

func TestPendingDeliveriesRecoversEventDPAPIAfterStoreRestart(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "restart-event", 0, base, "a", "w")
	var eventBlob []byte
	if err := db.QueryRowContext(ctx,
		"SELECT payload_json FROM received_events WHERE event_id='restart-event'",
	).Scan(&eventBlob); err != nil {
		t.Fatal(err)
	}
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, eventBlob) {
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	firstAttempt := base.Add(2 * time.Second)
	jobs, err := store.PendingDeliveries(ctx, 10, firstAttempt)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("нечитаемое событие не должно доставляться: jobs=%#v err=%v", jobs, err)
	}
	for _, destinationID := range []string{"a", "w"} {
		state, _, failures, nextAt, _ := decodeRetryState(t, ctx, db, "restart-event", destinationID)
		if state != "pending" || failures != 1 || !nextAt.Equal(firstAttempt.Add(2*time.Second)) {
			t.Fatalf("маршрут %s потерян до перезапуска: state=%q failures=%d next=%v", destinationID, state, failures, nextAt)
		}
	}
	restarted, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatalf("перезапуск хранилища после временного сбоя: %v", err)
	}
	jobs, err = restarted.PendingDeliveries(ctx, 10, firstAttempt.Add(time.Second))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("перезапуск сбросил задержку повтора: jobs=%#v err=%v", jobs, err)
	}
	jobsAtRetry := firstAttempt.Add(2 * time.Second)
	jobs, err = restarted.PendingDeliveries(ctx, 10, jobsAtRetry)
	if err != nil || len(jobs) != 2 {
		t.Fatalf("событие не восстановлено после перезапуска: jobs=%#v err=%v", jobs, err)
	}
	seen := map[string]bool{}
	for _, job := range jobs {
		if job.Event.ID != "restart-event" || seen[job.Delivery.Destination.ID] {
			t.Fatalf("дублирование маршрута после перезапуска: jobs=%#v", jobs)
		}
		seen[job.Delivery.Destination.ID] = true
		state, _, failures, _, reason := decodeRetryState(t, ctx, db, "restart-event", job.Delivery.Destination.ID)
		if state != "pending" || failures != 0 || reason != "" {
			t.Fatalf("не сброшена ошибка маршрута %s: state=%q failures=%d reason=%q", job.Delivery.Destination.ID, state, failures, reason)
		}
		if err := restarted.MarkDelivered(ctx, job.Event.ID, job.Delivery.Destination.ID, jobsAtRetry); err != nil {
			t.Fatal(err)
		}
	}
	if !seen["a"] || !seen["w"] {
		t.Fatalf("после перезапуска потерян маршрут: seen=%#v", seen)
	}
}

type cleanupDeliverySpy struct{ calls int }

func (gateway *cleanupDeliverySpy) Deliver(_ context.Context, _ domain.Event, delivery domain.PlannedDelivery) error {
	if delivery.RuleLevel != "cleanup" || delivery.Decision.Action != domain.RouteSkip {
		return errors.New("ожидалась корректирующая очистка")
	}
	gateway.calls++
	return nil
}

func TestCleanupDeliverySurvivesDPAPIRetryPastEventExpiry(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "before-route-change", 0, base, "a", "w")
	for _, destinationID := range []string{"a", "w"} {
		if err := store.MarkDelivered(ctx, "before-route-change", destinationID, base); err != nil {
			t.Fatal(err)
		}
	}
	acceptDecodeRetryEvent(t, ctx, store, "after-route-change", 1, base, "a")
	if err := store.MarkDelivered(ctx, "after-route-change", "a", base.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	var routeBlob []byte
	if err := db.QueryRowContext(ctx, `
		SELECT delivery_json FROM delivery_jobs
		WHERE event_id='after-route-change' AND destination_id='w'`,
	).Scan(&routeBlob); err != nil {
		t.Fatal(err)
	}
	failedOnce := false
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, routeBlob) && !failedOnce {
			failedOnce = true
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	firstAttempt := base.Add(time.Hour - time.Second)
	jobs, err := store.PendingDeliveries(ctx, 10, firstAttempt)
	if err != nil || len(jobs) != 0 || !failedOnce {
		t.Fatalf("ошибка DPAPI не задержала очистку: jobs=%#v failed=%v err=%v", jobs, failedOnce, err)
	}
	state, _, failures, nextAt, _ := decodeRetryState(t, ctx, db, "after-route-change", "w")
	if state != "pending" || failures != 1 || !nextAt.After(base.Add(time.Hour)) {
		t.Fatalf("очистка потеряна до восстановления: state=%q failures=%d next=%v", state, failures, nextAt)
	}

	restarted, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	gateway := &cleanupDeliverySpy{}
	service := application.DeliveryService{
		Queue:         restarted,
		Gateways:      map[string]application.DeliveryGateway{"w": gateway},
		Now:           func() time.Time { return nextAt },
		MaxConcurrent: 1,
	}
	count, err := service.RunOnce(ctx, 10)
	if err != nil || count != 1 || gateway.calls != 1 {
		t.Fatalf("после восстановления очистка не выполнена: count=%d calls=%d err=%v", count, gateway.calls, err)
	}
	state, _, failures, _, _ = decodeRetryState(t, ctx, db, "after-route-change", "w")
	if state != "delivered" || failures != 0 {
		t.Fatalf("очистка не завершена устойчиво: state=%q failures=%d", state, failures)
	}
	count, err = service.RunOnce(ctx, 10)
	if err != nil || count != 0 || gateway.calls != 1 {
		t.Fatalf("после завершения очистка выполнена повторно: count=%d calls=%d err=%v", count, gateway.calls, err)
	}
}

func TestPendingDeliveriesRetriesDPAPIUntilExpiryAndUnblocksStream(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "bad-head", 0, base, "a", "w")
	acceptDecodeRetryEvent(t, ctx, store, "following", 1, base.Add(30*time.Minute), "a", "w")
	var routeBlob []byte
	if err := db.QueryRowContext(ctx,
		"SELECT delivery_json FROM delivery_jobs WHERE event_id='bad-head' AND destination_id='a'",
	).Scan(&routeBlob); err != nil {
		t.Fatal(err)
	}
	store.pendingUnprotect = func(value []byte) ([]byte, error) {
		if bytes.Equal(value, routeBlob) {
			return nil, errors.New("The data is invalid")
		}
		return unprotectDatabaseValue(value)
	}
	attemptAt := base.Add(2 * time.Second)
	independentDelivered := map[string]bool{}
	for attempt := 1; attempt <= 10; attempt++ {
		jobs, err := store.PendingDeliveries(ctx, 10, attemptAt)
		if err != nil {
			t.Fatalf("попытка %d: %v", attempt, err)
		}
		for _, job := range jobs {
			if job.Delivery.Destination.ID != "w" || independentDelivered[job.Event.ID] {
				t.Fatalf("попытка %d пропустила запись зависимого потока: jobs=%#v", attempt, jobs)
			}
			independentDelivered[job.Event.ID] = true
			if err := store.MarkDelivered(ctx, job.Event.ID, "w", attemptAt); err != nil {
				t.Fatal(err)
			}
		}
		state, attempts, failures, nextAt, reason := decodeRetryState(t, ctx, db, "bad-head", "a")
		if state != "pending" || attempts != 0 || failures != attempt || !strings.Contains(reason, "The data is invalid") {
			t.Fatalf("попытка %d: state=%q attempts=%d failures=%d reason=%q", attempt, state, attempts, failures, reason)
		}
		if !nextAt.After(attemptAt) || !nextAt.Before(base.Add(time.Hour)) {
			t.Fatalf("попытка %d: повтор вне срока доставки: next=%v attempt=%v", attempt, nextAt, attemptAt)
		}
		attemptAt = nextAt
		followingState, _, _, _, _ := decodeRetryState(t, ctx, db, "following", "a")
		if followingState != "pending" {
			t.Fatalf("следующая запись преждевременно завершена: %q", followingState)
		}
	}
	if !independentDelivered["bad-head"] || !independentDelivered["following"] {
		t.Fatalf("ошибка одного направления заблокировала независимый поток: delivered=%#v", independentDelivered)
	}
	expiredAt := base.Add(time.Hour + time.Second)
	jobs, err := store.PendingDeliveries(ctx, 10, expiredAt)
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "following" || jobs[0].Delivery.Destination.ID != "a" {
		headState, _, headFailures, _, _ := decodeRetryState(t, ctx, db, "bad-head", "a")
		followingState, _, followingFailures, followingNext, _ := decodeRetryState(t, ctx, db, "following", "a")
		var headExpiry, followingExpiry string
		_ = db.QueryRowContext(ctx, "SELECT expires_at FROM delivery_jobs WHERE event_id='bad-head' AND destination_id='a'").Scan(&headExpiry)
		_ = db.QueryRowContext(ctx, "SELECT expires_at FROM delivery_jobs WHERE event_id='following' AND destination_id='a'").Scan(&followingExpiry)
		t.Fatalf("после штатного истечения поток не освобождён: jobs=%#v err=%v head=%s/%d expiry=%q following=%s/%d expiry=%q next=%v expiredAt=%v", jobs, err, headState, headFailures, headExpiry, followingState, followingFailures, followingExpiry, followingNext, expiredAt)
	}
	state, _, failures, _, _ := decodeRetryState(t, ctx, db, "bad-head", "a")
	if state != "expired" || failures != 10 {
		t.Fatalf("голова завершилась не по штатному сроку: state=%q failures=%d", state, failures)
	}
	if err := store.MarkDelivered(ctx, "following", "a", expiredAt); err != nil {
		t.Fatal(err)
	}
	jobs, err = store.PendingDeliveries(ctx, 10, expiredAt)
	if err != nil || len(jobs) != 0 {
		t.Fatalf("после доставки осталось задание: jobs=%#v err=%v", jobs, err)
	}
}
