package sqlite

import (
	"testing"
	"time"
)

func TestPendingDeliveriesExpiresAtFractionalSecondAfterWholeSecondDeadline(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "whole-second-expiry", 0, base, "a")
	deadline := base.Add(time.Minute)
	if _, err := db.ExecContext(ctx, `
		UPDATE delivery_jobs SET expires_at=?
		WHERE event_id='whole-second-expiry' AND destination_id='a'`,
		deadline.Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, deadline.Add(100*time.Millisecond))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("доставка после срока попала в очередь: jobs=%#v err=%v", jobs, err)
	}
	state, _, _, _, _ := decodeRetryState(t, ctx, db, "whole-second-expiry", "a")
	if state != "expired" {
		t.Fatalf("срок с целой секундой не завершил доставку: state=%q", state)
	}
}

func TestPendingDeliveriesDueAtFractionalSecondAfterWholeSecondRetryTime(t *testing.T) {
	t.Parallel()
	ctx, db, store, base := newDecodeRetryTestStore(t)
	acceptDecodeRetryEvent(t, ctx, store, "whole-second-retry", 0, base, "a")
	due := base.Add(time.Minute)
	if _, err := db.ExecContext(ctx, `
		UPDATE delivery_jobs SET next_attempt_at=?,expires_at=?
		WHERE event_id='whole-second-retry' AND destination_id='a'`,
		due.Format(time.RFC3339Nano), due.Add(time.Minute).Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	jobs, err := store.PendingDeliveries(ctx, 10, due.Add(100*time.Millisecond))
	if err != nil || len(jobs) != 1 || jobs[0].Event.ID != "whole-second-retry" || jobs[0].Delivery.Destination.ID != "a" {
		t.Fatalf("готовое задание не выбрано после срока повтора: jobs=%#v err=%v", jobs, err)
	}
}
