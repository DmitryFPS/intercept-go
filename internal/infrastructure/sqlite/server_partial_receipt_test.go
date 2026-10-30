package sqlite

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"intercept/internal/application"
)

func TestServerStoreKeepsPartialReceiptAfterRetryOrFailure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		state string
	}{
		{name: "retry", state: "pending"},
		{name: "permanent", state: "expired"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			store, ctx := openObservabilityStore(t)
			rules := serverTestRules()
			if err := store.PublishRules(ctx, rules, "test"); err != nil {
				t.Fatal(err)
			}
			event := serverTestEvent()
			plan, err := rules.Resolve(event)
			if err != nil {
				t.Fatal(err)
			}
			if accepted, err := store.Accept(ctx, event, plan); err != nil || !accepted {
				t.Fatalf("принять событие: accepted=%v err=%v", accepted, err)
			}
			receipt := application.DeliveryReceipt{
				Kind: "telegram", ChatID: -1003943357633, ThreadID: 17, MessageIDs: []int64{701},
				Blocks: []application.DeliveryBlockReceipt{{
					BlockUUID: "block-1", MessageIDs: []int64{701}, ExpectedMessageCount: 2,
					UnknownOutcome: true, UnconfirmedPart: 2,
				}},
			}
			if err := store.SavePartialDeliveryReceipt(ctx, event.ID, "a", receipt); err != nil {
				t.Fatal(err)
			}
			at := time.Now().UTC()
			if test.state == "pending" {
				err = store.MarkDeliveryRetry(ctx, event.ID, "a", at.Add(time.Minute), "Telegram временно недоступен")
			} else {
				err = store.MarkDeliveryFailed(ctx, event.ID, "a", at, "исход отправки Telegram неизвестен")
			}
			if err != nil {
				t.Fatal(err)
			}
			var state string
			var stored []byte
			if err := store.db.QueryRowContext(ctx, `SELECT state,result_json FROM delivery_jobs
				WHERE event_id=? AND destination_id=?`, event.ID, "a").Scan(&state, &stored); err != nil {
				t.Fatal(err)
			}
			if state != test.state || !bytes.HasPrefix(stored, protectedPrefix) {
				t.Fatalf("состояние=%q, квитанция не защищена=%v", state, !bytes.HasPrefix(stored, protectedPrefix))
			}
			plain, err := unprotectDatabaseValue(stored)
			if err != nil {
				t.Fatal(err)
			}
			var restored application.DeliveryReceipt
			if err := json.Unmarshal(plain, &restored); err != nil {
				t.Fatal(err)
			}
			if restored.Kind != "telegram" || len(restored.MessageIDs) != 1 || restored.MessageIDs[0] != 701 ||
				len(restored.Blocks) != 1 || !restored.Blocks[0].UnknownOutcome || restored.Blocks[0].UnconfirmedPart != 2 {
				t.Fatalf("частичная квитанция утрачена: %+v", restored)
			}
			delivered, err := store.HistoryPage(ctx, "delivered", 30, 0, at)
			if err != nil || delivered.Total != 0 {
				t.Fatalf("частичная отправка ошибочно учтена как доставленная: %+v err=%v", delivered, err)
			}
		})
	}
}

var _ application.PartialDeliveryReceiptQueue = (*ServerStore)(nil)
