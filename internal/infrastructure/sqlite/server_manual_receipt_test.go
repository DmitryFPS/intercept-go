package sqlite

import (
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestExpiredManualReceiptSyncReplacesStaleAttemptToken(t *testing.T) {
	t.Parallel()
	store, ctx := openObservabilityStore(t)
	rules := serverTestRules()
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	event := serverTestEvent()
	event.SourceBlockUUID = "block-1"
	event.Blocks = []domain.ContentBlock{{UUID: "block-1", Key: "10:15", Text: "10:15\n/1/ Тест"}}
	event.Content = event.Blocks[0].Text
	event.ContentDigestVersion = domain.ContentDigestVersionCurrent
	event.ContentDigest = event.CanonicalContentDigest()
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := store.Accept(ctx, event, plan); err != nil || !accepted {
		t.Fatalf("accept=%v err=%v", accepted, err)
	}
	now := time.Now().UTC()
	if err := store.MarkDeliveryFailed(ctx, event.ID, "a", now, "ручная сверка"); err != nil {
		t.Fatal(err)
	}
	first := application.DeliveryReceipt{Kind: "telegram", ChatID: -1001, ThreadID: 2, Blocks: []application.DeliveryBlockReceipt{{
		BlockUUID: "block-1", ManualRepublishPending: true, SendAttemptToken: "permit-old", UnconfirmedPart: 1,
	}}}
	if err := store.SaveManualDeliveryReceipt(ctx, event.ID, "a", first); err != nil {
		t.Fatal(err)
	}
	second := application.DeliveryReceipt{Kind: "telegram", ChatID: -1001, ThreadID: 2, Blocks: []application.DeliveryBlockReceipt{{
		BlockUUID: "block-1", UnknownOutcome: true, ExpectedMessageCount: 1,
		SendAttemptToken: "attempt-new", UnconfirmedPart: 1,
	}}}
	if err := store.SaveManualDeliveryReceipt(ctx, event.ID, "a", second); err != nil {
		t.Fatal(err)
	}
	page, err := store.HistoryPage(ctx, "expired", 30, 0, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range page.Items {
		if item.DestinationID == "a" {
			if len(item.TelegramBlocks) != 1 || item.TelegramBlocks[0].SendAttemptToken != "attempt-new" ||
				item.TelegramBlocks[0].ManualRepublishPending || !item.TelegramBlocks[0].UnknownOutcome {
				t.Fatalf("история содержит старый токен после новой неудачи: %+v", item)
			}
			return
		}
	}
	t.Fatal("Telegram-доставка не найдена в истории")
}
