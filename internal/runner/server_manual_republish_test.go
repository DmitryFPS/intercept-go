package runner

import (
	"testing"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestManualRepublishResumeRequiresCurrentPermitButNotSecondAbsenceClaim(t *testing.T) {
	t.Parallel()
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{ThreadID: 42}}
	receipt := application.DeliveryReceipt{ThreadID: 42, Blocks: []application.DeliveryBlockReceipt{{
		BlockUUID: "block-a", MessageIDs: []int64{801}, ExpectedMessageCount: 3,
		ManualRepublishPending: true, UnconfirmedPart: 2, SendAttemptToken: "permit-a",
	}}}
	resume := TelegramRepublishConfirmation{ManualResume: true, CheckedCurrentRevision: true, SendAttemptToken: "permit-a"}
	if part, err := validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", resume); err != nil || part != 0 {
		t.Fatalf("сохранённое разрешение для части 2 не продолжено: part=%d err=%v", part, err)
	}
	for name, changed := range map[string]func(*application.DeliveryReceipt, *domain.PlannedDelivery, *TelegramRepublishConfirmation){
		"старый токен": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.SendAttemptToken = "old"
		},
		"автоматический вызов": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.ManualResume = false
		},
		"непроверенная редакция": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.CheckedCurrentRevision = false
		},
		"новое заявление об отсутствии": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.ConfirmedPartAbsent = true
			c.UnconfirmedPart = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, d, c := receipt, delivery, resume
			r.Blocks = append([]application.DeliveryBlockReceipt(nil), receipt.Blocks...)
			changed(&r, &d, &c)
			if _, err := validateTelegramUnknownRepublishConfirmation(r, d, "block-a", c); err == nil {
				t.Fatal("изменившееся разрешение не должно продолжаться")
			}
		})
	}
}

func TestManualRepublishResumeAllowsChangedTopicOnlyAfterExistingPermit(t *testing.T) {
	t.Parallel()
	oldTopic := application.DeliveryReceipt{ThreadID: 42, Blocks: []application.DeliveryBlockReceipt{{
		BlockUUID: "block-a", ManualRepublishPending: true, SendAttemptToken: "permit-a",
	}}}
	newRoute := domain.PlannedDelivery{Decision: domain.RouteDecision{ThreadID: 99}}
	resume := TelegramRepublishConfirmation{
		ManualResume: true, CheckedCurrentRevision: true, SendAttemptToken: "permit-a",
	}
	if part, err := validateTelegramUnknownRepublishConfirmation(oldTopic, newRoute, "block-a", resume); err != nil || part != 0 {
		t.Fatalf("новый маршрут заблокировал сохранённое разрешение: part=%d err=%v", part, err)
	}
	oldTopic.Blocks[0].ManualRepublishPending = false
	oldTopic.Blocks[0].UnknownOutcome = true
	oldTopic.Blocks[0].ExpectedMessageCount = 1
	oldTopic.Blocks[0].UnconfirmedPart = 1
	if _, err := validateTelegramUnknownRepublishConfirmation(oldTopic, newRoute, "block-a", TelegramRepublishConfirmation{
		CheckedCurrentRevision: true, ConfirmedPartAbsent: true, UnconfirmedPart: 1, SendAttemptToken: "permit-a",
	}); err == nil {
		t.Fatal("первичная сверка спорной части в прежней теме не должна разрешать отправку в новую тему")
	}
}
