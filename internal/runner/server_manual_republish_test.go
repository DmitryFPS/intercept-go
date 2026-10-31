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

func TestFallbackReceiptMayUseOnlyOriginalThread(t *testing.T) {
	t.Parallel()
	base := domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 7}
	delivery := domain.PlannedDelivery{
		Destination:     domain.Destination{ID: "telegram", Kind: domain.DestinationTelegram},
		Decision:        domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42},
		TopicRedirected: true, BaseDecision: &base,
	}
	if !telegramReceiptMatchesPlannedRoute(42, delivery) || !telegramReceiptMatchesPlannedRoute(7, delivery) ||
		telegramReceiptMatchesPlannedRoute(99, delivery) {
		t.Fatal("квитанция должна относиться только к цели или исходному маршруту")
	}
	receipt := application.DeliveryReceipt{Kind: "telegram", ThreadID: 7, Blocks: []application.DeliveryBlockReceipt{{
		BlockUUID: "block-a", MessageIDs: []int64{801}, ExpectedMessageCount: 1, Complete: true,
	}}}
	job := application.DeliveryJob{
		Event:    domain.Event{SourceBlockUUID: "block-a", Blocks: []domain.ContentBlock{{UUID: "block-a", Text: "12:00\n-Проверка"}}},
		Delivery: delivery,
	}
	if !telegramReceiptCompletesCurrentJob(job, receipt, "block-a") {
		t.Fatal("фактическая квитанция в исходной теме не восстановила задание")
	}
	receipt.Blocks[0].Complete = false
	receipt.Blocks[0].UnknownOutcome = true
	receipt.Blocks[0].UnconfirmedPart = 2
	receipt.Blocks[0].ExpectedMessageCount = 2
	receipt.Blocks[0].SendAttemptToken = "attempt-a"
	confirmation := TelegramRepublishConfirmation{
		CheckedCurrentRevision: true, ConfirmedPartAbsent: true,
		UnconfirmedPart: 2, SendAttemptToken: "attempt-a",
	}
	if part, err := validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", confirmation); err != nil || part != 2 {
		t.Fatalf("ручная сверка спорной части исходной темы заблокирована: part=%d err=%v", part, err)
	}
	receipt.ThreadID = 99
	if _, err := validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", confirmation); err == nil {
		t.Fatal("посторонняя тема ошибочно принята для ручного разрешения")
	}
}
