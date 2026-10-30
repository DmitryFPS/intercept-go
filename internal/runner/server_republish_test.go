package runner

import (
	"testing"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestValidateTelegramRepublishBlockRequiresActiveCurrentBlock(t *testing.T) {
	t.Parallel()
	base := application.DeliveryJob{
		Event: domain.Event{
			SourceBlockUUID: "block-a",
			Blocks:          []domain.ContentBlock{{UUID: "block-a", Text: "12:00\n-Актуальная версия"}},
		},
		Delivery: domain.PlannedDelivery{
			Destination: domain.Destination{ID: "telegram", Kind: domain.DestinationTelegram},
			Decision:    domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 12},
		},
	}
	if err := validateTelegramRepublishBlock(base, "block-a"); err != nil {
		t.Fatalf("актуальный блок должен допускать ручную публикацию: %v", err)
	}
	for name, change := range map[string]func(*application.DeliveryJob){
		"другой блок":       func(job *application.DeliveryJob) { job.Event.SourceBlockUUID = "block-b" },
		"старый общий блок": func(job *application.DeliveryJob) { job.Event.SourceBlockUUID = "" },
		"блок удалён":       func(job *application.DeliveryJob) { job.Event.SourceBlockDeleted = true },
		"пустой блок":       func(job *application.DeliveryJob) { job.Event.Blocks[0].Text = "  " },
		"метка удаления":    func(job *application.DeliveryJob) { job.Event.Blocks[0].DeletedAt = "2026-10-29T12:05:00Z" },
		"очистка маршрута":  func(job *application.DeliveryJob) { job.Delivery.RuleLevel = "cleanup" },
		"пропуск маршрута":  func(job *application.DeliveryJob) { job.Delivery.Decision.Action = domain.RouteSkip },
		"не Telegram":       func(job *application.DeliveryJob) { job.Delivery.Destination.Kind = domain.DestinationWord },
	} {
		t.Run(name, func(t *testing.T) {
			job := base
			job.Event.Blocks = append([]domain.ContentBlock(nil), base.Event.Blocks...)
			change(&job)
			if err := validateTelegramRepublishBlock(job, "block-a"); err == nil {
				t.Fatal("неактуальный, удалённый или нерелевантный блок не должен публиковаться")
			}
		})
	}
}

func TestValidateTelegramUnknownRepublishConfirmation(t *testing.T) {
	t.Parallel()
	delivery := domain.PlannedDelivery{Decision: domain.RouteDecision{ThreadID: 42}}
	receipt := application.DeliveryReceipt{
		ThreadID: 42,
		Blocks: []application.DeliveryBlockReceipt{{
			BlockUUID: "block-a", UnknownOutcome: true, UnconfirmedPart: 2,
			ExpectedMessageCount: 3, MessageIDs: []int64{801}, SendAttemptToken: "attempt-a",
		}},
	}
	confirmed := TelegramRepublishConfirmation{
		CheckedCurrentRevision: true, ConfirmedPartAbsent: true, UnconfirmedPart: 2,
		SendAttemptToken: "attempt-a",
	}
	part, err := validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", confirmed)
	if err != nil || part != 2 {
		t.Fatalf("сверенная спорная часть отклонена: part=%d err=%v", part, err)
	}
	for name, change := range map[string]func(*application.DeliveryReceipt, *domain.PlannedDelivery, *TelegramRepublishConfirmation){
		"редакция не сверена": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.CheckedCurrentRevision = false
		},
		"отсутствие не подтверждено": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.ConfirmedPartAbsent = false
		},
		"другая часть": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.UnconfirmedPart = 1
		},
		"другая попытка": func(_ *application.DeliveryReceipt, _ *domain.PlannedDelivery, c *TelegramRepublishConfirmation) {
			c.SendAttemptToken = "attempt-before"
		},
		"нет ожидаемого количества": func(r *application.DeliveryReceipt, _ *domain.PlannedDelivery, _ *TelegramRepublishConfirmation) {
			r.Blocks[0].ExpectedMessageCount = 0
		},
		"потерян подтверждённый ID": func(r *application.DeliveryReceipt, _ *domain.PlannedDelivery, _ *TelegramRepublishConfirmation) {
			r.Blocks[0].MessageIDs = nil
		},
		"маршрут изменён": func(_ *application.DeliveryReceipt, d *domain.PlannedDelivery, _ *TelegramRepublishConfirmation) {
			d.Decision.ThreadID = 43
		},
	} {
		t.Run(name, func(t *testing.T) {
			currentReceipt, currentDelivery, currentConfirmation := receipt, delivery, confirmed
			currentReceipt.Blocks = append([]application.DeliveryBlockReceipt(nil), receipt.Blocks...)
			change(&currentReceipt, &currentDelivery, &currentConfirmation)
			if _, err := validateTelegramUnknownRepublishConfirmation(
				currentReceipt, currentDelivery, "block-a", currentConfirmation,
			); err == nil {
				t.Fatal("непроверенная или изменившаяся публикация не должна разрешаться")
			}
		})
	}
	receipt.Blocks[0].UnknownOutcome = false
	if _, err := validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", confirmed); err == nil {
		t.Fatal("подтверждение старой неопределённой попытки принято после смены состояния")
	}
	part, err = validateTelegramUnknownRepublishConfirmation(receipt, delivery, "block-a", TelegramRepublishConfirmation{})
	if err != nil || part != 0 {
		t.Fatalf("известное ручное удаление не требует подтверждений спорной части: part=%d err=%v", part, err)
	}
}
