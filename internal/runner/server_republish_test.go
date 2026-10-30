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
