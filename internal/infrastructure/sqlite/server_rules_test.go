package sqlite

import (
	"testing"

	"intercept/internal/domain"
)

func TestLegacyGeneralDeliveryMigratesToExplicitReview(t *testing.T) {
	t.Parallel()
	rules := serverTestRules()
	rules.Unknown["a"] = domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 0}
	migrateLegacyUnknownReview(&rules)
	if rules.Unknown["a"].Action != domain.RouteReview {
		t.Fatalf("старый General не преобразован в review: %+v", rules.Unknown["a"])
	}
	if err := rules.Validate(); err != nil {
		t.Fatalf("безопасный старый маршрут должен пережить обновление: %v", err)
	}
}

func TestLegacyMigrationDoesNotAuthorizeWordOrTopic(t *testing.T) {
	t.Parallel()
	rules := serverTestRules()
	rules.Unknown["w"] = domain.RouteDecision{Action: domain.RouteDeliver}
	rules.Unknown["a"] = domain.RouteDecision{Action: domain.RouteDeliver, ThreadID: 42}
	migrateLegacyUnknownReview(&rules)
	if rules.Unknown["w"].Action != domain.RouteDeliver || rules.Unknown["a"].Action != domain.RouteDeliver {
		t.Fatal("миграция не должна чинить опасные Word-или тематические маршруты")
	}
	if err := rules.Validate(); err == nil {
		t.Fatal("опасный старый маршрут не должен проходить проверку")
	}
}
