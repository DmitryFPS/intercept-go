package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"intercept/internal/domain"
)

func TestTopicRedirectSurvivesDraftAndPublication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	rules := serverTestRules()
	key := "name:" + domain.NormalizeUnitName("1 мб 155 омбр")
	rules.TopicRedirects = map[string]domain.TopicRedirect{
		key: {DestinationID: "a", ThreadID: 39, TopicName: "Доразведка"},
	}
	if err := store.SaveRuleDraft(ctx, rules); err != nil {
		t.Fatal(err)
	}
	draft, exists, err := store.RuleDraft(ctx)
	if err != nil || !exists || draft.TopicRedirects[key].ThreadID != 39 {
		t.Fatalf("перенаправление потерялось в черновике: %#v, %v", draft.TopicRedirects, err)
	}
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	active, err := store.ActiveRules(ctx)
	if err != nil || active.TopicRedirects[key].TopicName != "Доразведка" {
		t.Fatalf("перенаправление потерялось при публикации: %#v, %v", active.TopicRedirects, err)
	}
	if _, exists, err := store.RuleDraft(ctx); err != nil || exists {
		t.Fatalf("черновик не очищен после публикации: exists=%v err=%v", exists, err)
	}
}

func TestUnknownAliasSurvivesDraftPublicationAndRestore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db, err := OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	rules := serverTestRules()
	source := "name:н/у доразведка всу"
	target := "name:1 мб 155 омбр"
	rules.UnknownAliases = map[string]string{source: target}
	if err := store.SaveRuleDraft(ctx, rules); err != nil {
		t.Fatal(err)
	}
	draft, exists, err := store.RuleDraft(ctx)
	if err != nil || !exists || draft.UnknownAliases[source] != target {
		t.Fatalf("привязка потерялась в черновике: %+v, %v", draft, err)
	}
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	active, err := store.ActiveRules(ctx)
	if err != nil || active.UnknownAliases[source] != target {
		t.Fatalf("привязка потерялась после публикации: %+v, %v", active, err)
	}
}

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
