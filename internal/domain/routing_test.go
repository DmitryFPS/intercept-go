package domain

import (
	"testing"
	"time"
)

func TestNormalizeUnitNameDoesNotRepairMissingSpaces(t *testing.T) {
	t.Parallel()

	known := NormalizeUnitName(`2 об 3 обр ОсН «Спартан» НГУ`)
	typo := NormalizeUnitName(`2об3 обр ОсН "Спартан" НГУ`)
	if known == typo {
		t.Fatal("строгая нормализация не должна исправлять отсутствующие пробелы")
	}
	if known != `2 об 3 обр осн "спартан" нгу` {
		t.Fatalf("неожиданная нормализованная строка: %q", known)
	}
}

func TestRuleSetRoutesUnknownOnlyByUnknownRow(t *testing.T) {
	t.Parallel()

	rules := testRuleSet()
	event := testEvent(`2об3 обр ОсН "Спартан" НГУ`)
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if plan.Recognized {
		t.Fatal("опечатка не должна считаться известной позицией")
	}
	if len(plan.Deliveries) != 1 || plan.Deliveries[0].Destination.ID != "shabashka" {
		t.Fatalf("неопознанное событие должно идти только в Шабашку: %#v", plan.Deliveries)
	}
	if plan.Deliveries[0].Decision.ThreadID != 0 {
		t.Fatal("неопознанное событие должно идти в General")
	}
	if plan.Deliveries[0].Decision.Note == "" {
		t.Fatal("для близкой опечатки должна быть подсказка, но не автоматическая маршрутизация")
	}
}

func TestRuleSetDoesNotCreateWordForShabashkaPosition(t *testing.T) {
	t.Parallel()

	rules := testRuleSet()
	plan, err := rules.Resolve(testEvent(`2 об 3 обр ОсН "Спартан" НГУ`))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !plan.Recognized {
		t.Fatal("известная позиция должна быть распознана")
	}
	if len(plan.Deliveries) != 1 || plan.Deliveries[0].Destination.ID != "shabashka" {
		t.Fatalf("ожидалась только Шабашка: %#v", plan.Deliveries)
	}
}

func TestDeferredModeOverridesWordDestination(t *testing.T) {
	t.Parallel()

	rules := testRuleSet()
	event := testEvent("1 мб 155 омбр")
	event.Mode = SourceModeDeferred
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if len(plan.Deliveries) != 1 || plan.Deliveries[0].Destination.ID != "aerodrom" {
		t.Fatalf("отложенный режим должен отключить Word: %#v", plan.Deliveries)
	}
}

func testEvent(unitName string) Event {
	return Event{
		ID: "018f22d2-3f5c-7abc-8123-0123456789ab", ClientID: "textile",
		CreatedAt: time.Now(), ObservedAt: time.Now(),
		Source:   SourceRecord{ID: 1, UUID: "source-1"},
		Position: Position{UnitName: unitName}, Mode: SourceModeNormal,
		Content: "текст", ContentDigest: "digest",
	}
}

func testRuleSet() RuleSet {
	return RuleSet{
		Version: 1,
		Destinations: map[string]Destination{
			"aerodrom":  {ID: "aerodrom", Name: "Аэродром", Kind: DestinationTelegram, Enabled: true},
			"shabashka": {ID: "shabashka", Name: "Шабашка", Kind: DestinationTelegram, Enabled: true},
			"word-39":   {ID: "word-39", Name: "39-ка", Kind: DestinationWord, Enabled: true},
		},
		Positions: map[string]map[string]RouteDecision{
			"name:" + NormalizeUnitName(`2 об 3 обр ОсН "Спартан" НГУ`): {
				"shabashka": {Action: RouteDeliver, ThreadID: 592},
				"word-39":   {Action: RouteSkip},
			},
			"name:" + NormalizeUnitName("1 мб 155 омбр"): {
				"aerodrom": {Action: RouteDeliver, ThreadID: 2},
				"word-39":  {Action: RouteDeliver},
			},
		},
		Modes: map[SourceMode]map[string]RouteDecision{
			SourceModeDeferred: {"word-39": {Action: RouteSkip}},
		},
		Unknown: map[string]RouteDecision{
			"shabashka": {Action: RouteReview, ThreadID: 0},
			"word-39":   {Action: RouteSkip},
		},
	}
}
