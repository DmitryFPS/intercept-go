package domain

import (
	"strings"
	"testing"
	"time"
)

// Ошибку оператора в пробелах нельзя незаметно исправлять: такое название должно остаться неизвестным.
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

// UUID будущего каталога не должен отключать уже настроенное точное правило по
// названию, пока администратор не добавил отдельное правило UUID.
func TestRoutingFallsBackToNameWhenCatalogRuleIsAbsent(t *testing.T) {
	rules := testRuleSet()
	event := testEvent("1 мб 155 омбр")
	event.Position.CatalogUUID = "new-catalog-id"
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Recognized || plan.PositionKey != "name:1 мб 155 омбр" {
		t.Fatalf("правило по имени не применилось: %+v", plan)
	}
}

// Неизвестное подразделение уходит только по явно настроенному правилу для новых и ошибочных названий.
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
	if !strings.HasPrefix(plan.Deliveries[0].Decision.Note, "⚠️ Возможно, имелось в виду:") {
		t.Fatal("для близкой опечатки должна быть подсказка, но не автоматическая маршрутизация")
	}
}

// Если похожей позиции нет, оператор видит только обязательную пометку неизвестного
// подразделения: случайная или вводящая в заблуждение подсказка не добавляется.
func TestRuleSetDoesNotSuggestUnrelatedPosition(t *testing.T) {
	t.Parallel()

	rules := testRuleSet()
	plan, err := rules.Resolve(testEvent("совершенно неизвестное подразделение"))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if plan.Recognized || len(plan.Deliveries) != 1 {
		t.Fatalf("неожиданная схема неизвестной позиции: %#v", plan)
	}
	if plan.Deliveries[0].Decision.Note != "" {
		t.Fatalf("для несвязанного названия подсказки быть не должно: %q", plan.Deliveries[0].Decision.Note)
	}
}

func TestRuleSetRejectsUnsafeUnknownRoutes(t *testing.T) {
	t.Parallel()
	for name, change := range map[string]func(*RuleSet){
		"word": func(rules *RuleSet) {
			rules.Unknown["word-39"] = RouteDecision{Action: RouteDeliver}
		},
		"topic": func(rules *RuleSet) {
			rules.Unknown["shabashka"] = RouteDecision{Action: RouteReview, ThreadID: 42}
		},
		"two general routes": func(rules *RuleSet) {
			rules.Unknown["aerodrom"] = RouteDecision{Action: RouteReview}
		},
		"ordinary delivery": func(rules *RuleSet) {
			rules.Unknown["shabashka"] = RouteDecision{Action: RouteDeliver}
		},
	} {
		t.Run(name, func(t *testing.T) {
			rules := testRuleSet()
			change(&rules)
			if err := rules.Validate(); err == nil {
				t.Fatal("опасный маршрут неопознанной позиции принят")
			}
		})
	}
}

func TestRuleSetRejectsWorkingMatrixWithoutTelegramGeneral(t *testing.T) {
	t.Parallel()
	rules := RuleSet{
		Version: 1,
		Destinations: map[string]Destination{
			"word-39": {ID: "word-39", Name: "39-ка", Kind: DestinationWord, Enabled: true},
		},
		Unknown: map[string]RouteDecision{"word-39": {Action: RouteSkip}},
	}
	if err := rules.Validate(); err == nil {
		t.Fatal("рабочая матрица без Telegram General не должна публиковаться")
	}
}

// Перехват, назначенный только в Шабашку, не должен создавать документ для удалённой точки.
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

func TestTopicRedirectChangesOnlyTelegramTopic(t *testing.T) {
	t.Parallel()
	rules := testRuleSet()
	key := "name:" + NormalizeUnitName("1 мб 155 омбр")
	rules.TopicRedirects = map[string]TopicRedirect{
		key: {DestinationID: "aerodrom", ThreadID: 39, TopicName: "Доразведка"},
	}
	plan, err := rules.Resolve(testEvent("1 мб 155 омбр"))
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Deliveries) != 2 {
		t.Fatalf("ожидались Telegram и 39-ка: %#v", plan.Deliveries)
	}
	for _, delivery := range plan.Deliveries {
		switch delivery.Destination.ID {
		case "aerodrom":
			if delivery.Decision.ThreadID != 39 {
				t.Fatalf("перенаправление темы не применилось: %#v", delivery)
			}
		case "word-39":
			if delivery.Decision.Action != RouteDeliver || delivery.Decision.ThreadID != 0 {
				t.Fatalf("перенаправление изменило отправку на 39-ку: %#v", delivery)
			}
		default:
			t.Fatalf("неожиданный получатель: %#v", delivery)
		}
	}
	if rules.Positions[key]["aerodrom"].ThreadID != 2 {
		t.Fatal("перенаправление изменило исходный маршрут")
	}
	shabashkaKey := "name:" + NormalizeUnitName(`2 об 3 обр ОсН "Спартан" НГУ`)
	rules.TopicRedirects[shabashkaKey] = TopicRedirect{
		DestinationID: "shabashka", ThreadID: 593, TopicName: "Доразведка",
	}
	withoutWord, err := rules.Resolve(testEvent(`2 об 3 обр ОсН "Спартан" НГУ`))
	if err != nil || len(withoutWord.Deliveries) != 1 || withoutWord.Deliveries[0].Decision.ThreadID != 593 {
		t.Fatalf("перенаправление включило не заданную отправку на 39-ку: %#v, %v", withoutWord, err)
	}
	unknown, err := rules.Resolve(testEvent("совершенно неизвестная позиция"))
	if err != nil || unknown.Recognized || len(unknown.Deliveries) != 1 ||
		unknown.Deliveries[0].Decision.Action != RouteReview || unknown.Deliveries[0].Decision.ThreadID != 0 ||
		unknown.Deliveries[0].Destination.Kind != DestinationTelegram {
		t.Fatalf("неизвестная позиция должна остаться в General без DOCX: %#v, %v", unknown, err)
	}
	delete(rules.TopicRedirects, key)
	restored, err := rules.Resolve(testEvent("1 мб 155 омбр"))
	if err != nil || restored.Deliveries[0].Decision.ThreadID != 2 {
		t.Fatalf("после отмены перенаправления исходная тема не восстановилась: %#v, %v", restored, err)
	}
}

func TestRuleSetRejectsUnsafeTopicRedirects(t *testing.T) {
	t.Parallel()
	key := "name:" + NormalizeUnitName("1 мб 155 омбр")
	for name, change := range map[string]func(*RuleSet){
		"missing position": func(rules *RuleSet) { delete(rules.Positions, key) },
		"wrong recipient": func(rules *RuleSet) {
			rules.TopicRedirects[key] = TopicRedirect{DestinationID: "word-39", ThreadID: 39, TopicName: "Доразведка"}
		},
		"multiple Telegram recipients": func(rules *RuleSet) {
			rules.Positions[key]["shabashka"] = RouteDecision{Action: RouteDeliver, ThreadID: 5}
		},
		"disabled recipient": func(rules *RuleSet) {
			destination := rules.Destinations["aerodrom"]
			destination.Enabled = false
			rules.Destinations["aerodrom"] = destination
		},
		"same topic": func(rules *RuleSet) {
			rules.TopicRedirects[key] = TopicRedirect{DestinationID: "aerodrom", ThreadID: 2, TopicName: "Доразведка"}
		},
		"general": func(rules *RuleSet) {
			rules.TopicRedirects[key] = TopicRedirect{DestinationID: "aerodrom", ThreadID: 0, TopicName: "Доразведка"}
		},
		"blank name": func(rules *RuleSet) {
			rules.TopicRedirects[key] = TopicRedirect{DestinationID: "aerodrom", ThreadID: 39, TopicName: " "}
		},
		"long name": func(rules *RuleSet) {
			rules.TopicRedirects[key] = TopicRedirect{DestinationID: "aerodrom", ThreadID: 39, TopicName: strings.Repeat("я", 101)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			rules := testRuleSet()
			rules.TopicRedirects = map[string]TopicRedirect{
				key: {DestinationID: "aerodrom", ThreadID: 39, TopicName: "Доразведка"},
			}
			change(&rules)
			if err := rules.Validate(); err == nil {
				t.Fatal("некорректное перенаправление принято")
			}
		})
	}
}

// Пометка «отложенный режим» не блокирует перехват и не меняет назначенные ему группы и документы.
func TestDeferredSuffixKeepsNormalDestinations(t *testing.T) {
	t.Parallel()

	rules := testRuleSet()
	event := testEvent("1 мб 155 омбр (отложенный режим)")
	event.Mode = SourceModeDeferred
	plan, err := rules.Resolve(event)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if !plan.Recognized || len(plan.Deliveries) != 2 {
		t.Fatalf("отложенный перехват должен идти по маршрутам основной позиции: %#v", plan)
	}
	if event.Position.UnitName != "1 мб 155 омбр (отложенный режим)" {
		t.Fatal("служебная пометка не должна исчезать из отображаемого названия")
	}
}

func TestDeferredPositionRedirectUsesNameRuleWithOrWithoutCatalogUUID(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name        string
		catalogUUID string
	}{
		{name: "без UUID каталога"},
		{name: "с UUID каталога", catalogUUID: "aabbccdd-1122-3344-5566-77889900aabb"},
	} {
		t.Run(test.name, func(t *testing.T) {
			rules := testRuleSet()
			key := "name:" + NormalizeUnitName("1 мб 155 омбр")
			rules.TopicRedirects = map[string]TopicRedirect{
				key: {DestinationID: "aerodrom", ThreadID: 39, TopicName: "Доразведка"},
			}
			event := testEvent("1 мб 155 омбр (отложенный режим)")
			event.Mode = SourceModeDeferred
			event.Position.CatalogUUID = test.catalogUUID

			plan, err := rules.Resolve(event)
			if err != nil {
				t.Fatal(err)
			}
			if !plan.Recognized || plan.PositionKey != key || len(plan.Deliveries) != 2 {
				t.Fatalf("отложенная позиция не распознана по основному правилу: %#v", plan)
			}
			for _, delivery := range plan.Deliveries {
				switch delivery.Destination.ID {
				case "aerodrom":
					if !delivery.TopicRedirected || delivery.Decision.Action != RouteDeliver ||
						delivery.Decision.ThreadID != 39 || delivery.BaseDecision == nil ||
						delivery.BaseDecision.Action != RouteDeliver || delivery.BaseDecision.ThreadID != 2 {
						t.Fatalf("неверная целевая или исходная тема: %#v", delivery)
					}
				case "word-39":
					if delivery.TopicRedirected || delivery.BaseDecision != nil ||
						delivery.Decision.Action != RouteDeliver || delivery.Decision.ThreadID != 0 {
						t.Fatalf("перенаправление изменило Word-маршрут: %#v", delivery)
					}
				default:
					t.Fatalf("перехват ошибочно ушёл в General или другое направление: %#v", delivery)
				}
			}
		})
	}
}

// Для поиска правила снимается только конечная пометка «отложенный режим», остальное название остаётся без изменений.
func TestRoutingUnitNameRemovesOnlyTrailingDeferredMarker(t *testing.T) {
	t.Parallel()

	value := `  2 об 3 обр ОсН НГ  ( Отложенный   режим ) `
	if got := RoutingUnitName(value); got != "2 об 3 обр ОсН НГ" {
		t.Fatalf("неожиданное название для маршрутизации: %q", got)
	}
	if !IsDeferredUnitName(value) {
		t.Fatal("пометка отложенного режима не распознана")
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
		Unknown: map[string]RouteDecision{
			"shabashka": {Action: RouteReview, ThreadID: 0},
			"word-39":   {Action: RouteSkip},
		},
	}
}
