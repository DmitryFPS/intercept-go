package domain

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// DestinationKind определяет технический адаптер доставки.
type DestinationKind string

const (
	DestinationTelegram DestinationKind = "telegram"
	DestinationWord     DestinationKind = "word"
)

// RouteAction — явное решение одной ячейки матрицы маршрутов.
type RouteAction string

const (
	RouteSkip    RouteAction = "skip"
	RouteDeliver RouteAction = "deliver"
	RouteReview  RouteAction = "review"
)

// Destination описывает внешний получатель без его секретов.
type Destination struct {
	ID      string          `json:"id"`
	Name    string          `json:"name"`
	Kind    DestinationKind `json:"kind"`
	Enabled bool            `json:"enabled"`
}

// RouteDecision содержит действие и необязательную тему Telegram.
type RouteDecision struct {
	Action   RouteAction `json:"action"`
	ThreadID int64       `json:"thread_id,omitempty"`
	Note     string      `json:"note,omitempty"`
}

// TopicRedirect заменяет только тему единственного Telegram-получателя позиции.
// Название темы нужно оператору; для отправки используется её числовой ID.
type TopicRedirect struct {
	DestinationID string `json:"destination_id"`
	ThreadID      int64  `json:"thread_id"`
	TopicName     string `json:"topic_name"`
}

// RuleSet — атомарно публикуемая версия матрицы маршрутов.
type RuleSet struct {
	Version        int64                               `json:"version"`
	Destinations   map[string]Destination              `json:"destinations"`
	Positions      map[string]map[string]RouteDecision `json:"positions"`
	Unknown        map[string]RouteDecision            `json:"unknown"`
	TopicRedirects map[string]TopicRedirect            `json:"topic_redirects,omitempty"`
	// UnknownAliases связывает явно указанное неизвестное имя с готовым маршрутом.
	// Ключ всегда name:<нормализованное имя>, значение — ключ из Positions.
	UnknownAliases map[string]string `json:"unknown_aliases,omitempty"`
}

// PlannedDelivery — одно независимое задание для внешнего получателя.
type PlannedDelivery struct {
	Destination     Destination   `json:"destination"`
	Decision        RouteDecision `json:"decision"`
	RuleLevel       string        `json:"rule_level"`
	TopicRedirected bool          `json:"topic_redirected,omitempty"`
	// BaseDecision хранит исходный Telegram-маршрут под перенаправлением темы.
	// Он позволяет отличить отмену перенаправления от изменения самого маршрута.
	BaseDecision *RouteDecision `json:"base_decision,omitempty"`
	// SourcePositionKey сохраняет позицию EAVC после очистки истории, чтобы
	// последующие редакции продолжали менять сообщение в исходной теме.
	SourcePositionKey string `json:"source_position_key,omitempty"`
	// Нужен вместе с catalog UUID: изменение имени в EAVC должно снять старую
	// привязку, даже если идентификатор каталога не изменился.
	SourceUnitNameKey string `json:"source_unit_name_key,omitempty"`
	// FallbackDestination используется только для явно привязанного неизвестного
	// названия, если Telegram окончательно подтвердил удаление целевой темы.
	FallbackDestination *Destination   `json:"fallback_destination,omitempty"`
	FallbackDecision    *RouteDecision `json:"fallback_decision,omitempty"`
}

// DeliveryPlan объясняет как результат, так и правило, которое его создало.
type DeliveryPlan struct {
	EventID        string            `json:"event_id"`
	PositionKey    string            `json:"position_key"`
	Recognized     bool              `json:"recognized"`
	RuleSetVersion int64             `json:"rule_set_version"`
	Deliveries     []PlannedDelivery `json:"deliveries"`
}

// Validate не позволяет опубликовать неполную или противоречивую матрицу.
func (rules RuleSet) Validate() error {
	if rules.Version <= 0 {
		return errors.New("версия маршрутов должна быть положительной")
	}
	if len(rules.Destinations) == 0 {
		if len(rules.UnknownAliases) != 0 {
			return errors.New("привязка неизвестного названия не может ссылаться на отсутствующие маршруты")
		}
		if len(rules.TopicRedirects) != 0 {
			return errors.New("перенаправление темы не может ссылаться на отсутствующих получателей")
		}
		if len(rules.Unknown) != 0 {
			return errors.New("маршрут новых событий не может ссылаться на отсутствующих получателей")
		}
		for key, decisions := range rules.Positions {
			if len(decisions) != 0 {
				return fmt.Errorf("позиция %q ссылается на отсутствующих получателей", key)
			}
		}
		return nil
	}
	if len(rules.Unknown) == 0 {
		return errors.New("не задан маршрут для неопознанных событий")
	}
	for id, destination := range rules.Destinations {
		if strings.TrimSpace(id) == "" || destination.ID != id {
			return fmt.Errorf("некорректный ID направления %q", id)
		}
		if strings.TrimSpace(destination.Name) == "" {
			return fmt.Errorf("у направления %q нет названия", id)
		}
		switch destination.Kind {
		case DestinationTelegram, DestinationWord:
		default:
			return fmt.Errorf("у направления %q неизвестный тип %q", id, destination.Kind)
		}
	}
	for key, decisions := range rules.Positions {
		if !strings.HasPrefix(key, "catalog:") && !strings.HasPrefix(key, "name:") {
			return fmt.Errorf("некорректный ключ позиции %q", key)
		}
		if err := rules.validateDecisions("позиция "+key, decisions); err != nil {
			return err
		}
	}
	for source, target := range rules.UnknownAliases {
		if !strings.HasPrefix(source, "name:") ||
			source != "name:"+NormalizeUnitName(RoutingUnitName(strings.TrimPrefix(source, "name:"))) ||
			len(strings.TrimPrefix(source, "name:")) == 0 || utf8.RuneCountInString(source) > 205 {
			return fmt.Errorf("некорректное название для привязки %q", source)
		}
		if _, exists := rules.Positions[source]; exists {
			return fmt.Errorf("название %q уже имеет собственный маршрут", source)
		}
		decisions, exists := rules.Positions[target]
		if !exists {
			return fmt.Errorf("привязка %q ссылается на отсутствующий маршрут %q", source, target)
		}
		activeTelegram := 0
		for id, decision := range decisions {
			if rules.Destinations[id].Enabled && rules.Destinations[id].Kind == DestinationTelegram &&
				decision.Action != RouteSkip {
				activeTelegram++
				threadID := decision.ThreadID
				if redirect, ok := rules.TopicRedirects[target]; ok && redirect.DestinationID == id {
					threadID = redirect.ThreadID
				}
				if threadID <= 0 {
					return fmt.Errorf("маршрут %q не указывает существующую тему Telegram", target)
				}
			}
		}
		if activeTelegram != 1 {
			return fmt.Errorf("для привязки %q нужен маршрут с одним действующим Telegram-получателем", source)
		}
	}
	for key, redirect := range rules.TopicRedirects {
		decisions, exists := rules.Positions[key]
		if !exists {
			return fmt.Errorf("перенаправление ссылается на отсутствующий маршрут %q", key)
		}
		if redirect.ThreadID <= 0 {
			return fmt.Errorf("для перенаправления %q укажите ID темы Telegram больше нуля", key)
		}
		name := strings.TrimSpace(redirect.TopicName)
		if name == "" || utf8.RuneCountInString(name) > 100 || strings.ContainsAny(name, "\r\n\t") {
			return fmt.Errorf("для перенаправления %q укажите название темы длиной до 100 символов", key)
		}
		activeTelegram := 0
		for id, decision := range decisions {
			destination := rules.Destinations[id]
			if !destination.Enabled || destination.Kind != DestinationTelegram || decision.Action == RouteSkip {
				continue
			}
			activeTelegram++
			if id != redirect.DestinationID {
				return fmt.Errorf("у маршрута %q несколько Telegram-получателей для перенаправления", key)
			}
			if decision.ThreadID == redirect.ThreadID {
				return fmt.Errorf("маршрут %q уже отправляет в выбранную тему", key)
			}
		}
		if activeTelegram != 1 {
			return fmt.Errorf("для перенаправления %q нужен ровно один действующий Telegram-получатель", key)
		}
	}
	if err := rules.validateDecisions("неопознанные события", rules.Unknown); err != nil {
		return err
	}
	activeUnknown := 0
	for id, decision := range rules.Unknown {
		if decision.Action == RouteSkip {
			continue
		}
		destination := rules.Destinations[id]
		if !destination.Enabled || destination.Kind != DestinationTelegram ||
			decision.Action != RouteReview || decision.ThreadID != 0 {
			return errors.New("новые и ошибочные подразделения должны отправляться только на проверку в Telegram General")
		}
		activeUnknown++
	}
	if activeUnknown != 1 {
		return errors.New("для новых и ошибочных подразделений должен быть выбран ровно один Telegram General")
	}
	return nil
}

func (rules RuleSet) validateDecisions(owner string, decisions map[string]RouteDecision) error {
	for destinationID, decision := range decisions {
		destination, exists := rules.Destinations[destinationID]
		if !exists {
			return fmt.Errorf("%s ссылается на неизвестное направление %q", owner, destinationID)
		}
		switch decision.Action {
		case RouteSkip, RouteDeliver, RouteReview:
		default:
			return fmt.Errorf("%s содержит неизвестное действие %q", owner, decision.Action)
		}
		if destination.Kind != DestinationTelegram && decision.ThreadID != 0 {
			return fmt.Errorf("тема задана для направления %q не типа Telegram", destinationID)
		}
	}
	return nil
}

// Resolve выбирает правило позиции, а неизвестные события направляет только по
// обязательной системной строке. Режим источника является метаданными и не
// меняет получателей: отложенный перехват доставляется как обычный.
func (rules RuleSet) Resolve(event Event) (DeliveryPlan, error) {
	if err := rules.Validate(); err != nil {
		return DeliveryPlan{}, err
	}
	if err := event.Validate(); err != nil {
		return DeliveryPlan{}, err
	}

	key := event.PositionKey()
	decisions, recognized := rules.Positions[key]
	// Старая EAVC не знает catalog_uuid. Если будущая версия передаст UUID, но
	// администратор ещё не создал для него правило, действующее правило по имени
	// продолжит работать и событие не станет ошибочно неопознанным.
	if !recognized && strings.TrimSpace(event.Position.CatalogUUID) != "" {
		nameKey := "name:" + NormalizeUnitName(RoutingUnitName(event.Position.UnitName))
		if byName, exists := rules.Positions[nameKey]; exists {
			key, decisions, recognized = nameKey, byName, true
		}
	}
	routeKey := key
	level := "position"
	if !recognized {
		nameKey := "name:" + NormalizeUnitName(RoutingUnitName(event.Position.UnitName))
		if target, exists := rules.UnknownAliases[nameKey]; exists {
			key, routeKey, decisions, recognized, level = nameKey, target, rules.Positions[target], true, "alias"
		}
	}
	if !recognized {
		decisions = rules.Unknown
		level = "unknown"
		if suggestion := rules.closestPosition(event.Position.UnitName); suggestion != "" {
			decisions = withReviewNote(decisions, "⚠️ Возможно, имелось в виду: "+suggestion)
		}
	}

	ids := make([]string, 0, len(decisions))
	for id := range decisions {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	plan := DeliveryPlan{
		EventID: event.ID, PositionKey: key, Recognized: recognized,
		RuleSetVersion: rules.Version,
	}
	for _, id := range ids {
		decision := decisions[id]
		destination := rules.Destinations[id]
		if !destination.Enabled || decision.Action == RouteSkip {
			continue
		}
		redirected := false
		var baseDecision *RouteDecision
		if recognized && destination.Kind == DestinationTelegram {
			if redirect, exists := rules.TopicRedirects[routeKey]; exists && redirect.DestinationID == id {
				base := decision
				baseDecision = &base
				decision.ThreadID = redirect.ThreadID
				redirected = true
			}
		}
		var fallbackDestination *Destination
		var fallbackDecision *RouteDecision
		if level == "alias" && destination.Kind == DestinationTelegram {
			for unknownID, unknownDecision := range rules.Unknown {
				if unknownDecision.Action != RouteReview {
					continue
				}
				copyDestination := rules.Destinations[unknownID]
				copyDecision := unknownDecision
				fallbackDestination = &copyDestination
				fallbackDecision = &copyDecision
			}
		}
		plan.Deliveries = append(plan.Deliveries, PlannedDelivery{
			Destination:         destination,
			Decision:            decision,
			RuleLevel:           level,
			TopicRedirected:     redirected,
			BaseDecision:        baseDecision,
			SourcePositionKey:   event.PositionKey(),
			SourceUnitNameKey:   "name:" + NormalizeUnitName(RoutingUnitName(event.Position.UnitName)),
			FallbackDestination: fallbackDestination,
			FallbackDecision:    fallbackDecision,
		})
	}
	return plan, nil
}

func (rules RuleSet) closestPosition(unitName string) string {
	needle := []rune(NormalizeUnitName(unitName))
	if len(needle) == 0 {
		return ""
	}
	bestName := ""
	bestDistance := len(needle) + 1
	for key := range rules.Positions {
		if !strings.HasPrefix(key, "name:") {
			continue
		}
		name := strings.TrimPrefix(key, "name:")
		distance := levenshtein(needle, []rune(name))
		if distance < bestDistance {
			bestDistance = distance
			bestName = name
		}
	}
	threshold := len(needle) / 6
	if threshold < 3 {
		threshold = 3
	}
	if bestDistance > threshold {
		return ""
	}
	return bestName
}

func withReviewNote(decisions map[string]RouteDecision, note string) map[string]RouteDecision {
	result := make(map[string]RouteDecision, len(decisions))
	for id, decision := range decisions {
		if decision.Action == RouteReview {
			if decision.Note != "" {
				decision.Note += ". "
			}
			decision.Note += note
		}
		result[id] = decision
	}
	return result
}

func levenshtein(left, right []rune) int {
	previous := make([]int, len(right)+1)
	for index := range previous {
		previous[index] = index
	}
	for leftIndex, leftRune := range left {
		current := make([]int, len(right)+1)
		current[0] = leftIndex + 1
		for rightIndex, rightRune := range right {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			deletion := previous[rightIndex+1] + 1
			insertion := current[rightIndex] + 1
			replacement := previous[rightIndex] + cost
			current[rightIndex+1] = min(deletion, insertion, replacement)
		}
		previous = current
	}
	return previous[len(right)]
}
