package domain

import (
	"errors"
	"fmt"
	"sort"
	"strings"
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

// RuleSet — атомарно публикуемая версия матрицы маршрутов.
type RuleSet struct {
	Version      int64                                   `json:"version"`
	Destinations map[string]Destination                  `json:"destinations"`
	Positions    map[string]map[string]RouteDecision     `json:"positions"`
	Modes        map[SourceMode]map[string]RouteDecision `json:"modes,omitempty"`
	Unknown      map[string]RouteDecision                `json:"unknown"`
}

// PlannedDelivery — одно независимое задание для внешнего получателя.
type PlannedDelivery struct {
	Destination Destination   `json:"destination"`
	Decision    RouteDecision `json:"decision"`
	RuleLevel   string        `json:"rule_level"`
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
		return errors.New("не задано ни одного направления")
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
	for mode, decisions := range rules.Modes {
		switch mode {
		case SourceModeNormal, SourceModeDeferred, SourceModeUnknown:
		default:
			return fmt.Errorf("неизвестный режим %q", mode)
		}
		if err := rules.validateDecisions("режим "+string(mode), decisions); err != nil {
			return err
		}
	}
	return rules.validateDecisions("неопознанные события", rules.Unknown)
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
	level := "position"
	if !recognized {
		decisions = rules.Unknown
		level = "unknown"
		if suggestion := rules.closestPosition(event.Position.UnitName); suggestion != "" {
			decisions = withReviewNote(decisions, "Возможно, имелось в виду: "+suggestion)
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
		plan.Deliveries = append(plan.Deliveries, PlannedDelivery{
			Destination: destination,
			Decision:    decision,
			RuleLevel:   level,
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
