package runner

import (
	"testing"

	"intercept/internal/config"
	"intercept/internal/domain"
)

// Черновик настроек можно менять в панели, не затрагивая уже работающую копию сервера.
func TestCloneServerConfigDoesNotShareDestinationMap(t *testing.T) {
	t.Parallel()
	original := config.Server{Destinations: map[string]config.DestinationConfig{
		"a": {Kind: domain.DestinationTelegram, ChatID: 1},
	}}
	clone := cloneServerConfig(original)
	clone.Destinations["b"] = config.DestinationConfig{Kind: domain.DestinationWord}
	if _, exists := original.Destinations["b"]; exists {
		t.Fatal("копия конфигурации разделяет map направлений")
	}
}

func TestClearDestinationThreads(t *testing.T) {
	t.Parallel()
	rules := domain.RuleSet{
		Unknown: map[string]domain.RouteDecision{"receiver": {Action: domain.RouteDeliver, ThreadID: 11}},
		Positions: map[string]map[string]domain.RouteDecision{
			"name:test": {"receiver": {Action: domain.RouteReview, ThreadID: 22}},
		},
		Modes: map[domain.SourceMode]map[string]domain.RouteDecision{
			domain.SourceModeNormal: {"receiver": {Action: domain.RouteDeliver, ThreadID: 33}},
		},
	}
	clearDestinationThreads(rules, "receiver")
	if rules.Unknown["receiver"].ThreadID != 0 ||
		rules.Positions["name:test"]["receiver"].ThreadID != 0 ||
		rules.Modes[domain.SourceModeNormal]["receiver"].ThreadID != 0 {
		t.Fatal("ID темы остался после переключения получателя с Telegram")
	}
}

func TestLegacyBuiltInRulesDetection(t *testing.T) {
	t.Parallel()
	legacy := domain.RuleSet{Destinations: map[string]domain.Destination{
		"aerodrom": {}, "shabashka": {}, "word-main": {},
	}, Positions: map[string]map[string]domain.RouteDecision{}}
	for index := 0; index < 20; index++ {
		legacy.Positions[string(rune('a'+index))] = nil
	}
	if !isLegacyBuiltInRules(legacy) {
		t.Fatal("старая встроенная заготовка не распознана")
	}
	delete(legacy.Destinations, "word-main")
	if isLegacyBuiltInRules(legacy) {
		t.Fatal("пользовательские правила ошибочно распознаны как старая заготовка")
	}
}
