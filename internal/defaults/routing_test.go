package defaults

import (
	"testing"
)

// Новый сервер не должен подставлять группы, позиции или технические ID за пользователя.
func TestInitialRulesAreEmpty(t *testing.T) {
	t.Parallel()
	rules, err := InitialRules()
	if err != nil {
		t.Fatalf("rules: %v", err)
	}
	if rules.Version != 1 || len(rules.Destinations) != 0 || len(rules.Positions) != 0 || len(rules.Unknown) != 0 {
		t.Fatalf("начальные правила должны быть пустыми: %#v", rules)
	}
}
