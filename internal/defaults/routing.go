package defaults

import (
	_ "embed"
	"encoding/json"
	"fmt"

	"intercept/internal/domain"
)

//go:embed initial-routing.json
var initialRouting []byte

// InitialRules возвращает проверенную копию начальной настраиваемой матрицы.
func InitialRules() (domain.RuleSet, error) {
	var rules domain.RuleSet
	if err := json.Unmarshal(initialRouting, &rules); err != nil {
		return domain.RuleSet{}, fmt.Errorf("разобрать начальные маршруты: %w", err)
	}
	if err := rules.Validate(); err != nil {
		return domain.RuleSet{}, fmt.Errorf("проверить начальные маршруты: %w", err)
	}
	return rules, nil
}
