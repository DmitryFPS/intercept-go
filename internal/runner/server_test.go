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
