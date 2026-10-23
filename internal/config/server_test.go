package config

import (
	"testing"
	"time"
)

func TestServerDeliveryTimeoutUpgradesLegacyShortWindow(t *testing.T) {
	t.Parallel()
	for _, minutes := range []int{0, 10, 29} {
		cfg := Server{DeliveryTimeoutMinutes: minutes}
		if got, want := cfg.DeliveryTimeout(), 30*time.Minute; got != want {
			t.Fatalf("окно старой настройки %d минут: получено %s, ожидалось %s", minutes, got, want)
		}
	}
	cfg := Server{DeliveryTimeoutMinutes: 45}
	if got, want := cfg.DeliveryTimeout(), 45*time.Minute; got != want {
		t.Fatalf("явное увеличенное окно: получено %s, ожидалось %s", got, want)
	}
}
