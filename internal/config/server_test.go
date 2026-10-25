package config

import (
	"testing"
	"time"
)

func TestServerDeliveryTimeoutUpgradesLegacyShortWindow(t *testing.T) {
	t.Parallel()
	for _, minutes := range []int{0, 10, 29, 60, 119} {
		cfg := Server{DeliveryTimeoutMinutes: minutes}
		if got, want := cfg.DeliveryTimeout(), 120*time.Minute; got != want {
			t.Fatalf("окно старой настройки %d минут: получено %s, ожидалось %s", minutes, got, want)
		}
	}
	cfg := Server{DeliveryTimeoutMinutes: 180}
	if got, want := cfg.DeliveryTimeout(), 180*time.Minute; got != want {
		t.Fatalf("явное увеличенное окно: получено %s, ожидалось %s", got, want)
	}
}
