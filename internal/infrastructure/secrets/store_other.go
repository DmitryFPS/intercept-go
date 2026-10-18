//go:build !windows

package secrets

import (
	"fmt"
	"os"
)

// Save использует права 0600 вне Windows. Целевые поставки используют DPAPI.
func Save(path string, plain []byte) error {
	if err := os.WriteFile(path, plain, 0o600); err != nil {
		return fmt.Errorf("сохранить настройки: %w", err)
	}
	return nil
}

func Load(path string) ([]byte, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("прочитать настройки: %w", err)
	}
	return payload, nil
}
