//go:build !windows

package secrets

import (
	"fmt"
	"os"
)

// Save использует права 0600 вне Windows. Целевые поставки используют DPAPI.
func Save(path string, plain []byte) error {
	if err := writeAtomic(path, plain); err != nil {
		return fmt.Errorf("сохранить настройки: %w", err)
	}
	return nil
}

// Load читает значение, сохранённое вне целевой Windows-среды.
func Load(path string) ([]byte, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("прочитать настройки: %w", err)
	}
	return payload, nil
}

// Protect сохраняет единый интерфейс платформенного хранилища.
// Целевая Windows-сборка использует DPAPI.
func Protect(plain []byte) ([]byte, error) {
	return append([]byte(nil), plain...), nil
}

// Unprotect возвращает копию значения вне Windows; рабочая сборка использует DPAPI.
func Unprotect(protected []byte) ([]byte, error) {
	return append([]byte(nil), protected...), nil
}
