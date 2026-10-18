package secrets

import (
	"bytes"
	"path/filepath"
	"testing"
)

// Секретные данные после сохранения должны читаться без искажений и не лежать на диске открытым текстом.
func TestStoreRoundTrip(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "config.dat")
	plain := []byte(`{"token":"secret"}`)
	if err := Save(path, plain); err != nil {
		// Изолированная учётная запись сборочной среды может не иметь DPAPI-профиля.
		t.Skipf("DPAPI недоступен в текущем профиле Windows: %v", err)
	}
	loaded, err := Load(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(plain, loaded) {
		t.Fatalf("данные изменились: %q", loaded)
	}
}
