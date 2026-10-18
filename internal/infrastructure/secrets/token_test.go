package secrets

import (
	"path/filepath"
	"testing"
)

// Одна установка программы повторно использует тот же защищённый ключ доступа к локальной панели.
func TestLoadOrCreateTokenPersistsProtectedValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "access.dat")
	first, err := LoadOrCreateToken(path, "")
	if err != nil {
		// Изолированная учётная запись сборочной среды может не иметь DPAPI-профиля.
		t.Skipf("DPAPI недоступен в текущем профиле Windows: %v", err)
	}
	second, err := LoadOrCreateToken(path, "")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || len(first) < 32 {
		t.Fatalf("локальный токен не сохранился: first=%q second=%q", first, second)
	}
}

// Слишком короткий вручную заданный ключ доступа отклоняется как небезопасный.
func TestLoadOrCreateTokenRejectsShortOverride(t *testing.T) {
	if _, err := LoadOrCreateToken(filepath.Join(t.TempDir(), "access.dat"), "short"); err == nil {
		t.Fatal("короткий тестовый токен должен быть отклонён")
	}
}
