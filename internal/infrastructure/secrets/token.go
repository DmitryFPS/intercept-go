package secrets

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadOrCreateToken хранит локальный пароль интерфейса под защитой DPAPI.
// Override используется только автоматической проверкой переносимого выпуска.
// LoadOrCreateToken создаёт постоянный случайный ключ доступа к локальному интерфейсу.
func LoadOrCreateToken(path, override string) (string, error) {
	if override = strings.TrimSpace(override); override != "" {
		if len(override) < 32 {
			return "", errors.New("тестовый токен интерфейса слишком короткий")
		}
		return override, nil
	}
	if payload, err := Load(path); err == nil {
		token := strings.TrimSpace(string(payload))
		if len(token) < 32 {
			return "", errors.New("сохранённый токен интерфейса повреждён")
		}
		return token, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("создать токен интерфейса: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", fmt.Errorf("создать папку токена интерфейса: %w", err)
	}
	if err := Save(path, []byte(token)); err != nil {
		return "", err
	}
	return token, nil
}
