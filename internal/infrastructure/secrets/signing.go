package secrets

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// LoadOrCreateSigningKey загружает или создаёт ключ подписи конфигураций.
// На Windows файл привязан DPAPI к текущей учётной записи.
func LoadOrCreateSigningKey(path string) (ed25519.PrivateKey, error) {
	if payload, err := Load(path); err == nil {
		if len(payload) != ed25519.PrivateKeySize {
			return nil, errors.New("закрытый ключ подписи сервера повреждён")
		}
		return ed25519.PrivateKey(payload), nil
	} else if !errors.Is(rootCause(err), os.ErrNotExist) {
		return nil, err
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("создать ключ подписи сервера: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("создать папку ключа подписи: %w", err)
	}
	if err := Save(path, privateKey); err != nil {
		return nil, err
	}
	return privateKey, nil
}
