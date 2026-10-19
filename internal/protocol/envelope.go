package protocol

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"intercept/internal/domain"
)

// SchemaV1 обозначает текущий формат зашифрованного транспортного конверта.
const SchemaV1 = "intercept-envelope/v1"

// Envelope — безопасный внешний формат файла, передаваемого через Telegram.
type Envelope struct {
	Schema     string    `json:"schema"`
	ClientID   string    `json:"client_id"`
	EventID    string    `json:"event_id"`
	KeyID      string    `json:"key_id"`
	CreatedAt  time.Time `json:"created_at"`
	Nonce      string    `json:"nonce"`
	Ciphertext string    `json:"ciphertext"`
}

// Seal шифрует событие AES-256-GCM. Внешние идентификаторы входят в AAD.
func Seal(event domain.Event, keyID string, key []byte, now time.Time, random io.Reader) (Envelope, error) {
	if err := event.Validate(); err != nil {
		return Envelope{}, fmt.Errorf("проверить событие: %w", err)
	}
	if len(key) != 32 {
		return Envelope{}, errors.New("ключ пакета должен содержать 32 байта")
	}
	if strings.TrimSpace(keyID) == "" {
		return Envelope{}, errors.New("не задан ID ключа")
	}
	if random == nil {
		random = rand.Reader
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return Envelope{}, fmt.Errorf("создать AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return Envelope{}, fmt.Errorf("создать GCM: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(random, nonce); err != nil {
		return Envelope{}, fmt.Errorf("создать nonce: %w", err)
	}
	plain, err := json.Marshal(event)
	if err != nil {
		return Envelope{}, fmt.Errorf("кодировать событие: %w", err)
	}

	envelope := Envelope{
		Schema: SchemaV1, ClientID: event.ClientID, EventID: event.ID,
		KeyID: keyID, CreatedAt: now.UTC(),
		Nonce: base64.RawStdEncoding.EncodeToString(nonce),
	}
	ciphertext := gcm.Seal(nil, nonce, plain, envelope.additionalData())
	envelope.Ciphertext = base64.RawStdEncoding.EncodeToString(ciphertext)
	return envelope, nil
}

// Open проверяет подлинность конверта и возвращает событие.
func Open(envelope Envelope, key []byte) (domain.Event, error) {
	if envelope.Schema != SchemaV1 {
		return domain.Event{}, fmt.Errorf("неподдерживаемая схема %q", envelope.Schema)
	}
	if len(key) != 32 {
		return domain.Event{}, errors.New("ключ пакета должен содержать 32 байта")
	}
	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return domain.Event{}, errors.New("некорректный nonce")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return domain.Event{}, errors.New("некорректный шифротекст")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return domain.Event{}, fmt.Errorf("создать AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return domain.Event{}, fmt.Errorf("создать GCM: %w", err)
	}
	if len(nonce) != gcm.NonceSize() {
		return domain.Event{}, errors.New("некорректный размер nonce")
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, envelope.additionalData())
	if err != nil {
		return domain.Event{}, errors.New("пакет не прошёл проверку подлинности")
	}
	var event domain.Event
	if err := json.Unmarshal(plain, &event); err != nil {
		return domain.Event{}, errors.New("расшифрованное событие повреждено")
	}
	if event.ID != envelope.EventID || event.ClientID != envelope.ClientID {
		return domain.Event{}, errors.New("идентификаторы конверта и события не совпадают")
	}
	if err := event.Validate(); err != nil {
		return domain.Event{}, fmt.Errorf("событие не прошло проверку: %w", err)
	}
	return event, nil
}

func (envelope Envelope) additionalData() []byte {
	return []byte(strings.Join([]string{
		envelope.Schema, envelope.ClientID, envelope.EventID, envelope.KeyID,
	}, "\n"))
}
