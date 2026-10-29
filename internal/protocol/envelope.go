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

// SchemaV2 — новый базовый транспортный контракт после перехода на
// самостоятельные временные блоки. Старые пакеты намеренно несовместимы:
// Центр помещает их в карантин и продолжает разбирать следующие сообщения.
const SchemaV2 = "intercept-envelope/v2"

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
	if err := event.ValidateContentDigest(); err != nil {
		return Envelope{}, fmt.Errorf("проверить транспортный контракт: %w", err)
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
		Schema: SchemaV2, ClientID: event.ClientID, EventID: event.ID,
		KeyID: keyID, CreatedAt: now.UTC(),
		Nonce: base64.RawStdEncoding.EncodeToString(nonce),
	}
	ciphertext := gcm.Seal(nil, nonce, plain, envelope.additionalData())
	envelope.Ciphertext = base64.RawStdEncoding.EncodeToString(ciphertext)
	return envelope, nil
}

// Open проверяет подлинность конверта и возвращает событие.
func Open(envelope Envelope, key []byte) (domain.Event, error) {
	if envelope.Schema != SchemaV2 {
		return domain.Event{}, fmt.Errorf(
			"несовместимая схема транспортного пакета %q, требуется %q",
			envelope.Schema, SchemaV2,
		)
	}
	plain, err := openPayload(envelope, key)
	if err != nil {
		return domain.Event{}, err
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

// Authenticate проверяет только владение транспортным ключом. Подлинный, но
// несовместимый по структуре пакет должен попасть в штатный карантин Центра,
// а не исчезнуть во время раннего отбора файлов Telegram.
func Authenticate(envelope Envelope, key []byte) error {
	_, err := openPayload(envelope, key)
	return err
}

func openPayload(envelope Envelope, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, errors.New("ключ пакета должен содержать 32 байта")
	}
	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return nil, errors.New("некорректный nonce")
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, errors.New("некорректный шифротекст")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("создать AES: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("создать GCM: %w", err)
	}
	if len(nonce) != gcm.NonceSize() {
		return nil, errors.New("некорректный размер nonce")
	}
	plain, err := gcm.Open(nil, nonce, ciphertext, envelope.additionalData())
	if err != nil {
		return nil, errors.New("пакет не прошёл проверку подлинности")
	}
	return plain, nil
}

func (envelope Envelope) additionalData() []byte {
	return []byte(strings.Join([]string{
		envelope.Schema, envelope.ClientID, envelope.EventID, envelope.KeyID,
	}, "\n"))
}
