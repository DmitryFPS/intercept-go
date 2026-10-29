package protocol

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"testing"
	"time"

	"intercept/internal/domain"
)

// Сервер должен прочитать неизменённый пакет и отвергнуть его, если содержимое было подменено после отправки.
func TestEnvelopeRoundTripAndTamperProtection(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x42}, 32)
	now := time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "0199f72d-6f00-7000-8000-000000000001", ClientID: "textile",
		CreatedAt: now, ObservedAt: now,
		Source:   domain.SourceRecord{ID: 7, UUID: "source-7"},
		Position: domain.Position{UnitName: "1 мб 155 омбр"},
		Mode:     domain.SourceModeNormal, Content: "данные",
		ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	envelope, err := Seal(event, "key-1", key, now, bytes.NewReader(make([]byte, 12)))
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	opened, err := Open(envelope, key)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if opened.Content != event.Content || opened.ID != event.ID {
		t.Fatalf("событие изменилось: %#v", opened)
	}
	legacy := envelope
	legacy.Schema = "intercept-envelope/v1"
	if _, err := Open(legacy, key); err == nil {
		t.Fatal("конверт старого контракта ошибочно принят")
	}

	envelope.ClientID = "another-client"
	if _, err := Open(envelope, key); err == nil {
		t.Fatal("изменённый конверт не должен проходить проверку")
	}
}

func TestAuthenticateKeepsGenuineMalformedEventForQuarantine(t *testing.T) {
	t.Parallel()
	key := bytes.Repeat([]byte{0x42}, 32)
	nonce := make([]byte, 12)
	envelope := Envelope{
		Schema: SchemaV2, ClientID: "textile", EventID: "event-1", KeyID: "key-1",
		Nonce: base64.RawStdEncoding.EncodeToString(nonce),
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	envelope.Ciphertext = base64.RawStdEncoding.EncodeToString(
		gcm.Seal(nil, nonce, []byte("not-json"), envelope.additionalData()),
	)
	if err := Authenticate(envelope, key); err != nil {
		t.Fatalf("подлинный пакет должен дойти до карантина: %v", err)
	}
	if _, err := Open(envelope, key); err == nil {
		t.Fatal("повреждённое содержимое нельзя принимать как событие")
	}
	envelope.Ciphertext += "x"
	if err := Authenticate(envelope, key); err == nil {
		t.Fatal("неподписанный пакет не должен пройти ранний отбор")
	}
}
