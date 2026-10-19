package config

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"
)

// Клиент отклоняет файл настроек с неизвестными полями или испорченным ключом, чтобы не работать частично и непредсказуемо.
func TestClientConfigRejectsUnknownFieldsAndBadKey(t *testing.T) {
	t.Parallel()

	cfg := validClientConfig()
	_, privateKey, _ := ed25519.GenerateKey(rand.Reader)
	cfg, _ = SignClient(cfg, privateKey)
	payload, _ := json.Marshal(cfg)
	if _, err := DecodeClient(bytes.NewReader(payload)); err != nil {
		t.Fatalf("valid config: %v", err)
	}

	unknown := append(payload[:len(payload)-1], []byte(`,"unexpected":true}`)...)
	if _, err := DecodeClient(bytes.NewReader(unknown)); err == nil {
		t.Fatal("лишнее поле должно отклоняться")
	}
	cfg.Cryptography.KeyBase64 = base64.RawStdEncoding.EncodeToString([]byte("short"))
	payload, _ = json.Marshal(cfg)
	if _, err := DecodeClient(bytes.NewReader(payload)); err == nil {
		t.Fatal("короткий ключ должен отклоняться")
	}

	cfg = validClientConfig()
	cfg, _ = SignClient(cfg, privateKey)
	cfg.ClientName = "Подменённое название"
	payload, _ = json.Marshal(cfg)
	if _, err := DecodeClient(bytes.NewReader(payload)); err == nil {
		t.Fatal("подменённый подписанный файл должен отклоняться")
	}
}

// Проверяет безопасные значения, которые сервер выдаёт переносному клиенту:
// путь определяется на точке, база читается раз в 10 секунд, время московское.
func TestGenerateClientUsesPortableEAVCDefaults(t *testing.T) {
	random := bytes.NewReader(make([]byte, 128))
	cfg, err := GenerateClient(ClientRequest{
		Number: "001", Name: "Текстиль", BotToken: "123456:abcdefghijklmnopqrstuv",
		BotUsername: "point_bot", ServerBotUsername: "server_bot",
		ChatID: -1001, ThreadID: 10,
	}, time.Now(), random)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EAVC.SQLitePath != "auto" || cfg.EAVCTimeZone() != "Europe/Moscow" {
		t.Fatalf("неверные настройки EAVC: %+v", cfg.EAVC)
	}
	if cfg.Polling.EAVCSeconds != 10 {
		t.Fatalf("ожидался опрос раз в 10 секунд, получено %d", cfg.Polling.EAVCSeconds)
	}
}

func validClientConfig() Client {
	return Client{
		Schema: ClientSchemaV2, GeneratedAt: time.Now(), ClientID: "textile", ClientNumber: "001", ClientName: "Текстиль",
		EAVC: EAVC{SQLitePath: `C:\EAVC\main.sqlite`},
		Transport: ClientTransport{
			BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE", BotUsername: "textile_bot",
			ServerBotUsername: "intercept_server_bot", ChatID: -100123, ThreadID: 42,
		},
		Cryptography: ClientCrypto{KeyID: "key-1", KeyBase64: base64.RawStdEncoding.EncodeToString(make([]byte, 32))},
		Polling:      ClientPolling{EAVCSeconds: 2, BatchSize: 100, MaxEventAgeMinutes: 30},
	}
}
