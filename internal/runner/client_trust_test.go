package runner

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/config"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/provisioning"
)

func TestFirstClientConfigTrustsProvisionedCenterAndRejectsAnother(t *testing.T) {
	t.Parallel()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Client{
		Schema: config.ClientSchemaV2, GeneratedAt: time.Now(), ClientID: "id", ClientNumber: "01", ClientName: "Точка",
		EAVC:         config.EAVC{SQLitePath: "auto", TimeZone: "Europe/Moscow"},
		Transport:    config.ClientTransport{BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE", BotUsername: "point_bot", ServerBotID: 2, ServerBotUsername: "center_bot", ChatID: -1001, ThreadID: 2},
		Cryptography: config.ClientCrypto{KeyID: "key", KeyBase64: base64.RawStdEncoding.EncodeToString(make([]byte, 32))},
		Polling:      config.ClientPolling{EAVCSeconds: 10, BatchSize: 100, MaxEventAgeMinutes: 30},
	}
	cfg, err = config.SignClient(cfg, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	issuer, first, err := client.verifyTrustedIssuer(cfg)
	if err != nil || !first {
		t.Fatalf("подготовленный файл не принят: first=%v err=%v", first, err)
	}
	if err := client.persistConfig([]byte(`{}`), issuer, first); err != nil {
		t.Fatal(err)
	}
	_, otherPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	other, err := config.SignClient(cfg, otherPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.verifyTrustedIssuer(other); err == nil {
		t.Fatal("после первичной настройки файл другого центра должен отклоняться")
	}
}

func TestProvisionedConfigRequiresSeparateActivationCode(t *testing.T) {
	t.Parallel()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Client{
		Schema: config.ClientSchemaV2, GeneratedAt: time.Now(), ClientID: "id", ClientNumber: "01", ClientName: "Точка",
		EAVC:         config.EAVC{SQLitePath: "auto", TimeZone: "Europe/Moscow"},
		Transport:    config.ClientTransport{BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE", BotUsername: "point_bot", ServerBotID: 2, ServerBotUsername: "center_bot", ChatID: -1001, ThreadID: 2},
		Cryptography: config.ClientCrypto{KeyID: "key", KeyBase64: base64.RawStdEncoding.EncodeToString(make([]byte, 32))},
		Polling:      config.ClientPolling{EAVCSeconds: 10, BatchSize: 100, MaxEventAgeMinutes: 30},
	}
	cfg, err = config.SignClient(cfg, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	baseDir := t.TempDir()
	sealed, code, err := provisioning.Seal(payload, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	provisionedPath := filepath.Join(baseDir, provisionedClientActivationName)
	if err := os.WriteFile(provisionedPath, sealed, 0o600); err != nil {
		t.Fatal(err)
	}
	client := NewClient(baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := client.installProvisionedActivation("AAAA-AAAA-AAAA-AAAA-AAAA-AAAA-AA"); err == nil {
		t.Fatal("неверный код активации принят")
	}
	if err := client.installProvisionedActivation(code); err != nil {
		t.Fatalf("готовые настройки не установлены: %v", err)
	}
	if _, err := os.Stat(provisionedPath); !os.IsNotExist(err) {
		t.Fatalf("использованный файл активации не удалён: %v", err)
	}
	stored, err := secrets.Load(client.configPath())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := config.DecodeClient(bytes.NewReader(stored)); err != nil {
		t.Fatalf("защищённые настройки повреждены: %v", err)
	}
}
