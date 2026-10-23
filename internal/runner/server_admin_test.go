package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/provisioning"
)

// Готовый комплект точки должен содержать исполняемый файл, подписанные
// настройки и памятку. Этот тест защищает сценарий скачивания из интерфейса
// центра от неполной сборки или установки station-template.
func TestClientPackageContainsRunnableStation(t *testing.T) {
	baseDir := t.TempDir()
	stationTemplate := filepath.Join(baseDir, "station-template", "EAVC Connect Station.exe")
	if err := os.MkdirAll(filepath.Dir(stationTemplate), 0o755); err != nil {
		t.Fatal(err)
	}
	executable := []byte("test-station-executable")
	if err := os.WriteFile(stationTemplate, executable, 0o600); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	server, err := NewServer(ctx, baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	cfg, err := config.GenerateClient(config.ClientRequest{
		Number: "01", Name: "Текстиль", BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE",
		BotUsername: "textile_bot", ServerBotID: 77, ServerBotUsername: "server_bot",
		ChatID: -100123, ThreadID: 2,
	}, time.Now(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = config.SignClient(cfg, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	key, err := cfg.Key()
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Store.SaveClient(ctx, application.RegisteredClient{
		ID: cfg.ClientID, Number: cfg.ClientNumber, Name: cfg.ClientName,
		SenderBotID: 10, SenderBotUsername: cfg.Transport.BotUsername,
		ChatID: cfg.Transport.ChatID, ThreadID: cfg.Transport.ThreadID,
		KeyID: cfg.Cryptography.KeyID, Key: key, Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(server.clientConfigPath(cfg.ClientID)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := secrets.Save(server.clientConfigPath(cfg.ClientID), payload); err != nil {
		t.Fatal(err)
	}

	archivePayload, activationCode, client, err := server.ClientPackage(ctx, cfg.ClientID)
	if err != nil {
		t.Fatal(err)
	}
	if client.Number != "01" {
		t.Fatalf("получена другая точка: %q", client.Number)
	}
	archive, err := zip.NewReader(bytes.NewReader(archivePayload), int64(len(archivePayload)))
	if err != nil {
		t.Fatal(err)
	}
	entries := make(map[string][]byte, len(archive.File))
	for _, file := range archive.File {
		reader, openErr := file.Open()
		if openErr != nil {
			t.Fatal(openErr)
		}
		content, readErr := io.ReadAll(reader)
		reader.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		entries[filepath.ToSlash(file.Name)] = content
	}
	folder := "EAVC Connect Station-01/"
	if !bytes.Equal(entries[folder+"EAVC Connect Station.exe"], executable) {
		t.Fatal("в комплекте отсутствует исполняемый файл рабочей точки")
	}
	sealed := entries[folder+provisionedClientActivationName]
	if bytes.Contains(sealed, []byte(cfg.Transport.BotToken)) {
		t.Fatal("токен бота попал в комплект открытым текстом")
	}
	plain, err := provisioning.Open(sealed, activationCode)
	if err != nil {
		t.Fatalf("файл активации не открылся: %v", err)
	}
	if _, err := config.DecodeClient(bytes.NewReader(plain)); err != nil {
		t.Fatalf("в комплекте повреждены настройки точки: %v", err)
	}
	if len(entries[folder+"ПРОЧИТАТЬ ПЕРЕД ЗАПУСКОМ.txt"]) == 0 {
		t.Fatal("в комплекте отсутствует памятка оператору")
	}
}
