package runner

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/provisioning"
)

func TestServerUpdateValidationDoesNotStartProcessing(t *testing.T) {
	server, err := NewServer(context.Background(), t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.ValidateForUpdate(); err != nil {
		t.Fatal(err)
	}
	status := server.Status()
	if status.State != "paused" || status.Processing || status.Configured {
		t.Fatalf("неожиданное состояние изолированной проверки: %+v", status)
	}
}

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
	updaterTemplate := filepath.Join(baseDir, "station-template", "EAVC Connect Updater.exe")
	updater := []byte("test-updater-executable")
	if err := os.WriteFile(updaterTemplate, updater, 0o600); err != nil {
		t.Fatal(err)
	}
	publicKey := writeSignedStationTemplate(t, filepath.Dir(stationTemplate), "test")

	ctx := context.Background()
	server, err := NewServer(ctx, baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	server.UpdatePublicKey = publicKey

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
	if !bytes.Equal(entries[folder+"EAVC Connect Updater.exe"], updater) {
		t.Fatal("в комплекте отсутствует помощник безопасного обновления")
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

func TestClientPackageRejectsTamperedStationTemplate(t *testing.T) {
	root := filepath.Join(t.TempDir(), "station-template")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"EAVC Connect Station.exe": "station", "EAVC Connect Updater.exe": "updater",
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	publicKey := writeSignedStationTemplate(t, root, "test")
	if err := os.WriteFile(filepath.Join(root, "EAVC Connect Station.exe"), []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &Server{BaseDir: filepath.Dir(root), Version: "test", UpdatePublicKey: publicKey}
	if _, err := server.verifiedStationTemplate(); err == nil || !strings.Contains(err.Error(), "целостность") {
		t.Fatalf("tampered station template must be rejected: %v", err)
	}
}

func writeSignedStationTemplate(t *testing.T, root, version string) string {
	t.Helper()
	manifest := stationTemplateManifest{Schema: 1, Product: "eavc-connect-station-template", Version: version}
	for _, name := range []string{"EAVC Connect Station.exe", "EAVC Connect Updater.exe"} {
		payload, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(payload)
		manifest.Files = append(manifest.Files, struct {
			Path   string `json:"path"`
			Size   int64  `json:"size"`
			SHA256 string `json:"sha256"`
		}{Path: name, Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])})
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), payload, 0o600); err != nil {
		t.Fatal(err)
	}
	signature := base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, payload))
	if err := os.WriteFile(filepath.Join(root, "manifest.sig"), []byte(signature), 0o600); err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(publicKey)
}
