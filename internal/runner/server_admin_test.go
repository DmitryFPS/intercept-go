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
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
	"intercept/internal/provisioning"
)

func TestPublishRulesChecksChangedTopicRedirectsOnly(t *testing.T) {
	ctx := context.Background()
	db, err := stateDB.OpenState(filepath.Join(t.TempDir(), "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store, err := stateDB.NewServerStore(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	key := "name:" + domain.NormalizeUnitName("Позиция")
	rules := domain.RuleSet{
		Version: 1,
		Destinations: map[string]domain.Destination{
			"a": {ID: "a", Name: "Группа", Kind: domain.DestinationTelegram, Enabled: true},
		},
		Positions: map[string]map[string]domain.RouteDecision{
			key: {"a": {Action: domain.RouteDeliver, ThreadID: 2}},
		},
		Unknown: map[string]domain.RouteDecision{"a": {Action: domain.RouteReview}},
	}
	if err := store.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	server := &Server{Store: store, config: config.Server{Destinations: map[string]config.DestinationConfig{
		"a": {Kind: domain.DestinationTelegram, BotToken: "123:token", ChatID: -1001},
	}}}
	var checked []int64
	server.topicProbe = func(_ context.Context, destination config.DestinationConfig, threadID int64) error {
		if destination.BotToken != "123:token" || destination.ChatID != -1001 {
			t.Fatalf("проверка использует чужого бота или чат: %#v", destination)
		}
		checked = append(checked, threadID)
		if threadID == 40 {
			return errors.New("тема недоступна")
		}
		return nil
	}
	rules.TopicRedirects = map[string]domain.TopicRedirect{
		key: {DestinationID: "a", ThreadID: 39, TopicName: "Доразведка"},
	}
	if err := server.SaveRuleDraft(ctx, rules); err != nil {
		t.Fatal(err)
	}
	if len(checked) != 0 {
		t.Fatal("сохранение черновика вызвало проверку Telegram")
	}
	if err := server.PublishRules(ctx, rules, "test"); err != nil {
		t.Fatal(err)
	}
	if len(checked) != 1 || checked[0] != 39 {
		t.Fatalf("новое перенаправление не проверено ровно один раз: %v", checked)
	}
	active, err := store.ActiveRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := server.PublishRules(ctx, active, "test"); err != nil {
		t.Fatal(err)
	}
	if len(checked) != 1 {
		t.Fatalf("неизменное перенаправление проверено повторно: %v", checked)
	}
	active, err = store.ActiveRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	active.TopicRedirects[key] = domain.TopicRedirect{DestinationID: "a", ThreadID: 39, TopicName: "Новое название"}
	if err := server.PublishRules(ctx, active, "test"); err != nil {
		t.Fatal(err)
	}
	if len(checked) != 1 {
		t.Fatalf("переименование метки вызвало проверку Telegram: %v", checked)
	}
	active, err = store.ActiveRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	active.TopicRedirects[key] = domain.TopicRedirect{DestinationID: "a", ThreadID: 40, TopicName: "Другая тема"}
	if err := server.SaveRuleDraft(ctx, active); err != nil {
		t.Fatal(err)
	}
	if err := server.PublishRules(ctx, active, "test"); err == nil {
		t.Fatal("недоступная тема опубликована")
	}
	if len(checked) != 2 || checked[1] != 40 {
		t.Fatalf("изменённая тема не проверена: %v", checked)
	}
	published, err := store.ActiveRules(ctx)
	if err != nil || published.TopicRedirects[key].ThreadID != 39 {
		t.Fatalf("ошибка проверки изменила опубликованный маршрут: %#v, %v", published.TopicRedirects, err)
	}
	draft, exists, err := store.RuleDraft(ctx)
	if err != nil || !exists || draft.TopicRedirects[key].ThreadID != 40 {
		t.Fatalf("ошибка проверки потеряла черновик: %#v, %v", draft.TopicRedirects, err)
	}
}

func TestRuleDestinationKindMustMatchConfiguredTransport(t *testing.T) {
	server := &Server{config: config.Server{Destinations: map[string]config.DestinationConfig{
		"word-39": {Kind: domain.DestinationWord},
	}}}
	rules := domain.RuleSet{Destinations: map[string]domain.Destination{
		"word-39": {ID: "word-39", Name: "39-ка", Kind: domain.DestinationTelegram, Enabled: true},
	}}
	if err := server.validateRuleDestinations(rules); err == nil {
		t.Fatal("маршрут не должен выдавать Word-получателя за Telegram General")
	}
}

func TestDestinationWordTemplateRejectsChangedUnsafeFile(t *testing.T) {
	original, err := os.ReadFile(filepath.Join("..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "selected.docx")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}
	server := &Server{config: config.Server{Destinations: map[string]config.DestinationConfig{
		"word": {Kind: domain.DestinationWord, Template: path},
	}}}
	got, filename, err := server.DestinationWordTemplate("word")
	if err != nil || filename != "selected.docx" || !bytes.Equal(got, original) {
		t.Fatalf("первоначальный безопасный шаблон не скачан: filename=%q err=%v", filename, err)
	}

	for name, replacements := range map[string]map[string][]byte{
		"macro": {"word/vbaProject.bin": []byte("active")},
		"external relationship": {
			"word/_rels/document.xml.rels": []byte(`<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId9" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/attachedTemplate" Target="https://example.invalid/template.dotm" TargetMode="External"/></Relationships>`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := modifiedWordTemplateForDownloadTest(t, original, replacements)
			if err := os.WriteFile(path, changed, 0o600); err != nil {
				t.Fatal(err)
			}
			payload, _, err := server.DestinationWordTemplate("word")
			if err == nil || len(payload) != 0 {
				t.Fatalf("заменённый небезопасный шаблон выдан: bytes=%d err=%v", len(payload), err)
			}
		})
	}
}

func TestDestinationWordTemplateRetains64MiBDownloadLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "oversized.docx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(64*1024*1024 + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	server := &Server{config: config.Server{Destinations: map[string]config.DestinationConfig{
		"word": {Kind: domain.DestinationWord, Template: path},
	}}}
	if payload, _, err := server.DestinationWordTemplate("word"); err == nil || len(payload) != 0 || !strings.Contains(err.Error(), "слишком велик") {
		t.Fatalf("ограничение 64 МиБ не соблюдено: bytes=%d err=%v", len(payload), err)
	}
}

func modifiedWordTemplateForDownloadTest(t *testing.T, original []byte, replacements map[string][]byte) []byte {
	t.Helper()
	archive, err := zip.NewReader(bytes.NewReader(original), int64(len(original)))
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	written := make(map[string]bool, len(archive.File))
	for _, item := range archive.File {
		payload, replaced := replacements[item.Name]
		if !replaced {
			reader, err := item.Open()
			if err != nil {
				t.Fatal(err)
			}
			payload, err = io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		part, err := writer.Create(item.Name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(payload); err != nil {
			t.Fatal(err)
		}
		written[item.Name] = true
	}
	for name, payload := range replacements {
		if written[name] {
			continue
		}
		part, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := part.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

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

func TestServerUpdateActivationStartsBackgroundLoops(t *testing.T) {
	server, err := NewServer(context.Background(), t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.Store.SaveClient(context.Background(), application.RegisteredClient{
		ID: "point-01", Number: "01", Name: "Текстиль", SenderBotID: 101,
		SenderBotUsername: "point_bot", ChatID: -1001, ThreadID: 2,
		KeyID: "key-01", Key: make([]byte, 32), Active: true,
	}); err != nil {
		t.Fatal(err)
	}
	root := server.ensureRootLifecycle(context.Background())
	if repeated := server.ensureRootLifecycle(context.Background()); repeated != root {
		t.Fatal("повторная активация создала второй корневой жизненный цикл")
	}
	deadline := time.Now().Add(2 * time.Second)
	for server.Status().Stats.Clients != 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if clients := server.Status().Stats.Clients; clients != 1 {
		t.Fatalf("фоновая статистика после активации не запустилась: clients=%d", clients)
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
