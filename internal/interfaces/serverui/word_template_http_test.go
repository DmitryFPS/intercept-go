package serverui

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"intercept/internal/config"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/secrets"
	"intercept/internal/runner"
)

func TestWordTemplateHTTPRejectsFileChangedAfterConfiguration(t *testing.T) {
	baseDir := t.TempDir()
	original, err := os.ReadFile(filepath.Join("..", "..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	templatePath := filepath.Join(baseDir, "selected.docx")
	if err := os.WriteFile(templatePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Server{
		Schema: config.ServerSchemaV1, Listen: "127.0.0.1:17632",
		Transport: config.ServerTransport{
			BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE", BotID: 1,
			BotUsername: "test_bot", ChatID: -1001,
		},
		Destinations: map[string]config.DestinationConfig{
			"word": {Kind: domain.DestinationWord, Template: templatePath, Folder: baseDir},
		},
	}
	configBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := secrets.Save(filepath.Join(baseDir, "data", "server-config.dat"), configBytes); err != nil {
		t.Fatal(err)
	}
	server, err := runner.NewServer(context.Background(), baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	if err := server.ValidateForUpdate(); err != nil {
		t.Fatal(err)
	}
	handler := Handler{Server: server}.Routes()
	getTemplate := func() *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/destinations/word/word-template", nil))
		return response
	}
	valid := getTemplate()
	if valid.Code != http.StatusOK || !bytes.Equal(valid.Body.Bytes(), original) {
		t.Fatalf("исходный шаблон не скачан: status=%d bytes=%d", valid.Code, valid.Body.Len())
	}
	const marker = "substituted-unsafe-template"
	if err := os.WriteFile(templatePath, []byte(marker), 0o600); err != nil {
		t.Fatal(err)
	}
	changed := getTemplate()
	if changed.Code != http.StatusBadRequest || strings.Contains(changed.Body.String(), marker) ||
		changed.Header().Get("Content-Type") == "application/vnd.openxmlformats-officedocument.wordprocessingml.document" {
		t.Fatalf("подменённый шаблон выдан через HTTP: status=%d content-type=%q body=%q",
			changed.Code, changed.Header().Get("Content-Type"), changed.Body.String())
	}
}
