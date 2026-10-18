package observability

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Журнал не должен раскрывать токены и ключи, а старые записи должны переноситься в ограниченное число архивов.
func TestLoggerRedactsSecretsAndRotates(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "app.log")
	writer := &RotatingWriter{Path: path, MaxBytes: 100, Backups: 2}
	handler := slog.NewJSONHandler(writer, nil)
	logger := slog.New(&redactingHandler{next: handler})
	logger.Info("ошибка 123456:abcdefghijklmnopqrstuvwxyzABCDE", "bot_token", "secret")
	logger.Info(strings.Repeat("x", 120))
	writer.Close()
	for _, candidate := range []string{path, path + ".1"} {
		payload, _ := os.ReadFile(candidate)
		if strings.Contains(string(payload), "abcdefghijklmnopqrstuvwxyz") || strings.Contains(string(payload), "secret") {
			t.Fatal("секрет попал в журнал")
		}
	}
}
