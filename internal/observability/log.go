package observability

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

var telegramTokenPattern = regexp.MustCompile(`\b[0-9]{6,}:[A-Za-z0-9_-]{20,}\b`)

// RotatingWriter ограничивает размер журналов без фоновых процессов.
type RotatingWriter struct {
	Path     string
	MaxBytes int64
	Backups  int

	mu   sync.Mutex
	file *os.File
	size int64
}

// Write добавляет запись и заранее ограничивает размер текущего журнала.
func (writer *RotatingWriter) Write(payload []byte) (int, error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.MaxBytes <= 0 {
		writer.MaxBytes = 10 * 1024 * 1024
	}
	if writer.Backups <= 0 {
		writer.Backups = 5
	}
	if writer.file == nil {
		if err := writer.open(); err != nil {
			return 0, err
		}
	}
	if writer.size+int64(len(payload)) > writer.MaxBytes {
		if err := writer.rotate(); err != nil {
			return 0, err
		}
	}
	written, err := writer.file.Write(payload)
	writer.size += int64(written)
	return written, err
}

// Close закрывает файл журнала при штатной остановке программы.
func (writer *RotatingWriter) Close() error {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	if writer.file == nil {
		return nil
	}
	err := writer.file.Close()
	writer.file = nil
	return err
}

func (writer *RotatingWriter) open() error {
	if err := os.MkdirAll(filepath.Dir(writer.Path), 0o755); err != nil {
		return fmt.Errorf("создать папку журналов: %w", err)
	}
	file, err := os.OpenFile(writer.Path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("открыть журнал: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("проверить журнал: %w", err)
	}
	writer.file = file
	writer.size = info.Size()
	return nil
}

func (writer *RotatingWriter) rotate() error {
	if writer.file != nil {
		if err := writer.file.Close(); err != nil {
			return err
		}
		writer.file = nil
	}
	for index := writer.Backups - 1; index >= 1; index-- {
		oldPath := fmt.Sprintf("%s.%d", writer.Path, index)
		newPath := fmt.Sprintf("%s.%d", writer.Path, index+1)
		if _, err := os.Stat(oldPath); err == nil {
			_ = os.Rename(oldPath, newPath)
		}
	}
	if _, err := os.Stat(writer.Path); err == nil {
		if err := os.Rename(writer.Path, writer.Path+".1"); err != nil {
			return err
		}
	}
	writer.size = 0
	return writer.open()
}

// NewLogger создаёт JSON-журнал для диагностики.
func NewLogger(path, component string) (*slog.Logger, io.Closer, error) {
	writer := &RotatingWriter{Path: path, MaxBytes: 10 * 1024 * 1024, Backups: 5}
	if err := writer.open(); err != nil {
		return nil, nil, err
	}
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: slog.LevelInfo})
	return slog.New(&redactingHandler{next: handler}).With("component", component), writer, nil
}

type redactingHandler struct{ next slog.Handler }

// Enabled передаёт проверку уровня базовому обработчику журнала.
func (handler *redactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return handler.next.Enabled(ctx, level)
}

// Handle скрывает секреты во всех полях перед записью на диск.
func (handler *redactingHandler) Handle(ctx context.Context, record slog.Record) error {
	clean := slog.NewRecord(record.Time, record.Level, redact(record.Message), record.PC)
	record.Attrs(func(attribute slog.Attr) bool {
		clean.AddAttrs(redactAttribute(attribute))
		return true
	})
	return handler.next.Handle(ctx, clean)
}

// WithAttrs очищает постоянные поля до их привязки к журналу.
func (handler *redactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, len(attrs))
	for _, attribute := range attrs {
		clean = append(clean, redactAttribute(attribute))
	}
	return &redactingHandler{next: handler.next.WithAttrs(clean)}
}

func redactAttribute(attribute slog.Attr) slog.Attr {
	name := strings.ToLower(attribute.Key)
	if strings.Contains(name, "token") || strings.Contains(name, "password") || strings.Contains(name, "key") {
		return slog.String(attribute.Key, "***")
	}
	if attribute.Value.Kind() == slog.KindString {
		return slog.String(attribute.Key, redact(attribute.Value.String()))
	}
	if attribute.Value.Kind() == slog.KindGroup {
		children := attribute.Value.Group()
		clean := make([]any, 0, len(children))
		for _, child := range children {
			clean = append(clean, redactAttribute(child))
		}
		return slog.Group(attribute.Key, clean...)
	}
	return attribute
}

// WithGroup сохраняет стандартное группирование структурированных полей.
func (handler *redactingHandler) WithGroup(name string) slog.Handler {
	return &redactingHandler{next: handler.next.WithGroup(name)}
}

func redact(value string) string {
	value = telegramTokenPattern.ReplaceAllString(value, "***")
	fields := strings.Fields(value)
	for index, field := range fields {
		if colon := strings.IndexByte(field, ':'); colon >= 6 && colon < len(field)-20 {
			allDigits := true
			for _, char := range field[:colon] {
				if char < '0' || char > '9' {
					allDigits = false
					break
				}
			}
			if allDigits {
				fields[index] = "***"
			}
		}
	}
	return strings.Join(fields, " ")
}
