package architecture_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Пользовательские экраны получают только заранее сформулированные сообщения.
// Полная причина ошибки остаётся в журнале и диагностическом архиве.
func TestUIHandlersDoNotExposeInternalErrors(t *testing.T) {
	t.Parallel()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, relative := range []string{
		"internal/interfaces/clientui/handler.go",
		"internal/interfaces/serverui/handler.go",
	} {
		payload, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(relative)))
		if err != nil {
			t.Fatalf("прочитать %s: %v", relative, err)
		}
		if strings.Contains(string(payload), "err.Error()") {
			t.Errorf("%s напрямую передаёт техническую ошибку в интерфейс", relative)
		}
	}
}
