package docx

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
)

//go:embed default_word_template.docx
var defaultTemplate []byte

// RestoreDefaultTemplate возвращает стандартный DOCX в указанное место.
func RestoreDefaultTemplate(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("создать папку стандартного шаблона Word: %w", err)
	}
	if err := os.WriteFile(path, defaultTemplate, 0o600); err != nil {
		return fmt.Errorf("восстановить стандартный шаблон Word: %w", err)
	}
	return nil
}
