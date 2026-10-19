package docx

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var badFilename = regexp.MustCompile(`[<>:"/\\|?*]`)

type state struct {
	LastNumber int                    `json:"last_number"`
	Blocks     map[string]blockRecord `json:"blocks"`
}

type blockRecord struct {
	Number    int       `json:"number"`
	Hash      string    `json:"hash"`
	Render    string    `json:"render,omitempty"`
	Filename  string    `json:"filename"`
	Edited    bool      `json:"edited,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

func loadState(path string) (state, error) {
	current := state{Blocks: make(map[string]blockRecord)}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return current, nil
	}
	if err != nil {
		return current, fmt.Errorf("прочитать состояние Word: %w", err)
	}
	if err := json.Unmarshal(payload, &current); err != nil {
		return current, errors.New("состояние Word повреждено")
	}
	if current.Blocks == nil {
		current.Blocks = make(map[string]blockRecord)
	}
	legacyTime := time.Now().UTC()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyTime = info.ModTime().UTC()
	}
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for key, record := range current.Blocks {
		if record.UpdatedAt.IsZero() {
			record.UpdatedAt = legacyTime
			current.Blocks[key] = record
		}
		if record.UpdatedAt.Before(cutoff) {
			delete(current.Blocks, key)
		}
	}
	return current, nil
}

func saveState(path string, current state) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("создать папку состояния Word: %w", err)
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Word: %w", err)
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, payload, 0o600); err != nil {
		return fmt.Errorf("сохранить временное состояние Word: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("зафиксировать состояние Word: %w", err)
	}
	return nil
}

func writeAtomic(ctx context.Context, path string, payload []byte) error {
	temporary := path + ".tmp"
	defer os.Remove(temporary)
	if err := os.WriteFile(temporary, payload, 0o644); err != nil {
		return fmt.Errorf("записать временный Word: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("доставка Word отменена до публикации файла: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("доставить Word: %w", err)
	}
	return nil
}

func (destination *Destination) documentName(number int, unit string) string {
	prefix := safeFilenamePart(destination.Prefix, "СТНД")
	unit = safeFilenamePart(unit, "н-у")
	network := safeFilenamePart(destination.Network, "получатель")
	return fmt.Sprintf("%s%d %s - %s.docx", prefix, number, unit, network)
}

func safeFilenamePart(value, fallback string) string {
	value = strings.Join(strings.Fields(badFilename.ReplaceAllString(value, "")), " ")
	value = strings.Trim(value, ". ")
	if value == "" {
		return fallback
	}
	return value
}

func safeDestinationPath(folder, filename string) (string, error) {
	if filename == "" || filepath.Base(filename) != filename || filename == "." || filename == ".." {
		return "", errors.New("небезопасное имя Word-документа")
	}
	return filepath.Join(folder, filename), nil
}

func normalizeText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}

func digestText(value string) string {
	digest := sha256.Sum256([]byte(normalizeText(value)))
	return fmt.Sprintf("%x", digest[:])
}

// StateNumbers возвращает номера документов для диагностики, не раскрывая их содержимое.
func StateNumbers(path string) ([]int, error) {
	current, err := loadState(path)
	if err != nil {
		return nil, err
	}
	result := make([]int, 0, len(current.Blocks))
	for _, record := range current.Blocks {
		result = append(result, record.Number)
	}
	sort.Ints(result)
	return result, nil
}
