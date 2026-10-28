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

	"intercept/internal/application"
)

const (
	maxWordStateBytes       = 64 << 20
	maxWordActiveBlocks     = 100_000
	maxWordRecordTombstones = 100_000
)

var badFilename = regexp.MustCompile(`[<>:"/\\|?*]`)

type state struct {
	Scope            string                           `json:"scope,omitempty"`
	LastNumber       int                              `json:"last_number"`
	Blocks           map[string]blockRecord           `json:"blocks"`
	RecordTombstones map[string]recordRevisionBarrier `json:"record_tombstones,omitempty"`
}

type recordRevisionBarrier struct {
	SourceVersion string    `json:"source_version,omitempty"`
	SourceEventAt time.Time `json:"source_event_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type blockRecord struct {
	Number        int       `json:"number"`
	Hash          string    `json:"hash"`
	Render        string    `json:"render,omitempty"`
	Filename      string    `json:"filename"`
	Edited        bool      `json:"edited,omitempty"`
	Tombstone     bool      `json:"tombstone,omitempty"`
	SourceVersion string    `json:"source_version,omitempty"`
	SourceEventAt time.Time `json:"source_event_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func loadState(path string) (state, error) {
	current := state{Blocks: make(map[string]blockRecord)}
	if info, err := os.Stat(path); err == nil && info.Size() > maxWordStateBytes {
		return current, wordStateLimitError("файл состояния Word превышает 64 МиБ")
	}
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
	if current.RecordTombstones == nil {
		current.RecordTombstones = make(map[string]recordRevisionBarrier)
	}
	if err := enforceWordActiveBlockLimit(current, maxWordActiveBlocks); err != nil {
		return state{Blocks: make(map[string]blockRecord), RecordTombstones: make(map[string]recordRevisionBarrier)}, err
	}
	if err := enforceWordRecordTombstoneLimit(current, maxWordRecordTombstones); err != nil {
		return state{Blocks: make(map[string]blockRecord), RecordTombstones: make(map[string]recordRevisionBarrier)}, err
	}
	legacyTime := time.Now().UTC()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyTime = info.ModTime().UTC()
	}
	for key, record := range current.Blocks {
		if record.UpdatedAt.IsZero() {
			record.UpdatedAt = legacyTime
			current.Blocks[key] = record
		}
	}
	return current, nil
}

func saveState(path string, current state) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("создать папку состояния Word: %w", err)
	}
	compactWordTombstones(&current)
	if err := enforceWordActiveBlockLimit(current, maxWordActiveBlocks); err != nil {
		return err
	}
	if err := enforceWordRecordTombstoneLimit(current, maxWordRecordTombstones); err != nil {
		return err
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Word: %w", err)
	}
	if len(payload) > maxWordStateBytes {
		return wordStateLimitError("файл состояния Word превысит 64 МиБ")
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

func wordStateLimitError(message string) error {
	return application.NewPermanentDeliveryError(errors.New(message))
}

func enforceWordActiveBlockLimit(current state, limit int) error {
	active := 0
	for _, record := range current.Blocks {
		if !record.Tombstone {
			active++
		}
	}
	if limit >= 0 && active > limit {
		return wordStateLimitError(fmt.Sprintf("число активных блоков состояния Word превышает %d", limit))
	}
	return nil
}

func enforceWordRecordTombstoneLimit(current state, limit int) error {
	if limit >= 0 && len(current.RecordTombstones) > limit {
		return application.NewPermanentDeliveryError(fmt.Errorf(
			"число барьеров удаления записей Word превышает %d", limit,
		))
	}
	return nil
}

func compactWordTombstones(current *state) {
	const maxWordTombstones = 4096
	if current == nil {
		return
	}
	type candidate struct {
		key string
		at  time.Time
	}
	var tombstones []candidate
	for key, record := range current.Blocks {
		if record.Tombstone {
			tombstones = append(tombstones, candidate{key: key, at: record.UpdatedAt})
		}
	}
	if len(tombstones) <= maxWordTombstones {
		return
	}
	sort.SliceStable(tombstones, func(left, right int) bool {
		return tombstones[left].at.Before(tombstones[right].at)
	})
	for _, item := range tombstones[:len(tombstones)-maxWordTombstones] {
		delete(current.Blocks, item.key)
	}
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
		if !record.Tombstone && record.Number > 0 {
			result = append(result, record.Number)
		}
	}
	sort.Ints(result)
	return result, nil
}
