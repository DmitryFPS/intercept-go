package telegram

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

const (
	maxTelegramStateBytes       = 64 << 20
	maxTelegramActiveItems      = 100_000
	maxTelegramInactiveItems    = 4_096
	maxTelegramRecordTombstones = 100_000
)

type destinationState struct {
	Scope            string                                `json:"scope,omitempty"`
	Items            map[string]destinationItem            `json:"items"`
	RecordTombstones map[string]destinationRecordTombstone `json:"record_tombstones,omitempty"`
}

type destinationRecordTombstone struct {
	SourceVersion string    `json:"source_version,omitempty"`
	SourceEventAt time.Time `json:"source_event_at,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type destinationItem struct {
	ThreadID                   int64                       `json:"thread_id"`
	Blocks                     map[string]destinationBlock `json:"blocks"`
	RecordCleanupSourceVersion string                      `json:"record_cleanup_source_version,omitempty"`
	RecordCleanupSourceEventAt time.Time                   `json:"record_cleanup_source_event_at,omitempty"`
	UpdatedAt                  time.Time                   `json:"updated_at"`
}

type destinationBlock struct {
	Hash              string    `json:"hash"`
	SourceHash        string    `json:"source_hash,omitempty"`
	SourceVersion     string    `json:"source_version,omitempty"`
	SourceEventAt     time.Time `json:"source_event_at,omitempty"`
	ThreadID          int64     `json:"thread_id,omitempty"`
	MessageIDs        []int64   `json:"message_ids"`
	DeletedMessageIDs []int64   `json:"deleted_message_ids,omitempty"`
	Sequence          int       `json:"sequence"`
	Recovering        bool      `json:"recovering,omitempty"`
	Suppressed        bool      `json:"suppressed,omitempty"`
	SuppressedAt      string    `json:"suppressed_at,omitempty"`
	SuppressedReason  string    `json:"suppressed_reason,omitempty"`
	SourceDeleted     bool      `json:"source_deleted,omitempty"`
	SendPending       bool      `json:"send_pending,omitempty"`
	SendPendingIndex  int       `json:"send_pending_index,omitempty"`
	SendPendingAt     string    `json:"send_pending_at,omitempty"`
}

func loadDestinationState(path string) (destinationState, error) {
	current := destinationState{
		Items: make(map[string]destinationItem), RecordTombstones: make(map[string]destinationRecordTombstone),
	}
	if info, err := os.Stat(path); err == nil && info.Size() > maxTelegramStateBytes {
		return current, telegramStateLimitError("файл состояния Telegram превышает 64 МиБ")
	}
	payload, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return current, nil
	}
	if err != nil {
		return current, fmt.Errorf("прочитать состояние Telegram: %w", err)
	}
	if err := json.Unmarshal(payload, &current); err != nil {
		return current, errors.New("состояние Telegram повреждено")
	}
	if current.Items == nil {
		current.Items = make(map[string]destinationItem)
	}
	if current.RecordTombstones == nil {
		current.RecordTombstones = make(map[string]destinationRecordTombstone)
	}
	legacyTime := time.Now().UTC()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyTime = info.ModTime().UTC()
	}
	for key, item := range current.Items {
		migrateTelegramRecordTombstone(&current, key, &item, legacyTime)
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = legacyTime
		}
		current.Items[key] = item
	}
	compactTelegramInactiveItems(&current)
	if err := enforceTelegramActiveItemLimit(current, maxTelegramActiveItems); err != nil {
		return destinationState{Items: make(map[string]destinationItem), RecordTombstones: make(map[string]destinationRecordTombstone)}, err
	}
	if err := enforceTelegramRecordTombstoneLimit(current, maxTelegramRecordTombstones); err != nil {
		return destinationState{Items: make(map[string]destinationItem), RecordTombstones: make(map[string]destinationRecordTombstone)}, err
	}
	return current, nil
}

func saveDestinationState(path string, current destinationState) error {
	if current.RecordTombstones == nil {
		current.RecordTombstones = make(map[string]destinationRecordTombstone)
	}
	for key, item := range current.Items {
		migrateTelegramRecordTombstone(&current, key, &item, item.UpdatedAt)
		current.Items[key] = item
	}
	compactTelegramInactiveItems(&current)
	if err := enforceTelegramActiveItemLimit(current, maxTelegramActiveItems); err != nil {
		return err
	}
	if err := enforceTelegramRecordTombstoneLimit(current, maxTelegramRecordTombstones); err != nil {
		return err
	}
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("создать папку состояния Telegram: %w", err)
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Telegram: %w", err)
	}
	if len(payload) > maxTelegramStateBytes {
		return telegramStateLimitError("файл состояния Telegram превысит 64 МиБ")
	}
	temporary, err := os.CreateTemp(directory, ".telegram-state-*.tmp")
	if err != nil {
		return fmt.Errorf("создать временное состояние Telegram: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		temporary.Close()
		return fmt.Errorf("записать состояние Telegram: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("закрыть состояние Telegram: %w", err)
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return fmt.Errorf("зафиксировать состояние Telegram: %w", err)
	}
	return nil
}

func telegramStateLimitError(message string) error {
	return application.NewPermanentDeliveryError(errors.New(message))
}

// Неактивные записи не содержат Telegram message_id и не требуют продолжения
// незавершённой публикации. Для защиты от поздних повторов сохраняются последние
// записи; активные связи никогда не вытесняются молча.
func telegramItemInactive(item destinationItem) bool {
	if len(item.Blocks) == 0 {
		return true
	}
	for _, block := range item.Blocks {
		if len(block.MessageIDs) > 0 || block.Recovering || (!block.SourceDeleted && !block.Suppressed) {
			return false
		}
	}
	return true
}

func enforceTelegramActiveItemLimit(current destinationState, limit int) error {
	if limit >= 0 && telegramActiveItemCount(current) > limit {
		return telegramStateLimitError(fmt.Sprintf(
			"число активных записей состояния Telegram превышает %d", limit,
		))
	}
	return nil
}

func enforceTelegramRecordTombstoneLimit(current destinationState, limit int) error {
	if limit >= 0 && len(current.RecordTombstones) > limit {
		return telegramStateLimitError(fmt.Sprintf(
			"число барьеров удаления записей Telegram превышает %d", limit,
		))
	}
	return nil
}

func migrateTelegramRecordTombstone(
	current *destinationState, key string, item *destinationItem, fallback time.Time,
) {
	if current == nil || item == nil ||
		(item.RecordCleanupSourceVersion == "" && item.RecordCleanupSourceEventAt.IsZero()) {
		return
	}
	if current.RecordTombstones == nil {
		current.RecordTombstones = make(map[string]destinationRecordTombstone)
	}
	incoming := destinationRecordTombstone{
		SourceVersion: item.RecordCleanupSourceVersion,
		SourceEventAt: item.RecordCleanupSourceEventAt,
		UpdatedAt:     item.UpdatedAt,
	}
	if incoming.UpdatedAt.IsZero() {
		incoming.UpdatedAt = fallback
	}
	stored, exists := current.RecordTombstones[key]
	if !exists || domain.CompareSourceRevision(
		incoming.SourceVersion, incoming.SourceEventAt,
		stored.SourceVersion, stored.SourceEventAt,
	) > 0 {
		current.RecordTombstones[key] = incoming
	}
	item.RecordCleanupSourceVersion = ""
	item.RecordCleanupSourceEventAt = time.Time{}
}

func telegramActiveItemCount(current destinationState) int {
	count := 0
	for _, item := range current.Items {
		if !telegramItemInactive(item) {
			count++
		}
	}
	return count
}

func compactTelegramInactiveItems(current *destinationState) {
	if current == nil {
		return
	}
	type candidate struct {
		key string
		at  time.Time
	}
	candidates := make([]candidate, 0)
	for key, item := range current.Items {
		if telegramItemInactive(item) {
			candidates = append(candidates, candidate{key: key, at: item.UpdatedAt})
		}
	}
	if len(candidates) <= maxTelegramInactiveItems {
		return
	}
	sort.SliceStable(candidates, func(left, right int) bool {
		if candidates[left].at.Equal(candidates[right].at) {
			return candidates[left].key < candidates[right].key
		}
		return candidates[left].at.Before(candidates[right].at)
	})
	for _, item := range candidates[:len(candidates)-maxTelegramInactiveItems] {
		delete(current.Items, item.key)
	}
}
