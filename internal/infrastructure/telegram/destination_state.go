package telegram

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

type destinationState struct {
	Items map[string]destinationItem `json:"items"`
}

type destinationItem struct {
	ThreadID  int64                       `json:"thread_id"`
	Blocks    map[string]destinationBlock `json:"blocks"`
	UpdatedAt time.Time                   `json:"updated_at"`
}

type destinationBlock struct {
	Hash       string  `json:"hash"`
	MessageIDs []int64 `json:"message_ids"`
	Sequence   int     `json:"sequence"`
}

func loadDestinationState(path string) (destinationState, error) {
	current := destinationState{Items: make(map[string]destinationItem)}
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
	legacyTime := time.Now().UTC()
	if info, statErr := os.Stat(path); statErr == nil {
		legacyTime = info.ModTime().UTC()
	}
	cutoff := time.Now().UTC().Add(-30 * 24 * time.Hour)
	for key, item := range current.Items {
		if item.UpdatedAt.IsZero() {
			item.UpdatedAt = legacyTime
			current.Items[key] = item
		}
		if item.UpdatedAt.Before(cutoff) {
			delete(current.Items, key)
		}
	}
	return current, nil
}

func saveDestinationState(path string, current destinationState) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("создать папку состояния Telegram: %w", err)
	}
	payload, err := json.MarshalIndent(current, "", "  ")
	if err != nil {
		return fmt.Errorf("кодировать состояние Telegram: %w", err)
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
