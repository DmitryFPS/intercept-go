package telegram

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"intercept/internal/domain"
)

// Destination синхронизирует блоки одного перехвата с Telegram-сообщениями.
type Destination struct {
	API             *Client
	ChatID          int64
	Network         string
	MessageTemplate string
	StatePath       string
	Logger          *slog.Logger

	mu    sync.Mutex
	locks map[string]*itemLockEntry
}

type itemLockEntry struct {
	mutex sync.Mutex
	refs  int
}

// Deliver создаёт, редактирует и удаляет сообщения так, чтобы Telegram отражал
// текущее состояние записи EAVC без дубликатов.
func (destination *Destination) Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error {
	if destination.API == nil || destination.ChatID == 0 || strings.TrimSpace(destination.StatePath) == "" {
		return errors.New("Telegram-направление настроено не полностью")
	}
	itemKey := strings.TrimSpace(event.Source.UUID)
	if itemKey == "" {
		itemKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	unlockItem := destination.lockItem(itemKey)
	defer unlockItem()
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return err
	}
	if item.Blocks == nil {
		item.Blocks = make(map[string]destinationBlock)
	}
	threadID := delivery.Decision.ThreadID
	if item.ThreadID != 0 && item.ThreadID != threadID {
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		item.Blocks = make(map[string]destinationBlock)
	}
	item.ThreadID = threadID

	blocks := append([]domain.ContentBlock(nil), event.Blocks...)
	if blocks == nil {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	sort.SliceStable(blocks, func(left, right int) bool { return blocks[left].Sequence < blocks[right].Sequence })
	present := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		present[block.Key] = true
		chunks := formatBlockChunks(event, block, delivery, destination.Network, destination.MessageTemplate)
		renderHash := hashChunks(chunks)
		stored := item.Blocks[block.Key]
		if stored.Hash == renderHash && len(stored.MessageIDs) == len(chunks) && len(chunks) > 0 {
			continue
		}
		messageIDs, err := destination.upsertBlock(ctx, threadID, stored.MessageIDs, chunks)
		if err != nil {
			return fmt.Errorf("синхронизировать блок %s: %w", block.Key, err)
		}
		item.Blocks[block.Key] = destinationBlock{
			Hash: renderHash, MessageIDs: messageIDs, Sequence: block.Sequence,
		}
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	for key, stored := range item.Blocks {
		if present[key] {
			continue
		}
		if err := destination.deleteMessages(ctx, stored.MessageIDs); err != nil {
			return fmt.Errorf("удалить исчезнувший блок %s: %w", key, err)
		}
		delete(item.Blocks, key)
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	return destination.saveItem(itemKey, item, len(item.Blocks) == 0)
}

func (destination *Destination) lockItem(key string) func() {
	destination.mu.Lock()
	if destination.locks == nil {
		destination.locks = make(map[string]*itemLockEntry)
	}
	entry := destination.locks[key]
	if entry == nil {
		entry = &itemLockEntry{}
		destination.locks[key] = entry
	}
	entry.refs++
	destination.mu.Unlock()
	entry.mutex.Lock()
	return func() {
		entry.mutex.Unlock()
		destination.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(destination.locks, key)
		}
		destination.mu.Unlock()
	}
}

func (destination *Destination) loadItem(key string) (destinationItem, error) {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		return destinationItem{}, err
	}
	return current.Items[key], nil
}

func (destination *Destination) saveItem(key string, item destinationItem, remove bool) error {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		return err
	}
	if remove {
		delete(current.Items, key)
	} else {
		item.UpdatedAt = time.Now().UTC()
		current.Items[key] = item
	}
	return saveDestinationState(destination.StatePath, current)
}

func (destination *Destination) upsertBlock(
	ctx context.Context, threadID int64, oldIDs []int64, chunks []string,
) ([]int64, error) {
	ids := append([]int64(nil), oldIDs...)
	common := len(ids)
	if len(chunks) < common {
		common = len(chunks)
	}
	for index := 0; index < common; index++ {
		if err := destination.API.EditHTML(ctx, destination.ChatID, ids[index], chunks[index]); err != nil {
			if unchangedEditError(err) {
				// Telegram уже содержит нужный текст. Обычно это означает, что
				// предыдущая попытка успела выполниться до разрыва связи.
				continue
			}
			if !terminalEditError(err) {
				return nil, err
			}
			if destination.Logger != nil {
				destination.Logger.Warn("Telegram больше не разрешает изменить старое сообщение",
					"chat_id", destination.ChatID, "thread_id", threadID,
					"message_id", ids[index], "error", err,
				)
			}
			// Новое сообщение намеренно не создаётся: иначе исправление старого
			// перехвата появится в топике как новое и нарушит хронологию. Ошибка
			// остаётся видимой, а состояние не помечается синхронизированным.
			return nil, fmt.Errorf("старое сообщение Telegram невозможно исправить: %w", err)
		}
	}
	if len(ids) > len(chunks) {
		if err := destination.deleteMessages(ctx, ids[len(chunks):]); err != nil {
			return nil, err
		}
		ids = ids[:len(chunks)]
	}
	for index := len(ids); index < len(chunks); index++ {
		message, err := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
		if err != nil {
			return nil, err
		}
		ids = append(ids, message.MessageID)
	}
	return ids, nil
}

func (destination *Destination) deleteAll(ctx context.Context, blocks map[string]destinationBlock) error {
	for _, block := range blocks {
		if err := destination.deleteMessages(ctx, block.MessageIDs); err != nil {
			return err
		}
	}
	return nil
}

func (destination *Destination) deleteMessages(ctx context.Context, ids []int64) error {
	for _, id := range ids {
		if err := destination.API.DeleteMessage(ctx, destination.ChatID, id); err != nil {
			return err
		}
	}
	return nil
}

func terminalEditError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message can't be edited") ||
		strings.Contains(message, "message_id_invalid")
}

func unchangedEditError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}
