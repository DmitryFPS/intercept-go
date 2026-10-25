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

// Destination синхронизирует одну запись EAVC с одним сообщением Telegram либо
// с несколькими частями, только если запись превышает ограничение Telegram.
type Destination struct {
	API              *Client
	ChatID           int64
	Network          string
	Protection       string
	MessageTemplate  string
	StatePath        string
	StateScope       string
	LegacyStateScope string
	Logger           *slog.Logger

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
		return errors.New("telegram-направление настроено не полностью")
	}
	legacyItemKey := strings.TrimSpace(event.Source.UUID)
	if legacyItemKey == "" {
		legacyItemKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	// Локальные ID EAVC повторяются на разных рабочих точках. Без ClientID
	// исправление одной точки могло изменить сообщение другой.
	itemKey := strings.TrimSpace(event.ClientID) + "|" + legacyItemKey
	unlockItem := destination.lockItem(itemKey)
	defer unlockItem()
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return err
	}
	migratedLegacy := false
	if item.Blocks == nil {
		legacy, legacyErr := destination.loadItem(legacyItemKey)
		if legacyErr != nil {
			return legacyErr
		}
		if legacy.Blocks != nil {
			item = legacy
			migratedLegacy = true
		}
	}
	if item.Blocks == nil {
		item.Blocks = make(map[string]destinationBlock)
	}
	if delivery.Decision.Action == domain.RouteSkip {
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		if err := destination.saveItem(itemKey, destinationItem{}, true); err != nil {
			return err
		}
		if migratedLegacy {
			return destination.saveItem(legacyItemKey, destinationItem{}, true)
		}
		return nil
	}
	threadID := delivery.Decision.ThreadID
	if item.ThreadID != 0 && item.ThreadID != threadID {
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		item.Blocks = make(map[string]destinationBlock)
	}
	item.ThreadID = threadID

	const recordStateKey = "__record__"
	baseChunks := formatEventChunks(event, delivery, destination.Network, destination.Protection, destination.MessageTemplate)
	if len(baseChunks) == 0 {
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		if err := destination.saveItem(itemKey, destinationItem{}, true); err != nil {
			return err
		}
		if migratedLegacy {
			return destination.saveItem(legacyItemKey, destinationItem{}, true)
		}
		return nil
	}
	stored, combinedState := item.Blocks[recordStateKey]
	if !combinedState {
		// Состояние старых версий хранило отдельные сообщения для каждого
		// времени. Переиспользуем их идентификаторы при первом обновлении, чтобы
		// переход не создавал дубликаты и не терял возможность редактирования.
		stored.MessageIDs = legacyMessageIDs(item.Blocks)
	}
	baseHash := hashChunks(baseChunks)
	storedBaseHash := stored.SourceHash
	if storedBaseHash == "" {
		// Состояние до появления явной отметки хранило только итоговый текст.
		storedBaseHash = stored.Hash
	}
	if combinedState && storedBaseHash == baseHash {
		return nil
	}
	chunks := baseChunks
	if combinedState && len(stored.MessageIDs) > 0 {
		editedAt := event.SourceAt
		if editedAt.IsZero() {
			editedAt = time.Now().UTC()
		}
		chunks = formatEventChunksWithEditedAt(
			event, delivery, destination.Network, destination.Protection,
			destination.MessageTemplate, editedAt,
		)
	}
	renderHash := hashChunks(chunks)
	progress := func(messageIDs []int64) error {
		stored.MessageIDs = append([]int64(nil), messageIDs...)
		item.Blocks = map[string]destinationBlock{recordStateKey: stored}
		return destination.saveItem(itemKey, item, false)
	}
	messageIDs, err := destination.upsertBlock(ctx, itemKey, threadID, stored.MessageIDs, chunks, progress)
	if err != nil {
		return fmt.Errorf("синхронизировать запись: %w", err)
	}
	item.Blocks = map[string]destinationBlock{
		recordStateKey: {Hash: renderHash, SourceHash: baseHash, MessageIDs: messageIDs, Sequence: 0},
	}
	if err := destination.saveItem(itemKey, item, false); err != nil {
		return err
	}
	if migratedLegacy {
		return destination.saveItem(legacyItemKey, destinationItem{}, true)
	}
	return nil
}

func legacyMessageIDs(blocks map[string]destinationBlock) []int64 {
	ordered := make([]destinationBlock, 0, len(blocks))
	for _, block := range blocks {
		ordered = append(ordered, block)
	}
	sort.SliceStable(ordered, func(left, right int) bool { return ordered[left].Sequence < ordered[right].Sequence })
	var ids []int64
	for _, block := range ordered {
		ids = append(ids, block.MessageIDs...)
	}
	return ids
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
	current, err := destination.loadState()
	if err != nil {
		return destinationItem{}, err
	}
	return current.Items[key], nil
}

func (destination *Destination) saveItem(key string, item destinationItem, remove bool) error {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := destination.loadState()
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

func (destination *Destination) loadState() (destinationState, error) {
	current, err := loadDestinationState(destination.StatePath)
	if err != nil {
		return current, err
	}
	scope := strings.TrimSpace(destination.StateScope)
	if scope == "" {
		return current, nil
	}
	if current.Scope != "" && current.Scope != scope {
		if current.Scope == strings.TrimSpace(destination.LegacyStateScope) {
			// Однократная миграция с области token+chat на bot-id+chat. Ротация
			// токена того же бота не должна повторно отправить старые сообщения.
			current.Scope = scope
			return current, nil
		}
		// Идентификаторы сообщений имеют смысл только для конкретной пары
		// бот+группа. После смены назначения не редактируем сообщения старой пары.
		return destinationState{Scope: scope, Items: make(map[string]destinationItem)}, nil
	}
	current.Scope = scope
	return current, nil
}

func (destination *Destination) upsertBlock(
	ctx context.Context, itemKey string, threadID int64, oldIDs []int64, chunks []string,
	progress func([]int64) error,
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
				if destination.Logger != nil {
					destination.Logger.Info("Telegram-сообщение уже содержит исправление",
						"item_key", itemKey, "chat_id", destination.ChatID,
						"thread_id", threadID, "message_id", ids[index],
					)
				}
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
		if destination.Logger != nil {
			destination.Logger.Info("Telegram-сообщение отредактировано",
				"item_key", itemKey, "chat_id", destination.ChatID,
				"thread_id", threadID, "message_id", ids[index],
			)
		}
	}
	if len(ids) > len(chunks) {
		for len(ids) > len(chunks) {
			last := len(ids) - 1
			if err := destination.deleteMessage(ctx, ids[last]); err != nil {
				return nil, err
			}
			ids = ids[:last]
			if progress != nil {
				if err := progress(ids); err != nil {
					return nil, err
				}
			}
		}
	}
	for index := len(ids); index < len(chunks); index++ {
		message, err := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
		if err != nil {
			return nil, err
		}
		ids = append(ids, message.MessageID)
		if destination.Logger != nil {
			destination.Logger.Info("Telegram-сообщение создано",
				"item_key", itemKey, "chat_id", destination.ChatID,
				"thread_id", threadID, "message_id", message.MessageID,
			)
		}
		// Сохраняем каждый успешно созданный фрагмент немедленно. Если связь
		// оборвётся на следующем фрагменте, повтор продолжит с него без дубля.
		if progress != nil {
			if err := progress(ids); err != nil {
				return nil, err
			}
		}
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
		if err := destination.deleteMessage(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (destination *Destination) deleteMessage(ctx context.Context, id int64) error {
	err := destination.API.DeleteMessage(ctx, destination.ChatID, id)
	if err == nil || missingDeleteError(err) {
		return nil
	}
	return err
}

func missingDeleteError(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to delete not found") ||
		strings.Contains(message, "message_id_invalid")
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
