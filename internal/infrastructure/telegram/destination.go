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

	"intercept/internal/application"
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

var errTelegramPublicationMissing = errors.New("публикация Telegram удалена вручную")

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

	blocks := eventBlocks(event)
	if len(blocks) == 0 {
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
	const recordStateKey = "__record__"
	combined, hasCombined := item.Blocks[recordStateKey]
	active := make(map[string]bool, len(blocks))
	for index, block := range blocks {
		blockKey := block.Identity()
		active[blockKey] = true
		stored, exists := item.Blocks[blockKey]
		if !exists && index == 0 && hasCombined {
			// rc81 хранил всю запись под __record__. Первый блок принимает прежний
			// message_id, остальные публикуются отдельно. Маркер удаляется только
			// после успешного завершения миграции, поэтому перезапуск её продолжит.
			stored = combined
			exists = true
		}
		baseChunks := formatBlockChunks(
			event, block, delivery, destination.Network, destination.Protection, destination.MessageTemplate,
		)
		if len(baseChunks) == 0 {
			continue
		}
		baseHash := hashChunks(baseChunks)
		storedBaseHash := stored.SourceHash
		if storedBaseHash == "" {
			storedBaseHash = stored.Hash
		}
		if exists && storedBaseHash == baseHash {
			stored.Sequence = block.Sequence
			item.Blocks[blockKey] = stored
			continue
		}
		if stored.Suppressed {
			stored.Hash = baseHash
			stored.SourceHash = baseHash
			stored.Sequence = block.Sequence
			item.Blocks[blockKey] = stored
			if err := destination.saveItem(itemKey, item, false); err != nil {
				return err
			}
			continue
		}
		chunks := baseChunks
		if exists && (len(stored.MessageIDs) > 0 || stored.Recovering) {
			editedAt := event.SourceAt
			if editedAt.IsZero() {
				editedAt = time.Now().UTC()
			}
			chunks = formatBlockChunksWithEditedAt(
				event, block, delivery, destination.Network, destination.Protection,
				destination.MessageTemplate, editedAt,
			)
		}
		renderHash := hashChunks(chunks)
		progress := func(messageIDs []int64, recovering bool) error {
			stored.MessageIDs = append([]int64(nil), messageIDs...)
			stored.Recovering = recovering
			stored.Sequence = block.Sequence
			item.Blocks[blockKey] = stored
			return destination.saveItem(itemKey, item, false)
		}
		oldIDs := append([]int64(nil), stored.MessageIDs...)
		messageIDs, _, err := destination.upsertBlock(
			ctx, event.ID, itemKey, threadID, stored.MessageIDs, chunks, progress,
		)
		if errors.Is(err, errTelegramPublicationMissing) {
			stored.DeletedMessageIDs = appendUniqueIDs(stored.DeletedMessageIDs, oldIDs...)
			stored.MessageIDs = nil
			stored.Hash = renderHash
			stored.SourceHash = baseHash
			stored.Sequence = block.Sequence
			stored.Recovering = false
			stored.Suppressed = true
			stored.SuppressedAt = time.Now().UTC().Format(time.RFC3339Nano)
			stored.SuppressedReason = "сообщение удалено вручную в Telegram"
			item.Blocks[blockKey] = stored
			if saveErr := destination.saveItem(itemKey, item, false); saveErr != nil {
				return saveErr
			}
			if destination.Logger != nil {
				destination.Logger.Warn("удалённая вручную публикация Telegram не будет восстановлена автоматически",
					"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
					"item_key", itemKey, "chat_id", destination.ChatID, "thread_id", threadID,
					"deleted_message_ids", oldIDs,
				)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("синхронизировать блок %s: %w", blockKey, err)
		}
		item.Blocks[blockKey] = destinationBlock{
			Hash: renderHash, SourceHash: baseHash, MessageIDs: messageIDs, Sequence: block.Sequence,
		}
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	for key, obsolete := range item.Blocks {
		if key == recordStateKey || active[key] {
			continue
		}
		if err := destination.deleteMessages(ctx, obsolete.MessageIDs); err != nil {
			return err
		}
		delete(item.Blocks, key)
	}
	delete(item.Blocks, recordStateKey)
	if len(item.Blocks) == 0 {
		if err := destination.saveItem(itemKey, destinationItem{}, true); err != nil {
			return err
		}
	} else if err := destination.saveItem(itemKey, item, false); err != nil {
		return err
	}
	if migratedLegacy {
		return destination.saveItem(legacyItemKey, destinationItem{}, true)
	}
	return nil
}

// DeliveryReceipt возвращает сохранённые ID конечной публикации, а не ID
// транспортного .icevent-файла. Метод вызывается только после успешной Deliver.
func (destination *Destination) DeliveryReceipt(
	event domain.Event, delivery domain.PlannedDelivery,
) (application.DeliveryReceipt, error) {
	legacyItemKey := strings.TrimSpace(event.Source.UUID)
	if legacyItemKey == "" {
		legacyItemKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	itemKey := strings.TrimSpace(event.ClientID) + "|" + legacyItemKey
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return application.DeliveryReceipt{}, err
	}
	type orderedBlock struct {
		key   string
		block destinationBlock
	}
	ordered := make([]orderedBlock, 0, len(item.Blocks))
	for key, block := range item.Blocks {
		ordered = append(ordered, orderedBlock{key: key, block: block})
	}
	sort.SliceStable(ordered, func(left, right int) bool { return ordered[left].block.Sequence < ordered[right].block.Sequence })
	messageIDs := make([]int64, 0)
	blockReceipts := make([]application.DeliveryBlockReceipt, 0, len(ordered))
	for _, entry := range ordered {
		block := entry.block
		if !block.Suppressed {
			messageIDs = append(messageIDs, block.MessageIDs...)
		}
		blockReceipts = append(blockReceipts, application.DeliveryBlockReceipt{
			BlockUUID: entry.key, MessageIDs: append([]int64(nil), block.MessageIDs...),
			DeletedMessageIDs: append([]int64(nil), block.DeletedMessageIDs...),
			Suppressed:        block.Suppressed, SuppressedReason: block.SuppressedReason,
		})
	}
	return application.DeliveryReceipt{
		Kind: "telegram", ChatID: destination.ChatID,
		ThreadID: item.ThreadID, MessageIDs: messageIDs, Blocks: blockReceipts,
	}, nil
}

// AllowRepublish снимает только явное подавление выбранного UUID-блока.
// Следующий обычный проход доставки создаст новое сообщение и сохранит точный
// message_id из ответа Telegram; соседние и пользовательские сообщения не
// участвуют в выборе.
func (destination *Destination) AllowRepublish(blockUUID string) (bool, error) {
	blockUUID = strings.TrimSpace(blockUUID)
	if blockUUID == "" {
		return false, errors.New("не задан UUID блока для повторной публикации")
	}
	destination.mu.Lock()
	current, err := destination.loadState()
	if err != nil {
		destination.mu.Unlock()
		return false, err
	}
	itemKey := ""
	for key, item := range current.Items {
		if block, exists := item.Blocks[blockUUID]; exists && (block.Suppressed || (len(block.MessageIDs) == 0 && len(block.DeletedMessageIDs) > 0)) {
			itemKey = key
			break
		}
	}
	destination.mu.Unlock()
	if itemKey == "" {
		return false, nil
	}
	unlock := destination.lockItem(itemKey)
	defer unlock()
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return false, err
	}
	block, exists := item.Blocks[blockUUID]
	if !exists || (!block.Suppressed && (len(block.MessageIDs) > 0 || len(block.DeletedMessageIDs) == 0)) {
		return false, nil
	}
	block.Suppressed = false
	block.SuppressedAt = ""
	block.SuppressedReason = ""
	block.Hash = ""
	block.SourceHash = ""
	block.MessageIDs = nil
	block.Recovering = false
	item.Blocks[blockUUID] = block
	if err := destination.saveItem(itemKey, item, false); err != nil {
		return false, err
	}
	if destination.Logger != nil {
		destination.Logger.Info("оператор разрешил повторную публикацию Telegram",
			"block_uuid", blockUUID, "item_key", itemKey, "chat_id", destination.ChatID,
			"deleted_message_ids", block.DeletedMessageIDs,
		)
	}
	return true, nil
}

func eventBlocks(event domain.Event) []domain.ContentBlock {
	blocks := append([]domain.ContentBlock(nil), event.Blocks...)
	if len(blocks) == 0 {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	filtered := blocks[:0]
	for _, block := range blocks {
		if strings.TrimSpace(block.Text) != "" {
			filtered = append(filtered, block)
		}
	}
	sort.SliceStable(filtered, func(left, right int) bool { return filtered[left].Sequence < filtered[right].Sequence })
	return filtered
}

func appendUniqueIDs(existing []int64, values ...int64) []int64 {
	seen := make(map[int64]bool, len(existing)+len(values))
	result := make([]int64, 0, len(existing)+len(values))
	for _, value := range append(append([]int64(nil), existing...), values...) {
		if value != 0 && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	return result
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
	ctx context.Context, eventID, itemKey string, threadID int64, oldIDs []int64, chunks []string,
	progress func([]int64, bool) error,
) ([]int64, bool, error) {
	ids := append([]int64(nil), oldIDs...)
	recovered := false
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
						"event_id", eventID, "trace_id", eventID, "item_key", itemKey, "chat_id", destination.ChatID,
						"thread_id", threadID, "message_id", ids[index], "final_message_id", ids[index],
					)
				}
				continue
			}
			if missingEditError(err) {
				if destination.Logger != nil {
					destination.Logger.Warn("Telegram-сообщение отсутствует; автоматическое восстановление отключено",
						"event_id", eventID, "trace_id", eventID, "item_key", itemKey, "chat_id", destination.ChatID,
						"thread_id", threadID, "message_id", ids[index], "final_message_id", ids[index], "error", err,
					)
				}
				// Если запись была разбита на части, собираем её заново целиком.
				// Иначе новая часть оказалась бы в конце темы, отдельно от остальных.
				for _, messageID := range ids {
					if deleteErr := destination.deleteMessage(ctx, messageID); deleteErr != nil {
						return nil, false, deleteErr
					}
				}
				ids = nil
				if progress != nil {
					if progressErr := progress(ids, false); progressErr != nil {
						return nil, false, progressErr
					}
				}
				return nil, false, errTelegramPublicationMissing
			}
			if !terminalEditError(err) {
				return nil, false, err
			}
			if destination.Logger != nil {
				destination.Logger.Warn("Telegram больше не разрешает изменить старое сообщение",
					"event_id", eventID, "trace_id", eventID, "item_key", itemKey,
					"chat_id", destination.ChatID, "thread_id", threadID,
					"message_id", ids[index], "final_message_id", ids[index], "error", err,
				)
			}
			// Новое сообщение намеренно не создаётся: иначе исправление старого
			// перехвата появится в топике как новое и нарушит хронологию. Ошибка
			// остаётся видимой, а состояние не помечается синхронизированным.
			return nil, false, application.NewPermanentDeliveryError(
				fmt.Errorf("старое сообщение Telegram невозможно исправить: %w", err),
			)
		}
		if destination.Logger != nil {
			destination.Logger.Info("Telegram-сообщение отредактировано",
				"event_id", eventID, "trace_id", eventID, "item_key", itemKey, "chat_id", destination.ChatID,
				"thread_id", threadID, "message_id", ids[index], "final_message_id", ids[index],
			)
		}
	}
	if len(ids) > len(chunks) {
		for len(ids) > len(chunks) {
			last := len(ids) - 1
			if err := destination.deleteMessage(ctx, ids[last]); err != nil {
				return nil, false, err
			}
			ids = ids[:last]
			if progress != nil {
				if err := progress(ids, recovered); err != nil {
					return nil, false, err
				}
			}
		}
	}
	for index := len(ids); index < len(chunks); index++ {
		message, err := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
		if err != nil {
			return nil, false, err
		}
		ids = append(ids, message.MessageID)
		if destination.Logger != nil {
			destination.Logger.Info("Telegram-сообщение создано",
				"event_id", eventID, "trace_id", eventID, "item_key", itemKey, "chat_id", destination.ChatID,
				"thread_id", threadID, "message_id", message.MessageID, "final_message_id", message.MessageID,
			)
		}
		// Сохраняем каждый успешно созданный фрагмент немедленно. Если связь
		// оборвётся на следующем фрагменте, повтор продолжит с него без дубля.
		if progress != nil {
			if err := progress(ids, recovered); err != nil {
				return nil, false, err
			}
		}
	}
	return ids, recovered, nil
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
	var telegramError *apiError
	if errors.As(err, &telegramError) && telegramError.code != 400 {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message can't be edited") ||
		strings.Contains(message, "message_id_invalid")
}

func missingEditError(err error) bool {
	var telegramError *apiError
	if errors.As(err, &telegramError) && telegramError.code != 400 {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message_id_invalid")
}

func unchangedEditError(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}
