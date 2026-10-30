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
	scopedBlockUUID := strings.TrimSpace(event.SourceBlockUUID)
	willStoreRecordCleanup := scopedBlockUUID == "" &&
		(delivery.Decision.Action == domain.RouteSkip || len(eventBlocks(event)) == 0) &&
		(event.SourceVersion() != "" || !event.CreatedAt.IsZero())
	if err := destination.ensureTelegramItemCapacity(itemKey, legacyItemKey, willStoreRecordCleanup); err != nil {
		return err
	}
	migrateTelegramItemMetadata(&item)
	if telegramRecordCleanupRejects(item, event) {
		if destination.Logger != nil {
			destination.Logger.Warn("событие Telegram старее удаления всей записи и пропущено",
				"event_id", event.ID, "trace_id", event.ID, "block_uuid", scopedBlockUUID,
				"source_version", event.SourceVersion(),
				"cleanup_source_version", item.RecordCleanupSourceVersion,
			)
		}
		return nil
	}
	if scopedBlockUUID != "" {
		if stored, exists := item.Blocks[scopedBlockUUID]; exists {
			stale := (stored.SourceVersion != "" || !stored.SourceEventAt.IsZero()) &&
				domain.CompareSourceRevision(event.SourceVersion(), event.CreatedAt, stored.SourceVersion, stored.SourceEventAt) < 0
			if stale {
				if destination.Logger != nil {
					destination.Logger.Warn("устаревшее событие Telegram пропущено до изменения темы",
						"event_id", event.ID, "trace_id", event.ID, "block_uuid", scopedBlockUUID,
						"source_version", event.SourceVersion(), "stored_source_version", stored.SourceVersion,
					)
				}
				return nil
			}
		}
	}
	if scopedBlockUUID == "" && delivery.Decision.Action == domain.RouteSkip &&
		telegramRecordMutationIsStale(item, event) {
		return nil
	}
	// Неизвестный исход публикации нельзя забывать даже при удалении блока,
	// смене маршрута или следующей редакции: Telegram мог создать сообщение.
	for blockKey, stored := range item.Blocks {
		if scopedBlockUUID != "" && blockKey != scopedBlockUUID {
			continue
		}
		if stored.SendPending {
			stored.SendPending = false
			stored.UnknownOutcome = true
			stored.Suppressed = true
			stored.SuppressedAt = time.Now().UTC().Format(time.RFC3339Nano)
			stored.SuppressedReason = "исход отправки Telegram неизвестен; автоматический дубль запрещён"
			item.Blocks[blockKey] = stored
			if err := destination.saveItem(itemKey, item, false); err != nil {
				return err
			}
		}
		if stored.UnknownOutcome {
			return application.NewPermanentDeliveryError(errors.New(
				"исход предыдущей отправки Telegram неизвестен; проверьте тему и разрешайте повторную публикацию только если сообщения нет",
			))
		}
	}
	if delivery.Decision.Action == domain.RouteSkip {
		if scopedBlockUUID != "" {
			return destination.deleteScopedBlock(ctx, itemKey, legacyItemKey, item, event, scopedBlockUUID, migratedLegacy, !event.HasActiveBlocks)
		}
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		item.Blocks = make(map[string]destinationBlock)
		storeTelegramRecordCleanup(&item, event)
		removeState := item.RecordCleanupSourceVersion == "" && item.RecordCleanupSourceEventAt.IsZero()
		if err := destination.saveItem(itemKey, item, removeState); err != nil {
			return err
		}
		if migratedLegacy {
			return destination.saveItem(legacyItemKey, destinationItem{}, true)
		}
		return nil
	}
	threadID := delivery.Decision.ThreadID
	item.ThreadID = threadID
	if event.SourceBlockDeleted && scopedBlockUUID != "" {
		return destination.deleteScopedBlock(ctx, itemKey, legacyItemKey, item, event, scopedBlockUUID, migratedLegacy, !event.HasActiveBlocks)
	}

	blocks := eventBlocks(event)
	if len(blocks) == 0 {
		if scopedBlockUUID != "" {
			return destination.deleteScopedBlock(ctx, itemKey, legacyItemKey, item, event, scopedBlockUUID, migratedLegacy, !event.HasActiveBlocks)
		}
		if err := destination.deleteAll(ctx, item.Blocks); err != nil {
			return err
		}
		item.Blocks = make(map[string]destinationBlock)
		storeTelegramRecordCleanup(&item, event)
		removeState := item.RecordCleanupSourceVersion == "" && item.RecordCleanupSourceEventAt.IsZero()
		if err := destination.saveItem(itemKey, item, removeState); err != nil {
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
		revisionOrder := 1
		if exists && (stored.SourceVersion != "" || !stored.SourceEventAt.IsZero()) {
			revisionOrder = domain.CompareSourceRevision(
				event.SourceVersion(), event.CreatedAt, stored.SourceVersion, stored.SourceEventAt,
			)
			if revisionOrder < 0 {
				if destination.Logger != nil {
					destination.Logger.Warn("устаревшая версия блока Telegram пропущена",
						"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
						"source_version", event.SourceVersion(), "stored_source_version", stored.SourceVersion,
					)
				}
				continue
			}
		} else if exists && stored.Suppressed {
			// rc90 ещё не сохранял версию источника. Для такого состояния
			// восстанавливаем публикацию только событием, созданным уже после
			// фиксации ручного удаления; старый хвост очереди остаётся подавлен.
			revisionOrder = 0
			if suppressedAt, parseErr := time.Parse(time.RFC3339Nano, stored.SuppressedAt); parseErr == nil &&
				!event.CreatedAt.IsZero() && event.CreatedAt.After(suppressedAt) {
				revisionOrder = 1
			}
		}
		if exists && stored.SendPending {
			stored.SendPending = false
			stored.UnknownOutcome = true
			stored.Suppressed = true
			stored.SuppressedAt = time.Now().UTC().Format(time.RFC3339Nano)
			stored.SuppressedReason = "исход отправки Telegram неизвестен после прерывания; автоматический дубль запрещён"
			item.Blocks[blockKey] = stored
			if err := destination.saveItem(itemKey, item, false); err != nil {
				return err
			}
			if destination.Logger != nil {
				destination.Logger.Error("неопределённый исход отправки Telegram изолирован без автоматического дубля",
					"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
					"item_key", itemKey, "chat_id", destination.ChatID, "thread_id", threadID,
					"send_index", stored.SendPendingIndex, "send_started_at", stored.SendPendingAt,
				)
			}
			return application.NewPermanentDeliveryError(errors.New(
				"исход предыдущей отправки Telegram неизвестен; проверьте тему и используйте ручную повторную публикацию только если сообщения нет",
			))
		}
		storedBaseHash := stored.SourceHash
		if storedBaseHash == "" {
			storedBaseHash = stored.Hash
		}
		routeChanged := exists && stored.ThreadID != 0 && stored.ThreadID != threadID
		if routeChanged {
			oldThreadID := stored.ThreadID
			oldIDs := append([]int64(nil), stored.MessageIDs...)
			if err := destination.deleteMessages(ctx, oldIDs); err != nil {
				return err
			}
			stored.DeletedMessageIDs = appendUniqueIDs(stored.DeletedMessageIDs, oldIDs...)
			stored.MessageIDs = nil
			stored.ExpectedMessageCount = 0
			stored.Recovering = true
			stored.ThreadID = threadID
			storeTelegramRevision(&stored, event)
			item.Blocks[blockKey] = stored
			if err := destination.saveItem(itemKey, item, false); err != nil {
				return err
			}
			if destination.Logger != nil {
				destination.Logger.Info("публикация Telegram переносится в новую тему",
					"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
					"old_thread_id", oldThreadID, "new_thread_id", threadID,
					"deleted_message_ids", oldIDs,
				)
			}
		}
		storedExpectedMessageCount := destination.expectedMessageCountForBlock(
			event, delivery, block, stored, baseChunks,
		)
		if exists && storedBaseHash == baseHash && telegramBlockPublicationComplete(stored, storedExpectedMessageCount) {
			stored.Sequence = block.Sequence
			stored.ThreadID = threadID
			storeTelegramRevision(&stored, event)
			item.Blocks[blockKey] = stored
			continue
		}
		if stored.Suppressed || stored.SourceDeleted {
			if revisionOrder <= 0 {
				continue
			}
			stored.Suppressed = false
			stored.SuppressedAt = ""
			stored.SuppressedReason = ""
			stored.SourceDeleted = false
			stored.Recovering = true
			stored.MessageIDs = nil
			stored.ExpectedMessageCount = 0
			if destination.Logger != nil {
				destination.Logger.Info("новая редакция восстанавливает удалённую публикацию Telegram",
					"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
					"source_version", event.SourceVersion(), "previous_source_version", stored.SourceVersion,
				)
			}
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
			stored.ExpectedMessageCount = len(chunks)
			stored.Recovering = recovering
			stored.SendPending = false
			stored.SendPendingIndex = 0
			stored.SendPendingAt = ""
			stored.Sequence = block.Sequence
			stored.ThreadID = threadID
			storeTelegramRevision(&stored, event)
			item.Blocks[blockKey] = stored
			return destination.saveItem(itemKey, item, false)
		}
		sendIntent := func(pending bool, index int) error {
			stored.ExpectedMessageCount = len(chunks)
			stored.SendPending = pending
			stored.SendPendingIndex = index
			if pending {
				stored.SendPendingAt = time.Now().UTC().Format(time.RFC3339Nano)
				stored.Hash = renderHash
				stored.SourceHash = baseHash
			} else {
				stored.SendPendingAt = ""
			}
			stored.Sequence = block.Sequence
			stored.ThreadID = threadID
			storeTelegramRevision(&stored, event)
			item.Blocks[blockKey] = stored
			return destination.saveItem(itemKey, item, false)
		}
		oldIDs := append([]int64(nil), stored.MessageIDs...)
		messageIDs, _, err := destination.upsertBlock(
			ctx, event.ID, itemKey, threadID, stored.MessageIDs, chunks, progress, sendIntent,
		)
		if errors.Is(err, errTelegramPublicationMissing) {
			stored.DeletedMessageIDs = appendUniqueIDs(stored.DeletedMessageIDs, oldIDs...)
			stored.MessageIDs = nil
			stored.Hash = renderHash
			stored.SourceHash = baseHash
			stored.Sequence = block.Sequence
			stored.Recovering = true
			stored.Suppressed = false
			stored.SuppressedAt = ""
			stored.SuppressedReason = ""
			storeTelegramRevision(&stored, event)
			item.Blocks[blockKey] = stored
			if saveErr := destination.saveItem(itemKey, item, false); saveErr != nil {
				return saveErr
			}
			if destination.Logger != nil {
				destination.Logger.Warn("удалённая вручную публикация Telegram восстанавливается новой редакцией",
					"event_id", event.ID, "trace_id", event.ID, "block_uuid", blockKey,
					"item_key", itemKey, "chat_id", destination.ChatID, "thread_id", threadID,
					"deleted_message_ids", oldIDs,
				)
			}
			recoveryProgress := func(messageIDs []int64, _ bool) error {
				return progress(messageIDs, true)
			}
			messageIDs, _, err = destination.upsertBlock(
				ctx, event.ID, itemKey, threadID, nil, chunks, recoveryProgress, sendIntent,
			)
		}
		if err != nil {
			return fmt.Errorf("синхронизировать блок %s: %w", blockKey, err)
		}
		// История удалённых message_id остаётся частью диагностической цепочки
		// даже после явной повторной публикации оператором. Иначе новый
		// message_id сохранялся правильно, но связь с вручную удалённой
		// публикацией бесследно пропадала из интерфейса и журналов.
		item.Blocks[blockKey] = destinationBlock{
			Hash: renderHash, SourceHash: baseHash, MessageIDs: messageIDs,
			DeletedMessageIDs:    append([]int64(nil), stored.DeletedMessageIDs...),
			ExpectedMessageCount: len(chunks),
			Sequence:             block.Sequence, SourceVersion: event.SourceVersion(), SourceEventAt: event.CreatedAt,
			ThreadID: threadID,
		}
		if err := destination.saveItem(itemKey, item, false); err != nil {
			return err
		}
	}
	if scopedBlockUUID == "" {
		for key, obsolete := range item.Blocks {
			if key == recordStateKey || active[key] {
				continue
			}
			if err := destination.deleteMessages(ctx, obsolete.MessageIDs); err != nil {
				return err
			}
			delete(item.Blocks, key)
		}
	}
	delete(item.Blocks, recordStateKey)
	if len(item.Blocks) == 0 && item.RecordCleanupSourceVersion == "" && item.RecordCleanupSourceEventAt.IsZero() {
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
// транспортного .icevent-файла. Complete отличает полностью опубликованный
// длинный перехват от безопасно сохранённого промежуточного состояния.
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
		if scoped := strings.TrimSpace(event.SourceBlockUUID); scoped != "" && !strings.EqualFold(key, scoped) {
			continue
		}
		ordered = append(ordered, orderedBlock{key: key, block: block})
	}
	sort.SliceStable(ordered, func(left, right int) bool { return ordered[left].block.Sequence < ordered[right].block.Sequence })
	messageIDs := make([]int64, 0)
	blockReceipts := make([]application.DeliveryBlockReceipt, 0, len(ordered))
	receiptThreadID := item.ThreadID
	for _, entry := range ordered {
		block := entry.block
		expectedMessageCount := destination.expectedMessageCount(event, delivery, entry.key, block)
		complete := telegramBlockPublicationComplete(block, expectedMessageCount)
		if scoped := strings.TrimSpace(event.SourceBlockUUID); scoped != "" && block.ThreadID != 0 {
			receiptThreadID = block.ThreadID
		}
		if !block.Suppressed {
			messageIDs = append(messageIDs, block.MessageIDs...)
		}
		unknownOutcome := block.UnknownOutcome || block.SendPending
		unconfirmedPart := 0
		if unknownOutcome {
			unconfirmedPart = block.SendPendingIndex + 1
		}
		blockReceipts = append(blockReceipts, application.DeliveryBlockReceipt{
			BlockUUID: entry.key, MessageIDs: append([]int64(nil), block.MessageIDs...),
			DeletedMessageIDs:    append([]int64(nil), block.DeletedMessageIDs...),
			ExpectedMessageCount: expectedMessageCount, Complete: complete,
			Suppressed: block.Suppressed, SuppressedReason: block.SuppressedReason,
			UnknownOutcome: unknownOutcome, UnconfirmedPart: unconfirmedPart,
		})
	}
	if scoped := strings.TrimSpace(event.SourceBlockUUID); scoped != "" && len(blockReceipts) == 0 {
		blockReceipts = append(blockReceipts, application.DeliveryBlockReceipt{BlockUUID: scoped})
	}
	return application.DeliveryReceipt{
		Kind: "telegram", ChatID: destination.ChatID,
		ThreadID: receiptThreadID, MessageIDs: messageIDs, Blocks: blockReceipts,
	}, nil
}

// expectedMessageCount восстанавливает ожидаемое число частей и для состояния,
// записанного версиями до появления этого поля. SourceHash связывает сохранённую
// публикацию с точным исходным содержимым и не позволяет принять чужую редакцию.
func (destination *Destination) expectedMessageCount(
	event domain.Event, delivery domain.PlannedDelivery, blockUUID string, stored destinationBlock,
) int {
	for _, block := range eventBlocks(event) {
		if !strings.EqualFold(block.Identity(), strings.TrimSpace(blockUUID)) {
			continue
		}
		chunks := formatBlockChunks(
			event, block, delivery, destination.Network, destination.Protection, destination.MessageTemplate,
		)
		return destination.expectedMessageCountForBlock(event, delivery, block, stored, chunks)
	}
	return stored.ExpectedMessageCount
}

func (destination *Destination) expectedMessageCountForBlock(
	event domain.Event, delivery domain.PlannedDelivery, block domain.ContentBlock,
	stored destinationBlock, baseChunks []string,
) int {
	if len(baseChunks) == 0 {
		return 0
	}
	baseHash := hashChunks(baseChunks)
	if stored.SourceHash != "" && stored.SourceHash != baseHash {
		return 0
	}
	if stored.ExpectedMessageCount > 0 {
		return stored.ExpectedMessageCount
	}
	if stored.Hash == baseHash || (stored.Hash == "" && stored.SourceHash == baseHash) {
		return len(baseChunks)
	}
	// rc97 не сохранял ожидаемое количество, однако Hash уже содержал хеш
	// варианта с отметкой редактирования. Воспроизводим его, чтобы суффикс на
	// границе лимита Telegram не спрятал последнюю часть.
	if stored.Hash != "" && !event.SourceAt.IsZero() {
		editedChunks := formatBlockChunksWithEditedAt(
			event, block, delivery, destination.Network, destination.Protection,
			destination.MessageTemplate, event.SourceAt,
		)
		if stored.Hash == hashChunks(editedChunks) {
			return len(editedChunks)
		}
	}
	// Неизвестный старый Hash нельзя считать доказательством полной отправки.
	// Повторный Deliver сверит известные ID и сохранит точное количество.
	return 0
}

func telegramBlockPublicationComplete(block destinationBlock, expectedMessageCount int) bool {
	if expectedMessageCount <= 0 || len(block.MessageIDs) != expectedMessageCount ||
		block.Recovering || block.Suppressed || block.SourceDeleted || block.SendPending {
		return false
	}
	seen := make(map[int64]struct{}, len(block.MessageIDs))
	for _, messageID := range block.MessageIDs {
		if messageID <= 0 {
			return false
		}
		if _, exists := seen[messageID]; exists {
			return false
		}
		seen[messageID] = struct{}{}
	}
	return true
}

// deleteScopedBlock удаляет только публикацию выбранного временного блока.
// Соседние блоки той же записи EAVC остаются нетронутыми.
func (destination *Destination) deleteScopedBlock(
	ctx context.Context, itemKey, legacyItemKey string, item destinationItem,
	event domain.Event, blockUUID string, migratedLegacy, lastBlock bool,
) error {
	stored, exists := item.Blocks[blockUUID]
	if exists && (stored.SourceVersion != "" || !stored.SourceEventAt.IsZero()) {
		order := domain.CompareSourceRevision(event.SourceVersion(), event.CreatedAt, stored.SourceVersion, stored.SourceEventAt)
		if order < 0 || (order == 0 && stored.SourceDeleted) {
			return nil
		}
	}
	if exists {
		if err := destination.deleteMessages(ctx, stored.MessageIDs); err != nil {
			return err
		}
	}
	stored.DeletedMessageIDs = appendUniqueIDs(stored.DeletedMessageIDs, stored.MessageIDs...)
	stored.MessageIDs = nil
	stored.ExpectedMessageCount = 0
	stored.Hash = ""
	stored.SourceHash = ""
	stored.Recovering = false
	stored.Suppressed = false
	stored.SuppressedAt = ""
	stored.SuppressedReason = ""
	stored.SourceDeleted = true
	if stored.ThreadID == 0 {
		stored.ThreadID = item.ThreadID
	}
	storeTelegramRevision(&stored, event)
	item.Blocks[blockUUID] = stored
	// До rc82 запись могла храниться одним сообщением. Удалять его безопасно
	// только когда в EAVC больше не осталось активных временных блоков.
	if lastBlock {
		if stored, exists := item.Blocks["__record__"]; exists {
			if err := destination.deleteMessages(ctx, stored.MessageIDs); err != nil {
				return err
			}
			delete(item.Blocks, "__record__")
		}
	}
	if err := destination.saveItem(itemKey, item, false); err != nil {
		return err
	}
	if migratedLegacy {
		return destination.saveItem(legacyItemKey, destinationItem{}, true)
	}
	return nil
}

func storeTelegramRevision(block *destinationBlock, event domain.Event) {
	block.SourceVersion = event.SourceVersion()
	block.SourceEventAt = event.CreatedAt
}

func storeTelegramRecordCleanup(item *destinationItem, event domain.Event) {
	item.RecordCleanupSourceVersion = event.SourceVersion()
	item.RecordCleanupSourceEventAt = event.CreatedAt
}

func telegramRecordCleanupRejects(item destinationItem, event domain.Event) bool {
	if item.RecordCleanupSourceVersion == "" && item.RecordCleanupSourceEventAt.IsZero() {
		return false
	}
	return domain.CompareSourceRevision(
		event.SourceVersion(), event.CreatedAt,
		item.RecordCleanupSourceVersion, item.RecordCleanupSourceEventAt,
	) <= 0
}

// migrateTelegramItemMetadata переносит общую rc90-тему в поблочное
// состояние. item.UpdatedAt — это время локальной записи JSON, а не
// ревизия EAVC. Его нельзя подставлять в SourceEventAt: при разборе
// backlog новая SourceVersion законно может быть создана раньше этой записи.
// Первое принятое после обновления событие сохранит истинные
// SourceVersion/CreatedAt, и дальше каждый блок имеет строгий барьер.
func migrateTelegramItemMetadata(item *destinationItem) {
	if item == nil {
		return
	}
	for key, block := range item.Blocks {
		if block.ThreadID == 0 {
			block.ThreadID = item.ThreadID
		}
		item.Blocks[key] = block
	}
}

func telegramRecordMutationIsStale(item destinationItem, event domain.Event) bool {
	for _, block := range item.Blocks {
		if block.SourceVersion == "" && block.SourceEventAt.IsZero() {
			continue
		}
		if domain.CompareSourceRevision(
			event.SourceVersion(), event.CreatedAt, block.SourceVersion, block.SourceEventAt,
		) < 0 {
			return true
		}
	}
	return false
}

func compactTelegramTombstones(blocks map[string]destinationBlock) map[string]destinationBlock {
	const maxSourceTombstonesPerRecord = 4096
	type candidate struct {
		key string
		at  time.Time
	}
	var tombstones []candidate
	for key, block := range blocks {
		if block.SourceDeleted && len(block.MessageIDs) == 0 {
			tombstones = append(tombstones, candidate{key: key, at: block.SourceEventAt})
		}
	}
	if len(tombstones) <= maxSourceTombstonesPerRecord {
		return blocks
	}
	sort.SliceStable(tombstones, func(left, right int) bool {
		return tombstones[left].at.Before(tombstones[right].at)
	})
	for _, item := range tombstones[:len(tombstones)-maxSourceTombstonesPerRecord] {
		delete(blocks, item.key)
	}
	return blocks
}

// AllowRepublish снимает только явное подавление выбранного UUID-блока.
// Следующий обычный проход доставки создаст новое сообщение и сохранит точный
// message_id из ответа Telegram; соседние и пользовательские сообщения не
// участвуют в выборе.
func (destination *Destination) AllowRepublish(event domain.Event, blockUUID string) (bool, error) {
	blockUUID = strings.TrimSpace(blockUUID)
	if blockUUID == "" {
		return false, errors.New("не задан UUID блока для повторной публикации")
	}
	if strings.TrimSpace(event.ClientID) == "" ||
		(strings.TrimSpace(event.Source.UUID) == "" && event.Source.ID <= 0) {
		return false, errors.New("не задана рабочая точка или запись для повторной публикации")
	}
	legacyItemKey := strings.TrimSpace(event.Source.UUID)
	if legacyItemKey == "" {
		legacyItemKey = fmt.Sprintf("id:%d", event.Source.ID)
	}
	itemKey := strings.TrimSpace(event.ClientID) + "|" + legacyItemKey
	unlock := destination.lockItem(itemKey)
	defer unlock()
	item, err := destination.loadItem(itemKey)
	if err != nil {
		return false, err
	}
	block, exists := item.Blocks[blockUUID]
	if !exists || block.SendPending || (!block.Suppressed && (len(block.MessageIDs) > 0 || len(block.DeletedMessageIDs) == 0)) {
		return false, nil
	}
	block.Suppressed = false
	block.UnknownOutcome = false
	block.SuppressedAt = ""
	block.SuppressedReason = ""
	block.Hash = ""
	block.SourceHash = ""
	// Уже подтверждённые части длинного сообщения сохраняются. При
	// неопределённом исходе следующей части оператор проверяет только её;
	// повторная отправка всех известных частей создала бы дубль.
	if len(block.MessageIDs) == 0 {
		block.ExpectedMessageCount = 0
	}
	block.Recovering = false
	block.SendPending = false
	block.SendPendingIndex = 0
	block.SendPendingAt = ""
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
	const retainedDeletedMessageIDs = 64
	seen := make(map[int64]bool, len(existing)+len(values))
	result := make([]int64, 0, len(existing)+len(values))
	for _, value := range append(append([]int64(nil), existing...), values...) {
		if value != 0 && !seen[value] {
			seen[value] = true
			result = append(result, value)
		}
	}
	if len(result) > retainedDeletedMessageIDs {
		result = append([]int64(nil), result[len(result)-retainedDeletedMessageIDs:]...)
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
	item := current.Items[key]
	if barrier, exists := current.RecordTombstones[key]; exists {
		item.RecordCleanupSourceVersion = barrier.SourceVersion
		item.RecordCleanupSourceEventAt = barrier.SourceEventAt
	}
	return item, nil
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
		if item.RecordCleanupSourceVersion != "" || !item.RecordCleanupSourceEventAt.IsZero() {
			if current.RecordTombstones == nil {
				current.RecordTombstones = make(map[string]destinationRecordTombstone)
			}
			current.RecordTombstones[key] = destinationRecordTombstone{
				SourceVersion: item.RecordCleanupSourceVersion,
				SourceEventAt: item.RecordCleanupSourceEventAt,
				UpdatedAt:     time.Now().UTC(),
			}
			item.RecordCleanupSourceVersion = ""
			item.RecordCleanupSourceEventAt = time.Time{}
		}
		item.Blocks = compactTelegramTombstones(item.Blocks)
		item.UpdatedAt = time.Now().UTC()
		current.Items[key] = item
	}
	return saveDestinationState(destination.StatePath, current)
}

func (destination *Destination) ensureTelegramItemCapacity(
	itemKey, legacyItemKey string, willStoreRecordCleanup bool,
) error {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	current, err := destination.loadState()
	if err != nil {
		return err
	}
	if willStoreRecordCleanup {
		_, itemBarrierExists := current.RecordTombstones[itemKey]
		_, legacyBarrierExists := current.RecordTombstones[legacyItemKey]
		if !itemBarrierExists && !legacyBarrierExists && len(current.RecordTombstones) >= maxTelegramRecordTombstones {
			return telegramStateLimitError("достигнут предел 100000 барьеров удаления записей Telegram")
		}
		return nil
	}
	item, itemExists := current.Items[itemKey]
	legacy, legacyExists := current.Items[legacyItemKey]
	alreadyActive := itemExists && !telegramItemInactive(item) || legacyExists && !telegramItemInactive(legacy)
	if !alreadyActive && telegramActiveItemCount(current) >= maxTelegramActiveItems {
		return telegramStateLimitError("достигнут предел 100000 активных записей состояния Telegram")
	}
	return nil
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
		return destinationState{
			Scope: scope, Items: make(map[string]destinationItem),
			RecordTombstones: make(map[string]destinationRecordTombstone),
		}, nil
	}
	current.Scope = scope
	return current, nil
}

func (destination *Destination) upsertBlock(
	ctx context.Context, eventID, itemKey string, threadID int64, oldIDs []int64, chunks []string,
	progress func([]int64, bool) error, sendIntent func(bool, int) error,
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
					destination.Logger.Warn("Telegram-сообщение отсутствует; текущая редакция EAVC будет опубликована заново",
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
		if sendIntent != nil {
			if err := sendIntent(true, index); err != nil {
				return nil, false, err
			}
		}
		message, err := destination.API.SendHTML(ctx, destination.ChatID, threadID, chunks[index])
		if err != nil {
			if telegramSendDefinitelyNotCreated(err) && sendIntent != nil {
				if clearErr := sendIntent(false, index); clearErr != nil {
					return nil, false, fmt.Errorf("отправка Telegram отклонена: %v; снять маркер попытки: %w", err, clearErr)
				}
				return nil, false, err
			}
			return nil, false, fmt.Errorf("исход отправки Telegram неизвестен: %w", err)
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

func telegramSendDefinitelyNotCreated(err error) bool {
	var attempt *sendAttemptError
	if errors.As(err, &attempt) && !attempt.attempted {
		return true
	}
	var telegramError *apiError
	if errors.As(err, &telegramError) {
		return telegramError.httpStatus >= 200 && telegramError.httpStatus < 500 &&
			telegramError.code >= 400 && telegramError.code < 500
	}
	var limited *rateLimitError
	return errors.As(err, &limited) && limited.httpStatus >= 200 &&
		limited.httpStatus < 500 && limited.apiCode == 429
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
	if definitiveTelegramBadRequest(err) &&
		strings.Contains(strings.ToLower(err.Error()), "message can't be deleted") {
		return application.NewPermanentDeliveryError(err)
	}
	return err
}

func missingDeleteError(err error) bool {
	if !definitiveTelegramBadRequest(err) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to delete not found") ||
		strings.Contains(message, "message_id_invalid")
}

func terminalEditError(err error) bool {
	if !definitiveTelegramBadRequest(err) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message can't be edited") ||
		strings.Contains(message, "message_id_invalid")
}

func missingEditError(err error) bool {
	if !definitiveTelegramBadRequest(err) {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "message to edit not found") ||
		strings.Contains(message, "message_id_invalid")
}

func unchangedEditError(err error) bool {
	return definitiveTelegramBadRequest(err) &&
		strings.Contains(strings.ToLower(err.Error()), "message is not modified")
}

func definitiveTelegramBadRequest(err error) bool {
	var telegramError *apiError
	return errors.As(err, &telegramError) && telegramError.code == 400 &&
		(telegramError.httpStatus == 200 || telegramError.httpStatus == 400)
}
