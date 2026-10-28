package docx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/share"
)

var errDestinationBusy = errors.New("предыдущая операция Word ещё выполняется")

// Destination создаёт DOCX по внешнему шаблону и доставляет его атомарно.
type Destination struct {
	TemplatePath string
	Folder       string
	StatePath    string
	StateScope   string
	Network      string
	Protection   string
	Prefix       string
	Username     string
	Password     string
}

type destinationLock struct {
	mutex sync.Mutex
	refs  int
}

var destinationLocks = struct {
	sync.Mutex
	items map[string]*destinationLock
}{items: make(map[string]*destinationLock)}

// Deliver запускает одну Word-операцию для направления. Если предыдущая
// операция с тем же файлом состояния зависла, новая попытка сразу вернёт ошибку.
func (destination *Destination) Deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error {
	settings := *destination
	if settings.Prefix == "" {
		settings.Prefix = "СТНД"
	}
	if settings.TemplatePath == "" || settings.Folder == "" || settings.StatePath == "" {
		return errors.New("word-направление настроено не полностью")
	}
	unlock, acquired := tryDestinationLock(settings.StatePath)
	if !acquired {
		return errDestinationBusy
	}
	completed := make(chan error, 1)
	go func() {
		err := settings.deliver(ctx, event, delivery)
		// Результат нельзя публиковать раньше освобождения блокировки: при
		// последовательных заданиях вызывающий поток может немедленно начать
		// следующую доставку и ошибочно увидеть уже завершённую операцию занятой.
		unlock()
		completed <- err
	}()
	select {
	case err := <-completed:
		return err
	case <-ctx.Done():
		// Некоторые Windows/SMB-вызовы не умеют отменяться. Блокировка остаётся
		// у фоновой операции до её фактического завершения, поэтому повторная
		// доставка не сможет одновременно перезаписать тот же документ.
		return ctx.Err()
	}
}

func (destination Destination) deliver(ctx context.Context, event domain.Event, delivery domain.PlannedDelivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := os.Stat(destination.Folder); err != nil {
		if err := share.Connect(ctx, destination.Folder, destination.Username, destination.Password); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(destination.Folder, 0o755); err != nil {
		return fmt.Errorf("создать папку Word: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, err := loadState(destination.StatePath)
	if err != nil {
		return err
	}
	scope := strings.TrimSpace(destination.StateScope)
	if scope != "" {
		// Старое состояние без области принадлежит текущему назначению. Если же
		// папку назначения сменили, прежние записи нельзя считать доставленными:
		// для новой папки создаётся независимая нумерация и документ.
		if current.Scope != "" && current.Scope != scope {
			current = state{
				Scope: scope, Blocks: make(map[string]blockRecord),
				RecordTombstones: make(map[string]recordRevisionBarrier),
			}
		} else {
			current.Scope = scope
		}
	}
	legacySourceKey := strings.TrimSpace(event.Source.UUID)
	if legacySourceKey == "" {
		legacySourceKey = "id:" + strconv.FormatInt(event.Source.ID, 10)
	}
	sourceKey := strings.TrimSpace(event.ClientID) + "|" + legacySourceKey
	prefix := sourceKey + "|"
	legacyPrefix := legacySourceKey + "|"
	scopedBlockUUID := strings.TrimSpace(event.SourceBlockUUID)
	willStoreRecordCleanup := scopedBlockUUID == "" && delivery.Decision.Action == domain.RouteSkip &&
		(event.SourceVersion() != "" || !event.CreatedAt.IsZero())
	if willStoreRecordCleanup {
		if _, exists := current.RecordTombstones[sourceKey]; !exists &&
			len(current.RecordTombstones) >= maxWordRecordTombstones {
			return application.NewPermanentDeliveryError(errors.New(
				"достигнут предел 100000 барьеров удаления записей Word",
			))
		}
	}
	if docxRecordCleanupRejects(current, sourceKey, event) {
		return nil
	}
	if delivery.Decision.Action == domain.RouteSkip {
		if scopedBlockUUID != "" {
			if err := destination.removeScopedBlock(&current, prefix, legacyPrefix, event, scopedBlockUUID, !event.HasActiveBlocks); err != nil {
				return err
			}
			return saveState(destination.StatePath, current)
		}
		if docxRecordMutationIsStale(current, prefix, legacyPrefix, event) {
			return nil
		}
		for key, record := range current.Blocks {
			if !strings.HasPrefix(key, prefix) && !strings.HasPrefix(key, legacyPrefix) {
				continue
			}
			if record.Filename != "" {
				path, pathErr := safeDestinationPath(destination.Folder, record.Filename)
				if pathErr != nil {
					return pathErr
				}
				if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
					return fmt.Errorf("удалить прежний Word-документ: %w", removeErr)
				}
			}
			delete(current.Blocks, key)
		}
		storeWordRecordCleanup(&current, sourceKey, event)
		return saveState(destination.StatePath, current)
	}
	if event.SourceBlockDeleted && scopedBlockUUID != "" {
		if err := destination.removeScopedBlock(&current, prefix, legacyPrefix, event, scopedBlockUUID, !event.HasActiveBlocks); err != nil {
			return err
		}
		return saveState(destination.StatePath, current)
	}
	blocks := docxEventBlocks(event)
	active := make(map[string]bool, len(blocks))
	date := event.SourceAt
	if date.IsZero() {
		date = event.ObservedAt
	}
	combinedKey := prefix + "record"
	combined, hasCombined := current.Blocks[combinedKey]
	if !hasCombined {
		combined, hasCombined = current.Blocks[legacyPrefix+"record"]
	}
	for index, block := range blocks {
		key := prefix + block.Identity()
		active[key] = true
		record, exists := current.Blocks[key]
		technicalMigration := false
		migratedBlockKey := ""
		if !exists {
			for _, legacyKey := range []string{prefix + block.Key, legacyPrefix + block.Key} {
				if legacyRecord, ok := current.Blocks[legacyKey]; ok {
					record = legacyRecord
					exists = true
					technicalMigration = true
					migratedBlockKey = legacyKey
					break
				}
			}
		}
		if !exists && index == 0 && hasCombined {
			record = combined
			exists = true
			technicalMigration = true
		}
		content := strings.TrimSpace(block.Text)
		digest := digestText(content)
		document := Document{
			Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
			GroupCode: event.Position.GroupCode, Network: destination.Network, Protection: destination.Protection,
			Date: date, SourceUpdatedAt: event.SourceVersion(), Operator: event.UpdatedBy, Content: content,
			Callsigns: block.Callsigns,
		}
		render := renderDigest(document)
		revisionOrder := 1
		if exists && (record.SourceVersion != "" || !record.SourceEventAt.IsZero()) {
			revisionOrder = domain.CompareSourceRevision(
				event.SourceVersion(), event.CreatedAt, record.SourceVersion, record.SourceEventAt,
			)
			if revisionOrder < 0 || (record.Tombstone && revisionOrder <= 0) {
				continue
			}
		}
		if record.Tombstone {
			record.Tombstone = false
			record.Edited = true
			record.Hash = ""
			record.Render = ""
		}
		if !exists || record.Number <= 0 {
			current.LastNumber++
			record.Number = current.LastNumber
		}
		filename := destination.documentName(record.Number, event.Position.UnitName)
		previousFilename := record.Filename
		target, err := safeDestinationPath(destination.Folder, filename)
		if err != nil {
			return err
		}
		unchanged := exists && !technicalMigration && ((record.Render != "" && record.Render == render) ||
			(record.Render == "" && record.Hash == digest && record.Filename == filename))
		if !unchanged {
			document.Edited = record.Edited || (exists && !technicalMigration && record.Render != "" && record.Render != render)
			if document.Edited {
				document.EditedAt = event.SourceAt
				if document.EditedAt.IsZero() {
					document.EditedAt = time.Now().UTC()
				}
			}
			payload, buildErr := Build(destination.TemplatePath, document)
			if buildErr != nil {
				return buildErr
			}
			if err := writeAtomic(ctx, target, payload); err != nil {
				return err
			}
			record.Hash = digest
			record.Render = render
			record.Filename = filename
			record.Edited = document.Edited
			record.UpdatedAt = time.Now().UTC()
		}
		record.SourceVersion = event.SourceVersion()
		record.SourceEventAt = event.CreatedAt
		current.Blocks[key] = record
		if migratedBlockKey != "" && migratedBlockKey != key {
			delete(current.Blocks, migratedBlockKey)
		}
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
		if previousFilename != "" && previousFilename != filename {
			if obsolete, pathErr := safeDestinationPath(destination.Folder, previousFilename); pathErr == nil {
				_ = os.Remove(obsolete)
			}
		}
	}
	if scopedBlockUUID == "" {
		for key, record := range current.Blocks {
			if (!strings.HasPrefix(key, prefix) && !strings.HasPrefix(key, legacyPrefix)) || active[key] {
				continue
			}
			// Объединённый rc81-документ уже перезаписан первым UUID-блоком и имеет
			// то же имя. Его файл удалять нельзя; удаляется только старый ключ.
			if key != combinedKey && key != legacyPrefix+"record" && record.Filename != "" {
				if obsolete, pathErr := safeDestinationPath(destination.Folder, record.Filename); pathErr == nil {
					_ = os.Remove(obsolete)
				}
			}
			delete(current.Blocks, key)
			if err := saveState(destination.StatePath, current); err != nil {
				return err
			}
		}
	} else {
		// При миграции объединённый ключ уже перезаписан выбранным блоком.
		// Удаляем только устаревшую ссылку, не удаляя сам новый документ.
		delete(current.Blocks, combinedKey)
		delete(current.Blocks, legacyPrefix+"record")
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
	}
	return nil
}

func (destination Destination) removeScopedBlock(
	current *state, prefix, legacyPrefix string, event domain.Event, blockUUID string, lastBlock bool,
) error {
	keys := []string{prefix + blockUUID, legacyPrefix + blockUUID}
	if lastBlock {
		keys = append(keys, prefix+"record", legacyPrefix+"record")
	}
	seen := make(map[string]bool)
	tombstone := blockRecord{}
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		record, exists := current.Blocks[key]
		if !exists {
			continue
		}
		if record.SourceVersion != "" || !record.SourceEventAt.IsZero() {
			order := domain.CompareSourceRevision(event.SourceVersion(), event.CreatedAt, record.SourceVersion, record.SourceEventAt)
			if order < 0 || (order == 0 && record.Tombstone) {
				return nil
			}
		}
		if tombstone.Number == 0 || strings.HasSuffix(key, blockUUID) {
			tombstone = record
		}
		if record.Filename != "" {
			path, err := safeDestinationPath(destination.Folder, record.Filename)
			if err != nil {
				return err
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("удалить Word-документ временного блока: %w", err)
			}
		}
		delete(current.Blocks, key)
	}
	tombstone.Hash = ""
	tombstone.Render = ""
	tombstone.Edited = true
	tombstone.Tombstone = true
	tombstone.SourceVersion = event.SourceVersion()
	tombstone.SourceEventAt = event.CreatedAt
	tombstone.UpdatedAt = time.Now().UTC()
	current.Blocks[prefix+blockUUID] = tombstone
	return nil
}

func docxRecordMutationIsStale(current state, prefix, legacyPrefix string, event domain.Event) bool {
	for key, record := range current.Blocks {
		if !strings.HasPrefix(key, prefix) && !strings.HasPrefix(key, legacyPrefix) {
			continue
		}
		if record.SourceVersion == "" && record.SourceEventAt.IsZero() {
			continue
		}
		if domain.CompareSourceRevision(
			event.SourceVersion(), event.CreatedAt, record.SourceVersion, record.SourceEventAt,
		) < 0 {
			return true
		}
	}
	return false
}

func storeWordRecordCleanup(current *state, sourceKey string, event domain.Event) {
	if event.SourceVersion() == "" && event.CreatedAt.IsZero() {
		return
	}
	if current.RecordTombstones == nil {
		current.RecordTombstones = make(map[string]recordRevisionBarrier)
	}
	current.RecordTombstones[sourceKey] = recordRevisionBarrier{
		SourceVersion: event.SourceVersion(), SourceEventAt: event.CreatedAt, UpdatedAt: time.Now().UTC(),
	}
}

func docxRecordCleanupRejects(current state, sourceKey string, event domain.Event) bool {
	barrier, exists := current.RecordTombstones[sourceKey]
	if !exists || (barrier.SourceVersion == "" && barrier.SourceEventAt.IsZero()) {
		return false
	}
	return domain.CompareSourceRevision(
		event.SourceVersion(), event.CreatedAt, barrier.SourceVersion, barrier.SourceEventAt,
	) <= 0
}

func docxEventBlocks(event domain.Event) []domain.ContentBlock {
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

func tryDestinationLock(path string) (func(), bool) {
	key, err := filepath.Abs(path)
	if err != nil {
		key = filepath.Clean(path)
	}
	key = strings.ToLower(filepath.Clean(key))
	destinationLocks.Lock()
	entry := destinationLocks.items[key]
	if entry == nil {
		entry = &destinationLock{}
		destinationLocks.items[key] = entry
	}
	entry.refs++
	destinationLocks.Unlock()
	if !entry.mutex.TryLock() {
		releaseDestinationLock(key, entry, false)
		return nil, false
	}
	return func() { releaseDestinationLock(key, entry, true) }, true
}

func releaseDestinationLock(key string, entry *destinationLock, held bool) {
	if held {
		entry.mutex.Unlock()
	}
	destinationLocks.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(destinationLocks.items, key)
	}
	destinationLocks.Unlock()
}
