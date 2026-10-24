package docx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

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
		defer unlock()
		completed <- settings.deliver(ctx, event, delivery)
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
			current = state{Scope: scope, Blocks: make(map[string]blockRecord)}
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
	if delivery.Decision.Action == domain.RouteSkip {
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
		return saveState(destination.StatePath, current)
	}
	blocks := event.Blocks
	if blocks == nil {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	content := strings.TrimSpace(event.Content)
	if content == "" {
		parts := make([]string, 0, len(blocks))
		for _, block := range blocks {
			if text := strings.TrimSpace(block.Text); text != "" {
				parts = append(parts, text)
			}
		}
		content = strings.Join(parts, "\n\n")
	}
	callsigns := make([]domain.Callsign, 0)
	for _, block := range blocks {
		callsigns = append(callsigns, block.Callsigns...)
	}
	key := prefix + "record"
	record, exists := current.Blocks[key]
	legacyMigration := false
	if !exists {
		for legacyKey, legacyRecord := range current.Blocks {
			if (!strings.HasPrefix(legacyKey, prefix) && !strings.HasPrefix(legacyKey, legacyPrefix)) || legacyKey == key {
				continue
			}
			if !legacyMigration || legacyRecord.Number < record.Number {
				record = legacyRecord
				legacyMigration = true
			}
		}
	}
	if !exists && !legacyMigration {
		current.LastNumber++
		record.Number = current.LastNumber
	}
	digest := digestText(content)
	date := event.SourceAt
	if date.IsZero() {
		date = event.ObservedAt
	}
	document := Document{
		Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
		GroupCode: event.Position.GroupCode, Network: destination.Network, Protection: destination.Protection,
		Date: date, SourceUpdatedAt: event.Source.UpdatedAt, Operator: event.UpdatedBy, Content: content,
		Callsigns: callsigns,
	}
	render := renderDigest(document)
	filename := destination.documentName(record.Number, event.Position.UnitName)
	previousFilename := record.Filename
	target, err := safeDestinationPath(destination.Folder, filename)
	if err != nil {
		return err
	}
	unchanged := exists && ((record.Render != "" && record.Render == render) ||
		(record.Render == "" && record.Hash == digest && record.Filename == filename))
	if !unchanged {
		document.Edited = record.Edited || (exists && record.Render != "" && record.Render != render)
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
		if err := ctx.Err(); err != nil {
			return err
		}
		record.Hash = digest
		record.Render = render
		record.Filename = filename
		record.Edited = document.Edited
		record.UpdatedAt = time.Now().UTC()
	}
	current.Blocks[key] = record
	for key, record := range current.Blocks {
		if (!strings.HasPrefix(key, prefix) && !strings.HasPrefix(key, legacyPrefix)) || key == prefix+"record" {
			continue
		}
		if record.Filename != "" && record.Filename != filename {
			if obsolete, pathErr := safeDestinationPath(destination.Folder, record.Filename); pathErr == nil {
				_ = os.Remove(obsolete)
			}
		}
		delete(current.Blocks, key)
	}
	if err := saveState(destination.StatePath, current); err != nil {
		return err
	}
	// После успешной фиксации нового имени удаляем предыдущий документ этой
	// же записи, чтобы исправление подразделения не оставляло два DOCX.
	if previousFilename != "" && previousFilename != filename {
		if obsolete, pathErr := safeDestinationPath(destination.Folder, previousFilename); pathErr == nil {
			_ = os.Remove(obsolete)
		}
	}
	return nil
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
