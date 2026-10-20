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
	Network      string
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
func (destination *Destination) Deliver(ctx context.Context, event domain.Event, _ domain.PlannedDelivery) error {
	settings := *destination
	if settings.Prefix == "" {
		settings.Prefix = "СТНД"
	}
	if settings.Network == "" {
		settings.Network = defaultNetwork
	}
	if settings.TemplatePath == "" || settings.Folder == "" || settings.StatePath == "" {
		return errors.New("Word-направление настроено не полностью")
	}
	unlock, acquired := tryDestinationLock(settings.StatePath)
	if !acquired {
		return errDestinationBusy
	}
	defer unlock()
	// Файловые вызовы выполняются в текущей рабочей операции. Так сервер не
	// объявит доставку завершённой или просроченной, пока скрытая горутина ещё
	// способна перезаписать документ в сетевой папке.
	return settings.deliver(ctx, event)
}

func (destination Destination) deliver(ctx context.Context, event domain.Event) error {
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
	blocks := event.Blocks
	if blocks == nil {
		blocks = domain.SplitContentBlocks(event.Content)
	}
	sourceKey := strings.TrimSpace(event.Source.UUID)
	if sourceKey == "" {
		sourceKey = "id:" + strconv.FormatInt(event.Source.ID, 10)
	}
	prefix := sourceKey + "|"
	present := make(map[string]bool, len(blocks))
	for _, block := range blocks {
		key := prefix + block.Key
		present[key] = true
		digest := digestText(block.Text)
		record, exists := current.Blocks[key]
		if !exists {
			current.LastNumber++
			record.Number = current.LastNumber
		}
		date := event.SourceAt
		if date.IsZero() {
			date = event.ObservedAt
		}
		document := Document{
			Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
			GroupCode: event.Position.GroupCode, Network: destination.Network,
			Date: date, SourceUpdatedAt: event.Source.UpdatedAt, Operator: event.UpdatedBy, Content: block.Text,
			Callsigns: block.Callsigns,
		}
		render := renderDigest(document)
		filename := destination.documentName(record.Number, event.Position.UnitName)
		target, err := safeDestinationPath(destination.Folder, filename)
		if err != nil {
			return err
		}
		if exists && ((record.Render != "" && record.Render == render) ||
			(record.Render == "" && record.Hash == digest && record.Filename == filename)) {
			continue
		}
		document.Edited = record.Edited || (exists && record.Render != "" && record.Render != render)
		if document.Edited {
			document.EditedAt = time.Now().UTC()
		}
		payload, err := Build(destination.TemplatePath, document)
		if err != nil {
			return err
		}
		if err := writeAtomic(ctx, target, payload); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if record.Filename != "" && record.Filename != filename {
			if previous, pathErr := safeDestinationPath(destination.Folder, record.Filename); pathErr == nil {
				_ = os.Remove(previous)
			}
		}
		record.Hash = digest
		record.Render = render
		record.Filename = filename
		record.Edited = document.Edited
		record.UpdatedAt = time.Now().UTC()
		current.Blocks[key] = record
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
	}
	for key, record := range current.Blocks {
		if !strings.HasPrefix(key, prefix) || present[key] {
			continue
		}
		if record.Filename != "" {
			if obsolete, pathErr := safeDestinationPath(destination.Folder, record.Filename); pathErr == nil {
				_ = os.Remove(obsolete)
			}
		}
		delete(current.Blocks, key)
	}
	return saveState(destination.StatePath, current)
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
