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
func (destination *Destination) Deliver(ctx context.Context, event domain.Event, _ domain.PlannedDelivery) error {
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
	sourceKey := strings.TrimSpace(event.Source.UUID)
	if sourceKey == "" {
		sourceKey = "id:" + strconv.FormatInt(event.Source.ID, 10)
	}
	prefix := sourceKey + "|"
	key := prefix + "record"
	record, exists := current.Blocks[key]
	legacyMigration := false
	if !exists {
		for legacyKey, legacyRecord := range current.Blocks {
			if !strings.HasPrefix(legacyKey, prefix) || legacyKey == key {
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
		if !strings.HasPrefix(key, prefix) || key == prefix+"record" {
			continue
		}
		if record.Filename != "" && record.Filename != filename {
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
