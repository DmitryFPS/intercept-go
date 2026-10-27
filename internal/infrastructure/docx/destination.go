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
		if !exists {
			for _, legacyKey := range []string{prefix + block.Key, legacyPrefix + block.Key} {
				if legacyRecord, ok := current.Blocks[legacyKey]; ok {
					record = legacyRecord
					exists = true
					technicalMigration = true
					break
				}
			}
		}
		if !exists && index == 0 && hasCombined {
			record = combined
			exists = true
			technicalMigration = true
		}
		if !exists {
			current.LastNumber++
			record.Number = current.LastNumber
		}
		content := strings.TrimSpace(block.Text)
		digest := digestText(content)
		document := Document{
			Unit: event.Position.UnitName, Frequency: event.Position.Frequency,
			GroupCode: event.Position.GroupCode, Network: destination.Network, Protection: destination.Protection,
			Date: date, SourceUpdatedAt: event.Source.UpdatedAt, Operator: event.UpdatedBy, Content: content,
			Callsigns: block.Callsigns,
		}
		render := renderDigest(document)
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
		current.Blocks[key] = record
		if err := saveState(destination.StatePath, current); err != nil {
			return err
		}
		if previousFilename != "" && previousFilename != filename {
			if obsolete, pathErr := safeDestinationPath(destination.Folder, previousFilename); pathErr == nil {
				_ = os.Remove(obsolete)
			}
		}
	}
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
	return nil
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
