package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var deferredSuffix = regexp.MustCompile(`(?i)\s*\(\s*отложенный\s+режим\s*\)\s*$`)

// SourceMode описывает режим, в котором EAVC создал перехват.
type SourceMode string

const (
	SourceModeNormal   SourceMode = "normal"
	SourceModeDeferred SourceMode = "deferred"
	SourceModeUnknown  SourceMode = "unknown"
)

// Event — независимое от транспорта представление одного состояния перехвата.
type Event struct {
	ID                 string         `json:"event_id"`
	ClientID           string         `json:"client_id"`
	CreatedAt          time.Time      `json:"created_at"`
	ObservedAt         time.Time      `json:"observed_at"`
	SourceAt           time.Time      `json:"source_at"`
	ExpiresAt          time.Time      `json:"expires_at"`
	FreshnessIssue     string         `json:"freshness_issue,omitempty"`
	Source             SourceRecord   `json:"source"`
	Position           Position       `json:"position"`
	Mode               SourceMode     `json:"source_mode"`
	Content            string         `json:"content"`
	Blocks             []ContentBlock `json:"blocks,omitempty"`
	SourceBlockUUID    string         `json:"source_block_uuid,omitempty"`
	SourceBlockDeleted bool           `json:"source_block_deleted,omitempty"`
	ActiveBlockUUIDs   []string       `json:"active_block_uuids,omitempty"`
	UpdatedBy          string         `json:"updated_by,omitempty"`
	ContentDigest      string         `json:"content_digest"`
}

// Expired сообщает, что событие больше нельзя впервые передавать получателям.
func (event Event) Expired(now time.Time) bool {
	return event.FreshnessIssue != "" || (!event.ExpiresAt.IsZero() && now.UTC().After(event.ExpiresAt.UTC()))
}

// SourceRecord связывает событие с записью EAVC, но не выдаёт её за ID позиции.
type SourceRecord struct {
	ID          int64  `json:"id"`
	UUID        string `json:"uuid"`
	SessionID   int64  `json:"session_id,omitempty"`
	SessionUUID string `json:"session_uuid,omitempty"`
	UpdatedAt   string `json:"updated_at,omitempty"`
}

// Position содержит доступные в текущей схеме EAVC признаки позиции.
type Position struct {
	CatalogUUID string `json:"catalog_uuid,omitempty"`
	UnitName    string `json:"unit_name"`
	Frequency   string `json:"frequency,omitempty"`
	GroupCode   string `json:"group_code,omitempty"`
}

// Validate проверяет обязательные инварианты события до постановки в очередь.
func (event Event) Validate() error {
	if strings.TrimSpace(event.ID) == "" {
		return errors.New("не задан ID события")
	}
	if strings.TrimSpace(event.ClientID) == "" {
		return errors.New("не задан ID точки")
	}
	if event.CreatedAt.IsZero() {
		return errors.New("не задано время создания события")
	}
	if strings.TrimSpace(event.Source.UUID) == "" && event.Source.ID == 0 {
		return errors.New("не задан идентификатор записи EAVC")
	}
	if strings.TrimSpace(event.Position.UnitName) == "" {
		return errors.New("не задано подразделение")
	}
	if strings.TrimSpace(event.ContentDigest) == "" {
		return errors.New("не задан отпечаток содержимого")
	}
	blockUUID := strings.TrimSpace(event.SourceBlockUUID)
	if event.SourceBlockDeleted && blockUUID == "" {
		return errors.New("для удаления не задан UUID временного блока")
	}
	if blockUUID != "" {
		if len(event.Blocks) != 1 {
			return errors.New("событие временного блока должно содержать ровно один блок")
		}
		if !strings.EqualFold(strings.TrimSpace(event.Blocks[0].Identity()), blockUUID) {
			return errors.New("UUID события не соответствует временному блоку")
		}
	}
	switch event.Mode {
	case SourceModeNormal, SourceModeDeferred, SourceModeUnknown:
		return nil
	default:
		return errors.New("неподдерживаемый режим источника")
	}
}

// SourceLineageKey разделяет редакции разных временных блоков одной записи.
// Исходный UUID EAVC в самом событии не меняется и остаётся доступным аудиту.
func (event Event) SourceLineageKey() string {
	base := strings.TrimSpace(event.Source.UUID)
	if base == "" {
		base = fmt.Sprintf("id:%d", event.Source.ID)
	}
	if block := strings.TrimSpace(event.SourceBlockUUID); block != "" {
		return base + "|block:" + strings.ToLower(block)
	}
	return base
}

// SourceVersion возвращает версию именно самостоятельного блока. Изменение
// соседнего блока поэтому не создаёт ложную редакцию этого события.
func (event Event) SourceVersion() string {
	if blockUUID := strings.TrimSpace(event.SourceBlockUUID); blockUUID != "" {
		for _, block := range event.Blocks {
			if !strings.EqualFold(block.Identity(), blockUUID) {
				continue
			}
			if event.SourceBlockDeleted && strings.TrimSpace(block.DeletedAt) != "" {
				return strings.TrimSpace(block.DeletedAt)
			}
			if strings.TrimSpace(block.UpdatedAt) != "" {
				return strings.TrimSpace(block.UpdatedAt)
			}
		}
	}
	return strings.TrimSpace(event.Source.UpdatedAt)
}

// PositionKey возвращает стабильный ключ каталога либо строгий ключ названия.
func (event Event) PositionKey() string {
	if catalogUUID := strings.TrimSpace(event.Position.CatalogUUID); catalogUUID != "" {
		return "catalog:" + strings.ToLower(catalogUUID)
	}
	return "name:" + NormalizeUnitName(RoutingUnitName(event.Position.UnitName))
}

// RoutingUnitName убирает только служебную пометку отложенного перехвата.
// Исходное название в событии не меняется и полностью попадает в Telegram и Word.
// RoutingUnitName возвращает название, используемое только для поиска маршрута.
func RoutingUnitName(value string) string {
	return strings.TrimSpace(deferredSuffix.ReplaceAllString(strings.TrimSpace(value), ""))
}

// IsDeferredUnitName сообщает, что оператор добавил служебную пометку в EAVC.
func IsDeferredUnitName(value string) bool {
	return deferredSuffix.MatchString(strings.TrimSpace(value))
}

// NormalizeUnitName выполняет безопасную нормализацию без исправления опечаток.
func NormalizeUnitName(value string) string {
	replacer := strings.NewReplacer(
		"«", "\"",
		"»", "\"",
		"“", "\"",
		"”", "\"",
		"„", "\"",
		"’", "'",
		"`", "'",
	)
	return strings.ToLower(strings.Join(strings.Fields(replacer.Replace(value)), " "))
}
