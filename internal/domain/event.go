package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var (
	deferredSuffix   = regexp.MustCompile(`(?i)\s*\(\s*отложенный\s+режим\s*\)\s*$`)
	sourceDatePrefix = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})`)
)

// SourceMode описывает режим, в котором EAVC создал перехват.
type SourceMode string

const (
	SourceModeNormal   SourceMode = "normal"
	SourceModeDeferred SourceMode = "deferred"
	SourceModeUnknown  SourceMode = "unknown"

	// ContentDigestVersionCurrent связывает с отпечатком структуру блока,
	// справочник позиций и позывные, влияющие на маршрут и представление.
	ContentDigestVersionCurrent = "v2"
)

const (
	maxEventIDBytes         = 128
	maxClientIDBytes        = 128
	maxSourceUUIDBytes      = 256
	maxBlockUUIDBytes       = 256
	maxCatalogUUIDBytes     = 256
	maxUnitNameBytes        = 512
	maxFrequencyBytes       = 128
	maxGroupCodeBytes       = 256
	maxContentBlockKeyBytes = 256
	maxEventContentBytes    = 1 << 20
	maxEventBlocks          = 1024
	maxBlockCallsigns       = 4096
)

// Event — независимое от транспорта представление одного состояния перехвата.
type Event struct {
	ID                   string         `json:"event_id"`
	ClientID             string         `json:"client_id"`
	CreatedAt            time.Time      `json:"created_at"`
	ObservedAt           time.Time      `json:"observed_at"`
	SourceAt             time.Time      `json:"source_at"`
	ExpiresAt            time.Time      `json:"expires_at"`
	FreshnessIssue       string         `json:"freshness_issue,omitempty"`
	Source               SourceRecord   `json:"source"`
	Position             Position       `json:"position"`
	Mode                 SourceMode     `json:"source_mode"`
	Content              string         `json:"content"`
	Blocks               []ContentBlock `json:"blocks,omitempty"`
	SourceBlockUUID      string         `json:"source_block_uuid,omitempty"`
	SourceBlockDeleted   bool           `json:"source_block_deleted,omitempty"`
	HasActiveBlocks      bool           `json:"has_active_blocks,omitempty"`
	UpdatedBy            string         `json:"updated_by,omitempty"`
	ContentDigest        string         `json:"content_digest"`
	ContentDigestVersion string         `json:"content_digest_version,omitempty"`
}

// Expired сообщает, что событие больше нельзя впервые передавать получателям.
func (event Event) Expired(now time.Time) bool {
	return event.FreshnessIssue != "" || (!event.ExpiresAt.IsZero() && now.UTC().After(event.ExpiresAt.UTC()))
}

// CanonicalContentDigest пересчитывает отпечаток нового базового контракта.
func (event Event) CanonicalContentDigest() string {
	return event.canonicalContentDigestV2()
}

// ValidateContentDigest принимает только новый базовый контракт. Попытка
// открыть пакет прежней пары Клиент/Сервер завершается явной диагностикой.
func (event Event) ValidateContentDigest() error {
	version := strings.TrimSpace(event.ContentDigestVersion)
	if version != ContentDigestVersionCurrent {
		return fmt.Errorf(
			"несовместимая версия отпечатка %q, требуется %q",
			version, ContentDigestVersionCurrent,
		)
	}
	expected := event.CanonicalContentDigest()
	if !strings.EqualFold(strings.TrimSpace(event.ContentDigest), expected) {
		return errors.New("отпечаток содержимого события не совпадает с данными")
	}
	return nil
}

type digestV2Callsign struct {
	Number      int64  `json:"number"`
	Code        string `json:"code"`
	Label       string `json:"label"`
	Tag         string `json:"tag"`
	Description string `json:"description"`
}

type digestV2Block struct {
	UUID      string             `json:"uuid"`
	Key       string             `json:"key"`
	Sequence  int                `json:"sequence"`
	Text      string             `json:"text"`
	Hash      string             `json:"hash"`
	UpdatedBy string             `json:"updated_by"`
	UpdatedAt string             `json:"updated_at"`
	DeletedAt string             `json:"deleted_at"`
	Callsigns []digestV2Callsign `json:"callsigns"`
}

type digestV2Payload struct {
	Version             string          `json:"version"`
	SourceID            int64           `json:"source_id"`
	SourceUUID          string          `json:"source_uuid"`
	SourceSessionID     int64           `json:"source_session_id"`
	SourceSessionUUID   string          `json:"source_session_uuid"`
	SourceAt            string          `json:"source_at"`
	SourceVersion       string          `json:"source_version"`
	TelegramDate        string          `json:"telegram_date"`
	DocumentDate        string          `json:"document_date"`
	Position            Position        `json:"position"`
	Mode                SourceMode      `json:"source_mode"`
	Content             string          `json:"content"`
	Blocks              []digestV2Block `json:"blocks"`
	SourceBlockUUID     string          `json:"source_block_uuid"`
	SourceBlockDeleted  bool            `json:"source_block_deleted"`
	HasActiveBlockScope bool            `json:"has_active_block_scope"`
	UpdatedBy           string          `json:"updated_by"`
}

func (event Event) canonicalContentDigestV2() string {
	blocks := make([]digestV2Block, 0, len(event.Blocks))
	for _, block := range event.Blocks {
		callsigns := make([]digestV2Callsign, 0, len(block.Callsigns))
		for _, callsign := range block.Callsigns {
			callsigns = append(callsigns, digestV2Callsign{
				Number: callsign.Number, Code: callsign.Code, Label: callsign.Label,
				Tag: callsign.Tag, Description: callsign.Description,
			})
		}
		digestBlock := digestV2Block{
			Text: normalizeDigestText(block.Text), UpdatedBy: block.UpdatedBy, Callsigns: callsigns,
		}
		if strings.TrimSpace(event.SourceBlockUUID) == "" {
			// В старом многоблочном событии последовательность действительно
			// определяет порядок вывода. В scoped-событии блок ровно один, поэтому
			// сдвиг его Sequence после удаления соседа не является новой редакцией.
			digestBlock.UUID, digestBlock.Key, digestBlock.Sequence = block.UUID, block.Key, block.Sequence
			digestBlock.Hash = block.Hash
			digestBlock.UpdatedAt, digestBlock.DeletedAt = block.UpdatedAt, block.DeletedAt
		}
		blocks = append(blocks, digestBlock)
	}
	payload := digestV2Payload{
		Version:  ContentDigestVersionCurrent,
		SourceID: event.Source.ID, SourceUUID: strings.TrimSpace(event.Source.UUID),
		SourceSessionID: event.Source.SessionID, SourceSessionUUID: strings.TrimSpace(event.Source.SessionUUID),
		SourceAt: digestTime(event.SourceAt), SourceVersion: event.CanonicalSourceVersion(),
		TelegramDate: event.telegramDateDigest(), DocumentDate: event.documentDateDigest(),
		Position: event.Position, Mode: event.Mode,
		Content: normalizeDigestText(event.Content), Blocks: blocks,
		SourceBlockUUID:     strings.TrimSpace(event.SourceBlockUUID),
		SourceBlockDeleted:  event.SourceBlockDeleted,
		HasActiveBlockScope: event.HasActiveBlocks,
		UpdatedBy:           event.UpdatedBy,
	}
	encoded, _ := json.Marshal(payload) // структура содержит только JSON-совместимые типы
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func normalizeDigestText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	return strings.TrimSpace(value)
}

func digestTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func (event Event) telegramDateDigest() string {
	if match := sourceDatePrefix.FindStringSubmatch(strings.TrimSpace(event.Source.UpdatedAt)); match != nil {
		return match[1] + "-" + match[2] + "-" + match[3]
	}
	value := event.SourceAt
	if value.IsZero() {
		value = event.ObservedAt
	}
	if value.IsZero() {
		value = event.CreatedAt
	}
	if value.IsZero() {
		return ""
	}
	return value.In(time.FixedZone("MSK", 3*60*60)).Format("2006-01-02")
}

func (event Event) documentDateDigest() string {
	value := event.SourceAt
	if value.IsZero() {
		value = event.ObservedAt
	}
	if value.IsZero() {
		return ""
	}
	return value.In(time.FixedZone("MSK", 3*60*60)).Format("2006-01-02")
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
	if err := validateStringLimit("ID события", event.ID, maxEventIDBytes); err != nil {
		return err
	}
	if strings.TrimSpace(event.ClientID) == "" {
		return errors.New("не задан ID точки")
	}
	if err := validateStringLimit("ID точки", event.ClientID, maxClientIDBytes); err != nil {
		return err
	}
	if event.CreatedAt.IsZero() {
		return errors.New("не задано время создания события")
	}
	if strings.TrimSpace(event.Source.UUID) == "" && event.Source.ID == 0 {
		return errors.New("не задан идентификатор записи EAVC")
	}
	if err := validateStringLimit("UUID записи EAVC", event.Source.UUID, maxSourceUUIDBytes); err != nil {
		return err
	}
	if err := validateStringLimit("UUID смены EAVC", event.Source.SessionUUID, maxSourceUUIDBytes); err != nil {
		return err
	}
	if strings.TrimSpace(event.Position.UnitName) == "" {
		return errors.New("не задано подразделение")
	}
	for _, field := range []struct {
		name  string
		value string
		limit int
	}{
		{name: "UUID каталога", value: event.Position.CatalogUUID, limit: maxCatalogUUIDBytes},
		{name: "название подразделения", value: event.Position.UnitName, limit: maxUnitNameBytes},
		{name: "частота", value: event.Position.Frequency, limit: maxFrequencyBytes},
		{name: "код группы", value: event.Position.GroupCode, limit: maxGroupCodeBytes},
	} {
		if err := validateStringLimit(field.name, field.value, field.limit); err != nil {
			return err
		}
	}
	if strings.TrimSpace(event.ContentDigest) == "" {
		return errors.New("не задан отпечаток содержимого")
	}
	if len(event.Content) > maxEventContentBytes {
		return fmt.Errorf("текст перехвата превышает допустимый размер %d байт", maxEventContentBytes)
	}
	if len(event.Blocks) > maxEventBlocks {
		return fmt.Errorf("слишком много блоков в событии: %d", len(event.Blocks))
	}
	blockUUID := strings.TrimSpace(event.SourceBlockUUID)
	if err := validateStringLimit("UUID временного блока", event.SourceBlockUUID, maxBlockUUIDBytes); err != nil {
		return err
	}
	for _, block := range event.Blocks {
		if err := validateStringLimit("UUID блока", block.UUID, maxBlockUUIDBytes); err != nil {
			return err
		}
		if err := validateStringLimit("ключ блока", block.Key, maxContentBlockKeyBytes); err != nil {
			return err
		}
		if len(block.Text) > maxEventContentBytes {
			return fmt.Errorf("текст временного блока превышает допустимый размер %d байт", maxEventContentBytes)
		}
		if len(block.Callsigns) > maxBlockCallsigns {
			return fmt.Errorf("слишком много корреспондентов во временном блоке: %d", len(block.Callsigns))
		}
	}
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
		if !event.SourceBlockDeleted && normalizeDigestText(event.Content) != normalizeDigestText(event.Blocks[0].Text) {
			return errors.New("текст события не совпадает с текстом временного блока")
		}
	}
	switch event.Mode {
	case SourceModeNormal, SourceModeDeferred, SourceModeUnknown:
		return nil
	default:
		return errors.New("неподдерживаемый режим источника")
	}
}

func validateStringLimit(name, value string, limit int) error {
	if len(value) > limit {
		return fmt.Errorf("%s превышает допустимую длину %d байт", name, limit)
	}
	return nil
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

// CanonicalSourceVersion приводит эквивалентные представления SQLite и
// RFC3339 к одной UTC-строке. Она используется в криптографическом отпечатке и
// монотонных барьерах, чтобы смена записи времени не выглядела новой версией.
func (event Event) CanonicalSourceVersion() string {
	value := strings.TrimSpace(event.SourceVersion())
	if parsed, ok := parseSourceTime(value, time.UTC); ok {
		return parsed.UTC().Format(time.RFC3339Nano)
	}
	return value
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
