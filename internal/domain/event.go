package domain

import (
	"errors"
	"strings"
	"time"
)

// SourceMode описывает режим, в котором EAVC создал перехват.
type SourceMode string

const (
	SourceModeNormal   SourceMode = "normal"
	SourceModeDeferred SourceMode = "deferred"
	SourceModeUnknown  SourceMode = "unknown"
)

// Event — независимое от транспорта представление одного состояния перехвата.
type Event struct {
	ID             string       `json:"event_id"`
	ClientID       string       `json:"client_id"`
	CreatedAt      time.Time    `json:"created_at"`
	ObservedAt     time.Time    `json:"observed_at"`
	SourceAt       time.Time    `json:"source_at"`
	ExpiresAt      time.Time    `json:"expires_at"`
	FreshnessIssue string       `json:"freshness_issue,omitempty"`
	Source         SourceRecord `json:"source"`
	Position       Position     `json:"position"`
	Mode           SourceMode   `json:"source_mode"`
	Content        string       `json:"content"`
	UpdatedBy      string       `json:"updated_by,omitempty"`
	ContentDigest  string       `json:"content_digest"`
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
	switch event.Mode {
	case SourceModeNormal, SourceModeDeferred, SourceModeUnknown:
		return nil
	default:
		return errors.New("неподдерживаемый режим источника")
	}
}

// PositionKey возвращает стабильный ключ каталога либо строгий ключ названия.
func (event Event) PositionKey() string {
	if catalogUUID := strings.TrimSpace(event.Position.CatalogUUID); catalogUUID != "" {
		return "catalog:" + strings.ToLower(catalogUUID)
	}
	return "name:" + NormalizeUnitName(event.Position.UnitName)
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
