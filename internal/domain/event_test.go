package domain

import (
	"strings"
	"testing"
	"time"
)

func validScopedEventForDigestTest() Event {
	now := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	text := "10.15\n-Доклад принят. (61021)"
	event := Event{
		ID: "event-1", ClientID: "textile", CreatedAt: now, ObservedAt: now, SourceAt: now,
		ExpiresAt: now.Add(30 * time.Minute),
		Source: SourceRecord{
			ID: 17, UUID: "record-17", SessionID: 3, SessionUUID: "session-3",
			UpdatedAt: "2026-10-28 12:00:00",
		},
		Position: Position{
			CatalogUUID: "catalog-1", UnitName: "Игровая команда",
			Frequency: "119.009", GroupCode: "E2E",
		},
		Mode: SourceModeNormal, Content: text,
		Blocks: []ContentBlock{{
			UUID: "block-1", Key: "10.15", Sequence: 0, Text: text,
			Hash: "source-hash", UpdatedAt: "2026-10-28 12:00:00",
			Callsigns: []Callsign{{Number: 1, Code: "61021", Label: "Рубин", Description: "наблюдатель"}},
		}},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		ContentDigestVersion: ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	return event
}

func cloneDigestTestEvent(event Event) Event {
	clone := event
	clone.Blocks = append([]ContentBlock(nil), event.Blocks...)
	for index := range clone.Blocks {
		clone.Blocks[index].Callsigns = append([]Callsign(nil), event.Blocks[index].Callsigns...)
	}
	return clone
}

func TestEventValidateRequiresOneScopedSourceOfText(t *testing.T) {
	event := validScopedEventForDigestTest()
	event.Blocks[0].Text += "\n-Строка только в блоке"
	event.ContentDigest = event.CanonicalContentDigest()
	if err := event.Validate(); err == nil || !strings.Contains(err.Error(), "текст события") {
		t.Fatalf("расхождение Content и Blocks[0].Text не отклонено: %v", err)
	}
	legacyFormatting := validScopedEventForDigestTest()
	legacyFormatting.Blocks[0].Text = strings.ReplaceAll(legacyFormatting.Content, "\n", "\r\n") + "\r\n"
	legacyFormatting.ContentDigest = legacyFormatting.CanonicalContentDigest()
	if err := legacyFormatting.Validate(); err != nil {
		t.Fatalf("безопасное отличие CRLF/trim старого Клиента ошибочно отклонено: %v", err)
	}
}

func TestContentDigestV2BindsRenderingAndRoutingFields(t *testing.T) {
	base := validScopedEventForDigestTest()
	digest := base.ContentDigest
	mutations := []struct {
		name   string
		mutate func(*Event)
	}{
		{name: "catalog UUID", mutate: func(event *Event) { event.Position.CatalogUUID = "catalog-2" }},
		{name: "название маршрута", mutate: func(event *Event) { event.Position.UnitName = "Другая команда" }},
		{name: "частота", mutate: func(event *Event) { event.Position.Frequency = "120.500" }},
		{name: "группа", mutate: func(event *Event) { event.Position.GroupCode = "OTHER" }},
		{name: "позывной", mutate: func(event *Event) { event.Blocks[0].Callsigns[0].Label = "Вектор" }},
		{name: "описание позывного", mutate: func(event *Event) { event.Blocks[0].Callsigns[0].Description = "старший" }},
		{name: "оператор", mutate: func(event *Event) { event.UpdatedBy = "другой оператор" }},
		{name: "оператор блока", mutate: func(event *Event) { event.Blocks[0].UpdatedBy = "другой оператор блока" }},
		{name: "точное время блока", mutate: func(event *Event) { event.SourceAt = event.SourceAt.Add(time.Second) }},
		{name: "версия блока", mutate: func(event *Event) { event.Blocks[0].UpdatedAt = "2026-10-28 12:00:01" }},
		{name: "текст", mutate: func(event *Event) {
			event.Content += "\n-Уточнение"
			event.Blocks[0].Text = event.Content
		}},
		{name: "дата Telegram", mutate: func(event *Event) {
			event.Source.UpdatedAt = "2026-10-29 00:01:00"
		}},
		{name: "область очистки", mutate: func(event *Event) { event.HasActiveBlocks = false }},
	}
	for _, scenario := range mutations {
		t.Run(scenario.name, func(t *testing.T) {
			event := cloneDigestTestEvent(base)
			scenario.mutate(&event)
			if got := event.CanonicalContentDigest(); got == digest {
				t.Fatalf("поле %q не связано с v2 digest", scenario.name)
			}
		})
	}

	// Глобальное время записи EAVC и Sequence соседних блоков меняются при
	// редакции/удалении другого блока. Пока итоговая дата и содержимое этого
	// scoped-блока прежние, он не должен ложно становиться новой редакцией.
	unchanged := cloneDigestTestEvent(base)
	unchanged.Source.UpdatedAt = "2026-10-28 18:45:00"
	unchanged.ExpiresAt = unchanged.ExpiresAt.Add(6 * time.Hour)
	unchanged.CreatedAt = unchanged.CreatedAt.Add(6 * time.Hour)
	unchanged.ObservedAt = unchanged.ObservedAt.Add(6 * time.Hour)
	unchanged.Blocks[0].Sequence = 19
	unchanged.HasActiveBlocks = true
	if got := unchanged.CanonicalContentDigest(); got != digest {
		t.Fatalf("неизменный scoped-блок получил новый digest: %s != %s", got, digest)
	}
}

func TestContentDigestV2CanonicalizesEquivalentSourceVersionFormats(t *testing.T) {
	sqliteTime := validScopedEventForDigestTest()
	sqliteTime.Blocks[0].UpdatedAt = "2026-10-28 12:00:00"
	sqliteTime.ContentDigest = sqliteTime.CanonicalContentDigest()

	rfc3339Time := cloneDigestTestEvent(sqliteTime)
	rfc3339Time.Blocks[0].UpdatedAt = "2026-10-28T12:00:00Z"
	rfc3339Time.ContentDigest = rfc3339Time.CanonicalContentDigest()
	if sqliteTime.CanonicalSourceVersion() != rfc3339Time.CanonicalSourceVersion() {
		t.Fatalf("эквивалентные версии не канонизированы: %q != %q",
			sqliteTime.CanonicalSourceVersion(), rfc3339Time.CanonicalSourceVersion())
	}
	if sqliteTime.ContentDigest != rfc3339Time.ContentDigest {
		t.Fatalf("формат времени изменил v2 digest: %s != %s",
			sqliteTime.ContentDigest, rfc3339Time.ContentDigest)
	}
}

func TestContentDigestRequiresStrictCurrentContract(t *testing.T) {
	current := validScopedEventForDigestTest()
	current.Blocks[0].Callsigns[0].Label = "Подменённый"
	if err := current.ValidateContentDigest(); err == nil {
		t.Fatal("подмена позывного при v2 не обнаружена")
	}
	missingVersion := validScopedEventForDigestTest()
	missingVersion.ContentDigestVersion = ""
	if err := missingVersion.ValidateContentDigest(); err == nil || !strings.Contains(err.Error(), "несовместимая версия") {
		t.Fatalf("пакет без версии не отклонён: %v", err)
	}
	oldVersion := validScopedEventForDigestTest()
	oldVersion.ContentDigestVersion = "v1"
	if err := oldVersion.ValidateContentDigest(); err == nil || !strings.Contains(err.Error(), "несовместимая версия") {
		t.Fatalf("старый v1 не отклонён: %v", err)
	}
	current.ContentDigestVersion = "v999"
	if err := current.ValidateContentDigest(); err == nil || !strings.Contains(err.Error(), "несовместимая версия") {
		t.Fatalf("неизвестная версия digest не отклонена: %v", err)
	}
}

func TestEventValidateLimitsIdentifiersAndRoutingFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Event)
	}{
		{name: "event ID", mutate: func(event *Event) { event.ID = strings.Repeat("e", maxEventIDBytes+1) }},
		{name: "client ID", mutate: func(event *Event) { event.ClientID = strings.Repeat("c", maxClientIDBytes+1) }},
		{name: "source UUID", mutate: func(event *Event) { event.Source.UUID = strings.Repeat("s", maxSourceUUIDBytes+1) }},
		{name: "session UUID", mutate: func(event *Event) { event.Source.SessionUUID = strings.Repeat("s", maxSourceUUIDBytes+1) }},
		{name: "catalog UUID", mutate: func(event *Event) { event.Position.CatalogUUID = strings.Repeat("c", maxCatalogUUIDBytes+1) }},
		{name: "unit name", mutate: func(event *Event) { event.Position.UnitName = strings.Repeat("u", maxUnitNameBytes+1) }},
		{name: "frequency", mutate: func(event *Event) { event.Position.Frequency = strings.Repeat("f", maxFrequencyBytes+1) }},
		{name: "group code", mutate: func(event *Event) { event.Position.GroupCode = strings.Repeat("g", maxGroupCodeBytes+1) }},
		{name: "block UUID", mutate: func(event *Event) {
			value := strings.Repeat("b", maxBlockUUIDBytes+1)
			event.SourceBlockUUID, event.Blocks[0].UUID = value, value
		}},
		{name: "block key", mutate: func(event *Event) { event.Blocks[0].Key = strings.Repeat("k", maxContentBlockKeyBytes+1) }},
		{name: "event content", mutate: func(event *Event) { event.Content = strings.Repeat("x", maxEventContentBytes+1) }},
		{name: "block text", mutate: func(event *Event) {
			event.Blocks[0].Text = strings.Repeat("x", maxEventContentBytes+1)
			event.Content = event.Blocks[0].Text
		}},
		{name: "block count", mutate: func(event *Event) { event.Blocks = make([]ContentBlock, maxEventBlocks+1) }},
		{name: "callsign count", mutate: func(event *Event) {
			event.Blocks[0].Callsigns = make([]Callsign, maxBlockCallsigns+1)
		}},
	}
	for _, scenario := range tests {
		t.Run(scenario.name, func(t *testing.T) {
			event := cloneDigestTestEvent(validScopedEventForDigestTest())
			scenario.mutate(&event)
			event.ContentDigest = event.CanonicalContentDigest()
			if err := event.Validate(); err == nil {
				t.Fatalf("чрезмерное поле %q не отклонено", scenario.name)
			}
		})
	}
}
