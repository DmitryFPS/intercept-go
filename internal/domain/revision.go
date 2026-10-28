package domain

import (
	"errors"
	"strings"
	"time"
)

const (
	maxClientClockLead = 5 * time.Minute
	// Время без смещения хранится в локальном часовом поясе EAVC. Пятнадцать
	// часов покрывают все реальные часовые пояса и не позволяют случайной дате
	// из далёкого будущего навсегда отравить монотонный барьер.
	maxNaiveSourceLead = 15 * time.Hour
)

// ValidateRevisionClock ограничивает недоверенные часы рабочей точки до того,
// как версия сможет стать постоянным барьером Центра.
func ValidateRevisionClock(event Event, now time.Time) error {
	now = now.UTC()
	if event.CreatedAt.After(now.Add(maxClientClockLead)) {
		return errors.New("время создания события находится в будущем")
	}
	if !event.SourceAt.IsZero() && event.SourceAt.After(now.Add(maxClientClockLead)) {
		return errors.New("время источника находится в будущем")
	}
	version := strings.TrimSpace(event.SourceVersion())
	if version == "" {
		return nil
	}
	versionAt, ok := parseSourceTime(version, time.UTC)
	if !ok {
		return errors.New("версия источника имеет неподдерживаемый формат")
	}
	reference := event.SourceAt.UTC()
	if reference.IsZero() {
		reference = event.CreatedAt.UTC()
	}
	if versionAt.After(reference.Add(maxNaiveSourceLead)) {
		return errors.New("версия источника находится недопустимо далеко в будущем")
	}
	return nil
}

// CompareSourceRevision сравнивает две версии одного и того же блока EAVC.
// Версия источника имеет приоритет, а CreatedAt разрешает быстрые изменения с
// одинаковой секундой и сохраняет порядок при перестановке транспортных пакетов.
// Результат: -1 — входящая версия старее, 0 — та же, 1 — новее.
func CompareSourceRevision(
	incomingVersion string, incomingCreatedAt time.Time,
	storedVersion string, storedCreatedAt time.Time,
) int {
	incomingVersion = strings.TrimSpace(incomingVersion)
	storedVersion = strings.TrimSpace(storedVersion)
	if storedVersion == "" && storedCreatedAt.IsZero() {
		return 1
	}
	if incomingVersion != "" && storedVersion != "" {
		incomingTime, incomingOK := parseSourceTime(incomingVersion, time.UTC)
		storedTime, storedOK := parseSourceTime(storedVersion, time.UTC)
		if incomingOK && storedOK && !incomingTime.Equal(storedTime) {
			if incomingTime.Before(storedTime) {
				return -1
			}
			return 1
		}
		if incomingVersion == storedVersion || (incomingOK && storedOK) {
			return compareCreatedAt(incomingCreatedAt, storedCreatedAt)
		}
	}
	if !incomingCreatedAt.IsZero() && !storedCreatedAt.IsZero() {
		return compareCreatedAt(incomingCreatedAt, storedCreatedAt)
	}
	if incomingVersion == storedVersion {
		return 0
	}
	// Неизвестный формат нельзя безопасно объявить более новым. Современная
	// EAVC использует поддерживаемые временные форматы; этот случай остаётся
	// консервативной защитой от воскрешения старого состояния.
	return 0
}

func compareCreatedAt(incoming, stored time.Time) int {
	if incoming.IsZero() || stored.IsZero() || incoming.Equal(stored) {
		return 0
	}
	if incoming.Before(stored) {
		return -1
	}
	return 1
}
