package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"intercept/internal/domain"
)

const maxSourceTimeStateEntries = 10_000

// SourceTimeStateStore хранит локальные идентификаторы отдельных вхождений
// времени. Они не зависят от решётки и от физических UUID intercept_blocks.
type SourceTimeStateStore interface {
	LoadSourceTimeState(ctx context.Context, sourceKey string) (SourceTimeStateRecord, bool, error)
	SaveSourceTimeState(ctx context.Context, sourceKey string, record SourceTimeStateRecord) error
}

type SourceTimeStateRecord struct {
	Snapshot *SourceTimeSnapshot `json:"snapshot,omitempty"`
	Conflict *SourceTimeConflict `json:"conflict,omitempty"`
}

type SourceTimeSnapshot struct {
	SourceID      int64                  `json:"source_id"`
	SourceUUID    string                 `json:"source_uuid"`
	SourceVersion string                 `json:"source_version"`
	Blocks        []SourceTimeStateEntry `json:"blocks"`
}

// SourceTimeStateEntry описывает логическое вхождение заголовка. Несколько
// вхождений одного HH:MM имеют разные OccurrenceID. PhysicalUUID и
// PhysicalOrdinal используются только как подсказки следующего сопоставления.
type SourceTimeStateEntry struct {
	OccurrenceID    string              `json:"occurrence_id"`
	CanonicalTime   string              `json:"canonical_time"`
	DayOffset       int                 `json:"day_offset"`
	PhysicalUUID    string              `json:"physical_uuid,omitempty"`
	PhysicalOrdinal int                 `json:"physical_ordinal,omitempty"`
	Active          bool                `json:"active"`
	Block           domain.ContentBlock `json:"block"`
}

type SourceTimeConflict struct {
	Kind                  string   `json:"kind"`
	Reason                string   `json:"reason"`
	SourceVersion         string   `json:"source_version"`
	CanonicalTime         string   `json:"canonical_time,omitempty"`
	DayOffset             int      `json:"day_offset,omitempty"`
	PreviousOccurrenceIDs []string `json:"previous_occurrence_ids,omitempty"`
	CurrentOccurrences    int      `json:"current_occurrences,omitempty"`
	DetectedAt            string   `json:"detected_at"`
}

type sourceTimePlan struct {
	Item     SourceItem
	Record   SourceTimeStateRecord
	Conflict *SourceTimeConflict
}

func sourceTimeStateRecordsEqual(left, right SourceTimeStateRecord) bool {
	left = normalizedSourceTimeStateRecord(left)
	right = normalizedSourceTimeStateRecord(right)
	if !reflect.DeepEqual(left.Snapshot, right.Snapshot) {
		return false
	}
	if left.Conflict == nil || right.Conflict == nil {
		return left.Conflict == nil && right.Conflict == nil
	}
	leftConflict := *left.Conflict
	rightConflict := *right.Conflict
	leftConflict.DetectedAt = ""
	rightConflict.DetectedAt = ""
	return reflect.DeepEqual(leftConflict, rightConflict)
}

func normalizedSourceTimeStateRecord(record SourceTimeStateRecord) SourceTimeStateRecord {
	if record.Snapshot != nil {
		snapshot := *record.Snapshot
		snapshot.Blocks = append([]SourceTimeStateEntry(nil), snapshot.Blocks...)
		for index := range snapshot.Blocks {
			if len(snapshot.Blocks[index].Block.Callsigns) == 0 {
				snapshot.Blocks[index].Block.Callsigns = nil
			}
		}
		record.Snapshot = &snapshot
	}
	return record
}

type sourceTimeObservation struct {
	CanonicalTime   string
	Minutes         int
	DayOffset       int
	PhysicalUUID    string
	PhysicalOrdinal int
	Sequence        int
	Block           domain.ContentBlock
}

type timeObservationBuilder struct {
	canonical       string
	minutes         int
	dayOffset       int
	physicalUUID    string
	physicalOrdinal int
	sequence        int
	physical        domain.ContentBlock
	lines           []string
}

// sourceTimeStateKey использует ту же область записи, что и SourceLineageKey:
// UUID записи предпочтителен, локальный ID является запасным вариантом.
func sourceTimeStateKey(item SourceItem) string {
	if value := strings.ToLower(strings.TrimSpace(item.Record.UUID)); value != "" {
		return "uuid:" + value
	}
	return fmt.Sprintf("id:%d", item.Record.ID)
}

func planSourceTimeBlocks(
	item SourceItem, stored SourceTimeStateRecord, exists bool, now time.Time,
) sourceTimePlan {
	plan := sourceTimePlan{Item: item, Record: stored}
	observed, observeErr := observeSourceTimeBlocks(item.Blocks)
	if observeErr != nil {
		plan.Conflict = newSourceTimeConflict(
			"invalid_time_structure", observeErr.Error(), item.Record.UpdatedAt,
			"", 0, nil, 0, now,
		)
		plan.Record.Conflict = plan.Conflict
		return plan
	}
	if len(observed) > maxSourceTimeStateEntries {
		plan.Conflict = newSourceTimeConflict(
			"too_many_time_occurrences",
			fmt.Sprintf("в одной записи EAVC более %d временных блоков", maxSourceTimeStateEntries),
			item.Record.UpdatedAt, "", 0, nil, len(observed), now,
		)
		// При обновлении сохраняем последнюю безопасную структуру. При первом
		// чтении достаточно компактной записи конфликта без недоверенного текста.
		if exists {
			plan.Record = SourceTimeStateRecord{Snapshot: stored.Snapshot, Conflict: plan.Conflict}
		} else {
			plan.Record = SourceTimeStateRecord{Conflict: plan.Conflict}
		}
		return plan
	}

	if exists && stored.Snapshot != nil && !sameTimeStateSource(*stored.Snapshot, item) {
		plan.Conflict = newSourceTimeConflict(
			"source_scope_mismatch", "сохранённое состояние принадлежит другой записи EAVC",
			item.Record.UpdatedAt, "", 0, nil, len(observed), now,
		)
		plan.Record.Conflict = plan.Conflict
		return plan
	}

	if !exists || stored.Snapshot == nil {
		if len(observed) == 0 && len(item.DeletedBlocks) > 0 {
			plan.Conflict = newSourceTimeConflict(
				"missing_time_history",
				"нет локальной истории, поэтому физическое надгробие EAVC нельзя безопасно связать с публикацией",
				item.Record.UpdatedAt, "", 0, nil, 0, now,
			)
			plan.Record.Conflict = plan.Conflict
			return plan
		}
		entries := make([]SourceTimeStateEntry, 0, len(observed))
		for index, observation := range observed {
			occurrenceID := newTimeOccurrenceID(item, observation, index)
			entries = append(entries, activeTimeStateEntry(item, observation, occurrenceID, nil))
		}
		snapshot := sourceTimeSnapshot(item, entries)
		plan.Item = itemFromTimeState(item, entries)
		plan.Record = SourceTimeStateRecord{Snapshot: &snapshot}
		return plan
	}

	previous := *stored.Snapshot
	previousActive := activeTimeEntries(previous.Blocks)
	matches, matchedPrevious, conflict := matchTimeOccurrences(previousActive, observed, item, now)
	if conflict != nil {
		plan.Conflict = conflict
		plan.Record = SourceTimeStateRecord{Snapshot: stored.Snapshot, Conflict: conflict}
		return plan
	}

	entries := make([]SourceTimeStateEntry, 0, len(observed)+len(previous.Blocks))
	for currentIndex, observation := range observed {
		previousIndex, matched := matches[currentIndex]
		if matched {
			prior := previousActive[previousIndex]
			entries = append(entries, activeTimeStateEntry(item, observation, prior.OccurrenceID, &prior))
			continue
		}
		entries = append(entries, activeTimeStateEntry(
			item, observation, newTimeOccurrenceID(item, observation, currentIndex), nil,
		))
	}

	// Старые tombstone сохраняются. Повторное чтение одной версии снова создаёт
	// тот же deletion-event, а очередь подавляет его по версии и digest. Это
	// исключает потерю удаления, если процесс завершился между сохранением
	// состояния и постановкой события в очередь.
	for _, entry := range previous.Blocks {
		if !entry.Active {
			entries = append(entries, entry)
		}
	}
	deletedObserved, _ := observeSourceTimeBlocks(item.DeletedBlocks)
	for previousIndex, prior := range previousActive {
		if matchedPrevious[previousIndex] {
			continue
		}
		entries = append(entries, deletedTimeStateEntry(item, prior, deletedObserved))
	}
	if len(entries) > maxSourceTimeStateEntries {
		plan.Conflict = newSourceTimeConflict(
			"too_many_time_occurrences",
			fmt.Sprintf("история одной записи EAVC превысила %d временных блоков", maxSourceTimeStateEntries),
			item.Record.UpdatedAt, "", 0, nil, len(entries), now,
		)
		plan.Record = SourceTimeStateRecord{Snapshot: stored.Snapshot, Conflict: plan.Conflict}
		return plan
	}

	snapshot := sourceTimeSnapshot(item, entries)
	plan.Item = itemFromTimeState(item, entries)
	plan.Record = SourceTimeStateRecord{Snapshot: &snapshot}
	return plan
}

func sameTimeStateSource(snapshot SourceTimeSnapshot, item SourceItem) bool {
	if snapshot.SourceID != 0 && item.Record.ID != 0 && snapshot.SourceID != item.Record.ID &&
		(strings.TrimSpace(snapshot.SourceUUID) == "" || strings.TrimSpace(item.Record.UUID) == "") {
		return false
	}
	return strings.TrimSpace(snapshot.SourceUUID) == "" || strings.TrimSpace(item.Record.UUID) == "" ||
		strings.EqualFold(strings.TrimSpace(snapshot.SourceUUID), strings.TrimSpace(item.Record.UUID))
}

func sourceTimeSnapshot(item SourceItem, entries []SourceTimeStateEntry) SourceTimeSnapshot {
	return SourceTimeSnapshot{
		SourceID: item.Record.ID, SourceUUID: strings.TrimSpace(item.Record.UUID),
		SourceVersion: strings.TrimSpace(item.Record.UpdatedAt),
		Blocks:        append([]SourceTimeStateEntry(nil), entries...),
	}
}

func newSourceTimeConflict(
	kind, reason, version, canonical string, dayOffset int,
	previousIDs []string, currentCount int, now time.Time,
) *SourceTimeConflict {
	ids := append([]string(nil), previousIDs...)
	sort.Strings(ids)
	return &SourceTimeConflict{
		Kind: kind, Reason: reason, SourceVersion: strings.TrimSpace(version),
		CanonicalTime: canonical, DayOffset: dayOffset,
		PreviousOccurrenceIDs: ids, CurrentOccurrences: currentCount,
		DetectedAt: now.UTC().Format(time.RFC3339Nano),
	}
}

func observeSourceTimeBlocks(blocks []domain.ContentBlock) ([]sourceTimeObservation, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	allCallsigns := collectSourceCallsigns(blocks)
	observed := make([]sourceTimeObservation, 0, len(blocks))
	var current *timeObservationBuilder
	finalize := func() {
		if current == nil {
			return
		}
		text := strings.TrimSpace(strings.Join(current.lines, "\n"))
		digest := sha256.Sum256([]byte(text))
		block := current.physical
		block.UUID = ""
		block.Key = current.canonical
		block.Sequence = current.sequence
		block.TimeKey = current.minutes
		block.DayOffset = current.dayOffset
		block.Text = text
		block.Hash = hex.EncodeToString(digest[:])
		block.Callsigns = filterSourceCallsigns(text, allCallsigns)
		observed = append(observed, sourceTimeObservation{
			CanonicalTime: current.canonical, Minutes: current.minutes,
			DayOffset: current.dayOffset, PhysicalUUID: current.physicalUUID,
			PhysicalOrdinal: current.physicalOrdinal, Sequence: current.sequence, Block: block,
		})
		current = nil
	}

	for _, physical := range blocks {
		text := normalizeTimeStateText(physical.Text)
		if text == "" {
			continue
		}
		lines := strings.Split(text, "\n")
		localDayOffset := physical.DayOffset
		previousMinutes := -1
		physicalOrdinal := 0
		for _, rawLine := range lines {
			canonical, minutes, header := domain.CanonicalTimeHeader(rawLine)
			if header {
				finalize()
				if previousMinutes >= 0 && minutes < previousMinutes {
					localDayOffset++
				}
				previousMinutes = minutes
				current = &timeObservationBuilder{
					canonical: canonical, minutes: minutes, dayOffset: localDayOffset,
					physicalUUID:    strings.TrimSpace(physical.Identity()),
					physicalOrdinal: physicalOrdinal, sequence: len(observed), physical: physical,
					lines: []string{canonical},
				}
				physicalOrdinal++
				continue
			}
			if current == nil {
				if strings.TrimSpace(rawLine) == "" {
					continue
				}
				return nil, fmt.Errorf("текст до первого точного заголовка времени: %q", strings.TrimSpace(rawLine))
			}
			current.lines = append(current.lines, rawLine)
		}
	}
	finalize()
	if len(observed) == 0 {
		return nil, fmt.Errorf("в записи нет точного заголовка времени")
	}
	for index := range observed {
		observed[index].Sequence = index
		observed[index].Block.Sequence = index
	}
	return observed, nil
}

func collectSourceCallsigns(blocks []domain.ContentBlock) []domain.Callsign {
	seen := make(map[string]bool)
	var result []domain.Callsign
	for _, block := range blocks {
		for _, callsign := range block.Callsigns {
			key := domain.NormalizeCorrespondentCode(callsign.Code)
			if key == "" || seen[key] {
				continue
			}
			seen[key] = true
			result = append(result, callsign)
		}
	}
	return result
}

func filterSourceCallsigns(text string, callsigns []domain.Callsign) []domain.Callsign {
	wanted := make(map[string]bool)
	for _, code := range domain.ExtractCorrespondentCodes(text) {
		wanted[domain.NormalizeCorrespondentCode(code)] = true
	}
	result := make([]domain.Callsign, 0, len(wanted))
	for _, callsign := range callsigns {
		if wanted[domain.NormalizeCorrespondentCode(callsign.Code)] {
			result = append(result, callsign)
		}
	}
	return result
}

func activeTimeEntries(entries []SourceTimeStateEntry) []SourceTimeStateEntry {
	result := make([]SourceTimeStateEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Active {
			result = append(result, entry)
		}
	}
	return result
}

func matchTimeOccurrences(
	previous []SourceTimeStateEntry, current []sourceTimeObservation,
	item SourceItem, now time.Time,
) (map[int]int, map[int]bool, *SourceTimeConflict) {
	matches := make(map[int]int)
	matchedPrevious := make(map[int]bool)
	groups := make(map[string]bool)
	for _, entry := range previous {
		groups[timeGroup(entry.DayOffset, entry.CanonicalTime)] = true
	}
	for _, observation := range current {
		groups[timeGroup(observation.DayOffset, observation.CanonicalTime)] = true
	}
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		var previousIndexes, currentIndexes []int
		for index, entry := range previous {
			if timeGroup(entry.DayOffset, entry.CanonicalTime) == key {
				previousIndexes = append(previousIndexes, index)
			}
		}
		for index, observation := range current {
			if timeGroup(observation.DayOffset, observation.CanonicalTime) == key {
				currentIndexes = append(currentIndexes, index)
			}
		}
		if matchExactOrderedTimeGroup(
			previous, current, previousIndexes, currentIndexes, matches, matchedPrevious,
		) {
			continue
		}
		if matchSafePositionalTimeGroup(
			previous, current, previousIndexes, currentIndexes, matches, matchedPrevious,
		) {
			continue
		}

		matchUniqueTimeContent(previous, current, previousIndexes, currentIndexes, matches, matchedPrevious)
		matchUniquePhysicalTime(previous, current, previousIndexes, currentIndexes, matches, matchedPrevious)

		var remainingPrevious, remainingCurrent []int
		for _, index := range previousIndexes {
			if !matchedPrevious[index] {
				remainingPrevious = append(remainingPrevious, index)
			}
		}
		for _, index := range currentIndexes {
			if _, matched := matches[index]; !matched {
				remainingCurrent = append(remainingCurrent, index)
			}
		}
		if len(remainingPrevious) == 1 && len(remainingCurrent) == 1 {
			matches[remainingCurrent[0]] = remainingPrevious[0]
			matchedPrevious[remainingPrevious[0]] = true
			continue
		}
		if len(remainingPrevious) > 0 && len(remainingCurrent) > 0 {
			entry := previous[remainingPrevious[0]]
			ids := make([]string, 0, len(remainingPrevious))
			for _, index := range remainingPrevious {
				ids = append(ids, previous[index].OccurrenceID)
			}
			return nil, nil, newSourceTimeConflict(
				"ambiguous_time_occurrence",
				"несколько одинаковых времён одновременно переставлены или переписаны; безопасное сопоставление невозможно",
				item.Record.UpdatedAt, entry.CanonicalTime, entry.DayOffset,
				ids, len(remainingCurrent), now,
			)
		}
	}
	return matches, matchedPrevious, nil
}

// matchSafePositionalTimeGroup разрешает только два наблюдаемых случая:
// полностью неразличимые одинаковые вхождения (их порядок является единственной
// возможной стабильной идентичностью) и одну редакцию при неизменной физической
// позиции. Массовая одновременная перепись/перестановка по-прежнему уходит в
// конфликт, потому что угадать соответствие там нельзя.
func matchSafePositionalTimeGroup(
	previous []SourceTimeStateEntry, current []sourceTimeObservation,
	previousIndexes, currentIndexes []int, matches map[int]int, matchedPrevious map[int]bool,
) bool {
	if len(previousIndexes) == 0 || len(currentIndexes) == 0 {
		return false
	}
	allSameText := true
	baseline := normalizeTimeStateText(previous[previousIndexes[0]].Block.Text)
	for _, index := range previousIndexes {
		allSameText = allSameText && normalizeTimeStateText(previous[index].Block.Text) == baseline
	}
	for _, index := range currentIndexes {
		allSameText = allSameText && normalizeTimeStateText(current[index].Block.Text) == baseline
	}
	if allSameText {
		count := min(len(previousIndexes), len(currentIndexes))
		for offset := 0; offset < count; offset++ {
			matches[currentIndexes[offset]] = previousIndexes[offset]
			matchedPrevious[previousIndexes[offset]] = true
		}
		return true
	}
	if len(previousIndexes) != len(currentIndexes) {
		return false
	}
	changed := 0
	for offset := range previousIndexes {
		prior := previous[previousIndexes[offset]]
		observed := current[currentIndexes[offset]]
		if !strings.EqualFold(strings.TrimSpace(prior.PhysicalUUID), strings.TrimSpace(observed.PhysicalUUID)) ||
			prior.PhysicalOrdinal != observed.PhysicalOrdinal {
			return false
		}
		if normalizeTimeStateText(prior.Block.Text) != normalizeTimeStateText(observed.Block.Text) {
			changed++
		}
	}
	if changed != 1 {
		return false
	}
	for offset := range previousIndexes {
		matches[currentIndexes[offset]] = previousIndexes[offset]
		matchedPrevious[previousIndexes[offset]] = true
	}
	return true
}

func matchExactOrderedTimeGroup(
	previous []SourceTimeStateEntry, current []sourceTimeObservation,
	previousIndexes, currentIndexes []int, matches map[int]int, matchedPrevious map[int]bool,
) bool {
	if len(previousIndexes) == 0 || len(previousIndexes) != len(currentIndexes) {
		return false
	}
	for offset := range previousIndexes {
		prior := previous[previousIndexes[offset]]
		observed := current[currentIndexes[offset]]
		if normalizeTimeStateText(prior.Block.Text) != normalizeTimeStateText(observed.Block.Text) ||
			!strings.EqualFold(strings.TrimSpace(prior.PhysicalUUID), strings.TrimSpace(observed.PhysicalUUID)) ||
			prior.PhysicalOrdinal != observed.PhysicalOrdinal {
			return false
		}
	}
	for offset := range previousIndexes {
		matches[currentIndexes[offset]] = previousIndexes[offset]
		matchedPrevious[previousIndexes[offset]] = true
	}
	return true
}

func matchUniqueTimeContent(
	previous []SourceTimeStateEntry, current []sourceTimeObservation,
	previousIndexes, currentIndexes []int, matches map[int]int, matchedPrevious map[int]bool,
) {
	previousByText := make(map[string][]int)
	currentByText := make(map[string][]int)
	for _, index := range previousIndexes {
		previousByText[normalizeTimeStateText(previous[index].Block.Text)] = append(
			previousByText[normalizeTimeStateText(previous[index].Block.Text)], index,
		)
	}
	for _, index := range currentIndexes {
		currentByText[normalizeTimeStateText(current[index].Block.Text)] = append(
			currentByText[normalizeTimeStateText(current[index].Block.Text)], index,
		)
	}
	for text, previousMatches := range previousByText {
		currentMatches := currentByText[text]
		if text == "" || len(previousMatches) != 1 || len(currentMatches) != 1 {
			continue
		}
		matches[currentMatches[0]] = previousMatches[0]
		matchedPrevious[previousMatches[0]] = true
	}
}

func matchUniquePhysicalTime(
	previous []SourceTimeStateEntry, current []sourceTimeObservation,
	previousIndexes, currentIndexes []int, matches map[int]int, matchedPrevious map[int]bool,
) {
	previousByPhysical := make(map[string][]int)
	currentByPhysical := make(map[string][]int)
	for _, index := range previousIndexes {
		if matchedPrevious[index] {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(previous[index].PhysicalUUID))
		if key != "" {
			previousByPhysical[key] = append(previousByPhysical[key], index)
		}
	}
	for _, index := range currentIndexes {
		if _, matched := matches[index]; matched {
			continue
		}
		key := strings.ToLower(strings.TrimSpace(current[index].PhysicalUUID))
		if key != "" {
			currentByPhysical[key] = append(currentByPhysical[key], index)
		}
	}
	for key, previousMatches := range previousByPhysical {
		currentMatches := currentByPhysical[key]
		if len(previousMatches) != 1 || len(currentMatches) != 1 {
			continue
		}
		matches[currentMatches[0]] = previousMatches[0]
		matchedPrevious[previousMatches[0]] = true
	}
}

func activeTimeStateEntry(
	item SourceItem, observation sourceTimeObservation, occurrenceID string,
	previous *SourceTimeStateEntry,
) SourceTimeStateEntry {
	block := observation.Block
	block.UUID = logicalTimeBlockID(observation.DayOffset, observation.CanonicalTime, occurrenceID)
	block.Key = observation.CanonicalTime
	block.TimeKey = observation.Minutes
	block.DayOffset = observation.DayOffset
	if strings.TrimSpace(block.UpdatedAt) == "" {
		block.UpdatedAt = item.Record.UpdatedAt
	}
	if strings.TrimSpace(block.UpdatedBy) == "" {
		block.UpdatedBy = item.UpdatedBy
	}
	if previous != nil && sameLogicalTimeBlock(previous.Block, block) {
		block.UpdatedAt = previous.Block.UpdatedAt
		block.UpdatedBy = previous.Block.UpdatedBy
		block.Hash = previous.Block.Hash
		block.Callsigns = append([]domain.Callsign(nil), previous.Block.Callsigns...)
	}
	return SourceTimeStateEntry{
		OccurrenceID: occurrenceID, CanonicalTime: observation.CanonicalTime,
		DayOffset: observation.DayOffset, PhysicalUUID: observation.PhysicalUUID,
		PhysicalOrdinal: observation.PhysicalOrdinal, Active: true, Block: block,
	}
}

func deletedTimeStateEntry(
	item SourceItem, previous SourceTimeStateEntry, deleted []sourceTimeObservation,
) SourceTimeStateEntry {
	entry := previous
	entry.Active = false
	entry.PhysicalUUID = ""
	entry.PhysicalOrdinal = 0
	version := strings.TrimSpace(item.Record.UpdatedAt)
	updatedBy := strings.TrimSpace(item.UpdatedBy)
	if observation, ok := matchingDeletedObservation(previous, deleted); ok {
		if value := strings.TrimSpace(observation.Block.DeletedAt); value != "" {
			version = value
		} else if value := strings.TrimSpace(observation.Block.UpdatedAt); value != "" {
			version = value
		}
		if value := strings.TrimSpace(observation.Block.UpdatedBy); value != "" {
			updatedBy = value
		}
	}
	entry.Block.DeletedAt = version
	entry.Block.UpdatedAt = version
	entry.Block.UpdatedBy = updatedBy
	return entry
}

func matchingDeletedObservation(
	previous SourceTimeStateEntry, deleted []sourceTimeObservation,
) (sourceTimeObservation, bool) {
	var exact []sourceTimeObservation
	for _, observation := range deleted {
		if observation.DayOffset != previous.DayOffset || observation.CanonicalTime != previous.CanonicalTime {
			continue
		}
		if normalizeTimeStateText(observation.Block.Text) == normalizeTimeStateText(previous.Block.Text) {
			exact = append(exact, observation)
		}
	}
	if len(exact) == 1 {
		return exact[0], true
	}
	for _, observation := range deleted {
		if observation.DayOffset == previous.DayOffset && observation.CanonicalTime == previous.CanonicalTime &&
			strings.EqualFold(observation.PhysicalUUID, previous.PhysicalUUID) {
			return observation, true
		}
	}
	return sourceTimeObservation{}, false
}

func itemFromTimeState(item SourceItem, entries []SourceTimeStateEntry) SourceItem {
	result := item
	result.logicalTimeBlocks = true
	result.Blocks = make([]domain.ContentBlock, 0)
	result.DeletedBlocks = make([]domain.ContentBlock, 0)
	for _, entry := range entries {
		if entry.Active {
			block := entry.Block
			block.Sequence = len(result.Blocks)
			result.Blocks = append(result.Blocks, block)
			continue
		}
		result.DeletedBlocks = append(result.DeletedBlocks, entry.Block)
	}
	return result
}

func sameLogicalTimeBlock(left, right domain.ContentBlock) bool {
	if normalizeTimeStateText(left.Text) != normalizeTimeStateText(right.Text) ||
		len(left.Callsigns) != len(right.Callsigns) {
		return false
	}
	for index := range left.Callsigns {
		if left.Callsigns[index] != right.Callsigns[index] {
			return false
		}
	}
	return true
}

func newTimeOccurrenceID(item SourceItem, observation sourceTimeObservation, sequence int) string {
	seed := strings.Join([]string{
		sourceTimeStateKey(item), strings.TrimSpace(item.Record.UpdatedAt),
		timeGroup(observation.DayOffset, observation.CanonicalTime),
		strings.ToLower(strings.TrimSpace(observation.PhysicalUUID)),
		fmt.Sprintf("%d", observation.PhysicalOrdinal), fmt.Sprintf("%d", sequence),
		normalizeTimeStateText(observation.Block.Text),
	}, "\x00")
	digest := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(digest[:16])
}

func logicalTimeBlockID(dayOffset int, canonicalTime, occurrenceID string) string {
	compactTime := strings.ReplaceAll(canonicalTime, ":", "")
	return fmt.Sprintf("time:d%d:%s:%s", dayOffset, compactTime, occurrenceID)
}

func timeGroup(dayOffset int, canonicalTime string) string {
	return fmt.Sprintf("%08d|%s", dayOffset, canonicalTime)
}

func normalizeTimeStateText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = strings.ReplaceAll(value, "\r", "\n")
	lines := strings.Split(value, "\n")
	for index := range lines {
		lines[index] = strings.TrimRight(lines[index], " \t")
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}
