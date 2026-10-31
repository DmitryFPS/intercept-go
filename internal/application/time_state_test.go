package application

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"intercept/internal/domain"
)

func TestTimeStateKeepsDuplicateTimesAsIndependentOccurrences(t *testing.T) {
	t.Parallel()
	item := timeStateTestItem("2026-10-28 10:20:00", []domain.ContentBlock{{
		UUID: "physical-a", Text: "10.10\n-Первый # в реплике\n10.10#1\n-Второй",
		UpdatedAt: "2026-10-28 10:20:00",
	}})
	plan := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, time.Now())
	if plan.Conflict != nil || len(plan.Item.Blocks) != 2 {
		t.Fatalf("одинаковое время ошибочно склеено или отклонено: %#v", plan)
	}
	first, second := plan.Item.Blocks[0], plan.Item.Blocks[1]
	if first.UUID == second.UUID || first.Key != "10:10" || second.Key != "10:10" {
		t.Fatalf("вхождения не получили отдельные стабильные ID: %#v", plan.Item.Blocks)
	}
	if first.Text != "10:10\n-Первый # в реплике" || second.Text != "10:10\n-Второй" {
		t.Fatalf("решётка заголовка или реплики обработана неверно: %#v", plan.Item.Blocks)
	}
	if !strings.Contains(first.UUID, "time:d0:1010:") || !strings.Contains(second.UUID, "time:d0:1010:") {
		t.Fatalf("логическая идентичность не содержит день и время: %#v", plan.Item.Blocks)
	}
}

func TestTimeStateSurvivesMarkerTogglePhysicalUUIDChangeAndRestart(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 28, 10, 30, 0, 0, time.UTC)
	queue := newMemoryClientState()
	source := &replaySource{item: timeStateTestItem("2026-10-28 10:20:00", []domain.ContentBlock{{
		UUID: "physical-parent", Text: "10.10\n-Первый\n10.10#1\n-Второй",
		UpdatedAt: "2026-10-28 10:20:00",
	}})}
	source.item.ObservedAt = now
	service := ClientService{
		ClientID: "textile", Source: source, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 2 {
		t.Fatalf("первичный снимок: queued=%d err=%v", queued, err)
	}
	firstIDs := []string{queue.events[0].SourceBlockUUID, queue.events[1].SourceBlockUUID}
	firstDigests := []string{queue.events[0].ContentDigest, queue.events[1].ContentDigest}

	// После снятия # EAVC пересоздал физические строки. Новый экземпляр сервиса
	// имитирует перезапуск Клиента, локальная SQLite-модель представлена queue.
	now = now.Add(time.Minute)
	source.item = timeStateTestItem("2026-10-28 10:21:00", []domain.ContentBlock{
		{UUID: "physical-new-a", Text: "10.10 #\n-Первый", UpdatedAt: "2026-10-28 10:21:00"},
		{UUID: "physical-new-b", Text: "10.10\n-Второй", UpdatedAt: "2026-10-28 10:21:00"},
	})
	source.item.ObservedAt = now
	restarted := service
	restarted.Source = source
	if queued, err := restarted.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("решётка/physical UUID создали ложную редакцию: queued=%d err=%v", queued, err)
	}
	state, exists := queue.timeStates[sourceTimeStateKey(source.item)]
	if !exists || state.Snapshot == nil || len(activeTimeEntries(state.Snapshot.Blocks)) != 2 {
		t.Fatalf("долговечное состояние потеряно: %#v", state)
	}
	active := activeTimeEntries(state.Snapshot.Blocks)
	for index := range active {
		if active[index].Block.UUID != firstIDs[index] {
			t.Fatalf("occurrence ID изменился после перезапуска: %#v", active)
		}
		event, err := restarted.newBlockEvent(source.item, active[index].Block, false, true)
		if err != nil {
			t.Fatal(err)
		}
		if event.ContentDigest != firstDigests[index] {
			t.Fatalf("служебная решётка изменила digest: %s != %s", event.ContentDigest, firstDigests[index])
		}
	}
}

func TestTimeStateInsertionDoesNotShiftExistingDuplicateIDs(t *testing.T) {
	t.Parallel()
	initial := timeStateTestItem("2026-10-28 11:00:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-А\n10.10#\n-Б", UpdatedAt: "2026-10-28 11:00:00",
	}})
	before := planSourceTimeBlocks(initial, SourceTimeStateRecord{}, false, time.Now())
	beforeActive := activeTimeEntries(before.Record.Snapshot.Blocks)

	for _, scenario := range []struct {
		name         string
		text         string
		oldPositions []int
	}{
		{name: "выше", text: "10.10#\n-Новый\n10.10\n-А\n10.10#1\n-Б", oldPositions: []int{1, 2}},
		{name: "между", text: "10.10\n-А\n10.10 #\n-Новый\n10.10#1\n-Б", oldPositions: []int{0, 2}},
	} {
		scenario := scenario
		t.Run(scenario.name, func(t *testing.T) {
			next := timeStateTestItem("2026-10-28 11:01:00", []domain.ContentBlock{{
				UUID: "physical", Text: scenario.text, UpdatedAt: "2026-10-28 11:01:00",
			}})
			after := planSourceTimeBlocks(next, before.Record, true, time.Now())
			if after.Conflict != nil || len(after.Item.Blocks) != 3 {
				t.Fatalf("вставка стала конфликтом: %#v", after.Conflict)
			}
			if after.Item.Blocks[scenario.oldPositions[0]].UUID != beforeActive[0].Block.UUID ||
				after.Item.Blocks[scenario.oldPositions[1]].UUID != beforeActive[1].Block.UUID {
				t.Fatalf("вставка сдвинула старые ID: before=%#v after=%#v", before.Item.Blocks, after.Item.Blocks)
			}
		})
	}
}

func TestTimeStateEditsSameTimeOccurrencesSeparately(t *testing.T) {
	t.Parallel()
	initial := timeStateTestItem("2026-10-28 12:00:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-А\n10.10#\n-Б", UpdatedAt: "2026-10-28 12:00:00",
	}})
	first := planSourceTimeBlocks(initial, SourceTimeStateRecord{}, false, time.Now())
	ids := []string{first.Item.Blocks[0].UUID, first.Item.Blocks[1].UUID}

	editFirst := timeStateTestItem("2026-10-28 12:01:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10#1\n-А исправлен\n10.10\n-Б", UpdatedAt: "2026-10-28 12:01:00",
	}})
	second := planSourceTimeBlocks(editFirst, first.Record, true, time.Now())
	if second.Conflict != nil || second.Item.Blocks[0].UUID != ids[0] || second.Item.Blocks[1].UUID != ids[1] {
		t.Fatalf("редакция первого occurrence сменила ID: %#v", second)
	}

	editSecond := timeStateTestItem("2026-10-28 12:02:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-А исправлен\n10.10 #1\n-Б исправлен", UpdatedAt: "2026-10-28 12:02:00",
	}})
	third := planSourceTimeBlocks(editSecond, second.Record, true, time.Now())
	if third.Conflict != nil || third.Item.Blocks[0].UUID != ids[0] || third.Item.Blocks[1].UUID != ids[1] {
		t.Fatalf("редакция второго occurrence сменила ID: %#v", third)
	}
}

func TestTimeStateKeepsIDsForExactReplayOfIdenticalOccurrences(t *testing.T) {
	t.Parallel()
	item := timeStateTestItem("2026-10-28 12:30:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-Одинаково\n10.10#1\n-Одинаково", UpdatedAt: "2026-10-28 12:30:00",
	}})
	before := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, time.Now())
	after := planSourceTimeBlocks(item, before.Record, true, time.Now())
	if after.Conflict != nil || len(after.Item.Blocks) != 2 ||
		after.Item.Blocks[0].UUID != before.Item.Blocks[0].UUID ||
		after.Item.Blocks[1].UUID != before.Item.Blocks[1].UUID {
		t.Fatalf("точный повтор одинаковых occurrence потерял ID: before=%#v after=%#v", before, after)
	}
}

func TestClientDoesNotRewriteUnchangedBoundaryTimeState(t *testing.T) {
	now := time.Date(2026, 10, 28, 12, 35, 0, 0, time.UTC)
	item := timeStateTestItem("2026-10-28 12:34:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-Без изменений", UpdatedAt: "2026-10-28 12:34:00",
	}})
	item.ObservedAt = now
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: &replaySource{item: item}, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if _, err := service.CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CollectOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if queue.timeSaves != 1 {
		t.Fatalf("неизменённая граничная запись сохранена %d раз, ожидалось 1", queue.timeSaves)
	}
}

func TestLongUnclosedShiftOver1024BlocksRemainsProcessable(t *testing.T) {
	const count = 1025
	blocks := make([]domain.ContentBlock, count)
	for index := range blocks {
		blocks[index] = domain.ContentBlock{
			UUID:      fmt.Sprintf("physical-%d", index),
			Text:      fmt.Sprintf("10.10\n-Отдельный игровой перехват %d", index),
			UpdatedAt: "2026-10-28 12:36:00",
		}
	}
	item := timeStateTestItem("2026-10-28 12:36:00", blocks)
	plan := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, time.Now())
	if plan.Conflict != nil || len(plan.Item.Blocks) != count {
		t.Fatalf("длинная незакрытая смена заблокирована: blocks=%d conflict=%#v", len(plan.Item.Blocks), plan.Conflict)
	}
	service := ClientService{ClientID: "textile", Now: time.Now, MaxEventAge: time.Hour}
	events, err := service.newEvents(plan.Item)
	if err != nil || len(events) != count {
		t.Fatalf("длинная смена не разбита на самостоятельные события: events=%d err=%v", len(events), err)
	}
	for _, event := range events {
		if !event.HasActiveBlocks || len(event.Blocks) != 1 {
			t.Fatalf("событие длинной смены содержит неверную область: %#v", event)
		}
	}
}

func TestTimeStateAddsIndistinguishableOccurrenceWithoutLosingExistingIDs(t *testing.T) {
	t.Parallel()
	initial := timeStateTestItem("2026-10-28 12:40:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-Одинаково\n10.10#1\n-Одинаково", UpdatedAt: "2026-10-28 12:40:00",
	}})
	before := planSourceTimeBlocks(initial, SourceTimeStateRecord{}, false, time.Now())
	next := timeStateTestItem("2026-10-28 12:41:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-Одинаково\n10.10#1\n-Одинаково\n10.10 #2\n-Одинаково", UpdatedAt: "2026-10-28 12:41:00",
	}})
	after := planSourceTimeBlocks(next, before.Record, true, time.Now())
	if after.Conflict != nil || len(after.Item.Blocks) != 3 {
		t.Fatalf("допустимое одинаковое вхождение ошибочно изолировано: %#v", after)
	}
	if after.Item.Blocks[0].UUID != before.Item.Blocks[0].UUID ||
		after.Item.Blocks[1].UUID != before.Item.Blocks[1].UUID ||
		after.Item.Blocks[2].UUID == before.Item.Blocks[0].UUID ||
		after.Item.Blocks[2].UUID == before.Item.Blocks[1].UUID {
		t.Fatalf("одинаковые вхождения не получили устойчивые отдельные ID: before=%#v after=%#v", before.Item.Blocks, after.Item.Blocks)
	}
}

func TestTimeStateQuarantinesAmbiguousReorderAndRewriteOnlyForThatRecord(t *testing.T) {
	t.Parallel()
	initial := timeStateTestItem("2026-10-28 13:00:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-А\n10.10#\n-Б", UpdatedAt: "2026-10-28 13:00:00",
	}})
	before := planSourceTimeBlocks(initial, SourceTimeStateRecord{}, false, time.Now())
	rewritten := timeStateTestItem("2026-10-28 13:01:00", []domain.ContentBlock{{
		UUID: "physical", Text: "10.10\n-Б переписан\n10.10#1\n-А переписан", UpdatedAt: "2026-10-28 13:01:00",
	}})
	after := planSourceTimeBlocks(rewritten, before.Record, true, time.Now())
	if after.Conflict == nil || after.Conflict.Kind != "ambiguous_time_occurrence" {
		t.Fatalf("неоднозначная перестановка не изолирована: %#v", after)
	}
	if after.Record.Snapshot != before.Record.Snapshot {
		t.Fatal("конфликт перезаписал последнюю безопасную структуру")
	}
}

func TestTimeStateQuarantinesOversizedRecordWithoutPersistingUntrustedSnapshot(t *testing.T) {
	t.Parallel()
	blocks := make([]domain.ContentBlock, maxSourceTimeStateEntries+1)
	for index := range blocks {
		blocks[index] = domain.ContentBlock{
			UUID: fmt.Sprintf("physical-%d", index),
			Text: fmt.Sprintf("10.10\n-Строка %d", index),
		}
	}
	item := timeStateTestItem("2026-10-28 13:10:00", blocks)
	plan := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, time.Now())
	if plan.Conflict == nil || plan.Conflict.Kind != "too_many_time_occurrences" {
		t.Fatalf("чрезмерная запись EAVC не изолирована: %#v", plan)
	}
	if plan.Record.Snapshot != nil {
		t.Fatal("недоверенный чрезмерный снимок сохранён в локальную историю")
	}
}

func TestTimeStatePreservesOvernightBlocksAcrossArchive(t *testing.T) {
	t.Parallel()
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	active.Position.CatalogUUID = "catalog-77"
	before := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	if before.Conflict != nil || len(before.Item.Blocks) != 1 ||
		!strings.HasPrefix(before.Item.Blocks[0].UUID, "time:d1:0003:") {
		t.Fatalf("неверный активный блок: %#v", before)
	}
	archive := timeStateTestItem("2026-10-30T21:03:33Z", []domain.ContentBlock{{
		UUID: "legacy-synthetic", DayOffset: 0,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	archive.Archived = true
	archive.Mode = domain.SourceModeUnknown
	after := planSourceTimeBlocks(archive, before.Record, true, time.Now())
	if after.Conflict != nil || len(after.Item.Blocks) != 1 || len(after.Item.DeletedBlocks) != 0 ||
		after.Item.Blocks[0].UUID != before.Item.Blocks[0].UUID ||
		after.Item.Mode != active.Mode || after.Item.Position.CatalogUUID != active.Position.CatalogUUID ||
		!sourceTimeStateRecordsEqual(before.Record, after.Record) {
		t.Fatalf("архив породил ложную замену блока: before=%#v after=%#v", before, after)
	}
	legacySnapshot := *before.Record.Snapshot
	legacySnapshot.SourceMode = ""
	legacySnapshot.SourcePosition = nil
	legacySnapshot.SourceContentHash = ""
	legacy := planSourceTimeBlocks(archive, SourceTimeStateRecord{Snapshot: &legacySnapshot}, true, time.Now())
	if legacy.Conflict != nil || legacy.Item.Mode != domain.SourceModeUnknown ||
		legacy.Item.Position.CatalogUUID != "" {
		t.Fatalf("старый снимок без метаданных должен оставаться безопасно неоднозначным: %#v", legacy)
	}
}

func TestClientArchiveReplayDoesNotQueueOvernightReplacement(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 30, 21, 4, 0, 0, time.UTC)
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	active.Position.CatalogUUID = "catalog-77"
	active.ObservedAt = now
	source := &replaySource{item: active}
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: source, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("активный блок: queued=%d err=%v", queued, err)
	}
	first := queue.events[0]
	if !strings.HasPrefix(first.SourceBlockUUID, "time:d1:0003:") {
		t.Fatalf("неверная идентичность исходного блока: %q", first.SourceBlockUUID)
	}

	now = now.Add(time.Minute)
	archive := timeStateTestItem("2026-10-30T21:03:33Z", []domain.ContentBlock{{
		UUID: "legacy-synthetic", DayOffset: 0,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	archive.Archived = true
	archive.ArchiveMetadataAmbiguous = true
	archive.Mode = domain.SourceModeUnknown
	archive.ObservedAt = now
	source.item = archive
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("архив повторно поставил блок: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].ID != first.ID || queue.timeSaves != 1 {
		t.Fatalf("архив изменил очередь или историю: events=%#v saves=%d", queue.events, queue.timeSaves)
	}
	archive.Record.UpdatedAt = "2026-10-30T21:05:33Z"
	archive.Content = "00.03\n-Изменено"
	archive.Blocks[0].Text = archive.Content
	source.item = archive
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("изменённый архив вышел с потерянным маршрутом: queued=%d err=%v", queued, err)
	}
	if state := queue.timeStates[sourceTimeStateKey(active)]; state.Conflict == nil ||
		state.Conflict.Kind != "archive_metadata_unverifiable" {
		t.Fatalf("изменённый архив не изолирован: %#v", state.Conflict)
	}
}

func TestClientArchiveReplayRecoversAfterStateSaveBeforeEnqueue(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 30, 21, 4, 0, 0, time.UTC)
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	before := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, now)
	queue := newMemoryClientState()
	if err := queue.SaveSourceTimeState(context.Background(), sourceTimeStateKey(active), before.Record); err != nil {
		t.Fatal(err)
	}
	// A crash can leave the saved time state with no queue row or cursor.
	archive := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "legacy-synthetic", DayOffset: 0,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	archive.Archived = true
	archive.ObservedAt = now
	service := ClientService{
		ClientID: "textile", Source: &replaySource{item: archive}, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("архив не восстановил незаписанное событие: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].SourceBlockUUID != before.Item.Blocks[0].UUID {
		t.Fatalf("восстановление потеряло прежний ID блока: events=%#v", queue.events)
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("повтор восстановления создал событие: queued=%d err=%v", queued, err)
	}
}

func TestTimeStateArchiveReplayRetainsIndependentSameTimeOccurrences(t *testing.T) {
	t.Parallel()
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{
		{UUID: "physical-a", DayOffset: 1, Text: "00.03\n-Первый", UpdatedAt: version},
		{UUID: "physical-b", DayOffset: 1, Text: "00.03#1\n-Второй", UpdatedAt: version},
	})
	before := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	archive := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "legacy-synthetic", DayOffset: 0,
		Text: "00.03\n-Первый\n00.03#1\n-Второй", UpdatedAt: version,
	}})
	archive.Archived = true
	after := planSourceTimeBlocks(archive, before.Record, true, time.Now())
	if after.Conflict != nil || len(after.Item.Blocks) != 2 || len(after.Item.DeletedBlocks) != 0 ||
		after.Item.Blocks[0].UUID != before.Item.Blocks[0].UUID ||
		after.Item.Blocks[1].UUID != before.Item.Blocks[1].UUID ||
		after.Item.Blocks[0].UUID == after.Item.Blocks[1].UUID {
		t.Fatalf("архив смешал одинаковые времена: before=%#v after=%#v", before, after)
	}
}

func TestTimeStateArchiveFallbackWhenContentChangesOrSnapshotMissing(t *testing.T) {
	t.Parallel()
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	before := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	legacySnapshot := *before.Record.Snapshot
	legacySnapshot.SourceMode = ""
	legacySnapshot.SourcePosition = nil
	legacySnapshot.SourceContentHash = ""
	legacy := SourceTimeStateRecord{Snapshot: &legacySnapshot}
	archive := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "legacy-synthetic", DayOffset: 0,
		Text: "00.03\n-Изменено", UpdatedAt: version,
	}})
	archive.Archived = true
	changed := planSourceTimeBlocks(archive, legacy, true, time.Now())
	if changed.Conflict != nil || len(changed.Item.Blocks) != 1 || len(changed.Item.DeletedBlocks) != 1 ||
		changed.Item.Blocks[0].UUID == before.Item.Blocks[0].UUID {
		t.Fatalf("изменённый архив ошибочно принят за прежний блок: %#v", changed)
	}
	missing := planSourceTimeBlocks(archive, SourceTimeStateRecord{}, false, time.Now())
	if missing.Conflict != nil || len(missing.Item.Blocks) != 1 || len(missing.Item.DeletedBlocks) != 0 ||
		!strings.HasPrefix(missing.Item.Blocks[0].UUID, "time:d0:0003:") {
		t.Fatalf("архив без прежней истории не прочитан: %#v", missing)
	}
}

func TestClientLegacyArchiveMetadataQuarantineDoesNotBlockLaterActiveCorrection(t *testing.T) {
	now := time.Date(2026, 10, 30, 21, 4, 0, 0, time.UTC)
	version := "2026-10-30 21:03:33"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	active.Position.CatalogUUID = "catalog-77"
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, now)
	legacy := *initial.Record.Snapshot
	legacy.SourceMode, legacy.SourcePosition, legacy.SourceContentHash = "", nil, ""
	queue := newMemoryClientState()
	if err := queue.SaveSourceTimeState(context.Background(), sourceTimeStateKey(active), SourceTimeStateRecord{Snapshot: &legacy}); err != nil {
		t.Fatal(err)
	}
	archive := timeStateTestItem("2026-10-30T21:03:33Z", []domain.ContentBlock{{
		UUID: "legacy-synthetic", Text: "00.03\n-Проверка", UpdatedAt: version,
	}})
	archive.Archived, archive.ArchiveMetadataAmbiguous = true, true
	archive.Mode = domain.SourceModeUnknown
	source := &replaySource{item: archive}
	service := ClientService{
		ClientID: "textile", Source: source, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("неподтверждённый архив ушёл в очередь: queued=%d err=%v", queued, err)
	}
	state := queue.timeStates[sourceTimeStateKey(active)]
	if state.Conflict == nil || state.Conflict.Kind != "archive_metadata_unverifiable" || len(queue.events) != 0 {
		t.Fatalf("нет диагностического карантина: state=%#v events=%#v", state, queue.events)
	}
	archive.Record.UpdatedAt = "2026-10-30T21:04:33Z"
	archive.Content = "00.03\n-Изменено"
	archive.Blocks[0].Text = archive.Content
	source.item = archive
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("новая архивная версия вышла с неизвестным маршрутом: queued=%d err=%v", queued, err)
	}
	corrected := timeStateTestItem("2026-10-30 21:05:33", []domain.ContentBlock{{
		UUID: "physical-overnight", DayOffset: 1,
		Text: "00.03\n-Изменено", UpdatedAt: "2026-10-30 21:05:33",
	}})
	corrected.Position.CatalogUUID = "catalog-77"
	source.item = corrected
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("исправленная активная версия не принята: queued=%d err=%v", queued, err)
	}
	if state := queue.timeStates[sourceTimeStateKey(active)]; state.Conflict != nil {
		t.Fatalf("исправление не сняло карантин: %#v", state.Conflict)
	}
}

func TestClientArchiveWithoutSnapshotDoesNotGuessLostRoutingMetadata(t *testing.T) {
	now := time.Date(2026, 10, 30, 21, 4, 0, 0, time.UTC)
	archive := timeStateTestItem("2026-10-30T21:03:33Z", []domain.ContentBlock{{
		UUID: "legacy-synthetic", Text: "00.03\n-Проверка", UpdatedAt: "2026-10-30T21:03:33Z",
	}})
	archive.Archived, archive.ArchiveMetadataAmbiguous = true, true
	archive.Mode = domain.SourceModeUnknown
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "textile", Source: &replaySource{item: archive}, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("архив без истории отправлен с предположенным маршрутом: queued=%d err=%v", queued, err)
	}
	state := queue.timeStates[sourceTimeStateKey(archive)]
	if len(queue.events) != 0 || state.Conflict == nil || state.Conflict.Kind != "archive_metadata_unverifiable" {
		t.Fatalf("архив без истории не изолирован: events=%#v state=%#v", queue.events, state)
	}
}

func timeStateTestItem(version string, blocks []domain.ContentBlock) SourceItem {
	content := make([]string, 0, len(blocks))
	for _, block := range blocks {
		content = append(content, block.Text)
	}
	return SourceItem{
		Record:   domain.SourceRecord{ID: 77, UUID: "source-77", UpdatedAt: version},
		Position: domain.Position{UnitName: "Игровая команда"}, Mode: domain.SourceModeNormal,
		Content: strings.Join(content, "\n"), Blocks: blocks,
		ObservedAt: time.Date(2026, 10, 28, 13, 5, 0, 0, time.UTC),
	}
}
