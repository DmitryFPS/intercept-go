package application

import (
	"context"
	"errors"
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

func TestTimeStateArchiveReplayKeepsIdentityAcrossPhysicalSourceIDs(t *testing.T) {
	t.Parallel()
	version := "2026-11-16 23:53:21"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-active-a", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: version,
	}})
	active.Record.ID = 101
	active.Record.SessionID = 11
	active.Record.SessionUUID = "session-original"
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	if initial.Conflict != nil || len(initial.Item.Blocks) != 1 {
		t.Fatalf("исходный активный блок не подготовлен: %#v", initial)
	}
	wantBlockUUID := initial.Item.Blocks[0].UUID
	state := initial.Record

	archive := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "legacy-archive-a", DayOffset: 0,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: version,
	}})
	archive.Record.ID = 202
	archive.Record.SessionID = 22
	archive.Record.SessionUUID = "session-archive-copy"
	archive.Archived = true

	recreated := active
	recreated.Record.ID = 303
	recreated.Record.SessionID = 33
	recreated.Record.SessionUUID = "session-recreated-copy"
	recreated.Blocks = append([]domain.ContentBlock(nil), active.Blocks...)
	recreated.Blocks[0].UUID = "physical-active-b"

	for cycle := 0; cycle < 3; cycle++ {
		archived := planSourceTimeBlocks(archive, state, true, time.Now())
		if archived.Conflict != nil || len(archived.Item.Blocks) != 1 ||
			len(archived.Item.DeletedBlocks) != 0 || archived.Item.Blocks[0].UUID != wantBlockUUID ||
			archived.Item.Record.ID != active.Record.ID || archived.Item.Record.UUID != active.Record.UUID ||
			archived.Item.Record.SessionID != active.Record.SessionID ||
			archived.Item.Record.SessionUUID != active.Record.SessionUUID ||
			!sourceTimeStateRecordsEqual(state, archived.Record) {
			t.Fatalf("цикл %d: архив с другим физическим ID изменил логическое состояние: %#v", cycle, archived)
		}
		state = archived.Record

		activeAgain := planSourceTimeBlocks(recreated, state, true, time.Now())
		if activeAgain.Conflict != nil || len(activeAgain.Item.Blocks) != 1 ||
			len(activeAgain.Item.DeletedBlocks) != 0 || activeAgain.Item.Blocks[0].UUID != wantBlockUUID {
			t.Fatalf("цикл %d: возврат активной строки изменил логический блок: %#v", cycle, activeAgain)
		}
		state = activeAgain.Record
	}
}

func TestTimeStateArchiveReplayRequiresSameCallsignMetadata(t *testing.T) {
	t.Parallel()
	const version = "2026-11-16 23:53:21"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи (477914)", UpdatedAt: version,
		Callsigns: []domain.Callsign{{Code: "477914", Label: "Эней"}},
	}})
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	archive := active
	archive.Record.ID = 202
	archive.Archived = true
	archive.Blocks = append([]domain.ContentBlock(nil), active.Blocks...)
	archive.Blocks[0].UUID = "legacy-archive"
	archive.Blocks[0].DayOffset = 0
	archive.Blocks[0].Callsigns = []domain.Callsign{{Code: "477914", Label: "Другой позывной"}}
	observed, err := observeSourceTimeBlocks(archive.Blocks)
	if err != nil {
		t.Fatal(err)
	}
	if sameArchivedTimeObservations(archive, observed, *initial.Record.Snapshot) {
		t.Fatal("изменённые данные позывного ошибочно признаны точным архивным повтором")
	}
}

func TestTimeStateReactivatesDeletedOccurrenceOnNewerVersion(t *testing.T) {
	t.Parallel()
	active := timeStateTestItem("2026-11-17 08:00:00", []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-17 08:00:00",
	}})
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	if initial.Conflict != nil || len(initial.Item.Blocks) != 1 {
		t.Fatalf("исходный блок не создан: %#v", initial)
	}
	wantUUID := initial.Item.Blocks[0].UUID

	deleted := active
	deleted.Record.UpdatedAt = "2026-11-17 08:01:00"
	deleted.Blocks = nil
	deleted.DeletedBlocks = []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: active.Content, UpdatedAt: deleted.Record.UpdatedAt, DeletedAt: deleted.Record.UpdatedAt,
	}}
	afterDelete := planSourceTimeBlocks(deleted, initial.Record, true, time.Now())
	if afterDelete.Conflict != nil || len(afterDelete.Item.Blocks) != 0 ||
		len(afterDelete.Item.DeletedBlocks) != 1 || afterDelete.Item.DeletedBlocks[0].UUID != wantUUID {
		t.Fatalf("удаление потеряло логическую идентичность: %#v", afterDelete)
	}

	reappeared := active
	reappeared.Record.ID = 303
	reappeared.Record.UpdatedAt = "2026-11-17 08:02:00"
	reappeared.Content = "00.17\n-Эней, слышу уверенно, прием"
	reappeared.Blocks = []domain.ContentBlock{{
		UUID: "physical-recreated", DayOffset: 1,
		Text: reappeared.Content, UpdatedAt: reappeared.Record.UpdatedAt,
	}}
	afterReappear := planSourceTimeBlocks(reappeared, afterDelete.Record, true, time.Now())
	if afterReappear.Conflict != nil || len(afterReappear.Item.Blocks) != 1 ||
		len(afterReappear.Item.DeletedBlocks) != 0 || afterReappear.Item.Blocks[0].UUID != wantUUID ||
		!strings.Contains(afterReappear.Item.Blocks[0].Text, "слышу уверенно") ||
		afterReappear.Item.Blocks[0].UpdatedAt != reappeared.Record.UpdatedAt {
		t.Fatalf("повторное появление не восстановило прежний логический блок: %#v", afterReappear)
	}
}

func TestClientKeepsBlockLineageAcrossDeleteAndEditedRestore(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 8, 5, 0, 0, time.UTC)
	active := timeStateTestItem("2026-11-17 08:00:00", []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-17 08:00:00",
	}})
	active.ObservedAt = now
	source := &replaySource{item: active}
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "veseloe", Source: source, Queue: queue, State: queue,
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("исходный блок не поставлен в очередь: queued=%d err=%v", queued, err)
	}

	deleted := active
	deleted.Record.UpdatedAt = "2026-11-17 08:01:00"
	deleted.Blocks = nil
	deleted.DeletedBlocks = []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1, Text: active.Content,
		UpdatedAt: deleted.Record.UpdatedAt, DeletedAt: deleted.Record.UpdatedAt,
	}}
	source.item = deleted
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("удаление блока не поставлено в очередь: queued=%d err=%v", queued, err)
	}

	restored := active
	restored.Record.ID = 303
	restored.Record.UpdatedAt = "2026-11-17 08:02:00"
	restored.Content = "00.17\n-Эней, слышу уверенно, прием"
	restored.Blocks = []domain.ContentBlock{{
		UUID: "physical-restored", DayOffset: 1,
		Text: restored.Content, UpdatedAt: restored.Record.UpdatedAt,
	}}
	source.item = restored
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("редакция после удаления не поставлена в очередь: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 3 {
		t.Fatalf("ожидались create/delete/restore, получено: %#v", queue.events)
	}
	lineage := queue.events[0].SourceBlockUUID
	if lineage == "" || queue.events[0].SourceBlockDeleted || !queue.events[1].SourceBlockDeleted ||
		queue.events[2].SourceBlockDeleted || queue.events[1].SourceBlockUUID != lineage ||
		queue.events[2].SourceBlockUUID != lineage ||
		!strings.Contains(queue.events[2].Content, "слышу уверенно") {
		t.Fatalf("create/delete/restore потеряли общую идентичность: %#v", queue.events)
	}
}

func TestTimeStateRepairsLegacyArchiveActiveAndOriginalTombstone(t *testing.T) {
	t.Parallel()
	active := timeStateTestItem("2026-11-17 08:00:00", []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-17 08:00:00",
	}})
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	wantUUID := initial.Item.Blocks[0].UUID
	originalTombstone := initial.Record.Snapshot.Blocks[0]
	originalTombstone.Active = false
	originalTombstone.PhysicalUUID = ""
	originalTombstone.PhysicalOrdinal = 0
	originalTombstone.Block.UpdatedAt = "2026-11-17 08:01:00"
	originalTombstone.Block.DeletedAt = "2026-11-17 08:01:00"

	spuriousArchive := originalTombstone
	spuriousArchive.OccurrenceID = "legacy-archive-occurrence"
	spuriousArchive.CanonicalTime = "00:17"
	spuriousArchive.DayOffset = 0
	spuriousArchive.Active = true
	spuriousArchive.PhysicalUUID = "legacy-archive"
	spuriousArchive.Block.UUID = logicalTimeBlockID(0, "00:17", spuriousArchive.OccurrenceID)
	spuriousArchive.Block.DayOffset = 0
	spuriousArchive.Block.UpdatedAt = "2026-11-17 08:01:00"
	spuriousArchive.Block.DeletedAt = ""
	legacySnapshot := *initial.Record.Snapshot
	legacySnapshot.SourceID = 202
	legacySnapshot.SourceVersion = "2026-11-17 08:01:00"
	legacySnapshot.Blocks = []SourceTimeStateEntry{spuriousArchive, originalTombstone}
	legacy := SourceTimeStateRecord{Snapshot: &legacySnapshot}

	corrected := active
	corrected.Record.ID = 303
	corrected.Record.UpdatedAt = "2026-11-17 08:02:00"
	corrected.Content = "00.17\n-Эней, слышу уверенно, прием"
	corrected.Blocks = []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: corrected.Content, UpdatedAt: corrected.Record.UpdatedAt,
	}}
	repaired := planSourceTimeBlocks(corrected, legacy, true, time.Now())
	if repaired.Conflict != nil || len(repaired.Item.Blocks) != 1 ||
		repaired.Item.Blocks[0].UUID != wantUUID || len(repaired.Item.DeletedBlocks) != 1 ||
		repaired.Item.DeletedBlocks[0].UUID != spuriousArchive.Block.UUID {
		t.Fatalf("состояние старого клиента не восстановлено редакцией: %#v", repaired)
	}
	if collision := logicalTimeIdentityCollision(corrected, repaired.Record.Snapshot.Blocks, time.Now()); collision != nil {
		t.Fatalf("после восстановления осталась коллизия: %#v", collision)
	}
}

func TestTimeStateRepairsLegacyArchiveCycleAtSameVersion(t *testing.T) {
	t.Parallel()
	const version = "2026-11-17 08:01:00"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: version,
	}})
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	wantUUID := initial.Item.Blocks[0].UUID
	originalTombstone := initial.Record.Snapshot.Blocks[0]
	originalTombstone.Active = false
	originalTombstone.PhysicalUUID = ""
	originalTombstone.PhysicalOrdinal = 0
	originalTombstone.Block.UpdatedAt = version
	originalTombstone.Block.DeletedAt = version

	spuriousArchive := originalTombstone
	spuriousArchive.OccurrenceID = "legacy-archive-occurrence"
	spuriousArchive.CanonicalTime = "00:17"
	spuriousArchive.DayOffset = 0
	spuriousArchive.Active = true
	spuriousArchive.PhysicalUUID = "legacy-archive"
	spuriousArchive.Block.UUID = logicalTimeBlockID(0, "00:17", spuriousArchive.OccurrenceID)
	spuriousArchive.Block.DayOffset = 0
	spuriousArchive.Block.DeletedAt = ""
	legacySnapshot := *initial.Record.Snapshot
	legacySnapshot.SourceID = 202
	legacySnapshot.SourceSessionID = 0
	legacySnapshot.SourceSessionUUID = ""
	legacySnapshot.SourceIdentityKnown = false
	legacySnapshot.SourceVersion = version
	legacySnapshot.Blocks = []SourceTimeStateEntry{spuriousArchive, originalTombstone}

	replayed := active
	replayed.Record.ID = 303
	replayed.Blocks = []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: active.Content, UpdatedAt: version,
	}}
	repaired := planSourceTimeBlocks(
		replayed, SourceTimeStateRecord{Snapshot: &legacySnapshot}, true, time.Now(),
	)
	if repaired.Conflict != nil || len(repaired.Item.Blocks) != 1 ||
		repaired.Item.Blocks[0].UUID != wantUUID || len(repaired.Item.DeletedBlocks) != 1 ||
		repaired.Item.DeletedBlocks[0].UUID != spuriousArchive.Block.UUID {
		t.Fatalf("цикл старого клиента с той же версией не восстановлен: %#v", repaired)
	}
	if collision := logicalTimeIdentityCollision(replayed, repaired.Record.Snapshot.Blocks, time.Now()); collision != nil {
		t.Fatalf("после восстановления той же версии осталась коллизия: %#v", collision)
	}
}

func TestTimeStateDoesNotReactivateNewSnapshotAtSameVersion(t *testing.T) {
	t.Parallel()
	const version = "2026-11-17 08:01:00"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: version,
	}})
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, time.Now())
	tombstone := initial.Record.Snapshot.Blocks[0]
	tombstone.Active = false
	tombstone.PhysicalUUID = ""
	tombstone.PhysicalOrdinal = 0
	tombstone.Block.DeletedAt = version

	counterpart := tombstone
	counterpart.OccurrenceID = "another-day-occurrence"
	counterpart.DayOffset = 0
	counterpart.Active = true
	counterpart.Block.UUID = logicalTimeBlockID(0, "00:17", counterpart.OccurrenceID)
	counterpart.Block.DayOffset = 0
	counterpart.Block.DeletedAt = ""
	snapshot := *initial.Record.Snapshot
	snapshot.SourceID = 202
	snapshot.SourceIdentityKnown = true
	snapshot.SourceVersion = version
	snapshot.Blocks = []SourceTimeStateEntry{counterpart, tombstone}

	replayed := active
	replayed.Record.ID = 303
	result := planSourceTimeBlocks(
		replayed, SourceTimeStateRecord{Snapshot: &snapshot}, true, time.Now(),
	)
	if result.Conflict == nil || result.Conflict.Kind != "logical_identity_collision" {
		t.Fatalf("новый снимок ошибочно оживил tombstone той же версии: %#v", result)
	}
}

func TestTimeStateMatchesMaximumLegacyTombstonesWithoutQuadraticScan(t *testing.T) {
	const (
		version        = "2026-11-17 08:01:00"
		tombstoneCount = maxSourceTimeStateEntries / 2
	)
	item := timeStateTestItem(version, nil)
	item.Record.ID = 303
	item.Record.UUID = "source-veseloe"
	current := make([]sourceTimeObservation, 0, tombstoneCount)
	entries := make([]SourceTimeStateEntry, 0, maxSourceTimeStateEntries)
	for index := 0; index < tombstoneCount; index++ {
		canonical := fmt.Sprintf("%02d:%02d", (index/60)%24, index%60)
		text := fmt.Sprintf("%s\n-Осмысленный контрольный перехват %04d", canonical, index)
		callsigns := []domain.Callsign{{
			Number: int64(index + 1), Code: fmt.Sprintf("%06d", index+1),
			Label: "Контроль", Tag: "оператор", Description: "нагрузочный сценарий",
		}}
		observation := sourceTimeObservation{
			CanonicalTime: canonical,
			Minutes:       (index / 60 % 24 * 60) + index%60,
			DayOffset:     index + 1,
			PhysicalUUID:  fmt.Sprintf("physical-%04d", index),
			Sequence:      index,
			Block: domain.ContentBlock{
				Text: text, UpdatedAt: version, Callsigns: callsigns,
			},
		}
		occurrenceID := newTimeOccurrenceID(item, observation, index)
		tombstone := SourceTimeStateEntry{
			OccurrenceID: occurrenceID, CanonicalTime: canonical,
			DayOffset: observation.DayOffset, Active: false,
			Block: domain.ContentBlock{
				UUID: logicalTimeBlockID(observation.DayOffset, canonical, occurrenceID),
				Text: text, UpdatedAt: version, DeletedAt: version, Callsigns: callsigns,
			},
		}
		counterpart := tombstone
		counterpart.OccurrenceID = fmt.Sprintf("legacy-archive-%04d", index)
		counterpart.DayOffset = 0
		counterpart.Active = true
		counterpart.Block.UUID = logicalTimeBlockID(0, canonical, counterpart.OccurrenceID)
		counterpart.Block.DayOffset = 0
		counterpart.Block.DeletedAt = ""
		entries = append(entries, counterpart, tombstone)
		current = append(current, observation)
	}
	previous := SourceTimeSnapshot{
		SourceID: 202, SourceUUID: item.Record.UUID, SourceVersion: version,
		SourceIdentityKnown: false, Blocks: entries,
	}

	matches, matchedPrevious := matchReactivatedTimeTombstones(
		previous, current, map[int]int{}, item,
	)
	if len(matches) != tombstoneCount || len(matchedPrevious) != tombstoneCount {
		t.Fatalf(
			"не все legacy tombstone сопоставлены: matches=%d previous=%d want=%d",
			len(matches), len(matchedPrevious), tombstoneCount,
		)
	}
}

func TestClientUpgradeMigratesLegacyIdentityWhenArchiveArrivesAfterCrash(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 17, 8, 5, 0, 0, time.UTC)
	const version = "2026-11-17 08:01:00"
	active := timeStateTestItem(version, []domain.ContentBlock{{
		UUID: "physical-active", DayOffset: 1,
		Text: "00.17\n-Эней, Астра на связи", UpdatedAt: version,
	}})
	active.Record.ID = 101
	active.Record.UUID = "source-veseloe"
	active.Record.SessionID = 11
	active.Record.SessionUUID = "session-active"
	active.ObservedAt = now
	initial := planSourceTimeBlocks(active, SourceTimeStateRecord{}, false, now)
	legacySnapshot := *initial.Record.Snapshot
	legacySnapshot.SourceSessionID = 0
	legacySnapshot.SourceSessionUUID = ""
	legacySnapshot.SourceIdentityKnown = false

	archive := active
	archive.Record.ID = 202
	archive.Record.SessionID = 22
	archive.Record.SessionUUID = "session-archive"
	archive.Archived = true
	archive.Blocks = []domain.ContentBlock{{
		UUID: "legacy-archive", DayOffset: 0,
		Text: active.Content, UpdatedAt: version,
	}}

	queue := newMemoryClientState()
	queue.timeStates[sourceTimeStateKey(active)] = SourceTimeStateRecord{Snapshot: &legacySnapshot}
	service := ClientService{
		ClientID: "veseloe", Source: &memorySource{items: []SourceItem{archive}},
		Queue: queue, State: queue, BatchSize: 10, CatchUpExisting: true,
		Now: func() time.Time { return now },
	}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("архив после аварии не восстановил очередь: queued=%d err=%v", queued, err)
	}
	state := queue.timeStates[sourceTimeStateKey(active)]
	if state.Snapshot == nil || !state.Snapshot.SourceIdentityKnown ||
		state.Snapshot.SourceSessionID != archive.Record.SessionID ||
		state.Snapshot.SourceSessionUUID != archive.Record.SessionUUID {
		t.Fatalf("идентичность rc125 не мигрирована до очереди: %#v", state)
	}

	activeAgain := active
	activeAgain.Record.ID = 303
	service.Source = &memorySource{items: []SourceItem{activeAgain}}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 0 {
		t.Fatalf("активная копия после архива продублирована: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.SessionUUID != archive.Record.SessionUUID {
		t.Fatalf("после миграции осталось не одно каноническое событие: %#v", queue.events)
	}
}

func TestTimeStateQuarantinesLogicalIdentityCollision(t *testing.T) {
	t.Parallel()
	item := timeStateTestItem("2026-11-16 23:53:21", []domain.ContentBlock{{
		UUID: "physical-a", Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-16 23:53:21",
	}})
	initial := planSourceTimeBlocks(item, SourceTimeStateRecord{}, false, time.Now())
	poisoned := *initial.Record.Snapshot
	poisoned.Blocks = append([]SourceTimeStateEntry(nil), poisoned.Blocks...)
	poisoned.Blocks[0].Active = false
	poisoned.Blocks[0].PhysicalUUID = ""
	poisoned.Blocks[0].PhysicalOrdinal = 0
	stored := SourceTimeStateRecord{Snapshot: &poisoned}

	after := planSourceTimeBlocks(item, stored, true, time.Now())
	if after.Conflict == nil || after.Conflict.Kind != "logical_identity_collision" {
		t.Fatalf("повтор logical UUID не изолирован: %#v", after)
	}
	if after.Record.Snapshot != stored.Snapshot || after.Record.Conflict == nil {
		t.Fatalf("коллизия перезаписала последнее безопасное состояние: %#v", after.Record)
	}
}

func TestClientTimeStateCollisionDoesNotBlockLaterRecords(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	conflicting := timeStateTestItem("2026-11-16 23:53:21", []domain.ContentBlock{{
		UUID: "physical-a", Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-16 23:53:21",
	}})
	initial := planSourceTimeBlocks(conflicting, SourceTimeStateRecord{}, false, now)
	poisoned := *initial.Record.Snapshot
	poisoned.Blocks = append([]SourceTimeStateEntry(nil), poisoned.Blocks...)
	poisoned.Blocks[0].Active = false
	poisoned.Blocks[0].PhysicalUUID = ""
	poisoned.Blocks[0].PhysicalOrdinal = 0

	later := timeStateTestItem("2026-11-16 23:54:00", []domain.ContentBlock{{
		UUID: "physical-later", Text: "00.18\n-Астра, продолжаем движение", UpdatedAt: "2026-11-16 23:54:00",
	}})
	later.Record.ID = 88
	later.Record.UUID = "source-88"
	later.ObservedAt = now
	conflicting.ObservedAt = now

	queue := newMemoryClientState()
	queue.timeStates[sourceTimeStateKey(conflicting)] = SourceTimeStateRecord{Snapshot: &poisoned}
	service := ClientService{
		ClientID: "veseloe", Source: &memorySource{items: []SourceItem{conflicting, later}},
		Queue: queue, State: queue, BatchSize: 10, CatchUpExisting: true,
		Now: func() time.Time { return now },
	}
	queued, err := service.CollectOnce(context.Background())
	if err != nil || queued != 1 {
		t.Fatalf("коллизия одной записи остановила последующую: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.UUID != later.Record.UUID {
		t.Fatalf("последующая запись не дошла до очереди: %#v", queue.events)
	}
	if queue.cursors[eavcCursorName].ID != later.Record.ID {
		t.Fatalf("курсор не продвинут после изолированной записи: %#v", queue.cursors[eavcCursorName])
	}
}

func TestClientInvalidStoredTimeStateDoesNotBlockLaterRecords(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	invalid := timeStateTestItem("2026-11-16 23:53:21", []domain.ContentBlock{{
		UUID: "physical-invalid", Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-16 23:53:21",
	}})
	later := timeStateTestItem("2026-11-16 23:54:00", []domain.ContentBlock{{
		UUID: "physical-later", Text: "00.18\n-Астра, продолжаем движение", UpdatedAt: "2026-11-16 23:54:00",
	}})
	later.Record.ID = 88
	later.Record.UUID = "source-88"
	invalid.ObservedAt, later.ObservedAt = now, now

	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "veseloe", Source: &memorySource{items: []SourceItem{invalid}},
		Queue: queue, State: queue,
		TimeState: &invalidTimeStateStore{
			delegate: queue, invalidKey: sourceTimeStateKey(invalid), invalid: true,
		},
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	queued, err := service.CollectOnce(context.Background())
	if err != nil || queued != 0 {
		t.Fatalf("некорректная локальная история не изолирована: queued=%d err=%v", queued, err)
	}
	quarantined := queue.timeStates[sourceTimeStateKey(invalid)]
	if quarantined.Snapshot != nil || quarantined.Conflict == nil ||
		quarantined.Conflict.Kind != "invalid_local_time_state" {
		t.Fatalf("повреждённая история не заменена безопасным карантином: %#v", quarantined)
	}

	archive := invalid
	archive.Record.ID = 78
	archive.Archived = true
	archive.ArchiveMetadataAmbiguous = true
	archive.Mode = domain.SourceModeUnknown
	archive.Position.CatalogUUID = ""
	archive.Blocks = []domain.ContentBlock{{
		UUID: "legacy-archive", Text: invalid.Content, UpdatedAt: invalid.Record.UpdatedAt,
	}}
	archive.ObservedAt = now
	service.Source = &memorySource{items: []SourceItem{archive, later}}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("карантин остановил независимую запись: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 1 || queue.events[0].Source.UUID != later.Record.UUID {
		t.Fatalf("archive/active replay вышел из карантина или независимая запись потеряна: %#v", queue.events)
	}
	quarantined = queue.timeStates[sourceTimeStateKey(invalid)]
	if quarantined.Snapshot != nil || quarantined.Conflict == nil ||
		quarantined.Conflict.Kind != "invalid_local_time_state" {
		t.Fatalf("archive перезаписал постоянный карантин: %#v", quarantined)
	}
	if queue.cursors[eavcCursorName].ID != later.Record.ID {
		t.Fatalf("курсор не продвинут после изолированных replay: %#v", queue.cursors[eavcCursorName])
	}

	activeReplay := invalid
	activeReplay.Record.ID = 79
	activeReplay.Record.UpdatedAt = "2026-11-16 23:55:00"
	activeReplay.Blocks[0].UpdatedAt = activeReplay.Record.UpdatedAt
	activeReplay.Content = "00.17\n-Эней, исправленная активная версия"
	activeReplay.Blocks[0].Text = activeReplay.Content
	activeReplay.ObservedAt = now
	service.Source = &memorySource{items: []SourceItem{activeReplay}}
	if queued, err := service.CollectOnce(context.Background()); err != nil || queued != 1 {
		t.Fatalf("новая активная версия не сняла карантин: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 2 || queue.events[1].Source.UUID != invalid.Record.UUID {
		t.Fatalf("исправленная версия не дошла до очереди: %#v", queue.events)
	}
	if recovered := queue.timeStates[sourceTimeStateKey(invalid)]; recovered.Snapshot == nil || recovered.Conflict != nil {
		t.Fatalf("новая активная версия не заменила карантин: %#v", recovered)
	}
}

func TestClientGeneratedInvalidTimeStateStopsBeforeCursorAdvance(t *testing.T) {
	now := time.Date(2026, 11, 17, 0, 0, 0, 0, time.UTC)
	first := timeStateTestItem("2026-11-16 23:53:21", []domain.ContentBlock{{
		UUID: "physical-first", Text: "00.17\n-Эней, Астра на связи", UpdatedAt: "2026-11-16 23:53:21",
	}})
	later := timeStateTestItem("2026-11-16 23:54:00", []domain.ContentBlock{{
		UUID: "physical-later", Text: "00.18\n-Астра, продолжаем движение", UpdatedAt: "2026-11-16 23:54:00",
	}})
	later.Record.ID, later.Record.UUID = 88, "source-88"
	first.ObservedAt, later.ObservedAt = now, now
	queue := newMemoryClientState()
	service := ClientService{
		ClientID: "veseloe", Source: &memorySource{items: []SourceItem{first, later}},
		Queue: queue, State: queue, TimeState: invalidSaveTimeStateStore{delegate: queue},
		BatchSize: 10, CatchUpExisting: true, Now: func() time.Time { return now },
	}
	queued, err := service.CollectOnce(context.Background())
	if queued != 0 || !errors.Is(err, ErrSourceTimeStateInvalid) {
		t.Fatalf("ошибка сгенерированного состояния скрыта: queued=%d err=%v", queued, err)
	}
	if len(queue.events) != 0 || queue.cursors[eavcCursorName] != (Cursor{}) {
		t.Fatalf("после ошибки сдвинулись очередь или курсор: events=%#v cursor=%#v", queue.events, queue.cursors[eavcCursorName])
	}
}

type invalidTimeStateStore struct {
	delegate   *memoryClientState
	invalidKey string
	invalid    bool
}

type invalidSaveTimeStateStore struct {
	delegate *memoryClientState
}

func (store invalidSaveTimeStateStore) LoadSourceTimeState(
	ctx context.Context, sourceKey string,
) (SourceTimeStateRecord, bool, error) {
	return store.delegate.LoadSourceTimeState(ctx, sourceKey)
}

func (invalidSaveTimeStateStore) SaveSourceTimeState(
	context.Context, string, SourceTimeStateRecord,
) error {
	return fmt.Errorf("%w: тестовое сгенерированное состояние", ErrSourceTimeStateInvalid)
}

func (store *invalidTimeStateStore) LoadSourceTimeState(
	ctx context.Context, sourceKey string,
) (SourceTimeStateRecord, bool, error) {
	if sourceKey == store.invalidKey && store.invalid {
		return SourceTimeStateRecord{}, false, fmt.Errorf("%w: тестовая повреждённая история", ErrSourceTimeStateInvalid)
	}
	return store.delegate.LoadSourceTimeState(ctx, sourceKey)
}

func (store *invalidTimeStateStore) SaveSourceTimeState(
	ctx context.Context, sourceKey string, record SourceTimeStateRecord,
) error {
	if sourceKey == store.invalidKey {
		store.invalid = false
	}
	return store.delegate.SaveSourceTimeState(ctx, sourceKey, record)
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
