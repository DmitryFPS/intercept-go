package docx

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
)

func TestWordStateRejectsOversizedFileBeforeReadingIt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "word-state.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxWordStateBytes + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := loadState(path); err == nil || !application.IsPermanentDeliveryError(err) {
		t.Fatalf("чрезмерное состояние Word не изолировано: %v", err)
	}
}

func TestWordStateLimitsActiveBlocks(t *testing.T) {
	current := state{Blocks: make(map[string]blockRecord, maxWordActiveBlocks+1)}
	for index := 0; index <= maxWordActiveBlocks; index++ {
		current.Blocks[fmt.Sprintf("block-%d", index)] = blockRecord{}
	}
	if err := enforceWordActiveBlockLimit(current, maxWordActiveBlocks); err == nil ||
		!application.IsPermanentDeliveryError(err) {
		t.Fatalf("чрезмерное число активных блоков Word не отклонено: %v", err)
	}
}

// Шаблон в поставке сохраняет оформление предоставленного пользователем файла,
// а метки заменяются фрагментами в том же порядке.
func TestBuildMatchesProvidedReferenceDocumentXML(t *testing.T) {
	t.Parallel()
	template := filepath.Join("..", "..", "..", "assets", "word_template.docx")
	if err := ValidateTemplate(template); err != nil {
		t.Fatalf("шаблон поставки не прошёл проверку: %v", err)
	}
	payload, err := Build(template, Document{
		Unit: "2 об 3 обр ОсН НГ", Frequency: "159.5750", GroupCode: "3", Network: "39", Protection: "AES-256",
		Date: time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC),
		Callsigns: []domain.Callsign{
			{Number: 1, Code: "783693", Label: "Марс, Шило"},
			{Number: 15, Code: "17384", Label: "Швидкий, Лоза"},
		},
		Content: "03.51\n-... Марсу на прием. (783693)\n03.52\n-Швидкий на приеме, что? (17384)\n-... все нормально ... чтокаешь? (783693)\n-5-ые сутки них*я не жрал, потому что воды нихр*на нет, пришлите нам 10 литров воды, пожалуйста, я прошу. (17384)\n-Ну скидывали ... что нам той воды. (17384)",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.docx")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	wantRuns := []string{
		"ДНЕПРОПЕТРОВСКОЕ НАПРАВЛЕНИЕ",
		"Радиосеть ", "2 об 3 обр ОсН НГ", " - 39",
		"159.5750 МГц (гр. ID 3 AES-256)", "21.08.2026г.",
		"1.«", "Марс, Шило", "» (id: 783693);",
		"2.«", "Швидкий, Лоза", "» (id: 17384);",
		"03:51", "/1/ ... Марсу на прием.", "03:52", "/2/ Швидкий на приеме, что?",
		"/1/ ... все нормально ... чтокаешь?",
		"/2/ 5-ые сутки них*я не жрал, потому что воды нихр*на нет, пришлите нам 10 литров воды, пожалуйста, я прошу.",
		"/2/ Ну скидывали ... что нам той воды.",
		"Дежурный оператор: ", "Позывной", "Старший смены МГ ", "1", ": ", "Позывной",
	}
	if runs := documentRuns(t, documentXML(t, path)); !reflect.DeepEqual(runs, wantRuns) {
		t.Fatalf("содержимое документа отличается от эталона:\nполучено: %#v\nожидалось: %#v", runs, wantRuns)
	}
}

func TestEmbeddedDefaultTemplateMatchesReleaseAsset(t *testing.T) {
	t.Parallel()
	asset, err := os.ReadFile(filepath.Join("..", "..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(asset, defaultTemplate) {
		t.Fatal("встроенный шаблон Word отличается от файла поставки")
	}
	restored := filepath.Join(t.TempDir(), "word_template.docx")
	if err := RestoreDefaultTemplate(restored); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTemplate(restored); err != nil {
		t.Fatalf("восстановленный шаблон не прошёл проверку: %v", err)
	}
}

func TestValidateTemplateRejectsExternalRelationship(t *testing.T) {
	path := filepath.Join(t.TempDir(), "external.docx")
	createModifiedTemplate(t, path, map[string][]byte{
		"word/_rels/document.xml.rels": []byte(`<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId9" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/attachedTemplate" Target="https://example.invalid/template.dotm" TargetMode="External"/></Relationships>`),
	})
	if err := ValidateTemplate(path); err == nil || (!strings.Contains(err.Error(), "внешнюю связь") && !strings.Contains(err.Error(), "активную связь")) {
		t.Fatalf("внешняя связь должна быть отклонена: %v", err)
	}
}

func TestValidateTemplateRejectsMacroAndEmbeddedParts(t *testing.T) {
	for name := range map[string]bool{"word/vbaProject.bin": true, "word/embeddings/object1.bin": true, "word/activeX/activeX1.bin": true} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "active.docx")
			createModifiedTemplate(t, path, map[string][]byte{name: []byte("active")})
			if err := ValidateTemplate(path); err == nil || !strings.Contains(err.Error(), "активную часть") {
				t.Fatalf("активная часть должна быть отклонена: %v", err)
			}
		})
	}
}

func TestValidateTemplateBytesUsesSameSafetyChecksAsFile(t *testing.T) {
	valid, err := os.ReadFile(filepath.Join("..", "..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateTemplateBytes(valid); err != nil {
		t.Fatalf("шаблон поставки отклонён при проверке байтов: %v", err)
	}
	for name, replacements := range map[string]map[string][]byte{
		"macro": {"word/vbaProject.bin": []byte("active")},
		"external relationship": {
			"word/_rels/document.xml.rels": []byte(`<?xml version="1.0"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId9" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/attachedTemplate" Target="https://example.invalid/template.dotm" TargetMode="External"/></Relationships>`),
		},
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "modified.docx")
			createModifiedTemplate(t, path, replacements)
			payload, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateTemplateBytes(payload); err == nil {
				t.Fatal("небезопасный шаблон принят при проверке байтов")
			}
		})
	}
	if err := ValidateTemplateBytes([]byte("not a DOCX")); err == nil {
		t.Fatal("повреждённый DOCX принят при проверке байтов")
	}
}

func TestValidateTemplateRejectsActiveWordFields(t *testing.T) {
	for name, fragment := range map[string]string{
		"simple":    `<w:p><w:fldSimple w:instr="INCLUDEPICTURE http://example.invalid/x"/></w:p>`,
		"complex":   `<w:p><w:r><w:fldChar w:fldCharType="begin"/><w:instrText>INCLUDETEXT x</w:instrText></w:r></w:p>`,
		"alternate": `<w:altChunk r:id="rId9"/>`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "field.docx")
			original := documentXML(t, filepath.Join("..", "..", "..", "assets", "word_template.docx"))
			modified := strings.Replace(original, "</w:body>", fragment+"</w:body>", 1)
			if modified == original {
				t.Fatal("не найдена граница тела шаблона")
			}
			createModifiedTemplate(t, path, map[string][]byte{"word/document.xml": []byte(modified)})
			if err := ValidateTemplate(path); err == nil || !strings.Contains(err.Error(), "активное поле") {
				t.Fatalf("активное поле должно быть отклонено: %v", err)
			}
		})
	}
}

// Описание карточки EAVC сохраняется, а её внутренний номер не должен
// попадать в итоговый документ.
func TestBuildUsesLocalCallsignNumberAndEAVCDescription(t *testing.T) {
	t.Parallel()
	template := filepath.Join("..", "..", "..", "assets", "word_template.docx")
	payload, err := Build(template, Document{
		Unit: "Гольназ", Frequency: "159.3000", GroupCode: "18902", Network: "39",
		Date: time.Date(2026, 10, 20, 17, 34, 0, 0, time.UTC),
		Callsigns: []domain.Callsign{{
			Number: 6, Code: "52325", Label: "Торба",
			Description: "Старший группы Гольназ, мужской голос, украинская речь",
		}},
		Content: "17.30\n-Монако Торбе на связь. (52325)",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "callsign.docx")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	runs := documentRuns(t, documentXML(t, path))
	for _, expected := range []string{
		"1.«", "Торба", "» (id: 52325) – Старший группы Гольназ, мужской голос, украинская речь;",
		"17:30", "/1/ Монако Торбе на связь.",
	} {
		if !slices.Contains(runs, expected) {
			t.Fatalf("в Word отсутствует %q: %#v", expected, runs)
		}
	}
}

func TestBuildShowsMarkedTimesWithoutTelegramParentheses(t *testing.T) {
	t.Parallel()
	template := filepath.Join("..", "..", "..", "assets", "word_template.docx")
	payload, err := Build(template, Document{
		Unit: "Игровая команда", Frequency: "100.100", GroupCode: "GAME-1", Network: "Тест",
		Date:      time.Date(2026, 10, 28, 10, 45, 0, 0, time.UTC),
		Callsigns: []domain.Callsign{{Code: "7", Label: "Игрок"}},
		Content:   "10.15\n-Начало раунда. (7)\n10.18#\n-Уточнение. (7)\n10.28#1\n-Финиш. (7)",
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "marked-times.docx")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	runs := documentRuns(t, documentXML(t, path))
	for _, expected := range []string{"10:15", "10:18", "10:28"} {
		if !slices.Contains(runs, expected) {
			t.Fatalf("в Word отсутствует %q: %#v", expected, runs)
		}
	}
	for _, unexpected := range []string{"(10:18)", "(10:28)", "10.18#", "10.28#1"} {
		if slices.Contains(runs, unexpected) {
			t.Fatalf("в Word осталось Telegram/служебное оформление %q: %#v", unexpected, runs)
		}
	}
}

// Каждый временной UUID-блок получает самостоятельный Word-документ, а
// повторная обработка не порождает копий.
func TestDestinationBuildsOneDocumentPerBlockWithoutDuplicates(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "313", Protection: "AES-256", Prefix: "СТНД",
	}
	event := domain.Event{
		ID: "event-1", ClientID: "textile", CreatedAt: time.Now(), ObservedAt: time.Now(),
		Source:   domain.SourceRecord{ID: 1, UUID: "source-1"},
		Position: domain.Position{UnitName: "1 мб 155 омбр", Frequency: "100", GroupCode: "A"},
		Mode:     domain.SourceModeNormal, Content: "10.00\nПервый\n10.05\nВторой", ContentDigest: "digest",
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatalf("duplicate deliver: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(files) != 2 {
		t.Fatalf("ожидалось два документа без дублей: %#v", files)
	}
	numbers, err := StateNumbers(destination.StatePath)
	if err != nil || !reflect.DeepEqual(numbers, []int{1, 2}) {
		t.Fatalf("номера: %#v %v", numbers, err)
	}
	all := documentXML(t, files[0]) + documentXML(t, files[1])
	for _, expected := range []string{"1 мб 155 омбр", "10:00", "Первый", "10:05", "Второй"} {
		if !strings.Contains(all, expected) {
			t.Fatalf("в отдельных документах нет %q", expected)
		}
	}
}

func TestDestinationDeletesOnlyScopedWordBlock(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	folder := filepath.Join(root, "out")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	for _, name := range []string{"first.docx", "second.docx"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	statePath := filepath.Join(root, "state", "word.json")
	if err := saveState(statePath, state{LastNumber: 2, Blocks: map[string]blockRecord{
		"textile|source-1|block-a": {Number: 1, Filename: "first.docx"},
		"textile|source-1|block-b": {Number: 2, Filename: "second.docx"},
	}}); err != nil {
		t.Fatal(err)
	}
	destination := Destination{TemplatePath: template, Folder: folder, StatePath: statePath}
	event := domain.Event{
		ClientID: "textile", Source: domain.SourceRecord{UUID: "source-1"},
		SourceBlockUUID: "block-a", SourceBlockDeleted: true,
		HasActiveBlocks: true,
		Blocks:          []domain.ContentBlock{{UUID: "block-a", DeletedAt: "2026-10-27 19:00:00"}},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(folder, "first.docx")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("документ удалённого блока остался: %v", err)
	}
	if _, err := os.Stat(filepath.Join(folder, "second.docx")); err != nil {
		t.Fatalf("соседний документ удалён: %v", err)
	}
	current, err := loadState(statePath)
	deleted := current.Blocks["textile|source-1|block-a"]
	if err != nil || len(current.Blocks) != 2 || !deleted.Tombstone || deleted.SourceVersion != "2026-10-27 19:00:00" ||
		current.Blocks["textile|source-1|block-b"].Filename != "second.docx" {
		t.Fatalf("состояние соседнего блока повреждено: %#v err=%v", current, err)
	}
	numbers, err := StateNumbers(statePath)
	if err != nil || !reflect.DeepEqual(numbers, []int{2}) {
		t.Fatalf("надгробие ошибочно попало в активные документы: numbers=%v err=%v", numbers, err)
	}
}

func TestDestinationWordTombstoneRejectsLateEditAndAllowsNewerRestore(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "Тест", Prefix: "GAME-",
	}
	created := time.Date(2026, 10, 28, 10, 0, 0, 1, time.UTC)
	event := domain.Event{
		ID: "v1", ClientID: "point-01", CreatedAt: created, SourceAt: created,
		Source: domain.SourceRecord{UUID: "source-1"}, Position: domain.Position{UnitName: "Игровая команда"},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{UUID: "block-1", Text: "10.00\n-Старт", UpdatedAt: "2026-10-28 10:00:00"}},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	deletion := event
	deletion.ID = "v3-delete"
	deletion.CreatedAt = created.Add(3 * time.Minute)
	deletion.SourceAt = deletion.CreatedAt
	deletion.SourceBlockDeleted = true
	deletion.HasActiveBlocks = false
	deletion.Blocks = []domain.ContentBlock{{UUID: "block-1", DeletedAt: "2026-10-28 10:03:00"}}
	if err := destination.Deliver(context.Background(), deletion, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	stale := event
	stale.ID = "v2-late"
	stale.CreatedAt = created.Add(2 * time.Minute)
	stale.SourceAt = stale.CreatedAt
	stale.Blocks[0].Text = "10.00\n-Старая запоздалая редакция"
	stale.Blocks[0].UpdatedAt = "2026-10-28 10:02:00"
	if err := destination.Deliver(context.Background(), stale, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx")); len(files) != 0 {
		t.Fatalf("устаревшая версия воскресила Word: %v", files)
	}
	restored := event
	restored.ID = "v4-restore"
	restored.CreatedAt = created.Add(4 * time.Minute)
	restored.SourceAt = restored.CreatedAt
	restored.Blocks[0].Text = "10.00\n-Новая версия после восстановления источника"
	restored.Blocks[0].UpdatedAt = "2026-10-28 10:04:00"
	if err := destination.Deliver(context.Background(), restored, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(files) != 1 || !strings.Contains(documentXML(t, files[0]), "Новая версия после восстановления источника") {
		t.Fatalf("новая версия не восстановила Word: %v", files)
	}
	if err := destination.Deliver(context.Background(), deletion, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx")); len(files) != 1 {
		t.Fatalf("позднее удаление откатило новую версию: %v", files)
	}
	current, err := loadState(destination.StatePath)
	record := current.Blocks["point-01|source-1|block-1"]
	if err != nil || record.Tombstone || record.SourceVersion != "2026-10-28 10:04:00" {
		t.Fatalf("неверное состояние Word после restore: %#v err=%v", record, err)
	}
}

func TestDestinationVersionedWordStateRejectsLateEditAndUnscopedCleanup(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "Тест", Prefix: "GAME-",
	}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	currentEvent := domain.Event{
		ID: "v3", ClientID: "point-01", CreatedAt: base.Add(3 * time.Minute), SourceAt: base.Add(3 * time.Minute),
		Source: domain.SourceRecord{UUID: "source-1"}, Position: domain.Position{UnitName: "Игровая команда"},
		SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.03\n-Актуальная v3", UpdatedAt: "2026-10-28 10:03:00",
		}},
	}
	if err := destination.Deliver(context.Background(), currentEvent, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	legacy, err := loadState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	key := "point-01|source-1|block-1"
	record := legacy.Blocks[key]
	record.SourceVersion = "2026-10-28 10:03:00"
	record.SourceEventAt = base.Add(3 * time.Minute)
	record.UpdatedAt = base.Add(3 * time.Minute)
	legacy.Blocks[key] = record
	if err := saveState(destination.StatePath, legacy); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(destination.Folder, record.Filename)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	stale := currentEvent
	stale.ID = "v2-late"
	stale.CreatedAt = base.Add(2 * time.Minute)
	stale.SourceAt = stale.CreatedAt
	stale.Blocks[0].UpdatedAt = "2026-10-28 10:02:00"
	stale.Blocks[0].Text = "10.02\n-Старая v2"
	if err := destination.Deliver(context.Background(), stale, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	afterEdit, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, afterEdit) {
		t.Fatalf("запоздалая rc90-правка перезаписала Word: changed=%v err=%v", !bytes.Equal(before, afterEdit), err)
	}
	cleanup := stale
	cleanup.ID = "legacy-cleanup-v2"
	cleanup.SourceBlockUUID = ""
	cleanup.Blocks = nil
	cleanup.Source.UpdatedAt = "2026-10-28 10:02:00"
	if err := destination.Deliver(context.Background(), cleanup, domain.PlannedDelivery{
		Decision: domain.RouteDecision{Action: domain.RouteSkip},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("запоздалый unscoped cleanup удалил актуальный Word: %v", err)
	}
}

func TestDestinationLegacyWordStateAcceptsNewBacklogRevisionOlderThanLocalWrite(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "Тест", Prefix: "GAME-",
	}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	initial := domain.Event{
		ID: "v1", ClientID: "point-01", CreatedAt: base, SourceAt: base,
		Source:   domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:00:00"},
		Position: domain.Position{UnitName: "Игровая команда"}, SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.00\n-Первая редакция", UpdatedAt: "2026-10-28 10:00:00",
		}},
	}
	if err := destination.Deliver(context.Background(), initial, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	legacy, err := loadState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	key := "point-01|source-1|block-1"
	record := legacy.Blocks[key]
	record.SourceVersion = ""
	record.SourceEventAt = time.Time{}
	record.UpdatedAt = base.Add(2 * time.Hour)
	legacy.Blocks[key] = record
	if err := saveState(destination.StatePath, legacy); err != nil {
		t.Fatal(err)
	}

	backlog := initial
	backlog.ID = "backlog-v2"
	backlog.CreatedAt = base.Add(5 * time.Minute)
	backlog.SourceAt = backlog.CreatedAt
	backlog.Source.UpdatedAt = "2026-10-28 10:05:00"
	backlog.Blocks[0].UpdatedAt = "2026-10-28 10:05:00"
	backlog.Blocks[0].Text = "10.00\n-Новая редакция из очереди"
	if err := destination.Deliver(context.Background(), backlog, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	current, err := loadState(destination.StatePath)
	record = current.Blocks[key]
	if err != nil || record.SourceVersion != "2026-10-28 10:05:00" || !record.SourceEventAt.Equal(backlog.CreatedAt) {
		t.Fatalf("истинная Word-ревизия backlog не закреплена: %#v err=%v", record, err)
	}
	path := filepath.Join(destination.Folder, record.Filename)
	if !strings.Contains(documentXML(t, path), "Новая редакция из очереди") {
		t.Fatal("новая backlog-редакция не перезаписала legacy Word")
	}
}

func TestWordStateBoundsSourceTombstones(t *testing.T) {
	t.Parallel()
	current := state{Blocks: make(map[string]blockRecord)}
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 4100; index++ {
		current.Blocks[fmt.Sprintf("block-%04d", index)] = blockRecord{
			Tombstone: true, UpdatedAt: base.Add(time.Duration(index) * time.Second),
		}
	}
	compactWordTombstones(&current)
	if len(current.Blocks) != 4096 {
		t.Fatalf("надгробия Word не ограничены: %d", len(current.Blocks))
	}
	if _, exists := current.Blocks["block-0000"]; exists {
		t.Fatal("самое старое Word-надгробие не вытеснено")
	}
}

func TestWordRecordCleanupRejectsDelayedScopedEventAndAllowsNewerRevision(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "Тест", Prefix: "GAME-",
	}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "v1", ClientID: "point-01", CreatedAt: base, SourceAt: base,
		Source:   domain.SourceRecord{UUID: "source-1", UpdatedAt: "2026-10-28 10:00:00"},
		Position: domain.Position{UnitName: "Игровая команда"}, SourceBlockUUID: "block-1",
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.00\n-Первая версия", UpdatedAt: "2026-10-28 10:00:00",
		}},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	cleanup := event
	cleanup.ID = "v3-cleanup"
	cleanup.CreatedAt = base.Add(3 * time.Minute)
	cleanup.SourceAt = cleanup.CreatedAt
	cleanup.Source.UpdatedAt = "2026-10-28 10:03:00"
	cleanup.SourceBlockUUID = ""
	cleanup.Blocks = nil
	if err := destination.Deliver(context.Background(), cleanup, domain.PlannedDelivery{
		Decision: domain.RouteDecision{Action: domain.RouteSkip},
	}); err != nil {
		t.Fatal(err)
	}
	// Поблочные надгробия ограничены 4096, но удаление всей записи — отдельный
	// долговечный барьер и не должно исчезнуть вместе с ними.
	compacted, err := loadState(destination.StatePath)
	for index := 0; index < 4100; index++ {
		compacted.Blocks[fmt.Sprintf("other|record-%04d", index)] = blockRecord{
			Tombstone: true, UpdatedAt: base.Add(time.Duration(index) * time.Second),
		}
	}
	if err := saveState(destination.StatePath, compacted); err != nil {
		t.Fatal(err)
	}
	compacted, err = loadState(destination.StatePath)
	if err != nil || len(compacted.Blocks) != 4096 ||
		compacted.RecordTombstones["point-01|source-1"].SourceVersion != "2026-10-28 10:03:00" {
		t.Fatalf("барьер удаления записи потерян при компактации Word: blocks=%d barrier=%#v err=%v",
			len(compacted.Blocks), compacted.RecordTombstones["point-01|source-1"], err)
	}
	equal := event
	equal.ID = "v3-equal-delayed"
	equal.CreatedAt = cleanup.CreatedAt
	equal.SourceAt = cleanup.SourceAt
	equal.Source.UpdatedAt = "2026-10-28 10:03:00"
	equal.Blocks[0].UpdatedAt = "2026-10-28 10:03:00"
	equal.Blocks[0].Text = "10.00\n-Равная удалению запоздалая версия"
	if err := destination.Deliver(context.Background(), equal, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	delayed := event
	delayed.ID = "v2-delayed"
	delayed.CreatedAt = base.Add(2 * time.Minute)
	delayed.SourceAt = delayed.CreatedAt
	delayed.Source.UpdatedAt = "2026-10-28 10:02:00"
	delayed.Blocks[0].UpdatedAt = "2026-10-28 10:02:00"
	delayed.Blocks[0].Text = "10.00\n-Запоздалая версия"
	if err := destination.Deliver(context.Background(), delayed, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	if matches, err := filepath.Glob(filepath.Join(destination.Folder, "*.docx")); err != nil || len(matches) != 0 {
		t.Fatalf("запоздалое событие воскресило Word: files=%v err=%v", matches, err)
	}
	newer := event
	newer.ID = "v4-new"
	newer.CreatedAt = base.Add(4 * time.Minute)
	newer.SourceAt = newer.CreatedAt
	newer.Source.UpdatedAt = "2026-10-28 10:04:00"
	newer.Blocks[0].UpdatedAt = "2026-10-28 10:04:00"
	newer.Blocks[0].Text = "10.00\n-Новая версия после удаления записи"
	if err := destination.Deliver(context.Background(), newer, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	current, err := loadState(destination.StatePath)
	record := current.Blocks["point-01|source-1|block-1"]
	barrier := current.RecordTombstones["point-01|source-1"]
	if err != nil || record.Tombstone || record.Filename == "" ||
		barrier.SourceVersion != "2026-10-28 10:03:00" {
		t.Fatalf("барьер удаления записи или новый Word не сохранены: record=%#v barrier=%#v err=%v", record, barrier, err)
	}
	path := filepath.Join(destination.Folder, record.Filename)
	if !strings.Contains(documentXML(t, path), "Новая версия после удаления записи") {
		t.Fatal("после барьера создана не новая редакция Word")
	}
}

func TestWordRecordCleanupTombstonesUseHardLimitInsteadOfEviction(t *testing.T) {
	t.Parallel()
	current := state{RecordTombstones: make(map[string]recordRevisionBarrier)}
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 4100; index++ {
		current.RecordTombstones[fmt.Sprintf("record-%04d", index)] = recordRevisionBarrier{
			SourceVersion: "2026-10-01 00:00:00", UpdatedAt: base.Add(time.Duration(index) * time.Second),
		}
	}
	if err := enforceWordRecordTombstoneLimit(current, 4096); err == nil ||
		!application.IsPermanentDeliveryError(err) {
		t.Fatalf("переполнение барьеров Word не дало постоянную ошибку: %v", err)
	}
	if len(current.RecordTombstones) != 4100 {
		t.Fatalf("защитные барьеры молча вытеснены: %d", len(current.RecordTombstones))
	}
}

// Смена общей папки не должна использовать журнал прежней папки и ошибочно
// считать документ уже доставленным.
func TestDestinationResetsStateWhenFolderScopeChanges(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	statePath := filepath.Join(root, "state", "word.json")
	event := domain.Event{
		ClientID: "textile", Source: domain.SourceRecord{UUID: "source-folder-change"},
		Position: domain.Position{UnitName: "1 мб 155 омбр"}, Content: "10.00\n-Проверка",
		ObservedAt: time.Now(),
	}
	first := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "first"), StatePath: statePath,
		StateScope: "folder:first", Network: "39", Prefix: "СТНД",
	}
	if err := first.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	second := first
	second.Folder = filepath.Join(root, "second")
	second.StateScope = "folder:second"
	if err := second.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	for _, folder := range []string{first.Folder, second.Folder} {
		files, _ := filepath.Glob(filepath.Join(folder, "*.docx"))
		if len(files) != 1 {
			t.Fatalf("в папке %q ожидался документ: %#v", folder, files)
		}
	}
}

func TestDestinationAssignsSequentialNumbersAcrossTimeBlocks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "313", Prefix: "СТНД",
	}
	event := domain.Event{
		Source:   domain.SourceRecord{ID: 3, UUID: "source-multi-time"},
		SourceAt: time.Date(2026, 10, 21, 11, 16, 0, 0, time.UTC),
		Position: domain.Position{UnitName: "1 мб 155 омбр", Frequency: "100.100", GroupCode: "A1"},
		Content:  "11.02\n-Первый доклад. (52325)\n-Ответ. (52345)\n11.09\n-Повтор. (52325)\n11.16\n-Подтверждаю. (52345)",
		Blocks: []domain.ContentBlock{
			{Key: "11.02", Text: "11.02\n-Первый доклад. (52325)\n-Ответ. (52345)", Callsigns: []domain.Callsign{
				{Number: 25, Code: "52345", Label: "Монако"},
				{Number: 26, Code: "52325", Label: "Торба"},
			}},
			{Key: "11.09", Text: "11.09\n-Повтор. (52325)", Callsigns: []domain.Callsign{{Number: 26, Code: "52325", Label: "Торба"}}},
			{Key: "11.16", Text: "11.16\n-Подтверждаю. (52345)", Callsigns: []domain.Callsign{{Number: 25, Code: "52345", Label: "Монако"}}},
		},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(files) != 3 {
		t.Fatalf("вместо трёх документов созданы: %#v", files)
	}
	var runs []string
	for _, file := range files {
		runs = append(runs, documentRuns(t, documentXML(t, file))...)
	}
	for _, expected := range []string{
		"1.«", "Торба", "2.«", "Монако",
		"11:02", "/1/ Первый доклад.", "/2/ Ответ.",
		"11:09", "/1/ Повтор.", "11:16", "/1/ Подтверждаю.",
	} {
		if !slices.Contains(runs, expected) {
			t.Fatalf("в отдельных Word нет %q: %#v", expected, runs)
		}
	}
}

// Отложенный перехват сохраняет пометку в названии радиосети и выглядит как документ старой программы.
func TestDeferredInterceptKeepsMarkerAndUsesLegacyDocumentLayout(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "313", Protection: "AES-256", Prefix: "СТНД",
	}
	event := domain.Event{
		ID: "event-deferred", ClientID: "textile", CreatedAt: time.Now(),
		ObservedAt: time.Date(2026, 10, 18, 10, 0, 0, 0, time.UTC),
		SourceAt:   time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC),
		Source:     domain.SourceRecord{ID: 2, UUID: "source-deferred"},
		Position: domain.Position{
			UnitName: "2 об 3 обр ОсН НГ (отложенный режим)", Frequency: "159.5750", GroupCode: "3",
		},
		Mode: domain.SourceModeDeferred, Content: "03.51\n-На приём. (783693)", ContentDigest: "digest",
		Blocks: []domain.ContentBlock{{
			Key: "03.51", Sequence: 0, Text: "03.51\n-На приём. (783693)",
			Callsigns: []domain.Callsign{{Code: "783693", Label: "Марс"}},
		}},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(files) != 1 || !strings.Contains(filepath.Base(files[0]), "(отложенный режим) - 313.docx") {
		t.Fatalf("неожиданное имя документа: %#v", files)
	}
	xml := documentXML(t, files[0])
	for _, expected := range []string{
		"ДНЕПРОПЕТРОВСКОЕ НАПРАВЛЕНИЕ",
		"2 об 3 обр ОсН НГ (отложенный режим)",
		" - 313", "18.10.2026г.", "Марс", "783693", "03:51", "/1/ На приём.",
	} {
		if !strings.Contains(xml, expected) {
			t.Fatalf("в документе нет %q", expected)
		}
	}
	wantRuns := []string{
		"ДНЕПРОПЕТРОВСКОЕ НАПРАВЛЕНИЕ",
		"Радиосеть ", "2 об 3 обр ОсН НГ (отложенный режим)", " - 313",
		"159.5750 МГц (гр. ID 3 AES-256)", "18.10.2026г.",
		"1.«", "Марс", "» (id: 783693);",
		"03:51", "/1/ На приём.",
		"Дежурный оператор: ", "Позывной", "Старший смены МГ ", "1", ": ", "Позывной",
	}
	if runs := documentRuns(t, xml); !reflect.DeepEqual(runs, wantRuns) {
		t.Fatalf("текстовые фрагменты документа отличаются:\nполучено: %#v\nожидалось: %#v", runs, wantRuns)
	}
}

func TestDestinationCleanupRemovesPreviousDocumentAndState(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "39", Prefix: "СТНД",
	}
	event := domain.Event{
		ClientID: "textile", Source: domain.SourceRecord{UUID: "source-1"},
		Position: domain.Position{UnitName: "1 мб 155 омбр"}, Content: "12.00\n-Текст", SourceAt: time.Now(),
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{
		Decision: domain.RouteDecision{Action: domain.RouteDeliver},
	}); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx")); len(files) != 1 {
		t.Fatalf("исходный документ не создан: %#v", files)
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{
		Decision: domain.RouteDecision{Action: domain.RouteSkip},
	}); err != nil {
		t.Fatal(err)
	}
	if files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx")); len(files) != 0 {
		t.Fatalf("прежний документ остался: %#v", files)
	}
	current, err := loadState(destination.StatePath)
	if err != nil || len(current.Blocks) != 0 {
		t.Fatalf("состояние прежнего документа осталось: %#v, %v", current.Blocks, err)
	}
}

// Исправленный документ показывает время редакции отдельно от радиопереговоров.
func TestEditedDocumentShowsEditTimeOutsideTranscript(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	payload, err := Build(template, Document{
		Unit: "позиция", Content: "12.00\n-Текст", Edited: true,
		EditedAt: time.Date(2026, 10, 19, 15, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "edited.docx")
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
	if xml := documentXML(t, path); !strings.Contains(xml, "(отредактировано 18:30:00)") || strings.Contains(xml, "[ИЗМЕНЕНО") {
		t.Fatalf("неверная пометка редактирования: %s", xml)
	}
}

func TestEditedDocumentRequiresEditTime(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	_, err := Build(template, Document{Unit: "позиция", Content: "12.00\n-Текст", Edited: true})
	if err == nil || !strings.Contains(err.Error(), "не задано время изменения") {
		t.Fatalf("ожидалась проверка времени редакции: %v", err)
	}
}

// После правки перехвата старые документы удаляются, а вместо них появляются документы с новым содержимым и пометкой о редакции.
func TestDestinationUpdatesAndRemovesDocumentsAfterEAVCEdit(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "39", Prefix: "СТНД",
	}
	event := domain.Event{
		Source:     domain.SourceRecord{ID: 1, UUID: "source-edit"},
		Position:   domain.Position{UnitName: "1 мб 155 омбр", Frequency: "100", GroupCode: "A"},
		ObservedAt: time.Now(), Content: "10.00\n-Первый\n10.05\n-Второй",
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	before, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(before) != 2 {
		t.Fatalf("до правки ожидалось два документа: %#v", before)
	}
	firstName := filepath.Base(before[0])
	event.SourceAt = time.Date(2026, 10, 19, 6, 47, 31, 0, time.UTC)
	event.Content = "10.00\n-Первый исправлен"
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(destination.Folder, "*.docx"))
	if len(files) != 1 {
		t.Fatalf("удалённый блок должен убрать свой документ: %#v", files)
	}
	if filepath.Base(files[0]) != firstName {
		t.Fatalf("правка создала новый номер вместо перезаписи: до=%q после=%q", firstName, filepath.Base(files[0]))
	}
	if xml := documentXML(t, files[0]); !strings.Contains(xml, "(отредактировано 09:47:31)") || !strings.Contains(xml, "Первый исправлен") {
		t.Fatal("изменённый документ не перезаписан с пометкой")
	}
}

// Если работа с одним документом зависла, документ для другого направления должен создаться без ожидания.
func TestDestinationDoesNotWaitBehindStuckOperation(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "word-state.json")
	unlock, acquired := tryDestinationLock(statePath)
	if !acquired {
		t.Fatal("не удалось захватить тестовую блокировку")
	}
	destination := Destination{
		TemplatePath: "template.docx", Folder: "out", StatePath: statePath,
	}
	started := time.Now()
	err := destination.Deliver(context.Background(), domain.Event{}, domain.PlannedDelivery{})
	if !errors.Is(err, errDestinationBusy) {
		t.Fatalf("ожидалась занятая операция Word: %v", err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("вторая Word-доставка ждала зависшую операцию")
	}
	unlock()
	lockKey, _ := filepath.Abs(statePath)
	lockKey = strings.ToLower(filepath.Clean(lockKey))
	destinationLocks.Lock()
	_, remains := destinationLocks.items[lockKey]
	destinationLocks.Unlock()
	if remains {
		t.Fatal("собственная Word-блокировка не удалена из реестра")
	}
}

// Настраиваемые подпись и номер получателя не могут превратить имя документа
// в путь за пределами общей папки.
func TestDocumentNameRemovesPathSeparatorsFromConfiguredParts(t *testing.T) {
	t.Parallel()
	destination := Destination{Prefix: `..\СТНД/`, Network: `../39\`, Folder: t.TempDir()}
	name := destination.documentName(15, `../подразделение\`)
	if filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		t.Fatalf("небезопасное имя документа: %q", name)
	}
}

// Подозрительно большой шаблон Word отклоняется до распаковки и не может занять всю память или диск.
func TestBuildRejectsDecompressionBomb(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "oversized.docx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	part, err := writer.Create("word/document.xml")
	if err != nil {
		t.Fatal(err)
	}
	chunk := make([]byte, 1024*1024)
	for written := 0; written <= maxTemplatePartBytes; written += len(chunk) {
		if _, err := part.Write(chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Build(path, Document{}); err == nil {
		t.Fatal("DOCX с чрезмерно большой распакованной частью должен быть отклонён")
	}
}

func TestValidateTemplateListsMissingMarkers(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "without-markers.docx")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	part, _ := writer.Create("word/document.xml")
	_, _ = part.Write([]byte(`<?xml version="1.0"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p><w:r><w:t>Обычный текст</w:t></w:r></w:p></w:body></w:document>`))
	_ = writer.Close()
	_ = file.Close()
	err = ValidateTemplate(path)
	if err == nil || !strings.Contains(err.Error(), markerTranscript) || !strings.Contains(err.Error(), markerUnit) {
		t.Fatalf("ожидался список отсутствующих меток: %v", err)
	}
}

func documentXML(t *testing.T, path string) string {
	t.Helper()
	archive, err := zip.OpenReader(path)
	if err != nil {
		t.Fatal(err)
	}
	defer archive.Close()
	for _, file := range archive.File {
		if file.Name == "word/document.xml" {
			reader, err := file.Open()
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatal(err)
			}
			return string(payload)
		}
	}
	t.Fatal("нет word/document.xml")
	return ""
}

func documentRuns(t *testing.T, payload string) []string {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(payload))
	var runs []string
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return runs
		}
		if err != nil {
			t.Fatal(err)
		}
		start, ok := token.(xml.StartElement)
		if !ok || start.Name.Local != "t" {
			continue
		}
		var text string
		if err := decoder.DecodeElement(&text, &start); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, text)
	}
}

func createTemplate(t *testing.T, path string) {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func createModifiedTemplate(t *testing.T, path string, replacements map[string][]byte) {
	t.Helper()
	source, err := zip.OpenReader(filepath.Join("..", "..", "..", "assets", "word_template.docx"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	output, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(output)
	written := make(map[string]bool)
	for _, item := range source.File {
		payload, replaced := replacements[item.Name]
		if !replaced {
			reader, openErr := item.Open()
			if openErr != nil {
				t.Fatal(openErr)
			}
			payload, err = io.ReadAll(reader)
			reader.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
		part, createErr := writer.Create(item.Name)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, err := part.Write(payload); err != nil {
			t.Fatal(err)
		}
		written[item.Name] = true
	}
	for name, payload := range replacements {
		if written[name] {
			continue
		}
		part, createErr := writer.Create(name)
		if createErr != nil {
			t.Fatal(createErr)
		}
		if _, err := part.Write(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
}
