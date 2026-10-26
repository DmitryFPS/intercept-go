package docx

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"intercept/internal/domain"
)

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

// Все временные блоки одного перехвата остаются в одном Word-документе, а
// повторная обработка не порождает копий.
func TestDestinationBuildsOneDocumentForAllTimeBlocksWithoutDuplicates(t *testing.T) {
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
	if len(files) != 1 {
		t.Fatalf("ожидался один документ без дублей: %#v", files)
	}
	numbers, err := StateNumbers(destination.StatePath)
	if err != nil || len(numbers) != 1 || numbers[0] != 1 {
		t.Fatalf("номера: %#v %v", numbers, err)
	}
	archive, err := zip.OpenReader(files[0])
	if err != nil {
		t.Fatalf("open docx: %v", err)
	}
	defer archive.Close()
	for _, file := range archive.File {
		if file.Name != "word/document.xml" {
			continue
		}
		reader, _ := file.Open()
		payload, _ := io.ReadAll(reader)
		reader.Close()
		for _, expected := range []string{"1 мб 155 омбр", "10:00", "Первый", "10:05", "Второй"} {
			if !strings.Contains(string(payload), expected) {
				t.Fatalf("в объединённый документ не попало %q", expected)
			}
		}
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

func TestDestinationKeepsSharedLocalNumbersAcrossTimeBlocks(t *testing.T) {
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
	if len(files) != 1 {
		t.Fatalf("вместо одного документа созданы: %#v", files)
	}
	runs := documentRuns(t, documentXML(t, files[0]))
	for _, expected := range []string{
		"1.«", "Торба", "2.«", "Монако",
		"11:02", "/1/ Первый доклад.", "/2/ Ответ.",
		"11:09", "/1/ Повтор.", "11:16", "/2/ Подтверждаю.",
	} {
		if !slices.Contains(runs, expected) {
			t.Fatalf("в объединённом Word нет %q: %#v", expected, runs)
		}
	}
}

func TestDestinationMigratesLegacyBlockDocumentsWithoutEditedMarker(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	folder := filepath.Join(root, "out")
	if err := os.MkdirAll(folder, 0o755); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "state", "word.json")
	legacy := state{LastNumber: 8, Blocks: map[string]blockRecord{
		"legacy-source|10.00": {Number: 7, Filename: "old-7.docx", Render: "old"},
		"legacy-source|10.05": {Number: 8, Filename: "old-8.docx", Render: "old"},
	}}
	if err := saveState(statePath, legacy); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"old-7.docx", "old-8.docx"} {
		if err := os.WriteFile(filepath.Join(folder, name), []byte("old"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	destination := Destination{TemplatePath: template, Folder: folder, StatePath: statePath, Network: "313", Prefix: "СТНД"}
	event := domain.Event{
		Source: domain.SourceRecord{UUID: "legacy-source"}, Position: domain.Position{UnitName: "позиция"},
		Content: "10.00\n-Первый\n10.05\n-Второй", SourceAt: time.Now(),
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(folder, "*.docx"))
	if len(files) != 1 || !strings.HasPrefix(filepath.Base(files[0]), "СТНД7 ") {
		t.Fatalf("миграция не сохранила первый номер: %#v", files)
	}
	if xml := documentXML(t, files[0]); strings.Contains(xml, "(отредактировано") {
		t.Fatal("техническое объединение ошибочно помечено как редакция оператора")
	}
	numbers, err := StateNumbers(statePath)
	if err != nil || !reflect.DeepEqual(numbers, []int{7}) {
		t.Fatalf("состояние после миграции: %#v, %v", numbers, err)
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
	if len(before) != 1 {
		t.Fatalf("до правки ожидался один документ: %#v", before)
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
