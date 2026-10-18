package docx

import (
	"archive/zip"
	"context"
	"encoding/xml"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"intercept/internal/domain"
)

// Каждый временной блок перехвата создаёт один Word-документ, а повторная обработка не порождает копий.
func TestDestinationBuildsOneDocumentPerTimeBlockWithoutDuplicates(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"), Network: "313", Prefix: "СТНД",
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
	if err != nil || len(numbers) != 2 || numbers[0] != 1 || numbers[1] != 2 {
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
		if !strings.Contains(string(payload), "1 мб 155 омбр") {
			t.Fatal("подразделение не попало в документ")
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
		StatePath: filepath.Join(root, "state", "word.json"), Prefix: "СТНД",
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
		"1.«", "Марс", "» (id: 783693) – военнослужащий 2 об 3 обр ОсН НГ (отложенный режим), мужской голос, украинская речь;",
		"03:51", "/1/ На приём.",
		"Дежурный оператор: ", "Позывной", "Старший смены МГ ", "1", ": ", "Позывной",
	}
	if runs := documentRuns(t, xml); !reflect.DeepEqual(runs, wantRuns) {
		t.Fatalf("текстовые фрагменты документа отличаются:\nполучено: %#v\nожидалось: %#v", runs, wantRuns)
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
	if xml := documentXML(t, files[0]); !strings.Contains(xml, editedMark) || !strings.Contains(xml, "Первый исправлен") {
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
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(file)
	parts := map[string]string{
		"[Content_Types].xml": `<?xml version="1.0"?><Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"></Types>`,
		"word/document.xml":   `<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body><w:p/></w:body></w:document>`,
	}
	for name, content := range parts {
		part, _ := writer.Create(name)
		part.Write([]byte(content))
	}
	writer.Close()
	file.Close()
}
