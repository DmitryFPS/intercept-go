package docx

import (
	"archive/zip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"intercept/internal/domain"
)

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
