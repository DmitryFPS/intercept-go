package docx

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/windows"

	"intercept/internal/domain"
)

func TestDestinationRetriesRemovingOldFileAfterLockedRename(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	template := filepath.Join(root, "template.docx")
	createTemplate(t, template)
	destination := Destination{
		TemplatePath: template, Folder: filepath.Join(root, "out"),
		StatePath: filepath.Join(root, "state", "word.json"),
		Network:   "Тест", Prefix: "GAME-",
	}
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	event := domain.Event{
		ID: "first", ClientID: "point-01", CreatedAt: base, SourceAt: base,
		Source:          domain.SourceRecord{UUID: "source-1"},
		Position:        domain.Position{UnitName: "Позиция А"},
		SourceBlockUUID: "block-1", HasActiveBlocks: true,
		Blocks: []domain.ContentBlock{{
			UUID: "block-1", Text: "10.00\n-Первый", UpdatedAt: "2026-10-28 10:00:00",
		}},
	}
	if err := destination.Deliver(context.Background(), event, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(destination.Folder, destination.documentName(1, event.Position.UnitName))
	path, err := windows.UTF16PtrFromString(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	// Deny FILE_SHARE_DELETE so the old file cannot be removed during the rename.
	handle, err := windows.CreateFile(path, windows.GENERIC_READ, 0, nil,
		windows.OPEN_EXISTING, windows.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_ = windows.CloseHandle(handle)
		}
	}()

	revised := event
	revised.ID = "second"
	revised.CreatedAt = base.Add(time.Minute)
	revised.SourceAt = revised.CreatedAt
	revised.Position.UnitName = "Позиция Б"
	revised.Blocks = append([]domain.ContentBlock(nil), event.Blocks...)
	revised.Blocks[0].UpdatedAt = "2026-10-28 10:01:00"
	if err := destination.Deliver(context.Background(), revised, domain.PlannedDelivery{}); err == nil {
		t.Fatal("блокировка старого документа должна оставить доставку для повторной попытки")
	}
	current, err := loadState(destination.StatePath)
	if err != nil {
		t.Fatal(err)
	}
	if current.Blocks["point-01|source-1|block-1"].Filename != filepath.Base(oldPath) {
		t.Fatal("неудалённый старый документ исчез из сохранённого состояния")
	}
	if err := windows.CloseHandle(handle); err != nil {
		t.Fatal(err)
	}
	locked = false
	if err := destination.Deliver(context.Background(), revised, domain.PlannedDelivery{}); err != nil {
		t.Fatal(err)
	}
	newPath := filepath.Join(destination.Folder, destination.documentName(1, revised.Position.UnitName))
	if _, err := os.Stat(newPath); err != nil {
		t.Fatalf("новый Word-документ отсутствует: %v", err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("старый Word-документ остался после освобождения файла и повтора доставки: %v", err)
	}
}
