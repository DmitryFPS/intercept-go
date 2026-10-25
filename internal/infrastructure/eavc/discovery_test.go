package eavc

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// Проверяет, что быстрый поиск принимает рабочую базу рядом с клиентом и
// игнорирует одноимённый файл, не относящийся к EAVC.
func TestDiscoverReturnsOnlyValidatedEAVCDatabase(t *testing.T) {
	root := t.TempDir()
	validDir := filepath.Join(root, "EAVC", "data-hub")
	if err := os.MkdirAll(validDir, 0o755); err != nil {
		t.Fatal(err)
	}
	createDiscoveryDatabase(t, filepath.Join(validDir, databaseFilename))
	if err := os.WriteFile(filepath.Join(root, databaseFilename), []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}

	candidates, err := Discover(context.Background(), []string{root}, false)
	if err != nil {
		t.Fatalf("поиск завершился ошибкой: %v", err)
	}
	if len(candidates) != 1 {
		t.Fatalf("ожидалась одна рабочая база, получено %d", len(candidates))
	}
	if candidates[0].Path != filepath.Join(validDir, databaseFilename) {
		t.Fatalf("найден неожиданный путь: %s", candidates[0].Path)
	}
}

// Проверяет фактическую структуру установки EAVC на рабочих точках:
// переносимый клиент лежит рядом с каталогом EAVC, а база — в data-hub.
func TestDiscoverFindsProductionDataHubNextToStation(t *testing.T) {
	labRoot := t.TempDir()
	stationRoot := filepath.Join(labRoot, "EAVC Connect Station")
	databasePath := filepath.Join(labRoot, "EAVC", "data-hub", databaseFilename)
	if err := os.MkdirAll(stationRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o755); err != nil {
		t.Fatal(err)
	}
	createDiscoveryDatabase(t, databasePath)

	candidates, err := Discover(context.Background(), QuickRoots(stationRoot), false)
	if err != nil {
		t.Fatalf("быстрый поиск завершился ошибкой: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Path != databasePath {
		t.Fatalf("ожидалась база %q, получено %#v", databasePath, candidates)
	}
}

func TestDiscoverFindsDataHubAfterStationMovesOneLevelUp(t *testing.T) {
	desktop := t.TempDir()
	stationRoot := filepath.Join(desktop, "EAVC Connect Station-01")
	databasePath := filepath.Join(desktop, "Previous workspace", "EAVC", "data-hub", databaseFilename)
	if err := os.MkdirAll(stationRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o755); err != nil {
		t.Fatal(err)
	}
	createDiscoveryDatabase(t, databasePath)

	candidates, err := Discover(context.Background(), QuickRoots(stationRoot), false)
	if err != nil {
		t.Fatalf("быстрый поиск завершился ошибкой: %v", err)
	}
	if len(candidates) != 1 || candidates[0].Path != databasePath {
		t.Fatalf("ожидалась база %q в прежней общей папке, получено %#v", databasePath, candidates)
	}
}

// Проверяет ручной ввод как папки EAVC, так и полного пути к main.sqlite.
func TestValidatePathAcceptsDirectoryAndFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, databaseFilename)
	createDiscoveryDatabase(t, path)
	for _, value := range []string{root, path} {
		actual, err := ValidatePath(context.Background(), value)
		if err != nil {
			t.Fatalf("путь %q не принят: %v", value, err)
		}
		if actual != path {
			t.Fatalf("для %q получен путь %q", value, actual)
		}
	}
}

func createDiscoveryDatabase(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE intercept_items (
		id INTEGER, session_id INTEGER, session_uuid TEXT, unit_name TEXT,
		frequency TEXT, group_code TEXT, uuid TEXT, content TEXT,
		updated_by TEXT, updated_at TEXT
	)`)
	if err != nil {
		t.Fatal(err)
	}
}
