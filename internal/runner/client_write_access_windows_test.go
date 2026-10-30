//go:build windows

package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/windows"

	"intercept/internal/config"
)

func TestClientWriteAccessProbeChecksInstallationDirectories(t *testing.T) {
	for _, deniedDir := range []string{".", "data", "logs"} {
		t.Run(deniedDir, func(t *testing.T) {
			baseDir := t.TempDir()
			for _, name := range []string{"data", "logs"} {
				if err := os.Mkdir(filepath.Join(baseDir, name), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			denyCurrentUserWrite(t, filepath.Join(baseDir, deniedDir))
			accessErr := CheckClientWriteAccess(baseDir)
			if accessErr == nil {
				t.Fatalf("не обнаружен запрет записи в %s", deniedDir)
			}
			if accessErr.Path != filepath.Join(baseDir, deniedDir) {
				t.Fatalf("неверный путь отказа: %q", accessErr.Path)
			}
		})
	}
}

func TestClientWriteAccessProbeChecksExistingDatabase(t *testing.T) {
	baseDir := t.TempDir()
	databasePath := filepath.Join(baseDir, "data", "client.db")
	if err := os.MkdirAll(filepath.Dir(databasePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(databasePath, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(databasePath, 0o400); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(databasePath, 0o600) })
	accessErr := CheckClientWriteAccess(baseDir)
	if accessErr == nil || accessErr.Path != databasePath {
		t.Fatalf("не обнаружен запрет записи в существующую базу: %v", accessErr)
	}
	content, err := os.ReadFile(databasePath)
	if err != nil || string(content) != "unchanged" {
		t.Fatalf("проверка изменила базу: %q, %v", content, err)
	}
}

func TestClientWriteAccessProbeLeavesNoFiles(t *testing.T) {
	baseDir := t.TempDir()
	if err := CheckClientWriteAccess(baseDir); err != nil {
		t.Fatal(err)
	}
	for _, dir := range []string{baseDir, filepath.Join(baseDir, "data"), filepath.Join(baseDir, "logs")} {
		matches, err := filepath.Glob(filepath.Join(dir, ".eavc-write-check-*"))
		if err != nil || len(matches) != 0 {
			t.Fatalf("в %s остались проверочные файлы: %v, %v", dir, matches, err)
		}
	}
}

func TestClientStopsRestartOnConfirmedWriteDenial(t *testing.T) {
	baseDir := t.TempDir()
	denyCurrentUserWrite(t, baseDir)
	client := NewClient(baseDir, "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	attempts := 0
	client.storageOpener = func(context.Context, string) (localQueueHandle, error) {
		attempts++
		return localQueueHandle{}, errors.New("SQLITE_CANTOPEN (14)")
	}
	done := make(chan struct{})
	go func() {
		client.runSupervised(context.Background(), config.Client{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("клиент продолжил повторять запуск при подтверждённом отказе записи")
	}
	if attempts != 1 {
		t.Fatalf("при отказе записи выполнено %d попыток", attempts)
	}
	status := client.Status()
	if status.State != "error" || !strings.Contains(status.Detail, "право изменения") {
		t.Fatalf("оператор не видит требуемое действие: %+v", status)
	}
	select {
	case <-client.ProcessRestartRequests():
		t.Fatal("при отказе записи запрошен рестарт процесса")
	default:
	}
}

func denyCurrentUserWrite(t *testing.T, dir string) {
	t.Helper()
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Skipf("не удалось определить SID для проверки ACL: %v", err)
	}
	sid := "*" + user.User.Sid.String()
	output, err := exec.Command("icacls", dir, "/deny", sid+":(W)").CombinedOutput()
	if err != nil {
		t.Skipf("не удалось задать ACL для проверки: %v: %s", err, output)
	}
	t.Cleanup(func() {
		output, err := exec.Command("icacls", dir, "/remove:d", sid).CombinedOutput()
		if err != nil {
			t.Errorf("не удалось восстановить ACL %s: %v: %s", dir, err, output)
		}
	})
}
