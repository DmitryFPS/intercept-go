package runner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	stateDB "intercept/internal/infrastructure/sqlite"
)

func TestClientRetriesTransientLocalStorageFailure(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.storageRetry = time.Millisecond
	attempts := 0
	client.storageOpener = func(context.Context, string) (localQueueHandle, error) {
		attempts++
		if attempts == 1 {
			return localQueueHandle{}, errors.New("database is temporarily locked")
		}
		return localQueueHandle{}, nil
	}

	if _, err := client.waitForLocalQueue(context.Background(), "client.db"); err != nil {
		t.Fatalf("временный сбой не восстановлен: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("ожидалось две попытки, получено %d", attempts)
	}
	status := client.Status()
	if status.State != "waiting" {
		t.Fatalf("ожидалось понятное состояние ожидания, получено %q", status.State)
	}
	if status.StorageError != "database is temporarily locked" {
		t.Fatalf("storage diagnostic was not retained: %q", status.StorageError)
	}
}

func TestClientDoesNotRetryPermanentMigrationFailure(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	attempts := 0
	client.storageOpener = func(context.Context, string) (localQueueHandle, error) {
		attempts++
		return localQueueHandle{}, stateDB.ErrMigrationChanged
	}

	if _, err := client.waitForLocalQueue(context.Background(), "client.db"); !errors.Is(err, stateDB.ErrMigrationChanged) {
		t.Fatalf("ожидалась постоянная ошибка миграции, получено %v", err)
	}
	if attempts != 1 {
		t.Fatalf("постоянную ошибку повторили %d раз", attempts)
	}
}

func TestClientEndsTransientStorageCycleForAutomaticRestart(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.storageRetry = time.Millisecond
	client.storageCycle = 3 * time.Millisecond
	attempts := 0
	client.storageOpener = func(context.Context, string) (localQueueHandle, error) {
		attempts++
		return localQueueHandle{}, errors.New("SQLITE_CANTOPEN (14)")
	}

	if _, err := client.waitForLocalQueue(context.Background(), "client.db"); !errors.Is(err, errLocalStorageRetryCycle) {
		t.Fatalf("ожидалось завершение цикла восстановления, получено %v", err)
	}
	if attempts < 2 {
		t.Fatalf("временное хранилище проверено только %d раз", attempts)
	}
}

func TestClientSupervisorRestartsOnlyTransientCycle(t *testing.T) {
	t.Parallel()
	if !shouldRestartClientCycle(nil, "waiting") {
		t.Fatal("временная недоступность EAVC не запускает автоматический повтор")
	}
	if shouldRestartClientCycle(nil, "error") {
		t.Fatal("постоянная ошибка не должна бесконечно перезапускаться")
	}
	if shouldRestartClientCycle(context.Canceled, "waiting") {
		t.Fatal("остановленный клиент не должен перезапускаться")
	}
}

func TestClientRequestsOneProcessRestartAfterStorageCycle(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	if !client.requestProcessRestart() {
		t.Fatal("first exhausted storage cycle did not request a process restart")
	}
	select {
	case <-client.ProcessRestartRequests():
	default:
		t.Fatal("process restart request was not published")
	}
	if client.requestProcessRestart() {
		t.Fatal("a second process restart was requested")
	}
	if got := client.Status().State; got != "restarting" {
		t.Fatalf("unexpected restart status: %q", got)
	}
}

func TestRecoveryProcessCannotRequestAnotherRestart(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.DisableProcessRestart()
	if client.requestProcessRestart() {
		t.Fatal("recovery process would create a restart loop")
	}
}
