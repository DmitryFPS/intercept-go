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
