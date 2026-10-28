package runner

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/application"
	"intercept/internal/domain"
	"intercept/internal/infrastructure/sqlite"
	"intercept/internal/protocol"
)

type savedQueueSender struct{ sent int }

func (sender *savedQueueSender) Send(context.Context, protocol.Envelope) (int64, error) {
	sender.sent++
	return int64(sender.sent), nil
}

func (*savedQueueSender) Acknowledgements(context.Context, int64) ([]application.Acknowledgement, int64, error) {
	return nil, 0, nil
}

// Холодный запуск без EAVC не должен блокировать пакеты, уже надёжно
// сохранённые в локальной очереди. Эта проверка намеренно не задаёт Source.
func TestSavedQueueDispatchDoesNotRequireEAVCSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	database, err := sqlite.OpenState(filepath.Join(t.TempDir(), "client.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := sqlite.NewClientQueue(ctx, database)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	event := domain.Event{
		ID: "saved-event", ClientID: "point-01", CreatedAt: now, ObservedAt: now,
		SourceAt: now, ExpiresAt: now.Add(time.Hour),
		Source:   domain.SourceRecord{UUID: "source-1", ID: 1, UpdatedAt: now.Format(time.RFC3339Nano)},
		Position: domain.Position{UnitName: "1 мб 155 омбр"}, Mode: domain.SourceModeNormal,
		Content: "сохранённый перехват", ContentDigestVersion: domain.ContentDigestVersionCurrent,
	}
	event.ContentDigest = event.CanonicalContentDigest()
	if inserted, err := queue.Enqueue(ctx, event); err != nil || !inserted {
		t.Fatalf("поставить событие в очередь: inserted=%v err=%v", inserted, err)
	}
	sender := &savedQueueSender{}
	service := &application.ClientService{
		ClientID: "point-01", Queue: queue, State: queue, Sender: sender,
		KeyID: "key-1", Key: make([]byte, 32), AttemptTimeout: time.Second, Now: time.Now,
	}
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.status.State = "waiting"
	client.status.Title = "Ожидаем базу EAVC"
	pending, pendingErr := queue.Pending(ctx, 4, time.Now().UTC())
	if pendingErr != nil || len(pending) != 1 {
		t.Fatalf("сохранённое событие не готово к отправке: pending=%#v err=%v", pending, pendingErr)
	}
	client.dispatchSavedQueue(ctx, service, queue)
	if sender.sent != 1 {
		t.Fatalf("сохранённая очередь не передана без EAVC: sent=%d status=%+v", sender.sent, client.Status())
	}
	if client.Status().DatabaseOK {
		t.Fatal("передача очереди не должна ложно объявлять EAVC доступной")
	}
}

func TestClientOperationalStatusRemovesWorkstationIdentity(t *testing.T) {
	client := NewClient(t.TempDir(), "test", slog.New(slog.NewTextHandler(io.Discard, nil)))
	client.status.ClientID = "stable-id"
	client.status.ClientName = "Текстиль"
	client.status.ClientNumber = "01"
	client.status.EAVCPath = `C:\sensitive\main.sqlite`
	status := client.OperationalStatus()
	if status.ClientID != "" || status.ClientName != "" || status.ClientNumber != "" {
		t.Fatalf("диагностика раскрывает идентичность точки: %+v", status)
	}
	if status.EAVCPath != "main.sqlite" {
		t.Fatalf("в диагностике должно остаться только имя базы: %q", status.EAVCPath)
	}
}
