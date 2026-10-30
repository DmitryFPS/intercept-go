package runner

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"intercept/internal/config"
	"intercept/internal/infrastructure/secrets"
	stateDB "intercept/internal/infrastructure/sqlite"
)

// Ручная публикация останавливает worker и затем запускает его обратно.
// Команда «Стоп», пришедшая в этот промежуток, должна выполняться после
// публикации, а не отменяться её отложенным запуском.
func TestStopDuringManualRepublishRemainsStopped(t *testing.T) {
	ctx := context.Background()
	baseDir := t.TempDir()
	db, err := stateDB.OpenState(filepath.Join(baseDir, "data", "server.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	store, err := stateDB.NewServerStore(ctx, db)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	server := &Server{
		BaseDir: baseDir, Version: "test", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Store: store, status: ServerStatus{State: "running", Processing: true},
	}
	defer server.Close()
	cfg := config.Server{
		Schema: config.ServerSchemaV1, Listen: "127.0.0.1:17632",
		Transport: config.ServerTransport{
			BotToken: "123456789:abcdefghijklmnopqrstuvwxyzABCDE", BotID: 123456789,
			BotUsername: "test_bot", ChatID: -1001,
		},
		Destinations: map[string]config.DestinationConfig{},
	}
	payload, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := secrets.Save(server.configPath(), payload); err != nil {
		t.Fatal(err)
	}
	canceledRoot, cancelRoot := context.WithCancel(ctx)
	cancelRoot() // Отложенный restart не должен обращаться к настоящему Telegram.
	workerStopped := make(chan struct{})
	done := make(chan struct{})
	close(done)
	server.mu.Lock()
	server.rootContext = canceledRoot
	server.workerStop = func() { close(workerStopped) }
	server.workerDone = done
	server.mu.Unlock()

	// Занятое единственное соединение удерживает ручной вызов внутри
	// CurrentDeliveryJobForRepublish после остановки worker. Так момент гонки
	// задаётся барьером, а не случайной задержкой Telegram или планировщика.
	connection, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	manualDone := make(chan error, 1)
	go func() {
		manualDone <- server.RepublishTelegramBlock(ctx, "missing-event", "telegram", "block-1", TelegramRepublishConfirmation{})
	}()
	select {
	case <-workerStopped:
	case <-time.After(5 * time.Second):
		t.Fatal("ручная публикация не остановила worker")
	}
	stopStarted := make(chan struct{})
	stopDone := make(chan struct{})
	go func() {
		close(stopStarted)
		server.StopProcessing()
		close(stopDone)
	}()
	<-stopStarted
	select {
	case <-stopDone:
		t.Fatal("команда «Стоп» обошла незавершённую ручную публикацию")
	case <-time.After(100 * time.Millisecond):
	}
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-manualDone:
		if err == nil {
			t.Fatal("несуществующий перехват ошибочно опубликован")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ручная публикация не завершилась после освобождения БД")
	}
	select {
	case <-stopDone:
	case <-time.After(5 * time.Second):
		t.Fatal("команда «Стоп» не завершилась после ручной публикации")
	}
	if status := server.Status(); status.Processing || status.State != "paused" {
		t.Fatalf("отложенный запуск отменил команду «Стоп»: %+v", status)
	}
}
