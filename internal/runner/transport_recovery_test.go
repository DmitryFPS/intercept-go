package runner

import (
	"errors"
	"log/slog"
	"testing"
)

func TestClientTransportRecoveryClearsTemporaryWarning(t *testing.T) {
	client := &Client{status: ClientStatus{
		State: "waiting", Title: "Нет подтверждения центра", Detail: "сбой", TelegramOK: false,
	}}
	client.transportRecovered("Связь восстановлена.")
	status := client.Status()
	if status.State != "running" || status.Title != "Программа работает" || !status.TelegramOK {
		t.Fatalf("предупреждение клиента не снято: %#v", status)
	}
}

func TestServerTransportRecoveryClearsTemporaryWarning(t *testing.T) {
	server := &Server{status: ServerStatus{
		State: "waiting", Title: "Telegram временно недоступен", Detail: "сбой", TransportOK: false,
	}}
	server.transportRecovered()
	status := server.Status()
	if status.State != "running" || status.Title != "Центр работает" || !status.TransportOK {
		t.Fatalf("предупреждение центра не снято: %#v", status)
	}
}

func TestDeliveryWarningKeepsInboundTransportHealthy(t *testing.T) {
	t.Parallel()
	server := &Server{
		Logger: slog.Default(),
		status: ServerStatus{State: "running", Title: "Центр работает", TransportOK: true},
	}
	server.warnDelivery("Не удалось доставить результат", "Сохранено в очереди", errors.New("word unavailable"))
	status := server.Status()
	if !status.TransportOK {
		t.Fatal("сбой отдельного получателя не должен помечать входящий Telegram-транспорт недоступным")
	}
}

func TestTransportRecoveryDoesNotHideUnrelatedFailure(t *testing.T) {
	client := &Client{status: ClientStatus{
		State: "waiting", Title: "Не удалось прочитать EAVC", Detail: "сбой базы", TelegramOK: false,
	}}
	client.transportRecovered("Связь восстановлена.")
	status := client.Status()
	if status.Title != "Не удалось прочитать EAVC" || status.Detail != "сбой базы" {
		t.Fatalf("посторонняя ошибка была скрыта: %#v", status)
	}
}

func TestClientFailureDoesNotExposeTechnicalError(t *testing.T) {
	client := &Client{Logger: slog.Default()}
	client.fail("Не удалось подключиться", "Повторите попытку позже.", errors.New("deleteWebhook: TLS handshake timeout"))
	status := client.Status()
	if status.Detail != "Повторите попытку позже." {
		t.Fatalf("в интерфейс попала техническая ошибка: %#v", status)
	}
}

func TestServerFailureDoesNotExposeTechnicalError(t *testing.T) {
	server := &Server{Logger: slog.Default()}
	server.fail("Не удалось подключиться", "Повторите попытку позже.", errors.New("getUpdates: unexpected EOF"))
	status := server.Status()
	if status.Detail != "Повторите попытку позже." {
		t.Fatalf("в интерфейс попала техническая ошибка: %#v", status)
	}
}
