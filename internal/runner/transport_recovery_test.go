package runner

import "testing"

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
