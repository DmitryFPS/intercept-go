package clientui

import (
	"strings"
	"testing"
)

func TestWaitingStateUsesConciseOperatorText(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	content := string(application)
	if !strings.Contains(content, "waiting: 'НЕТ СВЯЗИ С ЦЕНТРОМ'") {
		t.Error("состояние ожидания должно прямо сообщать об отсутствии связи с центром")
	}
	if strings.Contains(content, "ПОВТОРИМ АВТОМАТИЧЕСКИ") {
		t.Error("на операторском экране не должно быть лишней надписи о повторе")
	}
}

func TestOperatorActionsFollowProcessingState(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	content := string(application)
	checks := []string{
		"setDisabled('controlStart', operationBusy || !configured || processing)",
		"setDisabled('controlStop', operationBusy || !processing)",
		"setDisabled('controlRefresh', operationBusy || !configured || !processing)",
		"setDisabled('checkTransport', operationBusy || !configured || !processing)",
	}
	for _, expected := range checks {
		if !strings.Contains(content, expected) {
			t.Errorf("не зафиксирована логика доступности кнопки: %s", expected)
		}
	}

	styles, err := assets.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(styles), "cursor: not-allowed") {
		t.Error("заблокированная кнопка должна визуально отличаться от доступной")
	}
}

func TestCenterConfirmationIndicatorAcceptsTransportCheck(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	content := string(application)
	if !strings.Contains(content, "Boolean(status.last_acknowledged || status.last_transport_check)") {
		t.Error("индикатор подтверждения центра должен загораться после успешной проверки связи")
	}
}

func TestQueueHistoryExplainsContentAndRestoresClearButton(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	content := string(application)
	for _, expected := range []string{
		"item.content_kind === 'text'",
		"Текст перехвата отсутствует",
		"Код трассировки",
		"UUID временного блока",
		"Пакет, подтверждённый Центром",
		"Последняя загрузка Клиента в Telegram",
		"button.textContent = 'Очистка…'",
		"button.textContent = label",
	} {
		if !strings.Contains(content, expected) {
			t.Errorf("в истории очереди отсутствует защита интерфейса: %s", expected)
		}
	}
}

func TestBrowserDialogsAreReplacedWithBrandedModal(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	for _, native := range []string{"confirm(", "prompt(", "alert("} {
		if strings.Contains(javascript, native) {
			t.Errorf("остался системный диалог браузера: %s", native)
		}
	}
	for _, expected := range []string{"function appConfirm", "dialog.showModal()", "dialog.addEventListener('cancel'", "Очистить список?"} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("фирменное модальное окно неполно: нет %q", expected)
		}
	}
}
