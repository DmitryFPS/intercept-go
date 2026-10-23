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
