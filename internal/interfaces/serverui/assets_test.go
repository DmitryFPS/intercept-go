package serverui

import (
	"strings"
	"testing"
)

func TestRouteRecipientFilterAssetsStayConnected(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatalf("прочитать интерфейс: %v", err)
	}
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatalf("прочитать сценарий интерфейса: %v", err)
	}

	html := string(index)
	javascript := string(application)
	for _, id := range []string{"recipientFilter", "clearRouteFilters", "routeFilterCount"} {
		if !strings.Contains(html, `id="`+id+`"`) {
			t.Errorf("в интерфейсе отсутствует элемент %q", id)
		}
		if !strings.Contains(javascript, `$('`+id+`')`) {
			t.Errorf("элемент %q не подключён в сценарии", id)
		}
	}
	if !strings.Contains(javascript, "decision.action !== 'skip'") {
		t.Error("фильтр не отличает действующий маршрут от действия «Не отправлять»")
	}
}

func TestDashboardHistoryCardsStayConnected(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	html := string(index)
	javascript := string(application)
	for _, kind := range []string{"clients", "received", "pending", "delivered", "expired", "rejected"} {
		if !strings.Contains(html, `data-history="`+kind+`"`) {
			t.Errorf("карточка %q не открывает историю", kind)
		}
		if !strings.Contains(javascript, kind+": [") {
			t.Errorf("для карточки %q нет описания", kind)
		}
	}
	for _, id := range []string{"historyDialog", "historyList", "historyClear", "historyPrev", "historyNext"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `'`+id+`'`) {
			t.Errorf("элемент истории %q не подключён", id)
		}
	}
	if !strings.Contains(javascript, "Активные очереди и настройки не изменятся") {
		t.Error("интерфейс не объясняет безопасные границы очистки")
	}
}

func TestTelegramGuideStaysAvailableAndComplete(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	guide, err := assets.ReadFile("assets/telegram-guide.html")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := assets.ReadFile("assets/telegram-guide.css"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(index), `href="/telegram-guide.html"`) {
		t.Error("на странице связи отсутствует ссылка на полную Telegram-инструкцию")
	}
	content := string(guide)
	for _, fragment := range []string{
		"Bot-to-Bot Communication",
		"Group Privacy",
		"ID группы",
		"ID темы",
		"Ограничения Telegram",
		"https://core.telegram.org/bots/tutorial",
	} {
		if !strings.Contains(content, fragment) {
			t.Errorf("в Telegram-инструкции отсутствует обязательный раздел %q", fragment)
		}
	}
}
