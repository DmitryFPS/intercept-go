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
