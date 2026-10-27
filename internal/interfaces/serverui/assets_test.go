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
	for _, id := range []string{"historyItemDialog", "historyItemMeta", "historyItemContent", "historyItemContentType"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `'`+id+`'`) {
			t.Errorf("просмотр содержания перехвата не подключает элемент %q", id)
		}
	}
	for _, fragment := range []string{"item.content_ready", "historyContent(item)", "historyContentLabel(item)", "data-history-item", "Текст из EAVC", "Клиент передал перехват без текста"} {
		if !strings.Contains(javascript, fragment) && !strings.Contains(html, fragment) {
			t.Errorf("история не показывает фактическое содержание перехвата: нет %q", fragment)
		}
	}
	for _, id := range []string{"clientIncidentLauncher", "openClientIncidents", "clientIncidentDialog", "incidentList", "incidentClear"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `'`+id+`'`) {
			t.Errorf("отдельное окно истории связи не подключает элемент %q", id)
		}
	}
	if !strings.Contains(javascript, "$('historyDialog').close()") {
		t.Error("история сбоев должна открываться отдельно от текущего состояния точек")
	}
	if !strings.Contains(javascript, "Активные очереди и настройки не изменятся") {
		t.Error("интерфейс не объясняет безопасные границы очистки")
	}
	if strings.Contains(javascript, "`ID: ${item.id}`") {
		t.Error("операторский экран не должен подставлять технический UUID вместо полезных сведений")
	}
	for _, fragment := range []string{
		"client.id === historyFilters.client ? 'selected' : ''",
		"Object.entries(destinations).map(([id, destination])",
		"id === historyFilters.destination ? 'selected' : ''",
		"$('historySearch').value = historyFilters.q",
		"$('historyQualityFilter').value = historyFilters.quality",
	} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("повторное открытие истории сбрасывает применённый фильтр: нет %q", fragment)
		}
	}
	for _, fragment := range []string{
		"kind === 'rejected'",
		"['pending', 'delivered', 'expired'].includes(kind)",
		"kind !== 'clients'",
		"if (historyFilters.q) query.set('q', historyFilters.q)",
		"if (historyFilters.client) query.set('client', historyFilters.client)",
		"if (historyFilters.destination) query.set('destination', historyFilters.destination)",
		"if (historyFilters.quality) query.set('quality', historyFilters.quality)",
	} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("набор фильтров шести карточек неполон: нет %q", fragment)
		}
	}
	for _, fragment := range []string{"Запись EAVC №", "время перехвата", "Данные старше разрешённого срока доставки"} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("в истории отсутствует понятное оператору поле %q", fragment)
		}
	}
	if strings.Contains(javascript, "setButtonBusy(event.currentTarget, false)") {
		t.Error("после await event.currentTarget уже сброшен: кнопка очистки останется в состоянии «Очищаем…»")
	}
	if strings.Count(javascript, "const button = event.currentTarget;") < 2 ||
		strings.Count(javascript, "setButtonBusy(button, false);") < 2 {
		t.Error("обе кнопки очистки должны сохранять ссылку и восстанавливаться после запроса")
	}
}

func TestDashboardUsesOperationalSummaryWithoutTrainingCopy(t *testing.T) {
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
	for _, obsolete := range []string{
		"Что нужно сделать до первого теста",
		"Диагностика содержит безопасные журналы",
		"Остановка сохраняет настройки и очереди",
	} {
		if strings.Contains(html, obsolete) {
			t.Errorf("на обзор вернулся лишний обучающий текст %q", obsolete)
		}
	}
	for _, id := range []string{"readinessTitle", "readinessMeta", "serviceControlState"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `$('`+id+`')`) {
			t.Errorf("операционная сводка не подключает элемент %q", id)
		}
	}
}

func TestServiceActionsFollowProcessingState(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	for _, expected := range []string{
		"setControlDisabled('controlStart', serviceOperationBusy || !configured || processing)",
		"setControlDisabled('controlStop', serviceOperationBusy || !processing)",
		"setControlDisabled('controlRefresh', serviceOperationBusy || !configured || !processing)",
		"setControlDisabled('downloadDiagnostics', serviceOperationBusy)",
	} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("не зафиксирована логика доступности серверной кнопки: %s", expected)
		}
	}
	for _, expected := range []string{
		"$('clearRouteFilters').disabled = !query && !recipientID",
		"$('publishRules').disabled = !rulesDirty",
		"$('positionPrev').disabled = currentPositionPage <= 1",
		"$('positionNext').disabled = currentPositionPage >= pages",
		"button.disabled = !rulesDirty",
	} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("не зафиксирована логика вспомогательной серверной кнопки: %s", expected)
		}
	}
}

func TestUnknownRouteDialogDoesNotRequireHiddenPositionName(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	for _, expected := range []string{
		"form.elements.name.disabled = unknown",
		"form.elements.name.required = !unknown",
	} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("форма неопознанного маршрута оставляет скрытое обязательное поле: нет %q", expected)
		}
	}
}

func TestHistoryColumnLabelsMatchOperatorScreens(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	styles, err := assets.ReadFile("assets/history.css")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	if strings.Contains(javascript, "clients: ['Рабочая точка', 'Состояние', 'Начало сбоя']") {
		t.Error("для карточки рабочих точек не нужны заголовки колонок")
	}
	if !strings.Contains(javascript, "received: ['Подразделение', 'Рабочая точка', 'Получено центром']") {
		t.Error("в истории полученных пакетов отсутствуют понятные заголовки колонок")
	}
	css := string(styles)
	grid := "grid-template-columns: minmax(190px, 1.3fr) minmax(220px, 1fr) minmax(165px, .7fr);"
	if strings.Count(css, grid) != 2 {
		t.Error("заголовки и строки истории должны использовать одну и ту же сетку колонок")
	}
	if !strings.Contains(css, ".history-row time { text-align: right; white-space: nowrap; }") {
		t.Error("время истории должно быть выровнено по правой границе своей колонки")
	}
	if !strings.Contains(javascript, "incident-row") || !strings.Contains(javascript, "Начало сбоя") {
		t.Error("сбои рабочих точек должны быть отделены от карточек состояния понятной временной шкалой")
	}
}

func TestFullInstallationGuideReplacesTelegramGuideOnConnectionPage(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	page := string(index)
	if !strings.Contains(page, "Полная установка и настройка EAVC Connect") {
		t.Error("на странице связи отсутствует название полной инструкции")
	}
	if !strings.Contains(page, `href="/installation-guide/index.html"`) {
		t.Error("на странице связи отсутствует ссылка на полную инструкцию")
	}
	if strings.Contains(page, "Настройка Telegram с нуля") || strings.Contains(page, `href="/telegram-guide.html"`) {
		t.Error("старая отдельная Telegram-инструкция не должна оставаться в карточке раздела связи")
	}
	for _, name := range []string{"assets/full-guide.js", "assets/full-guide.css"} {
		content, err := assets.ReadFile(name)
		if err != nil {
			t.Fatalf("read full guide asset %s: %v", name, err)
		}
		if len(content) == 0 {
			t.Fatalf("full guide asset %s is empty", name)
		}
	}
}
