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

func TestTopicRedirectScreenWiring(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	styles, err := assets.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	html, javascript := string(index), string(application)
	for _, id := range []string{
		"addTopicRedirect", "topicRedirectList",
		"topicRedirectForm", "publishTopicRedirects", "positionRedirectNotice",
	} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `$('`+id+`')`) {
			t.Errorf("раздел перенаправлений не подключает элемент %q", id)
		}
	}
	if !strings.Contains(html, `id="topicRedirects"`) || !strings.Contains(javascript, "topicRedirects: 'Перенаправление'") ||
		!strings.Contains(html, `id="topicRedirectDialog"`) || !strings.Contains(javascript, "openDialog('topicRedirectDialog')") {
		t.Error("отдельное меню и окно перенаправления не подключены")
	}
	for _, fragment := range []string{
		"rules.topic_redirects[key] = {destination_id: destinationID, thread_id: threadID, topic_name: topicName}",
		"delete rules.topic_redirects[key]",
		"await post('/api/rule-draft', rules)",
		"publishCurrentRules(event.currentTarget)",
		"eligibleTopicRedirects()",
		"topicOptionsFor(row)",
		"positionName(routeKey)",
		"form.elements.target_topic.selectedOptions[0]",
	} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("черновик перенаправления неполон: нет %q", fragment)
		}
	}
	if strings.Count(javascript, "initializeRuleMaps();") != 2 {
		t.Error("старый черновик без topic_redirects должен открываться и с обзора, и напрямую")
	}
	if !strings.Contains(javascript, "Будет опубликован весь общий черновик") {
		t.Error("подтверждение публикации не предупреждает о других изменениях маршрутов")
	}
	topicDialogStart := strings.Index(html, `<dialog id="topicRedirectDialog"`)
	if topicDialogStart < 0 {
		t.Fatal("модальное окно перенаправления не найдено")
	}
	topicDialogEnd := strings.Index(html[topicDialogStart:], `</dialog>`)
	if topicDialogEnd < 0 {
		t.Fatal("модальное окно перенаправления не закрыто")
	}
	topicDialog := html[topicDialogStart : topicDialogStart+topicDialogEnd]
	if !strings.Contains(topicDialog, `name="target_topic"`) || strings.Contains(topicDialog, `name="topic_name"`) || strings.Contains(topicDialog, `name="thread_id"`) {
		t.Error("форма перенаправления не должна требовать ручной ввод названия или ID темы")
	}
	if !strings.Contains(string(styles), ".topic-redirect-card") {
		t.Error("для списка перенаправлений нет отдельного оформления")
	}
	if strings.Contains(javascript, `${escapeHTML(option.topicName)} · тема`) || strings.Contains(javascript, `${escapeHTML(targetTopic)} · тема`) {
		t.Error("имя Telegram-темы не должно дублироваться технической подписью того же ID")
	}
}

func TestUnknownAliasScreenWiring(t *testing.T) {
	index, err := assets.ReadFile("assets/index.html")
	if err != nil {
		t.Fatal(err)
	}
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	html, javascript := string(index), string(application)
	for _, id := range []string{"addUnknownAlias", "unknownAliasList", "unknownAliasForm"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `$('`+id+`')`) {
			t.Errorf("нет связанного элемента %q", id)
		}
	}
	if !strings.Contains(html, `id="addUnknownAlias" class="button primary" type="button">Добавить перенаправление</button>`) {
		t.Fatal("кнопка неизвестного подразделения должна называть действие, а не добавление названия")
	}
	if !strings.Contains(html, `id="unknownAliasDialog"`) || !strings.Contains(javascript, "openDialog('unknownAliasDialog')") {
		t.Error("окно нового вида перенаправления не подключено")
	}
	for _, fragment := range []string{"rules.unknown_aliases ||= {}", "normalizedAliasKey(", "eligibleAliasTargets()", "delete rules.unknown_aliases[source]", "rules.unknown_aliases[source] = target"} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("привязка неизвестного названия не подключена: %q", fragment)
		}
	}
	if !strings.Contains(html, `>Перенаправление</button>`) {
		t.Error("пункт меню должен помещаться в одну строку")
	}
}

func TestWordTemplateDownloadUsesAuthenticatedAPI(t *testing.T) {
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
	if !strings.Contains(html, `<button id="downloadWordTemplate" type="button"`) || strings.Contains(html, `<a id="downloadWordTemplate"`) {
		t.Error("шаблон Word должен скачиваться кнопкой через авторизованный запрос, а не прямой ссылкой")
	}
	if strings.Contains(javascript, "download.href =") {
		t.Error("прямая ссылка на шаблон Word обходит заголовок доступа")
	}
	start := strings.Index(javascript, "$('downloadWordTemplate').addEventListener('click', async event => {")
	if start < 0 {
		t.Fatal("обработчик скачивания шаблона Word не найден")
	}
	end := strings.Index(javascript[start:], "\n});")
	if end < 0 {
		t.Fatal("не найден конец обработчика скачивания шаблона Word")
	}
	handler := javascript[start : start+end]
	for _, fragment := range []string{
		"if (!id) return;",
		"downloadAPI(`/api/destinations/${encodeURIComponent(id)}/word-template`",
		"notify(error.message)",
		"setButtonBusy(button, false)",
	} {
		if !strings.Contains(handler, fragment) {
			t.Errorf("авторизованное скачивание шаблона Word неполно: нет %q", fragment)
		}
	}
	if !strings.Contains(javascript, "headers: {'X-EAVC-Access': accessToken}") {
		t.Error("общая функция скачивания не передаёт заголовок доступа")
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
	for _, kind := range []string{"clients", "received", "pending", "delivered", "expired", "rejected", "transport"} {
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
	for _, id := range []string{"historyItemDialog", "historyItemMeta", "historyItemContent", "historyItemContentType", "historyTelegramResult", "historyTelegramLinks"} {
		if !strings.Contains(html, `id="`+id+`"`) || !strings.Contains(javascript, `'`+id+`'`) {
			t.Errorf("просмотр содержания перехвата не подключает элемент %q", id)
		}
	}
	for _, fragment := range []string{"telegram_message_ids", "telegram_message_urls", "Telegram message_id", "Открыть сообщение"} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("подробности доставки не показывают Telegram ID: нет %q", fragment)
		}
	}
	for _, fragment := range []string{"Код трассировки", "transport_message_id", "transportOutcomeLabel", "рабочие счётчики не изменил"} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("сквозная трассировка не показана: нет %q", fragment)
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
	if !strings.Contains(javascript, "Активные очереди и настройки сохранятся") {
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

func TestUnknownTelegramOutcomeHasManualVerificationWithoutInventedMessageID(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	styles, err := assets.ReadFile("assets/style.css")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	for _, fragment := range []string{
		"item.telegram_outcome_unknown === true",
		"!unknownOutcome",
		"item.telegram_blocks || []",
		"data-republish-unknown",
		"data-send-attempt-token",
		"Сверьте последнюю принятую Центром редакцию с EAVC",
		"Если спорная часть уже есть, не публикуйте её снова",
		"Проверил, части нет",
		"последней принятой им редакции EAVC",
		"подтверждённые ID",
		"block.unconfirmed_part",
		"block.expected_message_count",
		"block.observed_message_id",
		"block.observed_thread_id",
		"block.observed_chat_id",
		"data-observed-message-id",
		"confirmed.length === index - 1",
		"checked_current_revision: true",
		"confirmed_part_absent: true",
		"unconfirmed_part: unconfirmedPart",
		"send_attempt_token: button.dataset.sendAttemptToken",
		"Сведения о частях недоступны. Ручная публикация отключена",
		"Результат ищите в «Доставлено»",
	} {
		if !strings.Contains(javascript, fragment) {
			t.Errorf("неопределённая отправка Telegram не объяснена или небезопасно управляется: нет %q", fragment)
		}
	}
	if !strings.Contains(string(styles), ".telegram-uncertain") {
		t.Error("предупреждение об исходе Telegram не имеет заметного оформления")
	}
}

func TestClientShiftHistoryUsesAlignedOperatorLayout(t *testing.T) {
	application, err := assets.ReadFile("assets/app.js")
	if err != nil {
		t.Fatal(err)
	}
	styles, err := assets.ReadFile("assets/history.css")
	if err != nil {
		t.Fatal(err)
	}
	javascript := string(application)
	css := string(styles)
	for _, expected := range []string{
		`<div class="shift-history"><small>Смены</small>`,
		`class="shift-row"`,
		`class="shift-period"`,
		`client.recent_shifts || shiftHistory.filter(item => item.client_id === client.client_id)`,
		`const currentShift = clientShiftHistory.find(item => item.current) || clientShiftHistory[0];`,
		`const recentShifts = [currentShift, previousClosedShift].filter(Boolean);`,
		`'по н.в.'`,
		`${closed ? 'закрыта' : 'открыта'}`,
		`<dt>Статус связи</dt>`,
		`client.shift_read_state === 'error'`,
		`data.shift_warning`,
	} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("оформление истории смен неполно: нет %q", expected)
		}
	}
	if strings.Contains(javascript, "Последние смены без повторов") {
		t.Error("в интерфейсе осталось прежнее техническое название истории смен")
	}
	if strings.Contains(javascript, `class="shift-position"`) || strings.Contains(javascript, ".slice(0, 4)") {
		t.Error("карточка смены всё ещё дублирует название точки или показывает больше двух записей")
	}
	for _, expected := range []string{".shift-row {", ".shift-period {", ".shift-status.open", ".shift-status.closed"} {
		if !strings.Contains(css, expected) {
			t.Errorf("для выравнивания истории смен нет стиля %q", expected)
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
	for _, expected := range []string{"function appDialog", "const appConfirm", "showActivationCode", "navigator.clipboard.writeText", "dialog.showModal()"} {
		if !strings.Contains(javascript, expected) {
			t.Errorf("фирменное модальное окно неполно: нет %q", expected)
		}
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
		"publishButtons.forEach(item => { item.disabled = !rulesDirty; })",
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
