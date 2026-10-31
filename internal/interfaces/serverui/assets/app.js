const $ = id => document.getElementById(id);
const hashAccess = new URLSearchParams(location.hash.slice(1)).get('access');
if (hashAccess) {
  sessionStorage.setItem('eavc_access', hashAccess);
  history.replaceState(null, '', location.pathname + location.search);
}
const accessToken = sessionStorage.getItem('eavc_access') || '';

const pageTitles = {
  dashboard: 'Обзор',
  transport: 'Связь с рабочими точками',
  destinations: 'Получатели',
  clients: 'Рабочие точки',
  routes: 'Маршруты',
  topicRedirects: 'Перенаправление тем'
};

const pageContexts = {
  dashboard: 'СОСТОЯНИЕ СИСТЕМЫ',
  transport: 'ШАГ 1 · НАСТРОЙКА СИСТЕМЫ',
  destinations: 'ШАГ 2 · НАСТРОЙКА СИСТЕМЫ',
  clients: 'ШАГ 3 · НАСТРОЙКА СИСТЕМЫ',
  routes: 'ШАГ 4 · НАСТРОЙКА СИСТЕМЫ',
  topicRedirects: 'УПРАВЛЕНИЕ МАРШРУТАМИ'
};

let status = null;
let destinations = {};
let clients = [];
let rules = null;
let rulesDirty = false;
let currentPositionPage = 1;
let ruleHistory = [];
let ruleHistoryExpanded = false;
let templateDefaults = { telegram: '', telegram_markers: [], word_path: '', word_markers: [] };
const positionsPerPage = 12;
const visibleRuleVersions = 5;
const historyPageSize = 30;
let historyKind = '';
let historyOffset = 0;
let historyData = null;
let historyFilters = { q: '', client: '', destination: '', quality: '' };
let incidentOffset = 0;
let incidentData = null;
let incidentFilters = { q: '', client: '' };
let serviceOperationBusy = false;

function appDialog({ title, message, confirmLabel = 'Продолжить', danger = false, code = '' }) {
  return new Promise(resolve => {
    const dialog = document.createElement('dialog');
    dialog.className = 'app-confirm';
    dialog.innerHTML = `
      <form method="dialog" class="app-confirm-card">
        <div class="app-confirm-icon ${danger ? 'danger' : ''}" aria-hidden="true">${danger ? '!' : '✓'}</div>
        <div><h2></h2><p></p><div class="app-confirm-code hidden"><input readonly><button type="button" class="button ghost">Скопировать</button></div></div>
        <div class="app-confirm-actions">
          <button class="button ghost cancel" value="cancel">Отмена</button>
          <button class="button primary ${danger ? 'danger' : ''}" value="confirm"></button>
        </div>
      </form>`;
    dialog.querySelector('h2').textContent = title;
    dialog.querySelector('p').textContent = message;
    dialog.querySelector('[value="confirm"]').textContent = confirmLabel;
    if (code) {
      const codeBox = dialog.querySelector('.app-confirm-code');
      const input = codeBox.querySelector('input');
      codeBox.classList.remove('hidden');
      dialog.querySelector('.cancel').classList.add('hidden');
      input.value = code;
      codeBox.querySelector('button').addEventListener('click', async event => {
        try {
          await navigator.clipboard.writeText(code);
          event.currentTarget.textContent = 'Скопировано';
        } catch (_) {
          input.select();
          document.execCommand('copy');
          event.currentTarget.textContent = 'Скопировано';
        }
      });
    }
    let finished = false;
    const finish = answer => {
      if (finished) return;
      finished = true;
      dialog.remove();
      resolve(answer);
    };
    dialog.addEventListener('cancel', event => { event.preventDefault(); finish(false); });
    dialog.addEventListener('close', () => finish(dialog.returnValue === 'confirm'));
    document.body.append(dialog);
    dialog.showModal();
    dialog.querySelector(code ? '[value="confirm"]' : '[value="cancel"]').focus();
  });
}

const appConfirm = options => appDialog(options);
const showActivationCode = code => appDialog({
  title: 'Код активации',
  message: 'Сохраните код отдельно. Он понадобится один раз при первом запуске рабочей точки.',
  confirmLabel: 'Готово', code
});

const historySections = {
  clients: ['Рабочие точки', 'Текущее качество связи и последние сигналы от каждой рабочей точки.'],
  received: ['Получено', 'События, которые центр надёжно принял от рабочих точек.'],
  pending: ['Ожидает отправки', 'Живая очередь с попытками, ошибкой и временем следующего повтора.'],
  delivered: ['Доставлено', 'Успешные доставки по каждому Telegram- и Word-направлению.'],
  expired: ['Просрочено', 'Задания, которые не удалось доставить в разрешённое время.'],
  rejected: ['Отклонено', 'Пакеты, которые центр не принял, с безопасным описанием причины.'],
  transport: ['Журнал транспорта', 'Все замеченные в группе обмена сообщения: перехваты, проверки связи и то, что не повлияло на рабочие счётчики.']
};

const historyColumnLabels = {
  received: ['Подразделение', 'Рабочая точка', 'Получено центром'],
  pending: ['Подразделение', 'Рабочая точка → получатель', 'Принято центром'],
  delivered: ['Подразделение', 'Маршрут доставки', 'Доставлено'],
  expired: ['Подразделение', 'Маршрут доставки', 'Просрочено'],
  rejected: ['Отправитель', 'Тема', 'Отклонено'],
  transport: ['Источник', 'Результат классификации', 'Замечено']
};

function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[char]);
}

function formatDate(value) {
  return value ? new Date(value).toLocaleString('ru-RU') : 'Ещё не было';
}

async function request(url, options = {}) {
  options.headers = {...(options.headers || {}), 'X-EAVC-Access': accessToken};
  const response = await fetch(url, { cache: 'no-store', ...options });
  const type = response.headers.get('content-type') || '';
  const payload = type.includes('application/json') ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(payload?.error || payload || 'Не удалось выполнить действие');
  }
  return payload;
}

async function downloadAPI(url, fallbackName) {
  const response = await fetch(url, {cache: 'no-store', headers: {'X-EAVC-Access': accessToken}});
  if (!response.ok) throw new Error('Не удалось подготовить файл.');
	const activationCode = response.headers.get('X-EAVC-Activation-Code') || '';
  const blob = await response.blob();
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = fallbackName;
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
	if (activationCode) {
		await showActivationCode(activationCode);
	}
}

function post(url, payload) {
  return request(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
}

function notify(text, ok = false) {
  const element = $('globalMessage');
  element.textContent = text;
  element.className = `toast show${ok ? ' ok' : ''}`;
  clearTimeout(notify.timer);
  notify.timer = setTimeout(() => element.classList.remove('show'), 6000);
}

function setButtonBusy(button, busy, busyText = 'Проверяем…') {
  if (!button) return;
  if (busy) {
    button.dataset.label = button.textContent;
    button.textContent = busyText;
    button.disabled = true;
  } else {
    button.textContent = button.dataset.label || button.textContent;
    button.disabled = false;
  }
}

function setControlDisabled(id, disabled) {
  const control = $(id);
  control.disabled = disabled;
  control.setAttribute('aria-disabled', String(disabled));
}

function syncServiceActions(current = status) {
  if (!current) return;
  const configured = Boolean(current.configured);
  const processing = Boolean(current.processing);
  setControlDisabled('controlStart', serviceOperationBusy || !configured || processing);
  setControlDisabled('controlStop', serviceOperationBusy || !processing);
  setControlDisabled('controlRefresh', serviceOperationBusy || !configured || !processing);
  setControlDisabled('downloadDiagnostics', serviceOperationBusy);
  $('updateFile').disabled = serviceOperationBusy;
  $('updateUpload').classList.toggle('disabled', serviceOperationBusy);
  $('updateUpload').setAttribute('aria-disabled', String(serviceOperationBusy));
}

function setServiceOperationBusy(busy) {
  serviceOperationBusy = busy;
  syncServiceActions();
}

function openDialog(id) {
  const dialog = $(id);
  if (!dialog.open) dialog.showModal();
}

function closeDialog(id) {
  const dialog = $(id);
  if (dialog?.open) dialog.close();
}

document.querySelectorAll('[data-close]').forEach(button => {
  button.addEventListener('click', () => closeDialog(button.dataset.close));
});

document.querySelectorAll('.modal').forEach(dialog => {
  dialog.addEventListener('click', event => {
    if (event.target === dialog) dialog.close();
  });
});

document.querySelectorAll('.sidebar nav button').forEach(button => {
  button.addEventListener('click', async () => {
    document.querySelectorAll('.sidebar nav button,.page').forEach(item => item.classList.remove('active'));
    button.classList.add('active');
    $(button.dataset.page).classList.add('active');
    $('pageTitle').textContent = pageTitles[button.dataset.page];
    $('pageContext').textContent = pageContexts[button.dataset.page];
    try {
      if (button.dataset.page === 'dashboard') await loadOverview();
      if (button.dataset.page === 'destinations') await loadDestinations();
      if (button.dataset.page === 'clients') await loadClients();
      if (button.dataset.page === 'routes' || button.dataset.page === 'topicRedirects') {
        if (rulesDirty) {
          renderRules();
          await loadRuleHistory();
        } else {
          await loadRules();
        }
      }
    } catch (error) {
      notify(error.message);
    }
  });
});

async function refreshStatus() {
  status = await request('/api/status');
  const stateClass = ['running', 'error', 'waiting'].includes(status.state) ? status.state : 'waiting';
  $('hero').className = `hero ${stateClass}`;
  $('stateTitle').textContent = status.title;
  $('stateDetail').textContent = status.detail;
  $('version').textContent = status.version ? `Версия ${status.version}` : '';
  $('sidebarState').textContent = status.transport_ok ? 'Система работает' : (status.configured ? 'Нужна проверка' : 'Не настроено');
  $('sidebarDot').style.background = status.transport_ok ? '#16a565' : (status.configured ? '#dd941a' : '#8190a5');
  $('transportBadge').textContent = status.transport_ok ? 'Связь работает' : (status.configured ? 'Настроено, нужна связь' : 'Не настроено');
  $('transportBadge').className = `badge ${status.transport_ok ? 'success' : (status.configured ? 'warning' : 'neutral')}`;
  syncServiceActions(status);
  $('serviceControlState').textContent = !status.configured
    ? 'Настройка не завершена'
    : (status.processing ? 'Центр запущен' : 'Центр остановлен');

  const stats = status.stats || {};
  const workingClients = (stats.clients_online || 0) + (stats.clients_degraded || 0);
  $('statClients').textContent = `${workingClients}/${stats.clients || 0}`;
  $('statEvents').textContent = stats.events || 0;
  $('statPending').textContent = stats.pending || 0;
  $('statDelivered').textContent = stats.delivered || 0;
  $('statExpired').textContent = stats.expired || 0;
  $('statRejected').textContent = stats.rejected || 0;
  $('lastReceived').textContent = formatDate(status.last_received);
  $('lastDelivery').textContent = formatDate(status.last_delivery);
}

async function controlProcessing(action, button, busyText) {
  const label = button.textContent;
  setServiceOperationBusy(true);
  button.textContent = busyText;
  try {
    await post(`/api/control/${action}`, {});
    await refreshStatus();
    notify(action === 'stop' ? 'Обработка остановлена. Очереди сохранены.' : 'Команда выполнена.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    button.textContent = label;
    setServiceOperationBusy(false);
  }
}

$('controlStart').addEventListener('click', event => controlProcessing('start', event.currentTarget, 'Запускаем…'));
$('controlStop').addEventListener('click', event => controlProcessing('stop', event.currentTarget, 'Останавливаем…'));
$('controlRefresh').addEventListener('click', event => controlProcessing('refresh', event.currentTarget, 'Обновляем…'));
$('downloadDiagnostics').addEventListener('click', async () => {
  setServiceOperationBusy(true);
  try { await downloadAPI('/api/diagnostics', 'EAVC-Connect-Center-diagnostics.zip'); }
  catch (error) { notify(error.message); }
  finally { setServiceOperationBusy(false); }
});

$('updateFile').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  if (!await appConfirm({ title: 'Установить обновление?', message: 'Центр проверит обновление, перезапустится и вернёт предыдущую версию, если проверка не пройдёт.', confirmLabel: 'Установить' })) {
    event.target.value = '';
    return;
  }
  const form = new FormData();
  form.append('update', file);
  setServiceOperationBusy(true);
  $('updateResult').textContent = 'Проверяем пакет обновления…';
  try {
    const result = await request('/api/update', {method: 'POST', body: form});
    $('updateResult').textContent = result.message;
    notify(result.message, true);
  } catch (error) {
    $('updateResult').textContent = error.message;
    notify(error.message);
  } finally {
    event.target.value = '';
    setServiceOperationBusy(false);
  }
});

document.querySelectorAll('[data-history]').forEach(button => {
  button.addEventListener('click', () => openHistory(button.dataset.history));
});

function resetHistoryFilters() {
  historyFilters = { q: '', client: '', destination: '', quality: '' };
  $('historySearch').value = '';
  $('historyClientFilter').value = '';
  $('historyDestinationFilter').value = '';
  $('historyQualityFilter').value = '';
}

function prepareHistoryFilters(kind) {
  $('historyClientFilter').innerHTML = '<option value="">Все точки</option>' + clients.map(client =>
    `<option value="${escapeHTML(client.id)}" ${client.id === historyFilters.client ? 'selected' : ''}>${escapeHTML(client.number)} · ${escapeHTML(client.name)}</option>`
  ).join('');
  $('historyDestinationFilter').innerHTML = '<option value="">Все получатели</option>' + Object.entries(destinations).map(([id, destination]) =>
    `<option value="${escapeHTML(id)}" ${id === historyFilters.destination ? 'selected' : ''}>${escapeHTML(destination.name)}</option>`
  ).join('');
  $('historySearch').value = historyFilters.q;
  $('historyQualityFilter').value = historyFilters.quality;
  $('historyClientField').classList.toggle('hidden', kind === 'rejected');
  $('historyDestinationField').classList.toggle('hidden', !['pending', 'delivered', 'expired'].includes(kind));
  $('historyQualityField').classList.toggle('hidden', kind !== 'clients');
}

function historyRequestURL(kind, offset) {
  const query = new URLSearchParams({ limit: String(historyPageSize), offset: String(offset) });
  if (historyFilters.q) query.set('q', historyFilters.q);
  if (historyFilters.client) query.set('client', historyFilters.client);
  if (historyFilters.destination) query.set('destination', historyFilters.destination);
  if (historyFilters.quality) query.set('quality', historyFilters.quality);
  return `/api/history/${encodeURIComponent(kind)}?${query}`;
}

async function openHistory(kind, offset = 0, keepFilters = false) {
  if (!keepFilters || kind !== historyKind) resetHistoryFilters();
  historyKind = kind;
  historyOffset = Math.max(0, offset);
  const section = historySections[kind];
  if (!section) return;
  $('historyTitle').textContent = section[0];
  $('historyDescription').textContent = section[1];
  prepareHistoryFilters(kind);
  openDialog('historyDialog');
  $('historyList').innerHTML = '<div class="history-empty">Загружаем данные…</div>';
  $('clientHealthList').classList.add('hidden');
  try {
    historyData = await request(historyRequestURL(kind, historyOffset));
    renderHistory();
  } catch (error) {
    $('historyList').innerHTML = '';
    $('historyEmpty').textContent = error.message;
    $('historyEmpty').classList.remove('hidden');
  }
}

function incidentRequestURL(offset) {
  const query = new URLSearchParams({ limit: String(historyPageSize), offset: String(offset) });
  if (incidentFilters.q) query.set('q', incidentFilters.q);
  if (incidentFilters.client) query.set('client', incidentFilters.client);
  return `/api/history/clients?${query}`;
}

async function openClientIncidents(offset = 0, keepFilters = false) {
  if (!keepFilters) {
    incidentFilters = { q: '', client: '' };
    $('incidentSearch').value = '';
    $('incidentClientFilter').value = '';
  }
  incidentOffset = Math.max(0, offset);
  $('incidentClientFilter').innerHTML = '<option value="">Все точки</option>' + clients.map(client =>
    `<option value="${escapeHTML(client.id)}" ${client.id === incidentFilters.client ? 'selected' : ''}>${escapeHTML(client.number)} · ${escapeHTML(client.name)}</option>`
  ).join('');
  openDialog('clientIncidentDialog');
  $('incidentList').innerHTML = '<div class="history-empty">Загружаем историю…</div>';
  $('incidentEmpty').classList.add('hidden');
  try {
    incidentData = await request(incidentRequestURL(incidentOffset));
    renderClientIncidents();
  } catch (error) {
    $('incidentList').innerHTML = '';
    $('incidentEmpty').textContent = error.message;
    $('incidentEmpty').classList.remove('hidden');
  }
}

function renderClientIncidents() {
  const data = incidentData || {};
  const incidents = data.incidents || [];
  $('incidentList').innerHTML = incidents.map(item => `
    <article class="incident-row ${item.recovered_at ? 'recovered' : 'active'}">
      <span class="incident-marker" aria-hidden="true"></span>
      <div class="incident-main"><strong>${escapeHTML(item.client_name)}</strong><p>${escapeHTML(item.reason)}</p></div>
      <time><span>Начало сбоя</span>${formatDate(item.started_at)}</time>
      <div class="incident-state"><b>${item.recovered_at ? 'Связь восстановлена' : 'Сбой продолжается'}</b><span>${item.recovered_at ? `Восстановлено ${formatDate(item.recovered_at)}` : 'Ожидается восстановление'}</span></div>
    </article>`).join('');
  $('incidentEmpty').textContent = 'Сбоев и восстановлений пока не зафиксировано.';
  $('incidentEmpty').classList.toggle('hidden', incidents.length > 0);
  $('incidentFilteredCount').textContent = `Найдено: ${data.total || 0}`;
  $('incidentRetention').textContent = `Автоочистка: ${data.retention_days || 90} дней.`;
  const pageNumber = Math.floor(incidentOffset / historyPageSize) + 1;
  const pageCount = Math.max(1, Math.ceil((data.total || 0) / historyPageSize));
  $('incidentPage').textContent = `${pageNumber} из ${pageCount}`;
  $('incidentPrev').disabled = incidentOffset === 0;
  $('incidentNext').disabled = incidentOffset + historyPageSize >= (data.total || 0);
}

function qualityLabel(client) {
  if (client.quality === 'good') return 'Связь хорошая';
  if (client.quality === 'degraded') return 'Связь нестабильна';
  if (client.quality === 'offline') return 'Нет связи';
  return 'Ожидаем первый сигнал';
}

function formatDelay(seconds) {
  if (!Number.isFinite(seconds) || seconds < 1) return 'только что';
  if (seconds < 60) return `${seconds} сек. назад`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)} мин. назад`;
  return `${Math.floor(seconds / 3600)} ч. назад`;
}

function historyStateLabel(state) {
  return ({
    received: 'Принято центром',
    pending: 'Ожидает отправки',
    delivered: 'Доставлено',
    expired: 'Срок доставки истёк',
    rejected: 'Отклонено центром',
    transport: 'Сообщение транспорта классифицировано'
  })[state] || 'Состояние обновлено';
}

function transportOutcomeLabel(value) {
  return ({
    event_accepted: 'Новый перехват принят и учтён',
    processed_duplicate: 'Повтор: уже учтён ранее',
    event_expired: 'Перехват поступил после допустимого срока',
    probe_valid: 'Проверка связи выполнена, в счётчики не входит',
    probe_rejected: 'Проверка связи отклонена',
    ignored_non_transport: 'Сообщение пользователя или посторонний файл — не учитывается',
    ignored_unauthorized: 'Незарегистрированный бот или тема — не учитывается',
    ignored_invalid_size: 'Недопустимый размер файла — не учитывается',
    ignored_missing_sender: 'Отправитель не определён — не учитывается',
    unavailable_file: 'Файл недоступен; ожидается безопасный повтор Клиента',
    download_retry: 'Временный сбой скачивания; будет повторено',
    authorization_retry: 'Временный сбой проверки; будет повторено',
    processing_retry: 'Временный сбой обработки; будет повторено',
    quarantined: 'Повреждённый пакет помещён в карантин',
    invalid_probe: 'Повреждённая проверка связи отклонена',
    awaiting_processing: 'Ожидает классификации'
  })[value] || String(value || 'Неизвестный результат').replaceAll('_', ' ');
}

function historyReasonLabel(reason) {
  const value = String(reason || '').trim();
  if (!value) return '';
  const known = {
    older_than_delivery_window: 'Данные старше разрешённого срока доставки (30 минут)',
    invalid_signature: 'Не удалось подтвердить подлинность пакета',
    unknown_client: 'Рабочая точка не зарегистрирована в центре',
    invalid_payload: 'Получены повреждённые или неполные данные',
    duplicate_conflict: 'Повторный пакет отличается от уже принятого'
  };
  return known[value] || value.replaceAll('_', ' ');
}

function historyDetails(item) {
  const details = [];
  if (item.telegram_outcome_unknown) details.push(item.state === 'pending' ? 'исход отправки в Telegram уточняется' : 'исход отправки в Telegram неизвестен — нужна ручная проверка');
  if (item.state === 'transport') {
    details.push(transportOutcomeLabel(item.transport_outcome));
    details.push(item.counted ? 'увеличил «Получено»' : 'рабочие счётчики не изменил');
    if (item.transport_seen > 1) details.push(`Telegram update замечен повторно: ${item.transport_seen}`);
  }
  if (item.source_record) details.push(`Запись EAVC № ${item.source_record}`);
  if (item.source_at) details.push(`время перехвата: ${formatDate(item.source_at)}`);
  if (item.source_mode === 'deferred') details.push('отложенный режим');
  if (item.attempts) details.push(`попыток отправки: ${item.attempts}`);
  if (item.next_attempt_at && item.state === 'pending') details.push(`следующая попытка: ${formatDate(item.next_attempt_at)}`);
  const reason = historyReasonLabel(item.reason);
  if (reason) details.push(`причина: ${reason}`);
  if (!details.length) details.push(historyStateLabel(item.state));
  return details.join(' · ');
}

function historyContent(item) {
  if (item.content_kind === 'transport') return item.content_note || item.reason || 'Техническая классификация транспорта.';
  if (!item.content_ready) {
    return item.content_note || 'Содержимое этого пакета нельзя безопасно показать.';
  }
  if (item.content_kind === 'empty_text') {
    return item.content_note || 'Клиент передал перехват без текста. Вложений в этом событии нет.';
  }
  if (item.content_kind === 'deleted_block') {
    return item.content_note || 'Временной блок удалён в EAVC.';
  }
  return item.content;
}

function historyContentLabel(item) {
  if (item.content_kind === 'transport') return 'Почему сообщение учтено или проигнорировано';
  if (item.content_kind === 'text') return 'Клиент передал текст';
  if (item.content_kind === 'empty_text') return 'Клиент передал перехват без текста';
  if (item.content_kind === 'deleted_block') return 'Клиент передал удаление временного блока';
  return 'Содержимое не подтверждено';
}

function historyMetaEntry(label, value) {
  if (value === undefined || value === null || value === '') return '';
  return `<div><dt>${escapeHTML(label)}</dt><dd>${escapeHTML(value)}</dd></div>`;
}

function openHistoryItem(index) {
  const item = (historyData?.items || [])[index];
  if (!item) return;
  $('historyItemTitle').textContent = item.unit_name || item.client_name || 'Отклонённый пакет';
  $('historyItemState').textContent = `${item.telegram_outcome_unknown ? (item.state === 'pending' ? 'Исход публикации в Telegram уточняется' : 'Публикация в Telegram требует проверки') : historyStateLabel(item.state)} · ${formatDate(item.occurred_at)}`;
  const route = [item.client_name, item.destination].filter(Boolean).join(' → ');
  $('historyItemMeta').innerHTML = [
    historyMetaEntry('Рабочая точка', item.client_name),
    historyMetaEntry('Маршрут', route),
    historyMetaEntry('Подразделение', item.unit_name),
    historyMetaEntry('Частота', item.frequency),
    historyMetaEntry('Группа', item.group_code),
    historyMetaEntry('Время перехвата', item.source_at ? formatDate(item.source_at) : ''),
    historyMetaEntry('Принято / обработано', formatDate(item.occurred_at)),
    historyMetaEntry('Запись EAVC', item.source_record ? `№ ${item.source_record}` : ''),
    historyMetaEntry('UUID записи EAVC', item.source_uuid),
    historyMetaEntry('UUID временных блоков', (item.block_uuids || []).join(', ')),
    historyMetaEntry('Версия записи EAVC', item.source_version),
    historyMetaEntry('Отпечаток содержимого', item.content_digest),
    historyMetaEntry('Режим', item.source_mode === 'deferred' ? 'Отложенный' : (item.source_mode ? 'Обычный' : '')),
    historyMetaEntry('Изменил в EAVC', item.updated_by),
    historyMetaEntry('Попытки доставки', item.attempts || ''),
    historyMetaEntry('Причина', historyReasonLabel(item.reason)),
    historyMetaEntry('Что передал Клиент', historyContentLabel(item)),
    historyMetaEntry('ID Telegram-темы', item.telegram_thread_id || ''),
    historyMetaEntry('Telegram message_id', (item.telegram_message_ids || []).join(', ')),
    historyMetaEntry('Входящий Telegram message_id', item.transport_message_id || ''),
    historyMetaEntry('Telegram update_id', item.transport_update_id || ''),
    historyMetaEntry('Тип транспортного сообщения', item.transport_kind),
    historyMetaEntry('Результат транспорта', item.transport_outcome ? transportOutcomeLabel(item.transport_outcome) : ''),
    historyMetaEntry('Влияние на «Получено»', item.transport_outcome ? (item.counted ? 'Да — создан новый перехват' : 'Нет') : ''),
    historyMetaEntry('Код трассировки', item.id)
  ].join('');
  const contentSection = $('historyItemContent').closest('.history-item-content');
  contentSection.classList.toggle('unavailable', !item.content_ready);
  $('historyItemContentTitle').textContent = item.content_kind === 'transport' ? 'Классификация сообщения' : (item.content_ready ? 'Текст из EAVC' : 'Почему содержимое недоступно');
  $('historyItemContentType').textContent = item.content_kind === 'text' ? 'Текст' : (item.content_kind === 'empty_text' ? 'Без текста' : (item.content_kind === 'transport' ? 'Транспорт' : 'Недоступно'));
  $('historyItemContent').textContent = historyContent(item);
  const telegramURLs = item.telegram_message_urls || [];
  const telegramIDs = item.telegram_message_ids || [];
  const receiptBlocks = item.telegram_blocks || [];
  const isUnknownBlock = block => block.unknown_outcome === true || String(block.suppressed_reason || '').includes('неизвестен');
  const unknownBlocks = receiptBlocks.filter(isUnknownBlock);
  const manualBlocks = receiptBlocks.filter(block => block.manual_republish_pending === true);
  const suppressedBlocks = item.state === 'pending' ? [] : receiptBlocks.filter(block => block.suppressed && !isUnknownBlock(block) && !block.manual_republish_pending);
  const unknownOutcome = item.telegram_outcome_unknown === true || unknownBlocks.length > 0;
  $('historyTelegramResult').classList.toggle('hidden', telegramIDs.length === 0 && suppressedBlocks.length === 0 && manualBlocks.length === 0 && !unknownOutcome);
  $('historyTelegramLinks').innerHTML = telegramIDs.map((messageID, messageIndex) => {
    const url = telegramURLs[messageIndex];
    return url
      ? `<a class="button secondary" href="${escapeHTML(url)}" target="_blank" rel="noreferrer">Открыть сообщение ${escapeHTML(messageID)}</a>`
      : `<span class="badge">message_id ${escapeHTML(messageID)}</span>`;
  }).join('') + (unknownOutcome ? `
    <div class="telegram-uncertain">
      <strong>${item.state === 'pending' ? 'Исход отправки уточняется' : 'Исход публикации неизвестен'}</strong>
      <span>${item.state === 'pending'
        ? 'Telegram мог создать сообщение, но Центр пока не получил подтверждённый message_id. Дождитесь завершения проверки Центром; ручная повторная публикация сейчас недоступна.'
        : 'Telegram мог создать спорную часть сообщения, но Центр не получил её подтверждённый message_id. Сверьте последнюю принятую Центром редакцию с EAVC и проверьте нужную тему. Если новая редакция ещё в пути, дождитесь её. Автоматический повтор остановлен. Если спорная часть уже есть, не публикуйте её снова; автоматической привязки ID нет.'}</span>
      ${unknownBlocks.length === 0 ? '<span>Сведения о частях недоступны. Ручная публикация отключена до разбора состояния.</span>' : ''}
    </div>` : '') + unknownBlocks.map(block => {
    const confirmed = (block.message_ids || []).filter(id => Number.isSafeInteger(id) && id > 0);
    const index = Number(block.unconfirmed_part || 0);
    const total = Number(block.expected_message_count || 0);
    const sendAttemptToken = String(block.send_attempt_token || '');
    const canRepublish = item.state === 'expired' && index > 0 && total >= index && confirmed.length === index - 1 && sendAttemptToken !== '';
    return `
    <div class="telegram-suppressed">
      <span>Неизвестен исход части ${escapeHTML(index || '?')} из ${escapeHTML(total || '?')} · UUID ${escapeHTML(block.block_uuid)}${confirmed.length ? ` · подтверждённые ID: ${confirmed.map(escapeHTML).join(', ')}` : ''}</span>
      ${sendAttemptToken === '' ? '<span>Маркер попытки недоступен. Ручная публикация отключена до разбора состояния.</span>' : ''}
      ${canRepublish ? `<button class="button danger" data-republish-block="${escapeHTML(block.block_uuid)}" data-republish-unknown="true" data-unconfirmed-part="${escapeHTML(index)}" data-send-attempt-token="${escapeHTML(sendAttemptToken)}" data-event-id="${escapeHTML(item.id)}" data-destination-id="${escapeHTML(item.destination_id)}">После проверки опубликовать</button>` : ''}
    </div>`;
  }).join('') + manualBlocks.map(block => `
    <div class="telegram-suppressed">
      <span>Ручное разрешение сохранено · UUID ${escapeHTML(block.block_uuid)}. Публикация не завершена; автоматический повтор остановлен.</span>
      ${item.state === 'expired' && block.send_attempt_token ? `<button class="button danger" data-republish-block="${escapeHTML(block.block_uuid)}" data-republish-manual="true" data-send-attempt-token="${escapeHTML(block.send_attempt_token)}" data-event-id="${escapeHTML(item.id)}" data-destination-id="${escapeHTML(item.destination_id)}">Продолжить вручную</button>` : ''}
    </div>`).join('') + suppressedBlocks.map(block => `
    <div class="telegram-suppressed">
      <span>Сообщение удалено в Telegram · UUID ${escapeHTML(block.block_uuid)}</span>
      <button class="button danger" data-republish-block="${escapeHTML(block.block_uuid)}" data-event-id="${escapeHTML(item.id)}" data-destination-id="${escapeHTML(item.destination_id)}">Опубликовать снова</button>
    </div>`).join('');
  openDialog('historyItemDialog');
}

$('historyTelegramLinks').addEventListener('click', async event => {
  const button = event.target.closest('[data-republish-block]');
  if (!button) return;
  const unknownOutcome = button.dataset.republishUnknown === 'true';
  const manualResume = button.dataset.republishManual === 'true';
  let unconfirmedPart = 0;
  if (unknownOutcome) {
    const part = Number(button.dataset.unconfirmedPart);
    if (!await appConfirm({ title: 'Сначала проверьте тему Telegram', message: `Проверьте отсутствие именно спорной части ${part} перехвата. Ранее подтверждённые части могут оставаться в теме. Сверьте последнюю принятую Центром редакцию с EAVC. Если спорная часть уже есть или новая редакция ещё в пути, отмените действие.`, confirmLabel: 'Проверил, части нет', danger: true })) return;
    if (!await appConfirm({ title: 'Подтвердите повторную публикацию', message: `Вы подтверждаете, что части ${part} в нужной теме Telegram нет. Центр продолжит публикацию последней принятой им редакции EAVC.`, confirmLabel: 'Опубликовать', danger: true })) return;
    unconfirmedPart = part;
  } else if (manualResume) {
    if (!await appConfirm({ title: 'Продолжить ручную публикацию?', message: 'Сверьте последнюю принятую Центром редакцию с EAVC и нужную тему Telegram. Разрешение уже сохранено; если после него отправка началась, эта команда не создаст дубль.', confirmLabel: 'Сверил, продолжить', danger: true })) return;
  } else if (!await appConfirm({ title: 'Опубликовать снова?', message: 'Центр создаст новое Telegram-сообщение только для этого удалённого перехвата. Сообщения других участников затронуты не будут.', confirmLabel: 'Опубликовать' })) return;
  setButtonBusy(button, true, 'Отправляем…');
  try {
    const url = `/api/deliveries/${encodeURIComponent(button.dataset.eventId)}/destinations/${encodeURIComponent(button.dataset.destinationId)}/blocks/${encodeURIComponent(button.dataset.republishBlock)}/republish`;
    await post(url, unknownOutcome ? {
      checked_current_revision: true,
      confirmed_part_absent: true,
      unconfirmed_part: unconfirmedPart,
      send_attempt_token: button.dataset.sendAttemptToken
    } : (manualResume ? {
      manual_resume: true,
      checked_current_revision: true,
      send_attempt_token: button.dataset.sendAttemptToken
    } : {}));
    closeDialog('historyItemDialog');
    await openHistory(historyKind, historyOffset, true);
    notify(unknownOutcome || manualResume ? 'Центр обработал ручную публикацию. Результат ищите в «Доставлено» по последней принятой редакции; прежняя запись остаётся для разбора.' : 'Перехват опубликован заново и привязан к новому message_id.', true);
  } catch (error) {
    notify(unknownOutcome ? `${error.message} Обновите историю, затем повторно сверьте редакцию и отсутствие спорной части в нужной теме.` : (manualResume ? `${error.message} Обновите историю перед повторным действием.` : error.message));
    setButtonBusy(button, false);
  }
});

function renderHistory() {
  const data = historyData || {};
  const clients = data.clients || [];
  const incidents = data.incidents || [];
  const shiftHistory = data.shift_history || [];
  const items = historyKind === 'clients' ? incidents : (data.items || []);
  const healthList = $('clientHealthList');
  const columnLabels = historyColumnLabels[historyKind];
  const columns = $('historyColumns');
  columns.classList.toggle('hidden', !columnLabels);
  columns.innerHTML = columnLabels ? columnLabels.map(label => `<span>${escapeHTML(label)}</span>`).join('') : '';
  healthList.classList.toggle('hidden', historyKind !== 'clients');
  healthList.innerHTML = clients.map(client => {
    const currentShifts = client.shifts || [];
    const clientShiftHistory = client.recent_shifts || shiftHistory.filter(item => item.client_id === client.client_id);
    const currentShift = clientShiftHistory.find(item => item.current) || clientShiftHistory[0];
    const previousClosedShift = clientShiftHistory.find(item =>
      item.closed_at && (!currentShift || item.session_uuid !== currentShift.session_uuid));
    const recentShifts = [currentShift, previousClosedShift].filter(Boolean);
    const shiftState = client.shift_read_state === 'error'
      ? (client.shift_status_reason || 'Состояние смен временно недоступно; обработка перехватов продолжается')
      : (client.shift_read_state === 'unavailable'
        ? 'EAVC временно недоступен; последнее подтверждённое состояние сохранено'
        : (client.shift_read_state === 'conflict'
        ? 'Обнаружено противоречие истории EAVC; прежнее состояние сохранено'
        : (currentShifts.length ? currentShifts.map(item => {
          const position = item.position_name || 'Позиция без названия';
          return item.closed_at
            ? `${position}: закрыта ${formatDate(item.closed_at)}`
            : `${position}: открыта ${formatDate(item.opened_at)}`;
        }).join(' · ') : 'Смена ещё не обнаружена')));
    const recent = data.shift_warning
      ? `<span class="shift-warning">${escapeHTML(data.shift_warning)}</span>`
      : recentShifts.length
      ? recentShifts.map(item => {
        const closed = Boolean(item.closed_at);
        return `<div class="shift-row">
          <span class="shift-period"><time>${formatDate(item.opened_at)}</time><span aria-hidden="true">—</span><time>${closed ? formatDate(item.closed_at) : 'по н.в.'}</time></span>
          <span class="shift-status ${closed ? 'closed' : 'open'}">${closed ? 'закрыта' : 'открыта'}</span>
        </div>`;
      }).join('')
      : '<span>История смен пока пуста</span>';
    return `
    <article class="health-card">
      <strong>${escapeHTML(client.number)} · ${escapeHTML(client.name)}</strong>
      <span class="quality ${escapeHTML(client.quality)}">${escapeHTML(qualityLabel(client))}</span>
      <dl>
        <dt>Последний сигнал</dt><dd>${client.last_seen_at ? `${formatDate(client.last_seen_at)} · ${formatDelay(client.delay_seconds)}` : 'Ещё не получен'}</dd>
        <dt>Контрольный пакет</dt><dd>${formatDate(client.last_probe_at)}</dd>
        <dt>Последние данные</dt><dd>${formatDate(client.last_event_at)}</dd>
        <dt>Смена EAVC</dt><dd>${escapeHTML(shiftState)}</dd>
        <dt>Состояние смены получено</dt><dd>${formatDate(client.last_shift_success_at)}</dd>
        <dt>Статус связи</dt><dd>${escapeHTML(client.current_reason || 'Ошибок связи нет')}</dd>
      </dl>
      <div class="shift-history"><small>Смены</small>${recent}</div>
    </article>`;
  }).join('');

  const incidentLauncher = $('clientIncidentLauncher');
  incidentLauncher.classList.toggle('hidden', historyKind !== 'clients');
  const incidentCount = data.total || 0;
  $('clientIncidentCount').textContent = `${incidentCount} ${incidentCount === 1 ? 'событие' : (incidentCount > 1 && incidentCount < 5 ? 'события' : 'событий')}`;
  $('historyList').classList.toggle('incident-list', historyKind === 'clients');
  $('historyList').classList.toggle('hidden', historyKind === 'clients');

  $('historyList').innerHTML = historyKind === 'clients'
    ? ''
    : items.map((item, index) => {
      const route = historyKind === 'rejected'
        ? item.destination
        : (historyKind === 'transport' ? transportOutcomeLabel(item.transport_outcome) : [item.client_name, item.destination].filter(Boolean).join(' → '));
      return `<article class="history-row">
		<strong>${escapeHTML(item.unit_name || item.client_name || 'Пакет без названия')}</strong><span>${escapeHTML(route || historyStateLabel(item.state))}</span><time>${formatDate(item.occurred_at)}</time>
		<p>${escapeHTML(historyDetails(item))}</p>
        <div class="history-content-preview"><span>${escapeHTML(historyContentLabel(item))}</span><pre>${escapeHTML(historyContent(item))}</pre></div>
        <div class="history-row-actions"><button class="button ghost history-open-item" type="button" data-history-item="${index}">${item.content_ready ? 'Открыть полностью' : 'Подробнее'}</button></div>
      </article>`;
    }).join('');

  const hasRows = historyKind === 'clients' ? clients.length > 0 : items.length > 0;
  $('historyEmpty').textContent = 'Записей пока нет.';
  $('historyEmpty').classList.toggle('hidden', hasRows);
  $('historyRetention').textContent = `Автоочистка: ${data.retention_days || 30} дней.`;
  $('historyFilteredCount').textContent = `Найдено: ${historyKind === 'clients' ? clients.length : (data.total || 0)}`;
  const pageNumber = Math.floor(historyOffset / historyPageSize) + 1;
  const pageCount = Math.max(1, Math.ceil((data.total || 0) / historyPageSize));
  $('historyPage').textContent = `${pageNumber} из ${pageCount}`;
  $('historyPrev').disabled = historyOffset === 0;
  $('historyNext').disabled = historyOffset + historyPageSize >= (data.total || 0);
  $('historyClear').classList.toggle('hidden', !data.clearable);
  $('historyFooter').classList.toggle('hidden', historyKind === 'clients');
}

$('historyList').addEventListener('click', event => {
  const button = event.target.closest('[data-history-item]');
  if (!button) return;
  openHistoryItem(Number(button.dataset.historyItem));
});

$('openClientIncidents').addEventListener('click', () => {
  $('historyDialog').close();
  openClientIncidents();
});
$('incidentBackToClients').addEventListener('click', () => {
  $('clientIncidentDialog').close();
  openHistory('clients', 0, true);
});
$('incidentPrev').addEventListener('click', () => openClientIncidents(incidentOffset - historyPageSize, true));
$('incidentNext').addEventListener('click', () => openClientIncidents(incidentOffset + historyPageSize, true));
$('incidentApplyFilters').addEventListener('click', () => {
  incidentFilters = { q: $('incidentSearch').value.trim(), client: $('incidentClientFilter').value };
  openClientIncidents(0, true);
});
$('incidentResetFilters').addEventListener('click', () => openClientIncidents());
$('incidentSearch').addEventListener('keydown', event => {
  if (event.key === 'Enter') {
    event.preventDefault();
    $('incidentApplyFilters').click();
  }
});
$('incidentClear').addEventListener('click', async event => {
  if (!await appConfirm({ title: 'Очистить историю связи?', message: 'Записи о сбоях и восстановлениях будут удалены. Текущее состояние рабочих точек сохранится.', confirmLabel: 'Очистить', danger: true })) return;
  const button = event.currentTarget;
  setButtonBusy(button, true, 'Очищаем…');
  try {
    await request('/api/history/clients', { method: 'DELETE' });
    await Promise.all([openClientIncidents(0, true), refreshStatus()]);
    notify('История сбоев и восстановлений очищена. Текущее состояние точек сохранено.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

$('historyPrev').addEventListener('click', () => openHistory(historyKind, historyOffset - historyPageSize, true));
$('historyNext').addEventListener('click', () => openHistory(historyKind, historyOffset + historyPageSize, true));
$('historyApplyFilters').addEventListener('click', () => {
  historyFilters = {
    q: $('historySearch').value.trim(), client: $('historyClientFilter').value,
    destination: $('historyDestinationFilter').value, quality: $('historyQualityFilter').value
  };
  openHistory(historyKind, 0, true);
});
$('historyResetFilters').addEventListener('click', () => {
  resetHistoryFilters();
  openHistory(historyKind, 0, true);
});
$('historySearch').addEventListener('keydown', event => {
  if (event.key === 'Enter') {
    event.preventDefault();
    $('historyApplyFilters').click();
  }
});
$('historyClear').addEventListener('click', async event => {
  const label = historySections[historyKind]?.[0] || 'этого раздела';
  if (!await appConfirm({ title: `Очистить «${label}»?`, message: 'Будет удалена только видимая история. Активные очереди и настройки сохранятся.', confirmLabel: 'Очистить', danger: true })) return;
  const button = event.currentTarget;
  setButtonBusy(button, true, 'Очищаем…');
  try {
    await request(`/api/history/${encodeURIComponent(historyKind)}`, { method: 'DELETE' });
    await Promise.all([openHistory(historyKind, 0, true), refreshStatus()]);
    notify('История очищена. Активные очереди сохранены.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

async function loadOverview() {
  const results = await Promise.all([
    refreshStatus(),
    request('/api/destinations'),
    request('/api/clients'),
    request('/api/rules'),
    request('/api/rule-draft')
  ]);
  destinations = results[1] || {};
  clients = results[2] || [];
  if (!rulesDirty) {
    rules = results[4]?.exists ? results[4].rules : results[3];
    rulesDirty = Boolean(results[4]?.exists);
  }
  initializeRuleMaps();
  renderReadiness();
}

function renderReadiness() {
  const items = [
    [Boolean(status?.configured), 'Связь центра и рабочих точек'],
    [Object.keys(destinations).length > 0, 'Получатель доставки'],
    [clients.length > 0, 'Настройки рабочей точки'],
    [Object.keys(rules?.positions || {}).length > 0, 'Каталог позиций'],
    [Object.keys(rules?.positions || {}).length > 0 && !rulesDirty, 'Опубликованные маршруты']
  ];
  const completed = items.filter(([done]) => done).length;
  const ready = completed === items.length;
  $('readinessTitle').textContent = ready ? 'Система готова к работе' : 'Требуется завершить настройку';
  $('readinessMeta').textContent = `${completed} из ${items.length}`;
  $('readinessMeta').className = `readiness-meta ${ready ? 'complete' : 'pending'}`;
  $('readiness').innerHTML = items.map(([done, label]) => `
    <div class="check-item${done ? ' done' : ''}">
      <span class="check-mark">${done ? '✓' : ''}</span><span>${escapeHTML(label)}</span>
    </div>`).join('');
}

$('transportForm').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);
  const button = form.querySelector('button[type="submit"],button:not([type])');
  setButtonBusy(button, true, 'Проверяем связь…');
  try {
    await post('/api/setup', {
      bot_token: data.get('bot_token'),
      chat_id: Number(data.get('chat_id')),
      invite_link: data.get('invite_link')
    });
    form.reset();
    await loadOverview();
    notify('Связь проверена, настройки сохранены.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

function destinationType(kind) {
  return kind === 'telegram' ? 'Telegram' : 'Документ Word · DOCX';
}

function destinationDetails(destination) {
  if (destination.kind === 'telegram') {
    return [
      ['Группа или канал', destination.chat_id || 'Не указано'],
      ['Обозначение радиосети', destination.network || 'Не задано'],
      ['Шаблон сообщения', destination.message_template === templateDefaults.telegram ? 'Стандартный' : 'Пользовательский']
    ];
  }
  return [
    ['Куда сохраняются документы', destination.folder || 'Не указано'],
    ['Шаблон DOCX', destination.template || 'Не указан'],
    ['Обозначение радиосети', destination.network || 'Не задано']
  ];
}

async function loadDestinations() {
  destinations = await request('/api/destinations');
  renderDestinations();
}

function renderDestinations() {
  const query = $('destinationSearch').value.trim().toLocaleLowerCase('ru-RU');
  const entries = Object.entries(destinations)
    .filter(([, item]) => !query || `${item.name} ${destinationType(item.kind)}`.toLocaleLowerCase('ru-RU').includes(query))
    .sort((left, right) => String(left[1].name).localeCompare(String(right[1].name), 'ru'));
  $('destinationCount').textContent = `Показано: ${entries.length} из ${Object.keys(destinations).length}`;
  $('destinationsList').innerHTML = entries.map(([id, item]) => `
    <article class="entity-card" data-destination-id="${escapeHTML(id)}">
      <div class="entity-card-head"><div><h3>${escapeHTML(item.name)}</h3><p>${escapeHTML(destinationType(item.kind))}</p></div><span class="badge success">Настроено</span></div>
      <dl class="entity-meta">${destinationDetails(item).map(([label, value]) => `<div class="entity-meta-row"><dt>${escapeHTML(label)}</dt><dd title="${escapeHTML(value)}">${escapeHTML(value)}</dd></div>`).join('')}</dl>
      <div class="entity-actions"><button class="button ghost" data-action="edit">Изменить</button><button class="button danger" data-action="delete">Удалить</button></div>
    </article>`).join('');
}

$('destinationSearch').addEventListener('input', renderDestinations);

function generatedDestinationID() {
  return `receiver-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 7)}`;
}

function updateDestinationKind() {
  const kind = $('destinationForm').elements.kind.value;
  const form = $('destinationForm');
  document.querySelectorAll('#destinationForm .kind').forEach(section => section.classList.add('hidden'));
  document.querySelector(`#destinationForm .kind.${kind}`).classList.remove('hidden');
  form.elements.bot_token.required = kind === 'telegram' && form.dataset.originalKind !== 'telegram';
  form.elements.chat_id.required = kind === 'telegram';
  form.elements.message_template.required = kind === 'telegram';
  form.elements.network.required = true;
  form.elements.template.required = kind === 'word';
  form.elements.folder.required = kind === 'word';
}

$('destinationForm').elements.kind.addEventListener('change', updateDestinationKind);

function showDestinationDialog(id = '') {
  const form = $('destinationForm');
  form.reset();
  const item = id ? destinations[id] : null;
  form.dataset.editing = item ? 'true' : 'false';
  form.dataset.originalKind = item?.kind || '';
  form.elements.id.value = id || generatedDestinationID();
  form.elements.name.value = item?.name || '';
  form.elements.kind.value = item?.kind || 'telegram';
  form.elements.chat_id.value = item?.chat_id || '';
  form.elements.message_template.value = item?.message_template || templateDefaults.telegram;
  form.elements.template.value = item?.template || templateDefaults.word_path;
  form.elements.folder.value = item?.folder || '';
  form.elements.network.value = item?.network || '';
  form.elements.protection.value = item?.protection || 'AES-256';
  form.elements.prefix.value = item?.prefix || '';
  $('telegramTemplateMarkers').textContent = `Обязательные метки: ${templateDefaults.telegram_markers.join(', ')}`;
  $('wordTemplateMarkers').textContent = templateDefaults.word_markers.join(', ');
  const download = $('downloadWordTemplate');
  download.classList.toggle('hidden', !item || item.kind !== 'word');
  download.dataset.destinationId = item?.kind === 'word' ? id : '';
  download.dataset.filename = item?.kind === 'word' ? String(item.template || '').split(/[\\/]/).pop() || 'word_template.docx' : '';
  $('destinationDialogTitle').textContent = item ? 'Изменить получателя' : 'Новый получатель';
  updateDestinationKind();
  openDialog('destinationDialog');
}

$('addDestination').addEventListener('click', () => showDestinationDialog());

$('downloadWordTemplate').addEventListener('click', async event => {
  const button = event.currentTarget;
  const id = button.dataset.destinationId;
  if (!id) return;
  setButtonBusy(button, true, 'Скачиваем…');
  try {
    await downloadAPI(`/api/destinations/${encodeURIComponent(id)}/word-template`, button.dataset.filename || 'word_template.docx');
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

$('restoreTelegramTemplate').addEventListener('click', () => {
  $('destinationForm').elements.message_template.value = templateDefaults.telegram;
  notify('Стандартный шаблон Telegram восстановлен в форме. Сохраните получателя, чтобы применить его.', true);
});

$('restoreWordTemplate').addEventListener('click', async event => {
  const button = event.currentTarget;
  setButtonBusy(button, true, 'Восстанавливаем…');
  try {
    const restored = await post('/api/templates/restore-word', {});
    $('destinationForm').elements.template.value = restored.path;
    notify('Стандартный шаблон Word восстановлен. Сохраните получателя, чтобы применить его.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

$('destinationsList').addEventListener('click', async event => {
  const button = event.target.closest('button[data-action]');
  const card = event.target.closest('[data-destination-id]');
  if (!button || !card) return;
  const id = card.dataset.destinationId;
  if (button.dataset.action === 'edit') showDestinationDialog(id);
  if (button.dataset.action === 'delete') {
    const name = destinations[id]?.name || 'этого получателя';
    if (!await appConfirm({ title: `Удалить «${name}»?`, message: 'Получатель будет убран из всех маршрутов.', confirmLabel: 'Удалить', danger: true })) return;
    if (!await discardDraftForRecipientChange()) return;
    const hadDraft = rulesDirty;
    setButtonBusy(button, true, 'Удаляем…');
    try {
      await request(`/api/destinations/${encodeURIComponent(id)}`, { method: 'DELETE' });
      if (hadDraft) {
        rulesDirty = false;
        rules = null;
      }
      await loadDestinations();
      notify('Получатель удалён из настроек и маршрутов.', true);
    } catch (error) {
      notify(error.message);
      setButtonBusy(button, false);
    }
  }
});

$('destinationForm').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);
  const button = form.querySelector('button[value="save"]');
  if (!await discardDraftForRecipientChange()) return;
  const hadDraft = rulesDirty;
  setButtonBusy(button, true, 'Проверяем…');
  try {
    await post('/api/destinations', {
      id: data.get('id'),
      name: data.get('name'),
      kind: data.get('kind'),
      bot_token: data.get('bot_token'),
      chat_id: Number(data.get('chat_id') || 0),
      message_template: data.get('message_template'),
      template: data.get('template'),
      folder: data.get('folder'),
      network: data.get('network'),
      protection: data.get('protection'),
      prefix: data.get('prefix'),
      username: data.get('username'),
      password: data.get('password')
    });
    if (hadDraft) {
      rulesDirty = false;
      rules = null;
    }
    closeDialog('destinationDialog');
    await loadDestinations();
    notify('Получатель проверен и сохранён.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

async function discardDraftForRecipientChange() {
  if (!rulesDirty) return true;
  return appConfirm({ title: 'Отменить черновик?', message: 'Есть неопубликованные изменения маршрутов. Чтобы изменить получатей, их нужно отменить.', confirmLabel: 'Отменить черновик', danger: true });
}

async function loadClients() {
  clients = await request('/api/clients');
  renderClients();
}

function renderClients() {
  $('clientsList').innerHTML = clients.map(client => `
    <article class="entity-card">
      <div class="entity-card-head"><div><h3>${escapeHTML(client.name)}</h3><p>Точка № ${escapeHTML(client.number)}</p></div><span class="badge ${client.active ? 'success' : 'neutral'}">${client.active ? 'Активна' : 'Отключена'}</span></div>
      <dl class="entity-meta"><dt>Бот</dt><dd>${escapeHTML(client.bot || 'Не указан')}</dd><dt>Тема обмена</dt><dd>${escapeHTML(client.thread_id)}</dd></dl>
      <div class="entity-actions"><button class="button primary" data-client-download="package" data-client-id="${escapeHTML(client.id)}">Скачать готовую программу</button><button class="button ghost" data-client-download="config" data-client-id="${escapeHTML(client.id)}">Скачать защищённые настройки</button></div>
    </article>`).join('');
	$('clientsList').querySelectorAll('[data-client-download]').forEach(button => button.addEventListener('click', async () => {
		const client = clients.find(item => item.id === button.dataset.clientId);
		const kind = button.dataset.clientDownload;
		const extension = kind === 'package' ? 'zip' : 'activation';
		try { await downloadAPI(`/api/clients/${encodeURIComponent(button.dataset.clientId)}/${kind}`, `EAVC Connect Station-${client?.number || 'point'}.${extension}`); }
		catch (error) { notify(error.message); }
	}));
}

$('addClient').addEventListener('click', () => {
  $('clientForm').reset();
  openDialog('clientDialog');
});

$('clientForm').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const data = new FormData(form);
  const button = form.querySelector('button[value="save"]');
  setButtonBusy(button, true, 'Проверяем…');
  try {
    const response = await fetch('/api/clients', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-EAVC-Access': accessToken },
      body: JSON.stringify({
        number: data.get('number'),
        name: data.get('name'),
        time_zone: data.get('time_zone'),
        bot_token: data.get('bot_token'),
        thread_id: Number(data.get('thread_id')),
        catch_up_existing: data.get('catch_up_existing') === 'on'
      })
    });
    if (!response.ok) {
      const error = await response.json();
      throw new Error(error.error || 'Не удалось создать настройки');
    }
		const activationCode = response.headers.get('X-EAVC-Activation-Code') || '';
    const blob = await response.blob();
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
		link.download = `EAVC Connect Station-${data.get('number')}.activation`;
    link.click();
    URL.revokeObjectURL(link.href);
		if (activationCode) {
			await showActivationCode(activationCode);
		}
    closeDialog('clientDialog');
    await loadClients();
		notify('Защищённый файл активации скачан. Сохраните показанный код отдельно.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

async function loadRules() {
  const [published, draft] = await Promise.all([
    request('/api/rules'),
    request('/api/rule-draft')
  ]);
  rules = draft?.exists ? draft.rules : published;
  initializeRuleMaps();
  rulesDirty = Boolean(draft?.exists);
  currentPositionPage = 1;
  renderRules();
  await loadRuleHistory();
  if (rulesDirty) notify('Неопубликованный черновик восстановлен из центра.', true);
}

function initializeRuleMaps() {
  rules.destinations ||= {};
  rules.positions ||= {};
  rules.topic_redirects ||= {};
  rules.unknown ||= {};
}

function positionName(key) {
  return key.replace(/^(name|catalog):/, '');
}

function normalizedPositionKey(name) {
  return `name:${name.trim().replace(/\s+/g, ' ').toLocaleLowerCase('ru-RU')}`;
}

function sortedRuleDestinations() {
  return Object.values(rules?.destinations || {}).sort((left, right) => left.name.localeCompare(right.name, 'ru'));
}

function cloneRules(value) {
  return JSON.parse(JSON.stringify(value));
}

function routeDestinationPresentation(destination) {
  const configured = destinations[destination.id] || {};
  if (destination.kind === 'telegram') {
    return {
      type: 'Telegram',
      detail: configured.chat_id ? `Группа: ${configured.chat_id}` : 'Telegram-группа или канал'
    };
  }
  return {
    type: 'Документ Word · DOCX',
    detail: configured.folder ? `Папка: ${configured.folder}` : 'Папка назначения не указана'
  };
}

function decisionSummary(decisions, recipientID = '', positionKey = '') {
  const chips = sortedRuleDestinations().flatMap(destination => {
    if (recipientID && destination.id !== recipientID) return [];
    const decision = decisions?.[destination.id];
    if (!decision || decision.action === 'skip') return [];
    const action = decision.action === 'review' ? 'На проверку' : 'Отправлять';
    const redirect = positionKey ? rules.topic_redirects?.[positionKey] : null;
    const topicID = redirect?.destination_id === destination.id ? redirect.thread_id : decision.thread_id;
    const topicName = redirect?.destination_id === destination.id ? `${redirect.topic_name || `Тема ${topicID}`} · ` : '';
    const topic = destination.kind === 'telegram' ? ` · ${topicName}${topicID ? `тема ${topicID}` : 'General'}` : '';
    const type = destination.kind === 'telegram' ? 'Telegram' : 'DOCX';
    return [`<span class="route-chip${decision.action === 'review' ? ' review' : ''}">${escapeHTML(destination.name)} · ${type} · ${action}${escapeHTML(topic)}</span>`];
  });
  return chips.length ? chips.join('') : '<span class="route-chip empty">Не отправлять</span>';
}

function routeUsesRecipient(decisions, recipientID) {
  if (!recipientID) return true;
  const decision = decisions?.[recipientID];
  return Boolean(decision && decision.action !== 'skip');
}

function syncRecipientFilter(allKeys) {
  const select = $('recipientFilter');
  const previous = select.value;
  const options = sortedRuleDestinations().map(destination => {
    const count = allKeys.filter(key => routeUsesRecipient(rules.positions[key], destination.id)).length;
    const type = destination.kind === 'telegram' ? 'Telegram' : 'DOCX';
    return `<option value="${escapeHTML(destination.id)}">${escapeHTML(destination.name)} · ${type} (${count})</option>`;
  });
  select.innerHTML = `<option value="">Все получатели (${allKeys.length})</option>${options.join('')}`;
  if ([...select.options].some(option => option.value === previous)) select.value = previous;
}

function renderRules() {
  const allKeys = Object.keys(rules.positions).sort((left, right) => positionName(left).localeCompare(positionName(right), 'ru'));
  syncRecipientFilter(allKeys);
  const query = $('positionSearch').value.trim().toLocaleLowerCase('ru-RU');
  const recipientID = $('recipientFilter').value;
  const filtered = allKeys.filter(key =>
    positionName(key).toLocaleLowerCase('ru-RU').includes(query) &&
    routeUsesRecipient(rules.positions[key], recipientID)
  );
  const pages = Math.max(1, Math.ceil(filtered.length / positionsPerPage));
  currentPositionPage = Math.min(currentPositionPage, pages);
  const start = (currentPositionPage - 1) * positionsPerPage;
  const visible = filtered.slice(start, start + positionsPerPage);

  $('rulesMeta').textContent = rulesDirty
    ? `Черновик сохранён · основа: версия ${rules.version}`
    : `Опубликована версия ${rules.version}`;
  $('draftBadge').classList.toggle('hidden', !rulesDirty);
  $('routeFilterCount').textContent = `Показано: ${filtered.length} из ${allKeys.length}`;
  $('clearRouteFilters').disabled = !query && !recipientID;
  $('publishRules').disabled = !rulesDirty;
  $('publishTopicRedirects').disabled = !rulesDirty;
  $('positionPrev').disabled = currentPositionPage <= 1;
  $('positionNext').disabled = currentPositionPage >= pages;
  $('positionPage').textContent = `Страница ${currentPositionPage} из ${pages}`;
  $('positionPage').parentElement.classList.toggle('hidden', filtered.length <= positionsPerPage);
  $('positionsList').dataset.emptyMessage = query || recipientID
    ? 'По выбранным условиям позиции не найдены'
    : 'Пока ничего не добавлено';

  $('unknownRoute').innerHTML = `
    <div><h3>Новые и неопознанные позиции</h3><p>Правило применяется, если отдельная позиция ещё не добавлена.</p></div>
    <div class="route-summary">${decisionSummary(rules.unknown, recipientID)}</div>
    <div class="route-card-actions"><button class="button ghost" data-route-edit="__unknown__">Настроить</button></div>`;

  $('positionsList').innerHTML = visible.map(key => `
    <article class="route-card" data-position-key="${escapeHTML(key)}">
      <div><h3>${escapeHTML(positionName(key))}</h3><p>Индивидуальный маршрут</p></div>
      <div class="route-summary">${decisionSummary(rules.positions[key], recipientID, key)}</div>
      <div class="route-card-actions"><button class="button ghost" data-action="edit">Изменить</button><button class="button danger" data-action="delete">Удалить</button></div>
    </article>`).join('');
  renderTopicRedirects();
}

function eligibleTopicRedirects() {
  return Object.keys(rules.positions).sort((left, right) => positionName(left).localeCompare(positionName(right), 'ru')).flatMap(key => {
    const telegram = sortedRuleDestinations().filter(destination => {
      const decision = rules.positions[key]?.[destination.id];
      return destination.kind === 'telegram' && destination.enabled && decision && decision.action !== 'skip';
    });
    if (telegram.length !== 1) return [];
    const destination = telegram[0];
    return [{key, destination, decision: rules.positions[key][destination.id]}];
  });
}

function wordRouteLabel(key) {
  const enabled = sortedRuleDestinations().some(destination =>
    destination.kind === 'word' && destination.enabled &&
    rules.positions[key]?.[destination.id]?.action !== 'skip' &&
    rules.positions[key]?.[destination.id]);
  return enabled ? 'DOCX: отправляется по основному маршруту' : 'DOCX: не отправляется';
}

function topicNameForRoute(destinationID, threadID) {
  const routeKey = Object.keys(rules.positions || {}).sort((left, right) =>
    positionName(left).localeCompare(positionName(right), 'ru')
  ).find(key => {
    const decision = rules.positions[key]?.[destinationID];
    return decision && decision.action !== 'skip' && Number(decision.thread_id || 0) === Number(threadID);
  });
  if (routeKey) return positionName(routeKey);
  const known = Object.values(rules.topic_redirects || {}).find(redirect =>
    String(redirect.destination_id) === String(destinationID) && Number(redirect.thread_id) === Number(threadID) && redirect.topic_name
  );
  return known?.topic_name?.trim() || `Тема ${threadID}`;
}

function topicOptionValue(destinationID, threadID) {
  return `${destinationID}::${threadID}`;
}

function topicOptionsFor(row) {
  const sourceThreadID = Number(row.decision.thread_id || 0);
  const options = new Map();
  Object.keys(rules.positions).forEach(key => {
    const decision = rules.positions[key]?.[row.destination.id];
    if (!decision || decision.action === 'skip') return;
    const threadID = Number(decision.thread_id || 0);
    if (!Number.isSafeInteger(threadID) || threadID <= 0 || threadID === sourceThreadID) return;
    const value = topicOptionValue(row.destination.id, threadID);
    if (!options.has(value)) {
      options.set(value, {
        value,
        destinationID: row.destination.id,
        threadID,
        topicName: topicNameForRoute(row.destination.id, threadID)
      });
    }
  });
  Object.values(rules.topic_redirects || {}).forEach(redirect => {
    const threadID = Number(redirect.thread_id || 0);
    if (String(redirect.destination_id) !== String(row.destination.id) || !Number.isSafeInteger(threadID) || threadID <= 0 || threadID === sourceThreadID) return;
    const value = topicOptionValue(redirect.destination_id, threadID);
    if (!options.has(value)) {
      options.set(value, {
        value,
        destinationID: redirect.destination_id,
        threadID,
        topicName: redirect.topic_name?.trim() || `Тема ${threadID}`
      });
    }
  });
  const current = rules.topic_redirects?.[row.key];
  if (current && String(current.destination_id) === String(row.destination.id) && Number(current.thread_id) > 0 && Number(current.thread_id) !== sourceThreadID) {
    const value = topicOptionValue(current.destination_id, Number(current.thread_id));
    if (!options.has(value)) {
      options.set(value, {
        value,
        destinationID: current.destination_id,
        threadID: Number(current.thread_id),
        topicName: current.topic_name?.trim() || `Тема ${current.thread_id}`
      });
    }
  }
  return [...options.values()].sort((left, right) =>
    `${left.topicName} ${left.threadID}`.localeCompare(`${right.topicName} ${right.threadID}`, 'ru', {numeric: true})
  );
}

function renderTopicRedirects() {
  const eligible = eligibleTopicRedirects();
  const search = $('topicRedirectSearch').value.trim().toLocaleLowerCase('ru-RU');
  const rows = eligible.filter(row => rules.topic_redirects[row.key] &&
    positionName(row.key).toLocaleLowerCase('ru-RU').includes(search));
  $('topicRedirectCount').textContent = `${rows.length} перенаправлений · ${eligible.length} доступных маршрутов`;
  $('topicRedirectRulesMeta').textContent = rulesDirty
    ? `Черновик · основа: версия ${rules.version}`
    : `Опубликована версия ${rules.version}`;
  $('topicRedirectDraftBadge').classList.toggle('hidden', !rulesDirty);
  $('addTopicRedirect').disabled = eligible.length === 0;
  $('topicRedirectList').dataset.emptyMessage = search
    ? 'По запросу перенаправления не найдены'
    : eligible.length === 0
      ? 'Нет подходящих маршрутов. В разделе «Маршруты» назначьте подразделению ровно одного активного Telegram-получателя.'
      : 'Пока нет перенаправлений. Новые перехваты идут в темы основных маршрутов.';
  $('topicRedirectList').innerHTML = rows.map(({key, destination, decision}) => {
    const redirect = rules.topic_redirects[key];
    const originalTopic = decision.thread_id ? `Тема ${decision.thread_id}` : 'General';
    const targetTopic = redirect.topic_name?.trim() || `Тема ${redirect.thread_id}`;
    return `<article class="route-card topic-redirect-card" data-redirect-key="${escapeHTML(key)}">
      <div class="topic-redirect-source"><h3>${escapeHTML(positionName(key))}</h3><p>${escapeHTML(destination.name)} · ${escapeHTML(wordRouteLabel(key))}</p></div>
      <div class="topic-redirect-path"><span>${escapeHTML(originalTopic)}</span><span class="topic-redirect-arrow" aria-hidden="true">→</span><strong>${escapeHTML(targetTopic)}</strong></div>
      <div class="route-card-actions"><button class="button ghost" type="button" data-redirect-action="edit">Изменить</button><button class="button danger" type="button" data-redirect-action="remove">Отменить</button></div>
    </article>`;
  }).join('');
}

function showTopicRedirectDialog(key = '') {
  const form = $('topicRedirectForm');
  form.reset();
  const eligible = eligibleTopicRedirects();
  if (!eligible.length) {
    notify('Сначала создайте маршрут подразделения с одним активным Telegram-получателем.');
    return;
  }
  form.elements.position_key.innerHTML = eligible.map(row =>
    `<option value="${escapeHTML(row.key)}">${escapeHTML(positionName(row.key))}</option>`).join('');
  form.elements.position_key.value = key && eligible.some(row => row.key === key) ? key : eligible[0].key;
  $('topicRedirectDialogTitle').textContent = 'Перенаправление темы';
  updateTopicRedirectDialog();
  if (!form.elements.target_topic.options.length) {
    notify('Для этого маршрута нет другой темы. Сначала добавьте тему в разделе «Маршруты».');
    return;
  }
  openDialog('topicRedirectDialog');
}

function updateTopicRedirectDialog() {
  const form = $('topicRedirectForm');
  const key = form.elements.position_key.value;
  const row = eligibleTopicRedirects().find(item => item.key === key);
  if (!row) return;
  const redirect = rules.topic_redirects[key];
  const options = topicOptionsFor(row);
  const target = form.elements.target_topic;
  target.innerHTML = options.map(option =>
    `<option value="${escapeHTML(option.value)}" data-destination-id="${escapeHTML(option.destinationID)}" data-thread-id="${escapeHTML(option.threadID)}" data-topic-name="${escapeHTML(option.topicName)}">${escapeHTML(option.topicName)}</option>`
  ).join('');
  const currentValue = redirect ? topicOptionValue(redirect.destination_id, Number(redirect.thread_id)) : '';
  if ([...target.options].some(option => option.value === currentValue)) target.value = currentValue;
  target.disabled = options.length === 0;
  const saveButton = form.querySelector('button[value="save"]');
  saveButton.disabled = options.length === 0;
}

$('topicRedirectSearch').addEventListener('input', renderTopicRedirects);
$('addTopicRedirect').addEventListener('click', () => showTopicRedirectDialog());
$('topicRedirectForm').elements.position_key.addEventListener('change', updateTopicRedirectDialog);

$('topicRedirectList').addEventListener('click', async event => {
  const button = event.target.closest('button[data-redirect-action]');
  const row = event.target.closest('[data-redirect-key]');
  if (!button || !row) return;
  const key = row.dataset.redirectKey;
  if (button.dataset.redirectAction === 'edit') {
    showTopicRedirectDialog(key);
    return;
  }
  if (!await appConfirm({ title: `Отменить перенаправление «${positionName(key)}»?`, message: 'После публикации новые перехваты снова пойдут в тему основного маршрута. Ранее принятые Центром перехваты сохранят прежнюю тему.', confirmLabel: 'Отменить перенаправление' })) return;
  const previous = cloneRules(rules);
  delete rules.topic_redirects[key];
  setButtonBusy(button, true, 'Сохраняем…');
  try {
    await post('/api/rule-draft', rules);
    rulesDirty = true;
    renderRules();
    notify('Отмена перенаправления сохранена в черновике.', true);
  } catch (error) {
    rules = previous;
    renderRules();
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

$('topicRedirectForm').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const key = form.elements.position_key.value;
  const row = eligibleTopicRedirects().find(item => item.key === key);
  const option = form.elements.target_topic.selectedOptions[0];
  const destinationID = option?.dataset.destinationId || '';
  const threadID = Number(option?.dataset.threadId || 0);
  const topicName = option?.dataset.topicName?.trim() || `Тема ${threadID}`;
  if (!row || !option || String(destinationID) !== String(row.destination.id) || !Number.isSafeInteger(threadID) || threadID <= 0) {
    notify('Выберите существующую тему Telegram из списка.');
    return;
  }
  if (threadID === Number(row.decision.thread_id || 0)) {
    notify('Это уже тема основного маршрута. Перенаправление не требуется.');
    return;
  }
  const previous = cloneRules(rules);
  rules.topic_redirects[key] = {destination_id: destinationID, thread_id: threadID, topic_name: topicName};
  const button = form.querySelector('button[value="save"]');
  setButtonBusy(button, true, 'Сохраняем…');
  try {
    await post('/api/rule-draft', rules);
    rulesDirty = true;
    closeDialog('topicRedirectDialog');
    renderRules();
    notify('Перенаправление сохранено в черновике. Опубликуйте маршруты, чтобы оно заработало.', true);
  } catch (error) {
    rules = previous;
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

$('positionSearch').addEventListener('input', () => {
  currentPositionPage = 1;
  renderRules();
});

$('recipientFilter').addEventListener('change', () => {
  currentPositionPage = 1;
  renderRules();
});

$('clearRouteFilters').addEventListener('click', () => {
  $('positionSearch').value = '';
  $('recipientFilter').value = '';
  currentPositionPage = 1;
  renderRules();
});

$('positionPrev').addEventListener('click', () => {
  if (currentPositionPage > 1) currentPositionPage--;
  renderRules();
});

$('positionNext').addEventListener('click', () => {
  currentPositionPage++;
  renderRules();
});

function routeControl(destination, decision = { action: 'skip', thread_id: 0 }, unknown = false) {
  const presentation = routeDestinationPresentation(destination);
  const telegramUnknown = unknown && destination.kind === 'telegram';
  const wordUnknown = unknown && destination.kind === 'word';
  const topic = destination.kind === 'telegram' ? `
    <label>Тема получателя<input name="thread_${escapeHTML(destination.id)}" type="number" min="0" value="${telegramUnknown ? 0 : Number(decision.thread_id || 0)}"${telegramUnknown ? ' disabled' : ''}><small>${telegramUnknown ? 'Новые позиции направляются в General' : '0 — основная тема'}</small></label>` : `
    <div class="route-static-field"><span>Тема Telegram</span><strong>Не используется</strong><small>Файл сохраняется в папку</small></div>`;
  const actionOptions = wordUnknown
    ? '<option value="skip" selected>Не отправлять</option>'
    : unknown
      ? `<option value="skip"${decision.action === 'skip' ? ' selected' : ''}>Не отправлять</option><option value="review"${decision.action === 'review' ? ' selected' : ''}>Отправить на проверку</option>`
      : `<option value="skip"${decision.action === 'skip' ? ' selected' : ''}>Не отправлять</option><option value="deliver"${decision.action === 'deliver' ? ' selected' : ''}>Отправлять</option><option value="review"${decision.action === 'review' ? ' selected' : ''}>Отправить на проверку</option>`;
  return `<div class="route-control" data-route-destination="${escapeHTML(destination.id)}">
    <div class="route-destination"><strong>${escapeHTML(destination.name)}</strong><span>${escapeHTML(presentation.type)}</span><small title="${escapeHTML(presentation.detail)}">${escapeHTML(presentation.detail)}</small></div>
    <label>Действие<select name="action_${escapeHTML(destination.id)}"${wordUnknown ? ' disabled' : ''}>${actionOptions}</select></label>
    ${topic}
  </div>`;
}

function showPositionDialog(key = '') {
  const form = $('positionForm');
  form.reset();
  const unknown = key === '__unknown__';
  const decisions = unknown ? rules.unknown : (rules.positions[key] || {});
  form.elements.original_key.value = key;
  form.elements.name.value = unknown ? '' : positionName(key);
  form.elements.name.disabled = unknown;
  form.elements.name.required = !unknown;
  form.elements.name.closest('label').classList.toggle('hidden', unknown);
  $('positionDialogTitle').textContent = unknown ? 'Новые и неопознанные позиции' : (key ? 'Изменить позицию' : 'Новая позиция');
  $('positionRedirectNotice').classList.toggle('hidden', !rules.topic_redirects?.[key]);
  const destinationList = sortedRuleDestinations();
  $('routeEditor').innerHTML = destinationList.length
    ? destinationList.map(destination => routeControl(destination, decisions[destination.id], unknown)).join('')
    : '<div class="route-editor-empty">Сначала добавьте получателей. Позицию можно сохранить сейчас и настроить позже.</div>';
  openDialog('positionDialog');
}

$('addPosition').addEventListener('click', () => showPositionDialog());
$('unknownRoute').addEventListener('click', event => {
  if (event.target.closest('[data-route-edit]')) showPositionDialog('__unknown__');
});

$('positionsList').addEventListener('click', async event => {
  const button = event.target.closest('button[data-action]');
  const row = event.target.closest('[data-position-key]');
  if (!button || !row) return;
  const key = row.dataset.positionKey;
  if (button.dataset.action === 'edit') showPositionDialog(key);
  if (button.dataset.action === 'delete') {
    if (!await appConfirm({ title: `Удалить «${positionName(key)}»?`, message: 'Позиция будет удалена из черновика маршрутов.', confirmLabel: 'Удалить', danger: true })) return;
    const previous = cloneRules(rules);
    delete rules.positions[key];
    delete rules.topic_redirects[key];
    setButtonBusy(button, true, 'Удаляем…');
    try {
      await post('/api/rule-draft', rules);
      rulesDirty = true;
      renderRules();
      notify('Позиция удалена, черновик сохранён в центре.', true);
    } catch (error) {
      rules = previous;
      renderRules();
      notify(error.message);
    }
  }
});

$('positionForm').addEventListener('submit', async event => {
  event.preventDefault();
  const form = event.currentTarget;
  const button = form.querySelector('button[type="submit"],button:not([type])');
  const data = new FormData(form);
  const originalKey = data.get('original_key');
  const unknown = originalKey === '__unknown__';
  const previous = cloneRules(rules);
  const decisions = {};
  sortedRuleDestinations().forEach(destination => {
    decisions[destination.id] = {
      action: data.get(`action_${destination.id}`) || 'skip',
      thread_id: destination.kind === 'telegram' ? Number(data.get(`thread_${destination.id}`) || 0) : 0
    };
  });

  if (unknown) {
    const generalRoutes = sortedRuleDestinations().filter(destination =>
      destination.kind === 'telegram' && decisions[destination.id]?.action === 'review');
    if (generalRoutes.length !== 1) {
      rules = previous;
      notify('Выберите ровно один Telegram-получатель для отправки новых подразделений в General.');
      return;
    }
    rules.unknown = decisions;
  } else {
    const newKey = normalizedPositionKey(data.get('name'));
    if (newKey !== originalKey && rules.positions[newKey]) {
      notify('Позиция с таким названием уже существует.');
      return;
    }
    const oldRedirect = rules.topic_redirects[originalKey];
    if (originalKey) {
      delete rules.positions[originalKey];
      delete rules.topic_redirects[originalKey];
    }
    rules.positions[newKey] = decisions;
    if (oldRedirect) {
      const destination = rules.destinations[oldRedirect.destination_id];
      const activeTelegram = sortedRuleDestinations().filter(item =>
        item.kind === 'telegram' && item.enabled && decisions[item.id]?.action !== 'skip');
      if (destination?.kind === 'telegram' && activeTelegram.length === 1 &&
          activeTelegram[0].id === oldRedirect.destination_id &&
          Number(decisions[destination.id].thread_id || 0) !== Number(oldRedirect.thread_id)) {
        rules.topic_redirects[newKey] = oldRedirect;
      }
    }
  }
  setButtonBusy(button, true, 'Сохраняем…');
  try {
    await post('/api/rule-draft', rules);
    rulesDirty = true;
    closeDialog('positionDialog');
    renderRules();
    notify('Черновик сохранён в центре.', true);
  } catch (error) {
    rules = previous;
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

async function publishCurrentRules(button) {
  if (!rulesDirty || !rules) return;
  if (!await appConfirm({ title: 'Опубликовать все изменения?', message: 'Будет опубликован весь общий черновик: маршруты, перенаправления и изменения доставки Word, если вы их сохраняли. Новые правила начнут применяться к следующим перехватам.', confirmLabel: 'Опубликовать' })) return;
  const publishButtons = [$('publishRules'), $('publishTopicRedirects')];
  publishButtons.forEach(item => { item.disabled = true; });
  setButtonBusy(button, true, 'Публикуем…');
  try {
    await post('/api/rules', rules);
    await loadRules();
    notify('Маршруты опубликованы и уже действуют.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
    publishButtons.forEach(item => { item.disabled = !rulesDirty; });
  }
}

$('publishRules').addEventListener('click', event => publishCurrentRules(event.currentTarget));
$('publishTopicRedirects').addEventListener('click', event => publishCurrentRules(event.currentTarget));

async function loadRuleHistory() {
  ruleHistory = await request('/api/rule-versions');
  renderRuleHistory();
}

function renderRuleHistory() {
  const history = ruleHistoryExpanded ? ruleHistory : ruleHistory.slice(0, visibleRuleVersions);
  $('rulesHistory').innerHTML = history.map(item => `
    <div class="simple-row" data-rule-version="${item.version}">
      <strong>Версия ${item.version}</strong><span>${escapeHTML(item.author)}</span><span>${formatDate(item.created_at)}</span>
      ${item.active ? '<span class="badge success">Действует</span>' : '<button class="button ghost" data-action="restore">Восстановить</button>'}
    </div>`).join('');
  const toggle = $('toggleRuleHistory');
  toggle.classList.toggle('hidden', ruleHistory.length <= visibleRuleVersions);
  toggle.textContent = ruleHistoryExpanded ? 'Свернуть' : `Показать все (${ruleHistory.length})`;
}

$('toggleRuleHistory').addEventListener('click', () => {
  ruleHistoryExpanded = !ruleHistoryExpanded;
  renderRuleHistory();
});

$('rulesHistory').addEventListener('click', async event => {
  const button = event.target.closest('button[data-action="restore"]');
  const row = event.target.closest('[data-rule-version]');
  if (!button || !row) return;
  const version = Number(row.dataset.ruleVersion);
  if (!await appConfirm({ title: `Восстановить версию ${version}?`, message: 'Текущие настройки останутся в истории, а выбранная версия станет новой активной.', confirmLabel: 'Восстановить' })) return;
  setButtonBusy(button, true, 'Восстанавливаем…');
  try {
    await post(`/api/rule-versions/${version}/restore`, {});
    await loadRules();
    notify(`Настройки версии ${version} восстановлены как новая версия.`, true);
  } catch (error) {
    notify(error.message);
    setButtonBusy(button, false);
  }
});

async function initialize() {
  try {
    templateDefaults = await request('/api/template-defaults');
    await loadOverview();
  } catch (error) {
    notify(error.message);
  }
  setInterval(() => refreshStatus().catch(() => {}), 5000);
}

initialize();
