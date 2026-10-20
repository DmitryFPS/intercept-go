const $ = id => document.getElementById(id);

const pageTitles = {
  dashboard: 'Обзор',
  transport: 'Связь с рабочими точками',
  destinations: 'Получатели',
  clients: 'Рабочие точки',
  routes: 'Маршруты'
};

const pageContexts = {
  dashboard: 'СОСТОЯНИЕ СИСТЕМЫ',
  transport: 'ШАГ 1 · НАСТРОЙКА СИСТЕМЫ',
  destinations: 'ШАГ 2 · НАСТРОЙКА СИСТЕМЫ',
  clients: 'ШАГ 3 · НАСТРОЙКА СИСТЕМЫ',
  routes: 'ШАГ 4 · НАСТРОЙКА СИСТЕМЫ'
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

function escapeHTML(value) {
  return String(value ?? '').replace(/[&<>"']/g, char => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[char]);
}

function formatDate(value) {
  return value ? new Date(value).toLocaleString('ru-RU') : 'Ещё не было';
}

async function request(url, options = {}) {
  const response = await fetch(url, { cache: 'no-store', ...options });
  const type = response.headers.get('content-type') || '';
  const payload = type.includes('application/json') ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(payload?.error || payload || 'Не удалось выполнить действие');
  }
  return payload;
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
      if (button.dataset.page === 'routes') {
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

$('openHelp').addEventListener('click', () => openDialog('helpDialog'));

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

  const stats = status.stats || {};
  $('statClients').textContent = stats.clients || 0;
  $('statEvents').textContent = stats.events || 0;
  $('statPending').textContent = stats.pending || 0;
  $('statDelivered').textContent = stats.delivered || 0;
  $('statExpired').textContent = stats.expired || 0;
  $('statRejected').textContent = stats.rejected || 0;
  $('lastReceived').textContent = formatDate(status.last_received);
  $('lastDelivery').textContent = formatDate(status.last_delivery);
}

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
  renderReadiness();
}

function renderReadiness() {
  const items = [
    [Boolean(status?.configured), 'Настроить связь с рабочими точками'],
    [Object.keys(destinations).length > 0, 'Добавить хотя бы одного получателя'],
    [clients.length > 0, 'Создать файл настроек рабочей точки'],
    [Object.keys(rules?.positions || {}).length > 0, 'Добавить позиции'],
    [Object.keys(rules?.positions || {}).length > 0 && !rulesDirty, 'Опубликовать маршруты']
  ];
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
  form.elements.prefix.value = item?.prefix || '';
  $('telegramTemplateMarkers').textContent = `Обязательные метки: ${templateDefaults.telegram_markers.join(', ')}`;
  $('wordTemplateMarkers').textContent = templateDefaults.word_markers.join(', ');
  const download = $('downloadWordTemplate');
  download.classList.toggle('hidden', !item || item.kind !== 'word');
  download.href = item && item.kind === 'word' ? `/api/destinations/${encodeURIComponent(id)}/word-template` : '#';
  $('destinationDialogTitle').textContent = item ? 'Изменить получателя' : 'Новый получатель';
  updateDestinationKind();
  openDialog('destinationDialog');
}

$('addDestination').addEventListener('click', () => showDestinationDialog());

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
    if (!confirm(`Удалить «${name}»? Получатель будет убран из всех маршрутов.`)) return;
    if (!discardDraftForRecipientChange()) return;
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
  if (!discardDraftForRecipientChange()) return;
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

function discardDraftForRecipientChange() {
  if (!rulesDirty) return true;
  return confirm('Есть неопубликованный черновик маршрутов. Отменить его и продолжить изменение получателей?');
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
      <div class="entity-actions"><a class="button ghost" href="/api/clients/${encodeURIComponent(client.id)}/config">Скачать файл настроек</a></div>
    </article>`).join('');
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
      headers: { 'Content-Type': 'application/json' },
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
    const blob = await response.blob();
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = `EAVC Connect Station-${data.get('number')}.json`;
    link.click();
    URL.revokeObjectURL(link.href);
    closeDialog('clientDialog');
    await loadClients();
    notify('Файл настроек проверен и скачан.', true);
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
  rules.destinations ||= {};
  rules.positions ||= {};
  rules.unknown ||= {};
  rulesDirty = Boolean(draft?.exists);
  currentPositionPage = 1;
  renderRules();
  await loadRuleHistory();
  if (rulesDirty) notify('Неопубликованный черновик восстановлен из центра.', true);
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

function decisionSummary(decisions) {
  const chips = sortedRuleDestinations().flatMap(destination => {
    const decision = decisions?.[destination.id];
    if (!decision || decision.action === 'skip') return [];
    const action = decision.action === 'review' ? 'На проверку' : 'Отправлять';
    const topic = destination.kind === 'telegram' && decision.thread_id ? ` · тема ${decision.thread_id}` : '';
    const type = destination.kind === 'telegram' ? 'Telegram' : 'DOCX';
    return [`<span class="route-chip${decision.action === 'review' ? ' review' : ''}">${escapeHTML(destination.name)} · ${type} · ${action}${escapeHTML(topic)}</span>`];
  });
  return chips.length ? chips.join('') : '<span class="route-chip empty">Не отправлять</span>';
}

function renderRules() {
  const allKeys = Object.keys(rules.positions).sort((left, right) => positionName(left).localeCompare(positionName(right), 'ru'));
  const query = $('positionSearch').value.trim().toLocaleLowerCase('ru-RU');
  const filtered = allKeys.filter(key => positionName(key).toLocaleLowerCase('ru-RU').includes(query));
  const pages = Math.max(1, Math.ceil(filtered.length / positionsPerPage));
  currentPositionPage = Math.min(currentPositionPage, pages);
  const start = (currentPositionPage - 1) * positionsPerPage;
  const visible = filtered.slice(start, start + positionsPerPage);

  $('rulesMeta').textContent = rulesDirty
    ? `Черновик сохранён · основа: версия ${rules.version} · позиций: ${allKeys.length}`
    : `Опубликована версия ${rules.version} · позиций: ${allKeys.length}`;
  $('draftBadge').classList.toggle('hidden', !rulesDirty);
  $('publishRules').disabled = !rulesDirty;
  $('positionPrev').disabled = currentPositionPage <= 1;
  $('positionNext').disabled = currentPositionPage >= pages;
  $('positionPage').textContent = `Страница ${currentPositionPage} из ${pages}`;

  $('unknownRoute').innerHTML = `
    <div><h3>Новые и неопознанные позиции</h3><p>Правило применяется, если отдельная позиция ещё не добавлена.</p></div>
    <div class="route-summary">${decisionSummary(rules.unknown)}</div>
    <div class="route-card-actions"><button class="button ghost" data-route-edit="__unknown__">Настроить</button></div>`;

  $('positionsList').innerHTML = visible.map(key => `
    <article class="route-card" data-position-key="${escapeHTML(key)}">
      <div><h3>${escapeHTML(positionName(key))}</h3><p>Индивидуальный маршрут</p></div>
      <div class="route-summary">${decisionSummary(rules.positions[key])}</div>
      <div class="route-card-actions"><button class="button ghost" data-action="edit">Изменить</button><button class="button danger" data-action="delete">Удалить</button></div>
    </article>`).join('');
}

$('positionSearch').addEventListener('input', () => {
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

function routeControl(destination, decision = { action: 'skip', thread_id: 0 }) {
  const presentation = routeDestinationPresentation(destination);
  const topic = destination.kind === 'telegram' ? `
    <label>Тема получателя<input name="thread_${escapeHTML(destination.id)}" type="number" min="0" value="${Number(decision.thread_id || 0)}"><small>0 — основная тема</small></label>` : `
    <div class="route-static-field"><span>Тема Telegram</span><strong>Не используется</strong><small>Файл сохраняется в папку</small></div>`;
  return `<div class="route-control" data-route-destination="${escapeHTML(destination.id)}">
    <div class="route-destination"><strong>${escapeHTML(destination.name)}</strong><span>${escapeHTML(presentation.type)}</span><small title="${escapeHTML(presentation.detail)}">${escapeHTML(presentation.detail)}</small></div>
    <label>Действие<select name="action_${escapeHTML(destination.id)}"><option value="skip"${decision.action === 'skip' ? ' selected' : ''}>Не отправлять</option><option value="deliver"${decision.action === 'deliver' ? ' selected' : ''}>Отправлять</option><option value="review"${decision.action === 'review' ? ' selected' : ''}>Отправить на проверку</option></select></label>
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
  form.elements.name.closest('label').classList.toggle('hidden', unknown);
  $('positionDialogTitle').textContent = unknown ? 'Новые и неопознанные позиции' : (key ? 'Изменить позицию' : 'Новая позиция');
  const destinationList = sortedRuleDestinations();
  $('routeEditor').innerHTML = destinationList.length
    ? destinationList.map(destination => routeControl(destination, decisions[destination.id])).join('')
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
    if (!confirm(`Удалить позицию «${positionName(key)}» из черновика?`)) return;
    const previous = cloneRules(rules);
    delete rules.positions[key];
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
    rules.unknown = decisions;
  } else {
    const newKey = normalizedPositionKey(data.get('name'));
    if (newKey !== originalKey && rules.positions[newKey]) {
      notify('Позиция с таким названием уже существует.');
      return;
    }
    if (originalKey) delete rules.positions[originalKey];
    rules.positions[newKey] = decisions;
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

$('publishRules').addEventListener('click', async event => {
  if (!rulesDirty || !rules) return;
  if (!confirm('Опубликовать черновик? Новые маршруты сразу начнут применяться к следующим событиям.')) return;
  const button = event.currentTarget;
  setButtonBusy(button, true, 'Публикуем…');
  try {
    await post('/api/rules', rules);
    await loadRules();
    notify('Маршруты опубликованы и уже действуют.', true);
  } catch (error) {
    notify(error.message);
  } finally {
    setButtonBusy(button, false);
  }
});

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
  if (!confirm(`Восстановить настройки из версии ${version}? Текущая версия останется в истории.`)) return;
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
