const element = id => document.getElementById(id);
const hashAccess = new URLSearchParams(location.hash.slice(1)).get('access');
if (hashAccess) {
  sessionStorage.setItem('eavc_access', hashAccess);
  history.replaceState(null, '', location.pathname + location.search);
}
const accessToken = sessionStorage.getItem('eavc_access') || '';
const formatTime = value => value ? new Date(value).toLocaleString('ru-RU') : 'Ещё не было';
const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g, character => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
})[character]);
let searchController;
let lastStatus;
let operationBusy = false;

function appConfirm({ title, message, confirmLabel = 'Продолжить', danger = false }) {
  return new Promise(resolve => {
    const dialog = document.createElement('dialog');
    dialog.className = 'app-confirm';
    dialog.innerHTML = `
      <form method="dialog" class="app-confirm-card">
        <div class="app-confirm-icon ${danger ? 'danger' : ''}" aria-hidden="true">${danger ? '!' : '✓'}</div>
        <div><h2></h2><p></p></div>
        <div class="app-confirm-actions">
          <button class="secondary" value="cancel">Отмена</button>
          <button class="primary ${danger ? 'danger' : ''}" value="confirm"></button>
        </div>
      </form>`;
    dialog.querySelector('h2').textContent = title;
    dialog.querySelector('p').textContent = message;
    dialog.querySelector('[value="confirm"]').textContent = confirmLabel;
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
    dialog.querySelector('[value="cancel"]').focus();
  });
}

function setDisabled(id, disabled) {
  const control = element(id);
  control.disabled = disabled;
  control.setAttribute('aria-disabled', String(disabled));
}

function setUploadDisabled(labelId, inputId, disabled) {
  const label = element(labelId);
  element(inputId).disabled = disabled;
  label.classList.toggle('disabled', disabled);
  label.setAttribute('aria-disabled', String(disabled));
}

function applyActionAvailability(status = lastStatus) {
  if (!status) return;
  const processing = Boolean(status.processing);
  const configured = Boolean(status.configured);
  setDisabled('controlStart', operationBusy || !configured || processing);
  setDisabled('controlStop', operationBusy || !processing);
  setDisabled('controlRefresh', operationBusy || !configured || !processing);
  setDisabled('checkTransport', operationBusy || !configured || !processing);
  for (const id of ['downloadDiagnostics', 'saveEavc', 'quickSearch', 'fullSearch', 'activateClient']) {
    setDisabled(id, operationBusy);
  }
  setUploadDisabled('configUpload', 'configFile', operationBusy);
  setUploadDisabled('updateUpload', 'updateFile', operationBusy);
}

function setOperationBusy(busy) {
  operationBusy = busy;
  applyActionAvailability();
}

function showMessage(text = '', tone = 'error') {
  const message = element('message');
  message.textContent = text;
  message.className = text ? `message ${tone}` : 'message';
}

async function requestJSON(url, options = {}) {
	options.headers = {...(options.headers || {}), 'X-EAVC-Access': accessToken};
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Не удалось выполнить действие.');
  return data;
}

async function downloadAPI(url, fallbackName) {
  const response = await fetch(url, {cache: 'no-store', headers: {'X-EAVC-Access': accessToken}});
  if (!response.ok) throw new Error('Не удалось подготовить файл.');
  const blob = await response.blob();
  const link = document.createElement('a');
  link.href = URL.createObjectURL(blob);
  link.download = fallbackName;
  link.click();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
}

async function refresh() {
  try {
    const status = await requestJSON('/api/status', {cache: 'no-store'});
    lastStatus = status;
    element('hero').className = 'hero ' + status.state;
    element('title').textContent = status.title;
    element('detail').textContent = status.detail;
    element('stateLabel').textContent = ({
      running: 'ВСЁ В ПОРЯДКЕ', waiting: 'НЕТ СВЯЗИ С ЦЕНТРОМ',
      error: 'ТРЕБУЕТСЯ ПОМОЩЬ', setup: 'ПЕРВЫЙ ЗАПУСК', starting: 'ЗАПУСК',
      paused: 'ОСТАНОВЛЕНО'
    })[status.state] || 'СОСТОЯНИЕ';
    element('version').textContent = 'Версия ' + status.version;
    element('identity').classList.toggle('hidden', !status.configured);
    element('activation').classList.toggle('hidden', status.configured);
    element('clientName').textContent = status.client_name || '—';
    element('clientNumber').textContent = status.client_number || '—';
    element('clientId').textContent = status.client_id || '—';
    element('dbDot').classList.toggle('ok', status.database_ok);
    element('tgDot').classList.toggle('ok', status.telegram_ok);
    element('ackDot').classList.toggle('ok', Boolean(status.last_acknowledged || status.last_transport_check));
    element('dbText').textContent = status.database_ok ? 'Последнее чтение: ' + formatTime(status.last_database_read) : 'Нет подключения';
    element('tgText').textContent = status.telegram_ok ? 'Последняя загрузка: ' + formatTime(status.last_upload) : 'Нет подключения';
    element('ackText').textContent = status.last_acknowledged ? 'Последнее: ' + formatTime(status.last_acknowledged) : 'Ещё не получено';
	if (status.last_transport_check) element('ackText').textContent = 'Связь проверена: ' + formatTime(status.last_transport_check);
    element('pending').textContent = status.queue?.pending || 0;
    element('uploaded').textContent = status.queue?.uploaded || 0;
    element('acknowledged').textContent = status.queue?.acknowledged || 0;
    element('expired').textContent = status.queue?.expired || 0;
    applyActionAvailability(status);
    if (status.eavc_path && !element('eavcPath').value) element('eavcPath').value = status.eavc_path;
  } catch (error) {
    console.error(error);
    showMessage('Интерфейс временно не отвечает. Программа попробует обновить состояние автоматически.');
  }
}

async function controlProcessing(action, button, busyText) {
  const original = button.textContent;
  setOperationBusy(true);
  button.textContent = busyText;
  try {
    await requestJSON(`/api/control/${action}`, {method: 'POST'});
    showMessage(action === 'stop' ? 'Обработка остановлена. Очередь сохранена.' : 'Команда выполнена.', 'success');
  } catch (error) {
    showMessage(error.message);
  } finally {
    button.textContent = original;
    try { await refresh(); } finally { setOperationBusy(false); }
  }
}

element('controlStart').addEventListener('click', event => controlProcessing('start', event.currentTarget, 'Запускаем…'));
element('controlStop').addEventListener('click', event => controlProcessing('stop', event.currentTarget, 'Останавливаем…'));
element('controlRefresh').addEventListener('click', event => controlProcessing('refresh', event.currentTarget, 'Обновляем…'));

element('activateClient').addEventListener('click', async event => {
  const button = event.currentTarget;
  const code = element('activationCode').value.trim();
  if (!code) {
    showMessage('Введите код активации, показанный центром управления.');
    return;
  }
  const label = button.textContent;
  setOperationBusy(true);
  button.textContent = 'Проверяем…';
  showMessage('Расшифровываем и проверяем настройки…', 'info');
  try {
    await requestJSON('/api/config/activate', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({code})
    });
    element('activationCode').value = '';
    showMessage('Рабочая точка активирована. Программа запускается.', 'success');
    await refresh();
  } catch (error) {
    showMessage(error.message);
  } finally {
    button.textContent = label;
    setOperationBusy(false);
  }
});

const queueLabels = {
  pending: {
    title: 'Ожидают отправки',
    description: 'Сообщения ещё не переданы. Программа отправит их автоматически; удалять их вручную нельзя.'
  },
  uploaded: {
    title: 'Загружены, ждут центр',
    description: 'Сообщения переданы через Telegram и исчезнут отсюда после подтверждения центра.'
  },
  acknowledged: {
    title: 'Подтверждены центром',
    description: 'Центр подтвердил получение. Этот архив можно очистить вручную.'
  },
  expired: {
    title: 'Не отправлены старше 30 минут',
    description: 'Срок безопасной отправки истёк. Эти сообщения остались только в локальном архиве.'
  }
};
const queuePageSize = 20;
let queueState = 'pending';
let queueOffset = 0;

async function loadQueuePage(offset = 0) {
  const data = await requestJSON(`/api/queue/${queueState}?limit=${queuePageSize}&offset=${Math.max(0, offset)}`, {cache: 'no-store'});
  queueOffset = data.offset;
  const list = element('queueList');
  if (!data.items.length) {
    list.innerHTML = '<p class="empty-expired">В этом списке сообщений нет.</p>';
  } else {
    list.innerHTML = data.items.map(item => `<article>
      <div class="expired-meta"><strong>${escapeHTML(item.position || 'Позиция не указана')}</strong><span>${escapeHTML(formatTime(item.source_at || item.status_at))}</span></div>
      <p>${escapeHTML(item.reason)}</p>
      <p class="content-note">${escapeHTML(item.content_note || (item.content ? 'Текст перехвата, прочитанный Клиентом из EAVC.' : 'EAVC передал запись без текста.'))}</p>
      <pre>${escapeHTML(item.content_kind === 'text' ? item.content : (item.content_kind === 'deleted_block' ? 'Временной блок удалён в EAVC' : 'Текст перехвата отсутствует'))}</pre>
      <dl class="queue-trace">
        <div><dt>Код трассировки</dt><dd>${escapeHTML(item.event_id)}</dd></div>
        <div><dt>Запись EAVC</dt><dd>${escapeHTML(item.source_record || 'не указана')}</dd></div>
        <div><dt>UUID записи EAVC</dt><dd>${escapeHTML(item.source_uuid || 'не указан')}</dd></div>
        <div><dt>UUID временного блока</dt><dd>${escapeHTML(item.source_block_uuid || 'не указан')}</dd></div>
        <div><dt>Версия записи</dt><dd>${escapeHTML(item.source_version || 'не указана')}</dd></div>
        <div><dt>Отпечаток содержимого</dt><dd>${escapeHTML(item.content_digest || 'не указан')}</dd></div>
        <div><dt>Попытки передачи</dt><dd>${escapeHTML(item.attempts)}</dd></div>
        <div><dt>Пакет, подтверждённый Центром</dt><dd>${escapeHTML(item.center_transport_message_id || 'ID не передан Центром')}</dd></div>
        <div><dt>Последняя загрузка Клиента в Telegram</dt><dd>${escapeHTML(item.telegram_upload_message_id || 'Telegram не вернул ID')}</dd></div>
      </dl>
    </article>`).join('');
  }
  const current = data.total ? Math.floor(data.offset / data.limit) + 1 : 1;
  const pages = Math.max(1, Math.ceil(data.total / data.limit));
  element('queuePage').textContent = `Страница ${current} из ${pages} · всего ${data.total}`;
  element('queuePrev').disabled = data.offset === 0;
  element('queueNext').disabled = data.offset + data.limit >= data.total;
  element('clearQueue').classList.toggle('hidden', !data.clearable || data.total === 0);
}

document.querySelectorAll('[data-queue-state]').forEach(button => button.addEventListener('click', async () => {
  queueState = button.dataset.queueState;
  queueOffset = 0;
  element('queueDialogTitle').textContent = queueLabels[queueState].title;
  element('queueDialogDescription').textContent = queueLabels[queueState].description;
  try {
    await loadQueuePage();
    element('queueDialog').showModal();
  } catch (error) {
    showMessage(error.message);
  }
}));
element('closeQueue').addEventListener('click', () => element('queueDialog').close());
element('queuePrev').addEventListener('click', () => loadQueuePage(queueOffset - queuePageSize));
element('queueNext').addEventListener('click', () => loadQueuePage(queueOffset + queuePageSize));
element('clearQueue').addEventListener('click', async () => {
  if (!await appConfirm({
    title: 'Очистить список?',
    message: 'Будут удалены только завершённые записи. Ожидающие доставки останутся в очереди.',
    confirmLabel: 'Очистить', danger: true
  })) return;
  const button = element('clearQueue');
  const label = button.textContent;
  button.disabled = true;
  button.textContent = 'Очистка…';
  try {
    const result = await requestJSON(`/api/queue/${queueState}`, {method: 'DELETE'});
    showMessage(`Удалено записей: ${result.removed}.`, 'success');
    await loadQueuePage(0);
    await refresh();
  } catch (error) {
    showMessage(error.message);
  } finally {
    button.disabled = false;
    button.textContent = label;
  }
});

async function searchEAVC(extended) {
  searchController = new AbortController();
  setOperationBusy(true);
  element('cancelSearch').classList.remove('hidden');
  showMessage(extended ? 'Ищем базы EAVC на дисках…' : 'Ищем EAVC рядом с программой…', 'info');
  element('eavcCandidates').replaceChildren();
  try {
    const data = await requestJSON('/api/eavc/search', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({extended}), signal: searchController.signal
    });
    showCandidates(data.candidates || []);
  } catch (error) {
    showMessage(error.name === 'AbortError' ? 'Поиск остановлен.' : error.message, error.name === 'AbortError' ? 'info' : 'error');
  } finally {
    searchController = undefined;
    element('cancelSearch').classList.add('hidden');
    setOperationBusy(false);
  }
}

function showCandidates(candidates) {
  if (candidates.length === 0) {
    showMessage('Подходящая база EAVC не найдена. Можно указать путь вручную.');
    return;
  }
  showMessage(candidates.length === 1 ? 'База найдена и выбрана.' : 'Найдено несколько баз. Выберите рабочую.', candidates.length === 1 ? 'success' : 'info');
  for (const candidate of candidates) {
    const button = document.createElement('button');
    button.type = 'button';
    const path = document.createElement('strong');
    path.textContent = candidate.path;
    const date = document.createElement('small');
    date.textContent = 'Изменена: ' + formatTime(candidate.modified_at);
    button.append(path, date);
    button.addEventListener('click', () => selectEAVC(candidate.path));
    element('eavcCandidates').append(button);
  }
}

async function selectEAVC(path) {
  setOperationBusy(true);
  showMessage('Проверяем выбранную базу…', 'info');
  try {
    await requestJSON('/api/eavc/select', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({path})
    });
    element('eavcPath').value = path;
    showMessage('База EAVC выбрана. Рабочая точка запускается.', 'success');
    await refresh();
  } catch (error) {
    showMessage(error.message);
  } finally {
    setOperationBusy(false);
  }
}

element('configFile').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  setOperationBusy(true);
  showMessage('Проверяем настройки…', 'info');
  const form = new FormData();
  form.append('config', file);
  try {
    await requestJSON('/api/config/import', {method: 'POST', body: form});
    showMessage('Настройки приняты. Программа запускается.', 'success');
    await refresh();
  } catch (error) {
    showMessage(error.message);
  } finally {
    event.target.value = '';
    setOperationBusy(false);
  }
});

element('quickSearch').addEventListener('click', () => searchEAVC(false));
element('fullSearch').addEventListener('click', () => searchEAVC(true));
element('cancelSearch').addEventListener('click', () => searchController?.abort());
element('saveEavc').addEventListener('click', () => selectEAVC(element('eavcPath').value));
element('checkTransport').addEventListener('click', async () => {
  setOperationBusy(true);
  showMessage('Отправляем проверочный пакет…', 'info');
  try {
    await requestJSON('/api/transport/check', {method: 'POST'});
    showMessage('Пакет отправлен. Ответ центра появится в блоке «Подтверждение центра».', 'success');
  } catch (error) {
    showMessage(error.message);
  } finally {
    setOperationBusy(false);
  }
});
element('downloadDiagnostics').addEventListener('click', async () => {
  setOperationBusy(true);
  try { await downloadAPI('/api/diagnostics', 'EAVC-Connect-Station-diagnostics.zip'); }
  catch (error) { showMessage(error.message); }
  finally { setOperationBusy(false); }
});

element('updateFile').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  if (!await appConfirm({
    title: 'Установить обновление?',
    message: 'Клиент проверит обновление, перезапустится и вернёт предыдущую версию, если проверка не пройдёт.',
    confirmLabel: 'Установить'
  })) {
    event.target.value = '';
    return;
  }
  const form = new FormData();
  form.append('update', file);
  setOperationBusy(true);
  showMessage('Проверяем цифровую подпись и готовим обновление…', 'info');
  try {
    const result = await requestJSON('/api/update', {method: 'POST', body: form});
    showMessage(result.message + ' После запуска окно снова станет доступно.', 'success');
  } catch (error) {
    showMessage(error.message);
  } finally {
    event.target.value = '';
    setOperationBusy(false);
  }
});

refresh();
setInterval(refresh, 3000);
