const element = id => document.getElementById(id);
const formatTime = value => value ? new Date(value).toLocaleString('ru-RU') : 'Ещё не было';
const escapeHTML = value => String(value ?? '').replace(/[&<>"']/g, character => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
})[character]);
let searchController;

function showMessage(text = '', tone = 'error') {
  const message = element('message');
  message.textContent = text;
  message.className = text ? `message ${tone}` : 'message';
}

async function requestJSON(url, options = {}) {
  const response = await fetch(url, options);
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || 'Не удалось выполнить действие.');
  return data;
}

async function refresh() {
  try {
    const status = await requestJSON('/api/status', {cache: 'no-store'});
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
    element('clientName').textContent = status.client_name || '—';
    element('clientNumber').textContent = status.client_number || '—';
    element('clientId').textContent = status.client_id || '—';
    element('dbDot').classList.toggle('ok', status.database_ok);
    element('tgDot').classList.toggle('ok', status.telegram_ok);
    element('ackDot').classList.toggle('ok', Boolean(status.last_acknowledged));
    element('dbText').textContent = status.database_ok ? 'Последнее чтение: ' + formatTime(status.last_database_read) : 'Нет подключения';
    element('tgText').textContent = status.telegram_ok ? 'Последняя загрузка: ' + formatTime(status.last_upload) : 'Нет подключения';
    element('ackText').textContent = status.last_acknowledged ? 'Последнее: ' + formatTime(status.last_acknowledged) : 'Ещё не получено';
	if (status.last_transport_check) element('ackText').textContent = 'Связь проверена: ' + formatTime(status.last_transport_check);
    element('pending').textContent = status.queue?.pending || 0;
    element('uploaded').textContent = status.queue?.uploaded || 0;
    element('acknowledged').textContent = status.queue?.acknowledged || 0;
    element('expired').textContent = status.queue?.expired || 0;
    element('controlStart').disabled = status.processing || !status.configured;
    element('controlStop').disabled = !status.processing;
    element('controlRefresh').disabled = !status.configured;
    if (status.eavc_path && !element('eavcPath').value) element('eavcPath').value = status.eavc_path;
  } catch (error) {
    console.error(error);
    showMessage('Интерфейс временно не отвечает. Программа попробует обновить состояние автоматически.');
  }
}

async function controlProcessing(action, button, busyText) {
  const original = button.textContent;
  button.disabled = true;
  button.textContent = busyText;
  try {
    await requestJSON(`/api/control/${action}`, {method: 'POST'});
    showMessage(action === 'stop' ? 'Обработка остановлена. Очередь сохранена.' : 'Команда выполнена.', 'success');
    await refresh();
  } catch (error) {
    showMessage(error.message);
  } finally {
    button.textContent = original;
  }
}

element('controlStart').addEventListener('click', event => controlProcessing('start', event.currentTarget, 'Запускаем…'));
element('controlStop').addEventListener('click', event => controlProcessing('stop', event.currentTarget, 'Останавливаем…'));
element('controlRefresh').addEventListener('click', event => controlProcessing('refresh', event.currentTarget, 'Обновляем…'));

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
      <p>${escapeHTML(item.reason)}</p><pre>${escapeHTML(item.content || 'Текст отсутствует')}</pre>
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
  if (!confirm('Очистить этот завершённый список? Ожидающие доставки сообщения затронуты не будут.')) return;
  try {
    const result = await requestJSON(`/api/queue/${queueState}`, {method: 'DELETE'});
    showMessage(`Удалено записей: ${result.removed}.`, 'success');
    await loadQueuePage(0);
    await refresh();
  } catch (error) {
    showMessage(error.message);
  }
});

async function searchEAVC(extended) {
  searchController = new AbortController();
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
  }
}

element('configFile').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
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
  }
});

element('quickSearch').addEventListener('click', () => searchEAVC(false));
element('fullSearch').addEventListener('click', () => searchEAVC(true));
element('cancelSearch').addEventListener('click', () => searchController?.abort());
element('saveEavc').addEventListener('click', () => selectEAVC(element('eavcPath').value));
element('checkTransport').addEventListener('click', async () => {
  showMessage('Отправляем проверочный пакет…', 'info');
  try {
    await requestJSON('/api/transport/check', {method: 'POST'});
    showMessage('Пакет отправлен. Ответ центра появится в блоке «Подтверждение центра».', 'success');
  } catch (error) {
    showMessage(error.message);
  }
});

refresh();
setInterval(refresh, 3000);
