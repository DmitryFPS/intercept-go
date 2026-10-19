const element = id => document.getElementById(id);
const formatTime = value => value ? new Date(value).toLocaleString('ru-RU') : 'Ещё не было';
let searchController;

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
      running: 'ВСЁ В ПОРЯДКЕ', waiting: 'ПОВТОРИМ АВТОМАТИЧЕСКИ',
      error: 'ТРЕБУЕТСЯ ПОМОЩЬ', setup: 'ПЕРВЫЙ ЗАПУСК', starting: 'ЗАПУСК'
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
    if (status.eavc_path && !element('eavcPath').value) element('eavcPath').value = status.eavc_path;
  } catch (error) {
    element('message').textContent = 'Интерфейс временно не отвечает: ' + error.message;
  }
}

async function searchEAVC(extended) {
  searchController = new AbortController();
  element('cancelSearch').classList.remove('hidden');
  element('message').textContent = extended ? 'Ищем базы EAVC на дисках…' : 'Ищем EAVC рядом с программой…';
  element('eavcCandidates').replaceChildren();
  try {
    const data = await requestJSON('/api/eavc/search', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({extended}), signal: searchController.signal
    });
    showCandidates(data.candidates || []);
  } catch (error) {
    element('message').textContent = error.name === 'AbortError' ? 'Поиск остановлен.' : error.message;
  } finally {
    searchController = undefined;
    element('cancelSearch').classList.add('hidden');
  }
}

function showCandidates(candidates) {
  if (candidates.length === 0) {
    element('message').textContent = 'Подходящая база EAVC не найдена. Можно указать путь вручную.';
    return;
  }
  element('message').textContent = candidates.length === 1 ? 'База найдена и выбрана.' : 'Найдено несколько баз. Выберите рабочую.';
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
  element('message').textContent = 'Проверяем выбранную базу…';
  try {
    await requestJSON('/api/eavc/select', {
      method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({path})
    });
    element('eavcPath').value = path;
    element('message').textContent = 'База EAVC выбрана. Клиент запускается.';
    await refresh();
  } catch (error) {
    element('message').textContent = error.message;
  }
}

element('configFile').addEventListener('change', async event => {
  const file = event.target.files[0];
  if (!file) return;
  element('message').textContent = 'Проверяем настройки…';
  const form = new FormData();
  form.append('config', file);
  try {
    await requestJSON('/api/config/import', {method: 'POST', body: form});
    element('message').textContent = 'Настройки приняты. Программа запускается.';
    await refresh();
  } catch (error) {
    element('message').textContent = error.message;
  } finally {
    event.target.value = '';
  }
});

element('quickSearch').addEventListener('click', () => searchEAVC(false));
element('fullSearch').addEventListener('click', () => searchEAVC(true));
element('cancelSearch').addEventListener('click', () => searchController?.abort());
element('saveEavc').addEventListener('click', () => selectEAVC(element('eavcPath').value));
element('checkTransport').addEventListener('click', async () => {
  element('message').textContent = 'Отправляем проверочный пакет…';
  try {
    await requestJSON('/api/transport/check', {method: 'POST'});
    element('message').textContent = 'Пакет отправлен. Ответ сервера появится в блоке «Подтверждение сервера».';
  } catch (error) {
    element('message').textContent = error.message;
  }
});

refresh();
setInterval(refresh, 3000);
