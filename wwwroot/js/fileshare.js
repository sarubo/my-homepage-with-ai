const input = document.getElementById('file-input');
const form = document.getElementById('upload-form');
const message = document.getElementById('message');
const list = document.getElementById('file-list');
const selectAll = document.getElementById('select-all');

function setMessage(text, isError = false) {
  message.textContent = text;
  message.className = isError ? 'error' : 'success';
}

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  const unit = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / (1024 ** unit)).toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
}

function selectedIds() {
  return [...list.querySelectorAll('input[type=checkbox]:checked')].map((item) => item.value);
}

async function refresh() {
  const response = await fetch('/api/files');
  if (!response.ok) throw new Error('一覧を取得できませんでした。');
  const files = await response.json();
  list.replaceChildren();
  selectAll.checked = false;

  for (const file of files) {
    const row = document.createElement('tr');
    const selectCell = document.createElement('td');
    selectCell.className = 'file-select';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = file.id;
    checkbox.setAttribute('aria-label', `${file.name} を選択`);
    checkbox.addEventListener('change', () => {
      const boxes = list.querySelectorAll('input[type=checkbox]');
      selectAll.checked = boxes.length > 0 && [...boxes].every((box) => box.checked);
    });
    selectCell.append(checkbox);

    const nameCell = document.createElement('td');
    nameCell.className = 'file-name';
    nameCell.dataset.label = '名前';
    nameCell.textContent = file.name;
    const sizeCell = document.createElement('td');
    sizeCell.dataset.label = 'サイズ';
    sizeCell.textContent = formatBytes(file.sizeBytes);
    const timeCell = document.createElement('td');
    timeCell.dataset.label = '登録時刻 (UTC)';
    timeCell.textContent = file.uploadedAtUtc;
    const actionCell = document.createElement('td');
    actionCell.dataset.label = '操作';
    const download = document.createElement('a');
    download.href = `/api/files/${encodeURIComponent(file.id)}/download`;
    download.textContent = 'ダウンロード';
    actionCell.append(download);
    row.append(selectCell, nameCell, sizeCell, timeCell, actionCell);
    list.append(row);
  }
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (input.files.length === 0) return;
  const data = new FormData();
  for (const file of input.files) data.append('files', file, file.name);
  setMessage('アップロード中…');
  const response = await fetch('/api/files', { method: 'POST', body: data });
  if (!response.ok) {
    setMessage(await response.text() || 'アップロードに失敗しました。', true);
    return;
  }
  input.value = '';
  setMessage('アップロードしました。');
  await refresh();
});

document.getElementById('delete-selected').addEventListener('click', async () => {
  const ids = selectedIds();
  if (ids.length === 0) return;
  const response = await fetch('/api/files', {
    method: 'DELETE',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ ids })
  });
  if (!response.ok) {
    setMessage('削除に失敗しました。', true);
    return;
  }
  const result = await response.json();
  setMessage(`${result.deleted} 件を削除しました。`);
  await refresh();
});

document.getElementById('refresh').addEventListener('click', () => refresh().catch((error) => setMessage(error.message, true)));
selectAll.addEventListener('change', () => {
  for (const box of list.querySelectorAll('input[type=checkbox]')) box.checked = selectAll.checked;
});

refresh().catch((error) => setMessage(error.message, true));
