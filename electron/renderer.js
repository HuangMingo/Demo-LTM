const $ = id => document.getElementById(id);
// Renderer chỉ quản lý giao diện; HTTP, gRPC và truy cập file đều nằm ở main process.
let mode = 'rest', connected = false, busy = false, hostActive = false;
const ids = new Set();
function log(text) { const row = document.createElement('div'); row.textContent = new Date().toLocaleTimeString() + ' · ' + text; $('log').prepend(row); while ($('log').children.length > 150) $('log').lastChild.remove(); }
// Khóa thao tác có thể đổi kết nối trong lúc truyền file hoặc benchmark.
function controls() {
  for (const id of ['send','text','upload','benchmark']) $(id).disabled = !connected || busy;
  for (const id of ['connect','rest','grpc','role','host','username','room','restPort','grpcPort','pollMs']) $(id).disabled = busy || (id === 'role' && hostActive);
  document.querySelectorAll('[data-download]').forEach(b => { b.disabled = !connected || busy; });
}
// Gom xử lý lỗi và mở khóa UI vào finally để tác vụ thất bại không làm treo giao diện.
async function action(task, lock = true) {
  if (lock) { busy = true; controls(); }
  try { return await task(); } catch (e) { log(e.message); $('status').textContent = e.message; }
  finally { if (lock) { busy = false; controls(); } }
}
function selectedMode(value) { mode = value; $('rest').classList.toggle('active', mode === 'rest'); $('grpc').classList.toggle('active', mode === 'grpc'); }
function showHost() { $('hostInfo').hidden = !hostActive; $('roleInfo').textContent = hostActive ? 'Máy này đang làm server. Các client dùng IP adapter Radmin của máy này và cùng mã phòng.' : 'Client không khởi động backend. localhost chỉ dùng khi thử trên cùng máy.'; }
// Loại trùng theo ID, chèn theo seq của server và luôn hiển thị nội dung bằng textContent.
function message(record) {
  if (ids.has(record.id)) return; ids.add(record.id);
  if (!document.querySelector('.bubble')) $('chatbox').replaceChildren();
  const row = document.createElement('div'); row.className = 'bubble' + (record.mine ? ' mine' : ''); row.dataset.seq = record.seq;
  const meta = document.createElement('div'); meta.className = 'bubble-meta'; meta.textContent = `${record.username} · #${record.seq} · ${new Date(Number(record.sentAt)).toLocaleTimeString()}`;
  const text = document.createElement('div'); text.className = 'bubble-text'; text.textContent = record.text;
  if (record.file?.id) {
    const button = document.createElement('button'); button.className = 'btn secondary'; button.dataset.download = record.file.id;
    button.textContent = `Tải ${record.file.name} (${record.file.size} byte)`; button.disabled = busy || !connected;
    button.onclick = () => action(() => window.relay.call('download', record.file.id)); text.append(button);
  }
  row.append(meta, text);
  const next = [...$('chatbox').children].find(element => Number(element.dataset.seq) > Number(record.seq));
  $('chatbox').insertBefore(row, next || null); while ($('chatbox').children.length > 500) $('chatbox').firstChild.remove(); $('chatbox').scrollTop = $('chatbox').scrollHeight;
}
// Nhận sự kiện nhỏ từ main: trạng thái, metadata tin/file, số liệu và tiến trình truyền.
window.relay.onEvent(({ event, value }) => {
  if (event === 'status') { connected = value.connected; $('status').textContent = value.text; controls(); }
  if (event === 'notice') log(value);
  if (event === 'reset') { ids.clear(); $('chatbox').replaceChildren(); if (value.restarted) log('Server đã khởi động lại: phiên trước và dữ liệu cũ không còn.'); }
  if (event === 'message') message(value);
  if (event === 'stats') $('stats').textContent = `HTTP requests: ${value.http}\nRPC calls: ${value.rpc}\nMessages/chunks: ${value.chunks}\nTin nhận duy nhất: ${value.messages}\nLỗi: ${value.errors}`;
  if (event === 'measurement') $('measurement').textContent = `${value.mode.toUpperCase()} · ${value.operation}\n${value.ms.toFixed(2)} ms` + (value.mibps === undefined ? '' : `\n${value.mibps.toFixed(2)} MiB/s`);
  if (event === 'progress') { $('progress').value = value.total ? value.bytes / value.total : 1; $('progressText').textContent = `${value.operation}: ${value.bytes} / ${value.total} byte`; }
});
$('connect').onclick = () => action(async () => {
  connected = false; controls();
  const config = Object.fromEntries(['role','username','room','host','restPort','grpcPort','pollMs'].map(id => [id,$(id).value])); config.mode = mode;
  const result = await window.relay.call('connect', config); hostActive = result.host; showHost();
  $('identity').textContent = `${result.host ? 'HOST' : 'CLIENT'} · ${result.session.username} · ${result.session.room}`;
});
for (const value of ['rest','grpc']) $(value).onclick = () => action(async () => { if (connected) await window.relay.call('mode', value); selectedMode(value); });
$('compose').onsubmit = async event => {
  event.preventDefault();
  const input = $('text'); const text = input.value;
  if (text.trim()) await action(async () => { await window.relay.call('send', text); input.value = ''; });
  if (connected) input.focus();
};
$('upload').onclick = () => action(() => window.relay.call('upload'));
$('copy').onclick = () => action(async () => { await window.relay.call('copy', $('shareIp').value); log('Đã sao chép IP, cổng và mã phòng.'); });
$('adapters').onchange = () => { $('shareIp').value = $('adapters').value; };
$('benchmark').onclick = () => action(async () => {
  const result = await window.relay.call('benchmark', $('vpnNote').value);
  const measured = result.summary.filter(r => r.samples).map(r => `${r.mode} ${r.operation}: n=${r.samples}, median=${r.median?.toFixed(2) ?? '—'} ms, p95=${r.p95?.toFixed(2) ?? '—'} ms`).join('\n');
  $('benchmarkResult').textContent = `${result.canceled ? 'Đã hủy lưu file.\n' : ''}${measured || 'Chưa có thao tác có độ trễ.'}\nMẫu thao tác: ${result.samples}\nTin nhắn quan sát: ${result.messages}\nLỗi: ${result.errors}`;
});
action(async () => {
  const initial = await window.relay.call('init');
  for (const id of ['role','username','room','host','restPort','grpcPort','pollMs']) if (initial.saved[id] !== undefined) $(id).value = initial.saved[id];
  selectedMode(initial.saved.mode || 'rest');
  for (const adapter of initial.adapters) { const option = document.createElement('option'); option.value = adapter.address; option.textContent = `${adapter.radmin ? 'RADMIN VPN · ' : ''}${adapter.name}${adapter.description ? ' ('+adapter.description+')' : ''} · ${adapter.address}`; $('adapters').append(option); }
  $('shareIp').value = initial.adapters[0]?.address || '';
  if (initial.host) { hostActive = true; $('role').value = 'host'; $('host').value = '127.0.0.1'; $('restPort').value = initial.host.restPort; $('grpcPort').value = initial.host.grpcPort; }
  else $('role').value = 'client';
  showHost();
});
