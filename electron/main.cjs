const { app, BrowserWindow, ipcMain, dialog, clipboard } = require('electron');
const fs = require('node:fs');
const path = require('node:path');
const runtime = require('../scripts/runtime.cjs');
const { Transport } = require('./transport.cjs');
// backend do Electron tạo; external do npm run host tạo và launcher chịu trách nhiệm dừng.
let window, backend, external, quitting = false, exporting = false, activeSessionKey = '';
const transport = new Transport();
// Renderer chỉ gửi ID file; đường dẫn thực tế được chọn qua hộp thoại ở main process.
const fileInfos = new Map();
const measurements = [];
const conversation = new Map();
let lastConfig = {};
const settingsPath = () => path.join(app.getPath('userData'), 'connection.json');
function emit(event, value) { if (window && !window.isDestroyed()) window.webContents.send('relay:event', { event, value }); }
for (const event of ['status', 'stats', 'message', 'reset', 'notice', 'measurement', 'progress']) {
  transport.on(event, value => {
    if (event === 'message') {
      if (value.file?.id) fileInfos.set(value.file.id, value.file);
      conversation.set(value.id, { observedAt: new Date().toISOString(), mode: transport.config?.mode || '', direction: value.userId === transport.userId ? 'sent' : 'received',
        username: value.username, userId: value.userId, seq: value.seq, sentAt: value.sentAt, messageId: value.id, text: value.text,
        fileName: value.file?.name || '', fileBytes: value.file?.size || '' });
    }
    if (event === 'measurement') measurements.push({ measuredAt: new Date().toISOString(), room: transport.session?.room || '',
      username: value.username || transport.session?.username || '', ...value, http: transport.stats.http, rpc: transport.stats.rpc, chunks: transport.stats.chunks });
    if (event === 'reset') fileInfos.clear();
    emit(event, event === 'message' ? { ...value, mine: value.userId === transport.userId } : value);
  });
}
function available() { if (exporting || transport.busy) throw Error('Chờ truyền file hoặc xuất số liệu hoàn tất.'); }
// Điểm tiếp nhận IPC duy nhất: chỉ cung cấp các thao tác cụ thể của ứng dụng chat.
async function handle(action, value) {
  switch (action) {
    case 'init': {
      let saved = {}; try { saved = JSON.parse(await fs.promises.readFile(settingsPath(), 'utf8')); } catch {}
      return { saved, adapters: runtime.adapters(), host: external || (backend && { restPort: backend.restPort, grpcPort: backend.grpcPort }) };
    }
    case 'connect': {
      available();
      const config = { host: String(value.host || ''), room: String(value.room || ''), username: String(value.username || ''),
        mode: value.mode, pollMs: Number(value.pollMs), ...runtime.ports(value) };
      const sessionKey = `${config.host}:${config.restPort}:${config.grpcPort}/${config.room}/${config.username}`;
      if (activeSessionKey && activeSessionKey !== sessionKey) { measurements.length = 0; conversation.clear(); }
      if (value.role === 'host') {
        if (!external && (!backend || backend.child.exitCode !== null)) {
          try { backend = await runtime.startBackend({ ...config, onSpawn: b => { backend = b; } }, line => emit('notice', line)); runtime.printServer(backend); }
          catch (e) { backend = null; throw e; }
        }
        const active = external || backend;
        if (config.restPort !== active.restPort || config.grpcPort !== active.grpcPort) throw Error('Host đang chạy ở cổng khác. Dừng host rồi khởi động lại để đổi cổng.');
        // Client của host gọi backend ngay trên máy này; IP chia sẻ cho máy B là ô riêng.
        config.host = '127.0.0.1'; config.create = true;
      } else {
        if (external || backend) throw Error('Instance này đang làm server. Mở npm start ở terminal khác để chạy client.');
        config.create = false;
      }
      const result = await transport.connect(config); lastConfig = config; activeSessionKey = sessionKey;
      await fs.promises.mkdir(path.dirname(settingsPath()), { recursive: true });
      // Chỉ nhớ cấu hình kết nối, không lưu token phiên hay mật khẩu mạng Radmin.
      await fs.promises.writeFile(settingsPath(), JSON.stringify({ ...config, host: value.host || config.host, role: value.role, create: undefined }, null, 2));
      return { session: result, host: !!(external || backend) };
    }
    case 'send': available(); return transport.send(String(value));
    case 'mode': available(); await transport.switchMode(value); lastConfig.mode = value; return true;
    case 'upload': {
      available(); const selected = await dialog.showOpenDialog(window, { properties: ['openFile'], filters: [{ name: 'Text', extensions: ['txt'] }] });
      if (selected.canceled) return null;
      const info = await transport.upload(selected.filePaths[0]); fileInfos.set(info.id, info); return info;
    }
    case 'download': {
      available(); const info = fileInfos.get(value); if (!info) throw Error('File không nằm trong lịch sử của phòng hiện tại.');
      const chosen = await dialog.showSaveDialog(window, { defaultPath: path.basename(info.name) });
      if (!chosen.canceled) return transport.download(info, chosen.filePath); return null;
    }
    case 'copy': {
      if (!external && !backend) throw Error('Máy này chưa làm server.');
      const selected = String(value).trim();
      if (!/^[a-zA-Z0-9.-]+$/.test(selected) || ['0.0.0.0','localhost','127.0.0.1'].includes(selected)) throw Error('Chọn/nhập IP adapter Radmin để chia sẻ với máy B.');
      const active = external || backend;
      clipboard.writeText(`Server: ${selected}\nREST: ${active.restPort}\ngRPC: ${active.grpcPort}\nPhòng: ${lastConfig.room || ''}`); return true;
    }
    case 'benchmark': {
      available(); if (!transport.session) throw Error('Kết nối phòng trước khi xuất số liệu.');
      exporting = true;
      try { return await exportSession(String(value || '')); } finally { exporting = false; }
    }
    default: throw Error('Thao tác IPC không được hỗ trợ.');
  }
}
// Chỉ xuất các phép đo phát sinh từ thao tác thật; không tự gửi tin hoặc truyền file mẫu.
async function exportSession(note) {
  const rows = measurements.map(row => ({ ...row, note }));
  const messages = [...conversation.values()];
  if (!rows.length && !messages.length) throw Error('Chưa có tin nhắn hoặc thao tác thực tế để xuất.');
  const summary = [];
  const percentile = (values, p) => [...values].sort((a,b) => a-b)[Math.ceil(values.length*p)-1];
  for (const mode of ['rest','grpc']) for (const operation of ['message','upload','download']) {
    const values = rows.filter(r => r.mode === mode && r.operation === operation).map(r => Number(r.ms)).filter(Number.isFinite);
    summary.push({ mode, operation, samples: values.length, median: values.length ? percentile(values, .5) : null, p95: values.length ? percentile(values, .95) : null });
  }
  const selected = await dialog.showSaveDialog(window, { defaultPath: 'relay-session.csv', filters: [{ name: 'CSV', extensions: ['csv'] }] });
  if (!selected.canceled) {
    const sampleHeaders = ['measuredAt','username','room','mode','operation','ms','bytes','mibps','messageId','seq','text','fileName','fileId','note','http','rpc','chunks'];
    const messageHeaders = ['observedAt','mode','direction','username','userId','seq','sentAt','messageId','text','fileName','fileBytes'];
    const escape = value => { let cell = String(value ?? ''); if (/^[=+\-@]/.test(cell)) cell = "'" + cell; return '"' + cell.replaceAll('"','""') + '"'; };
    const csv = ['THAO_TAC_THUC_TE', sampleHeaders.join(','), ...rows.map(r => sampleHeaders.map(h => escape(r[h])).join(',')), '',
      'mode,operation,samples,median_ms,p95_ms', ...summary.map(r => [r.mode,r.operation,r.samples,r.median ?? '',r.p95 ?? ''].join(',')), '',
      'HOI_THOAI_QUAN_SAT', messageHeaders.join(','), ...messages.map(r => messageHeaders.map(h => escape(r[h])).join(','))].join('\r\n');
    await fs.promises.writeFile(selected.filePath, '\uFEFF' + csv);
  }
  return { summary, errors: 0, samples: rows.length, messages: messages.length, canceled: selected.canceled };
}
app.whenReady().then(() => {
  try { external = process.env.RELAY_EXTERNAL_HOST ? JSON.parse(process.env.RELAY_EXTERNAL_HOST) : null; } catch {}
  window = new BrowserWindow({ width: 1250, height: 900, show: process.env.RELAY_SMOKE !== '1', webPreferences: { preload: path.join(__dirname, 'preload.cjs'), contextIsolation: true, nodeIntegration: false, sandbox: true } });
  // Giữ renderer ở trang nội bộ, không cho mở trang ngoài với quyền IPC của ứng dụng.
  window.webContents.setWindowOpenHandler(() => ({ action: 'deny' }));
  window.webContents.on('will-navigate', event => event.preventDefault());
  ipcMain.handle('relay:call', async (event, action, value) => {
    if (event.sender !== window.webContents || event.senderFrame !== window.webContents.mainFrame) throw Error('IPC sender không hợp lệ.');
    try { return { ok: true, value: await handle(action, value) }; }
    catch (e) { emit('notice', e.message); return { ok: false, error: e.message }; }
  });
  // Host phải xác nhận trước khi đóng vì việc dừng backend ảnh hưởng mọi client.
  window.on('close', event => {
    if (quitting) return;
    event.preventDefault();
    if ((backend || external) && dialog.showMessageBoxSync(window, { type: 'question', buttons: ['Ở lại', 'Dừng host'], defaultId: 0, cancelId: 0,
      message: 'Đóng ứng dụng host sẽ dừng server. Các client sẽ mất kết nối. Tiếp tục?' }) !== 1) return;
    quitting = true; transport.stop(); Promise.resolve(backend?.stop()).finally(() => app.quit());
  });
  window.loadFile(path.join(__dirname, 'index.html'));
});
app.on('window-all-closed', () => app.quit());
app.on('before-quit', () => transport.stop());
