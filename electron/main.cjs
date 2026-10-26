const { app, BrowserWindow, ipcMain, dialog, clipboard } = require('electron');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { randomUUID } = require('node:crypto');
const runtime = require('../scripts/runtime.cjs');
const { Transport } = require('./transport.cjs');
// backend do Electron tạo; external do npm run host tạo và launcher chịu trách nhiệm dừng.
let window, backend, external, quitting = false, benchmark = false;
const transport = new Transport();
// Renderer chỉ gửi ID file; đường dẫn thực tế được chọn qua hộp thoại ở main process.
const fileInfos = new Map();
let lastConfig = {};
const settingsPath = () => path.join(app.getPath('userData'), 'connection.json');
function emit(event, value) { if (window && !window.isDestroyed()) window.webContents.send('relay:event', { event, value }); }
for (const event of ['status', 'stats', 'message', 'reset', 'notice', 'measurement', 'progress']) {
  transport.on(event, value => { if (event === 'message' && value.file?.id) fileInfos.set(value.file.id, value.file); if (event === 'reset') fileInfos.clear(); emit(event, event === 'message' ? { ...value, mine: value.userId === transport.userId } : value); });
}
function available() { if (benchmark || transport.busy) throw Error('Chờ truyền file/benchmark hoàn tất.'); }
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
      const result = await transport.connect(config); lastConfig = config;
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
      available(); if (!transport.session) throw Error('Kết nối phòng trước khi benchmark.');
      benchmark = true;
      try { return await runBenchmark(String(value || '')); } finally { benchmark = false; }
    }
    default: throw Error('Thao tác IPC không được hỗ trợ.');
  }
}
// Client benchmark riêng giúp tách counters/phép đo khỏi hoạt động chat của người dùng.
async function runBenchmark(note) {
  const bench = new Transport();
  const directory = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'relay-bench-'));
  const fixture = path.join(directory, 'fixture.txt');
  await fs.promises.writeFile(fixture, Buffer.alloc(256 * 1024, 65));
  const rows = []; const summary = [];
  const percentile = (values, p) => [...values].sort((a,b) => a-b)[Math.ceil(values.length*p)-1];
  try {
    // Vòng 0 làm nóng; ba vòng sau luân phiên thứ tự để giảm thiên lệch do chạy trước/sau.
    for (let round = 0; round < 4; round++) {
      for (const mode of round % 2 ? ['grpc','rest'] : ['rest','grpc']) {
        const measurements = [];
        const receive = m => measurements.push(m); bench.on('measurement', receive);
        let error = '';
        try {
          emit('notice', `Benchmark ${round ? 'lượt '+round : 'warm-up'} · ${mode.toUpperCase()}`);
          await bench.connect({ ...lastConfig, username: 'Benchmark', create: false, mode });
          for (let i = 0; i < 5; i++) await bench.send('REST/gRPC benchmark: cùng nội dung ✓');
          const info = await bench.upload(fixture);
          await bench.download(info, path.join(directory, `download-${round}-${mode}.txt`));
        } catch (e) { error = e.message; }
        finally { bench.off('measurement', receive); bench.stop(); }
        // Không đưa mẫu warm-up vào trung vị, p95 hoặc các dòng số liệu CSV.
        if (round) {
          for (const m of measurements) rows.push({ round, ...m, error: '', note, host: lastConfig.host, restPort: lastConfig.restPort, grpcPort: lastConfig.grpcPort, pollMs: lastConfig.pollMs, fileBytes: 262144, compression: 'none', http: bench.stats.http, rpc: bench.stats.rpc, chunks: bench.stats.chunks });
          if (error) rows.push({ round, mode, operation: 'error', error, note });
        }
      }
    }
    for (const mode of ['rest','grpc']) for (const operation of ['message','upload','download']) {
      const values = rows.filter(r => r.mode === mode && r.operation === operation).map(r => r.ms);
      summary.push({ mode, operation, samples: values.length, median: values.length ? percentile(values, .5) : null, p95: values.length ? percentile(values, .95) : null });
    }
    const selected = await dialog.showSaveDialog(window, { defaultPath: 'relay-benchmark.csv', filters: [{ name: 'CSV', extensions: ['csv'] }] });
    if (!selected.canceled) {
      const headers = ['round','mode','operation','ms','bytes','mibps','error','note','host','restPort','grpcPort','pollMs','fileBytes','compression','http','rpc','chunks'];
      const escape = v => '"' + String(v ?? '').replaceAll('"','""') + '"';
      const csv = [headers.join(','), ...rows.map(r => headers.map(h => escape(r[h])).join(',')), '', 'mode,operation,samples,median_ms,p95_ms', ...summary.map(r => [r.mode,r.operation,r.samples,r.median,r.p95].join(','))].join('\r\n');
      await fs.promises.writeFile(selected.filePath, '\uFEFF' + csv);
    }
    return { summary, errors: rows.filter(r => r.error).length };
  } finally { bench.stop(); await fs.promises.rm(directory, { recursive: true, force: true }); }
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
