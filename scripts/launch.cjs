const { spawn } = require('node:child_process');
const runtime = require('./runtime.cjs');
// Launcher chỉ giữ các tiến trình do chính lần chạy này tạo để dọn khi thoát.
let backend, desktop, stopping = false;
async function shutdown(code = 0) {
  if (stopping) return; stopping = true;
  await runtime.stop(desktop); await backend?.stop(); process.exit(code);
}
process.on('SIGINT', () => shutdown());
process.on('SIGTERM', () => shutdown());
(async () => {
  const electron = runtime.dependencies();
  const host = process.argv[2] === 'host';
  // Chế độ client bỏ qua hoàn toàn bước Java; host đợi health check trước khi mở UI.
  if (host) {
    backend = await runtime.startBackend({ onSpawn: b => { backend = b; } });
    runtime.printServer(backend);
  }
  const desktopEnv = { ...process.env, RELAY_EXTERNAL_HOST: host ? JSON.stringify({ restPort: backend.restPort, grpcPort: backend.grpcPort, epoch: backend.epoch }) : '' };
  // Biến này dù là chuỗi rỗng vẫn có thể khiến Electron chạy như Node thay vì desktop.
  delete desktopEnv.ELECTRON_RUN_AS_NODE;
  desktop = spawn(electron, [runtime.root], {
    cwd: runtime.root, stdio: 'inherit', windowsHide: false,
    env: desktopEnv
  });
  desktop.on('error', async e => { console.error(e.message); await shutdown(1); });
  desktop.on('exit', code => shutdown(code || 0));
  backend?.child.on('exit', code => { if (!stopping) { console.error(`Backend đã dừng (${code}). Đóng host.`); shutdown(1); } });
})().catch(async e => { console.error('\n' + e.message); await shutdown(1); });
