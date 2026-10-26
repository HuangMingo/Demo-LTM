const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const net = require('node:net');
const { spawn, spawnSync, execFile } = require('node:child_process');
const root = path.resolve(__dirname, '..');
const jar = path.join(root, 'target', 'relay-backend.jar');
const sleep = ms => new Promise(r => setTimeout(r, ms));
// Tìm Java 17+ trên máy host; client chỉ mở Electron nên không cần gọi hàm này.
function java() {
  const candidates = [process.env.JAVA_HOME && path.join(process.env.JAVA_HOME, 'bin', 'java.exe'), 'java'];
  // IntelliJ's downloaded JDKs: read only; never install a JDK silently.
  const jdks = path.join(os.homedir(), '.jdks');
  try { for (const dir of fs.readdirSync(jdks)) candidates.push(path.join(jdks, dir, 'bin', 'java.exe')); } catch {}
  for (const exe of candidates.filter(Boolean)) {
    const r = spawnSync(exe, ['-version'], { encoding: 'utf8', windowsHide: true });
    const version = (r.stderr || '') + (r.stdout || '');
    if (r.status === 0 && Number(version.match(/version "(\d+)/)?.[1]) >= 17) return exe;
  }
  throw Error('Không tìm thấy Java 17+. Cài JDK 17 trở lên, đặt JAVA_HOME và mở lại terminal. Máy client không cần Java.');
}
// Kiểm tra cả package Node và file thực thi Electron, không tự tải khi chạy host.
function dependencies(binary = true) {
  try {
    require.resolve('@grpc/grpc-js'); require.resolve('@grpc/proto-loader');
    const directory = path.dirname(require.resolve('electron/package.json'));
    if (!binary) return directory;
    const e = path.join(directory, 'dist', fs.readFileSync(path.join(directory, 'path.txt'), 'utf8').trim());
    if (!fs.existsSync(e)) throw Error(); return e;
  }
  catch { throw Error('Thiếu dependencies hoặc Electron chưa tải xong. Chạy npm install ở thư mục gốc.'); }
}
// Ưu tiên cổng người dùng nhập, rồi biến môi trường, cuối cùng mới dùng mặc định.
function ports(config = {}) {
  const restPort = Number(config.restPort || process.env.HTTP_PORT || 8080);
  const grpcPort = Number(config.grpcPort || process.env.GRPC_PORT || 50051);
  for (const p of [restPort, grpcPort]) if (!Number.isInteger(p) || p < 1 || p > 65535) throw Error('Cổng phải là số nguyên 1–65535.');
  if (restPort === grpcPort) throw Error('REST và gRPC phải dùng hai cổng khác nhau.');
  return { restPort, grpcPort };
}
// Thử bind rồi đóng ngay để phát hiện cổng bận trước khi tạo tiến trình Java.
async function free(port) {
  await new Promise((resolve, reject) => {
    const server = net.createServer();
    server.once('error', () => reject(Error(`Cổng ${port} đang bận hoặc không thể bind. Dừng server cũ hoặc chọn cổng khác; ứng dụng không tự đổi cổng.`)));
    server.listen(port, '0.0.0.0', () => server.close(resolve));
  });
}
// Chỉ đọc adapter đang có IPv4; gợi ý Radmin theo tên/mô tả, không theo dải IP.
function adapters() {
  let descriptions = [];
  if (process.platform === 'win32') {
    const result = spawnSync('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', 'Get-NetAdapter | Where-Object Status -eq Up | Select-Object Name,InterfaceDescription | ConvertTo-Json -Compress'], { encoding: 'utf8', windowsHide: true, timeout: 4000 });
    try { const parsed = JSON.parse(result.stdout); descriptions = Array.isArray(parsed) ? parsed : parsed ? [parsed] : []; } catch {}
  }
  return Object.entries(os.networkInterfaces()).flatMap(([name, addresses]) => addresses
    .filter(a => a.family === 'IPv4' && !a.internal)
    .map(a => {
      const description = descriptions.find(d => d.Name === name)?.InterfaceDescription || '';
      return { name, description, address: a.address, radmin: /radmin|famatech/i.test(name + ' ' + description) };
    }))
    .sort((a, b) => Number(b.radmin) - Number(a.radmin));
}
// Cả hai dịch vụ phải trả cùng epoch để chắc chắn thuộc cùng một backend.
async function health(host, config) {
  const { restPort, grpcPort } = ports(config);
  const grpc = require('@grpc/grpc-js');
  const loader = require('@grpc/proto-loader');
  const def = grpc.loadPackageDefinition(loader.loadSync(path.join(root, 'src/main/proto/chat.proto'), { longs: String, defaults: true })).chat;
  const client = new def.DemoService(`${host}:${grpcPort}`, grpc.credentials.createInsecure());
  const result = await Promise.allSettled([
    fetch(`http://${host}:${restPort}/demo/health`, { signal: AbortSignal.timeout(1500) }).then(async r => { if (!r.ok) throw Error(`HTTP ${r.status}`); return r.json(); }),
    new Promise((resolve, reject) => client.health({}, { deadline: Date.now() + 1500 }, (e, r) => e ? reject(e) : resolve(r)))
  ]);
  client.close();
  const errors = result.map((r, i) => r.status === 'rejected' ? `${i ? 'gRPC' : 'REST'}: ${r.reason.message}` : null).filter(Boolean);
  if (errors.length) throw Error(errors.join('\n'));
  if (!result[0].value.epoch || result[0].value.epoch !== result[1].value.epoch) throw Error('REST và gRPC không thuộc cùng phiên server. Kiểm tra lại hai cổng.');
  return result[0].value;
}
async function stop(child) {
  if (!child || child.exitCode !== null || !child.pid) return;
  if (process.platform === 'win32') {
    // Chỉ dừng cây tiến trình của PID đã tạo, không dừng mọi tiến trình Java/Electron.
    await new Promise(resolve => execFile('taskkill.exe', ['/PID', String(child.pid), '/T', '/F'], { windowsHide: true }, () => resolve()));
  } else child.kill('SIGTERM');
}
// Dùng chung cho npm run host và lựa chọn Làm server trong giao diện Electron.
async function startBackend(config = {}, log = console.log) {
  const selected = ports(config);
  const exe = java();
  if (!fs.existsSync(jar)) throw Error('Chưa có backend build. Chạy npm run setup trước npm run host.');
  await free(selected.restPort); await free(selected.grpcPort);
  const child = spawn(exe, ['-jar', jar], { cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, RELAY_MANAGED: '1', HTTP_PORT: String(selected.restPort), GRPC_PORT: String(selected.grpcPort) } });
  let failure;
  child.on('error', e => { failure = e; });
  child.stdout.on('data', b => log(b.toString().trimEnd()));
  child.stderr.on('data', b => log(b.toString().trimEnd()));
  let shutdown;
  const handle = { child, ...selected, stop: () => {
    if (shutdown) return shutdown;
    shutdown = (async () => {
      if (child.exitCode !== null) return;
      await new Promise(resolve => {
        const timer = setTimeout(resolve, 3000);
        child.once('exit', () => { clearTimeout(timer); resolve(); });
        // Cho Java tối đa 3 giây chạy shutdown hook và dọn file tạm trước khi buộc dừng.
        child.stdin.on('error', () => {}); child.stdin.end('shutdown\n');
      });
      await stop(child);
    })(); return shutdown;
  } };
  // Trả quyền quản lý tiến trình ngay để Ctrl+C trong lúc chờ cũng dọn được backend.
  config.onSpawn?.(handle);
  try {
    for (let n = 0; n < 40; n++) {
      if (failure || child.exitCode !== null) throw Error(`Backend không khởi động được (${failure?.message || 'exit ' + child.exitCode}). Xem log Java ở trên.`);
      try { const status = await health('127.0.0.1', selected); return { ...handle, epoch: status.epoch }; } catch {}
      await sleep(250);
    }
    throw Error('Hết thời gian chờ REST/gRPC. Kiểm tra log Java và các cổng.');
  } catch (e) { await handle.stop(); throw e; }
}
function printServer(config) {
  console.log(`\nMáy này đang làm server. REST ${config.restPort}; gRPC ${config.grpcPort}`);
  console.log('Localhost: 127.0.0.1 (chỉ để client trên chính máy này kết nối)');
  for (const a of adapters()) console.log(`${a.radmin ? 'RADMIN VPN' : 'Adapter'} | ${a.name} ${a.description ? '(' + a.description + ')' : ''} | ${a.address}`);
  console.log('Chọn IP adapter Radmin trong giao diện; không dùng 0.0.0.0 hoặc localhost trên máy B.\n');
}
module.exports = { root, jar, java, dependencies, ports, health, startBackend, stop, adapters, printServer };
