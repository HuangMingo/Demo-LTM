const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { spawn, spawnSync } = require('node:child_process');
const { root, java, dependencies, jar } = require('./runtime.cjs');
async function run(exe, args, env) {
  await new Promise((resolve, reject) => {
    const p = spawn(exe, args, { cwd: root, env: env || process.env, stdio: 'inherit', windowsHide: true,
      windowsVerbatimArguments: process.platform === 'win32' && /cmd\.exe$/i.test(exe) });
    p.on('error', reject); p.on('exit', code => code === 0 ? resolve() : reject(Error(`Build thất bại (exit ${code}). Kiểm tra JDK và kết nối Maven Central.`)));
  });
}
// Setup chỉ chuẩn bị backend; không chạy server và không tạo bộ cài/exe.
(async () => {
  dependencies();
  const exe = java();
  let javaHome = process.env.JAVA_HOME;
  if (path.isAbsolute(exe)) javaHome = path.dirname(path.dirname(exe));
  const env = { ...process.env, ...(javaHome ? { JAVA_HOME: javaHome } : {}) };
  // Tận dụng Maven Wrapper nếu repo có; chỉ tải Maven local khi chưa có công cụ phù hợp.
  const wrapper = path.join(root, process.platform === 'win32' ? 'mvnw.cmd' : 'mvnw');
  const local = path.join(root, '.tools', 'apache-maven-3.9.9', 'bin', 'mvn.cmd');
  let mvn = fs.existsSync(wrapper) ? wrapper : fs.existsSync(local) ? local : 'mvn';
  if (mvn === 'mvn' && spawnSync(process.platform === 'win32' ? 'mvn.cmd' : 'mvn', ['-v'], { env, shell: process.platform === 'win32', windowsHide: true }).status !== 0) {
    if (process.platform !== 'win32') throw Error('Cài Maven 3.9+ hoặc thêm Maven Wrapper rồi chạy lại npm run setup.');
    console.log('Repo chưa có Maven Wrapper/Maven. Tải Maven 3.9.9 vào .tools (xác minh SHA-512).');
    const base = 'https://repo.maven.apache.org/maven2/org/apache/maven/apache-maven/3.9.9/apache-maven-3.9.9-bin.zip';
    const [archive, checksum] = await Promise.all([fetch(base), fetch(base + '.sha512')]);
    if (!archive.ok || !checksum.ok) throw Error('Không tải được Maven. Kiểm tra Internet hoặc cài Maven và thêm PATH.');
    // Xác minh bản tải xuống trước khi giải nén hoặc thực thi Maven.
    const bytes = Buffer.from(await archive.arrayBuffer());
    if (crypto.createHash('sha512').update(bytes).digest('hex') !== (await checksum.text()).trim().split(/\s/)[0]) throw Error('Maven checksum không khớp.');
    fs.mkdirSync(path.join(root, '.tools'), { recursive: true });
    const zip = path.join(root, '.tools', 'maven.zip'); fs.writeFileSync(zip, bytes);
    const quote = s => "'" + s.replaceAll("'", "''") + "'";
    await run('powershell.exe', ['-NoProfile', '-NonInteractive', '-Command', `Expand-Archive -LiteralPath ${quote(zip)} -DestinationPath ${quote(path.join(root, '.tools'))} -Force`]);
    fs.unlinkSync(zip); mvn = local;
  }
  // .cmd wrappers require cmd.exe. Quote only these controlled repo/tool paths.
  if (process.platform === 'win32') await run(process.env.ComSpec || 'cmd.exe', ['/d', '/s', '/c', `""${mvn === 'mvn' ? 'mvn.cmd' : mvn}" -B -ntp package"`], env);
  else await run(mvn, ['-B', '-ntp', 'package'], env);
  if (!fs.existsSync(jar)) throw Error('Build không tạo target/relay-backend.jar.');
  console.log('Setup hoàn tất. Chạy npm run host trên máy A; npm start trên máy B.');
})().catch(e => { console.error(e.message); process.exitCode = 1; });
