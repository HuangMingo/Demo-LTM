// Chạy Java thật cùng nhiều client Node để kiểm tra chat/file chéo REST và native gRPC.
// Dùng cổng trống và thư mục tạm riêng; finally chỉ dọn tài nguyên của bài kiểm thử này.
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const net = require('node:net');
const { once } = require('node:events');
const { Transport } = require('../electron/transport.cjs');
const { startBackend, health } = require('../scripts/runtime.cjs');
const pause = ms => new Promise(r => setTimeout(r, ms));
async function unused() { const s = net.createServer(); s.listen(0, '0.0.0.0'); await once(s, 'listening'); const p = s.address().port; await new Promise(r => s.close(r)); return p; }
async function waitFor(predicate, message) { for (let i=0;i<80;i++) { if (predicate()) return; await pause(100); } throw Error(message); }
(async () => {
  const config = { host: '127.0.0.1', restPort: await unused(), grpcPort: await unused(), room: 'integration', username: 'Host', mode: 'rest', pollMs: 100, create: true };
  const temp = await fs.mkdtemp(path.join(os.tmpdir(), 'relay-test-'));
  let backend; const clients = [];
  try {
    backend = await startBackend(config, line => { if (/ERROR|Exception/.test(line)) console.log(line); });
    await health(config.host, config);
    const host = new Transport(), rest = new Transport(), grpc = new Transport(); clients.push(host, rest, grpc);
    const received = clients.map(() => []); clients.forEach((client,i) => client.on('message', m => received[i].push(m)));
    await host.connect(config);
    await rest.connect({ ...config, create: false, username: 'REST' });
    await grpc.connect({ ...config, create: false, mode: 'grpc', username: 'gRPC' });
    await assert.rejects(new Transport().connect({ ...config, create: false, room: 'wrong-room' }), /phòng/);
    await host.send('REST to REST and gRPC ✓'); await rest.send('REST reply'); await grpc.send('gRPC to REST ✓');
    await waitFor(() => received.every(messages => messages.length === 3), 'cross-protocol delivery');
    console.log('PASS REST–REST, REST–gRPC, gRPC–REST');
    await host.switchMode('grpc'); await host.send('gRPC to gRPC');
    await waitFor(() => received.every(messages => messages.length === 4), 'gRPC peers');
    for (let i=0;i<4;i++) { await rest.switchMode(i%2 ? 'rest' : 'grpc'); await grpc.send('switch '+i); }
    await waitFor(() => received.every(messages => messages.length === 8), 'switch resync');
    for (const messages of received) assert.equal(new Set(messages.map(m=>m.id)).size, messages.length);
    console.log('PASS gRPC–gRPC, switching without duplicate IDs');
    const fixture = path.join(temp, 'tiếng-Việt.txt'); const bytes = Buffer.from(('Tiếng Việt ✓\r\nline two\n\u0000').repeat(12000)); await fs.writeFile(fixture, bytes);
    for (const uploadMode of ['rest','grpc']) {
      await rest.switchMode(uploadMode); const info = await rest.upload(fixture); assert.equal(Number(info.size),bytes.length); assert.equal(info.name,path.basename(fixture));
      for (const downloadMode of ['rest','grpc']) {
        await grpc.switchMode(downloadMode); const output = path.join(temp, `${uploadMode}-${downloadMode}.txt`);
        await grpc.download(info,output); assert.deepEqual(await fs.readFile(output),bytes);
      }
    }
    console.log('PASS all four upload/download combinations, bytes + SHA-256');
    const empty = path.join(temp,'empty.txt'); await fs.writeFile(empty,'');
    for (const mode of ['rest','grpc']) {
      await rest.switchMode(mode); const info = await rest.upload(empty);
      await grpc.download(info,path.join(temp,'empty-'+mode+'.txt')); assert.equal(Number(info.size),0);
    }
    const sameEpoch=rest.session.epoch; rest.stop(); await host.send('offline catch-up');
    await rest.connect({...config,create:false,username:'REST'}); assert.equal(rest.session.epoch,sameEpoch);
    assert.equal(received[1].filter(m=>m.text==='offline catch-up').length,1);
    console.log('PASS empty files and reconnect catch-up within same epoch');
    // Cancel a native upload before half-close: it must never publish a file record.
    await rest.switchMode('grpc');
    const aborted = rest.client.upload(() => {}); aborted.on('error',()=>{});
    aborted.write({ token: rest.session.token, name: 'interrupted.txt', data: Buffer.alloc(65536) }); await pause(150); aborted.cancel(); await pause(300);
    const afterCancel = await rest.rpc('history',{token:rest.session.token,after:0});
    assert.equal(afterCancel.messages.some(m=>m.file?.name === 'interrupted.txt'),false);
    console.log('PASS interrupted upload never published');
    const oldEpoch = host.session.epoch; await backend.stop();
    await waitFor(() => clients.some(c=>c.reconnecting), 'disconnect state');
    backend = await startBackend(config, ()=>{});
    await assert.rejects(health(config.host,{ ...config, grpcPort: await unused() }), /gRPC/);
    await host.connect(config); assert.notEqual(host.session.epoch,oldEpoch); assert.equal(host.cursor,0);
    await rest.connect({ ...config, create:false }); await host.send('new epoch');
    await waitFor(() => rest.cursor === 1, 'reconnect history');
    console.log('PASS server loss, restart epoch, reconnect and individual service health');
  } finally { clients.forEach(c=>c.stop()); await backend?.stop(); await fs.rm(temp,{recursive:true,force:true}); }
})().catch(e=>{ console.error(e); process.exitCode=1; });
