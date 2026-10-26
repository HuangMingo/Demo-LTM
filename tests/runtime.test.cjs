// Kiểm tra cấu hình và cổng bận; không được tạo backend nếu kiểm tra đầu vào thất bại.
const test = require('node:test');
const assert = require('node:assert/strict');
const net = require('node:net');
const { ports, adapters, startBackend } = require('../scripts/runtime.cjs');
test('port configuration rejects collisions and invalid numbers', () => {
  assert.throws(() => ports({ restPort: 'abc' }));
  assert.throws(() => ports({ restPort: 50051, grpcPort: 50051 }));
  assert.throws(() => ports({ grpcPort: 65536 }));
  assert.deepEqual(ports({ restPort: 18080, grpcPort: 15051 }), { restPort: 18080, grpcPort: 15051 });
});
test('adapter discovery never advertises localhost or wildcard', () => {
  for (const a of adapters()) { assert.notEqual(a.address, '0.0.0.0'); assert.notEqual(a.address, '127.0.0.1'); assert.equal(a.radmin, /radmin|famatech/i.test(a.name + ' ' + a.description)); }
});
test('busy REST port fails before spawning a backend', async () => {
  const server = net.createServer(); await new Promise(r => server.listen(0, '0.0.0.0', r));
  let spawned = false;
  try { await assert.rejects(startBackend({ restPort: server.address().port, onSpawn: () => { spawned = true; } }), /đang bận/); assert.equal(spawned, false); }
  finally { await new Promise(r => server.close(r)); }
});
