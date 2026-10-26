const { EventEmitter, once } = require('node:events');
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID, createHash } = require('node:crypto');
const { Readable, Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');
const { performance } = require('node:perf_hooks');
const grpc = require('@grpc/grpc-js');
const loader = require('@grpc/proto-loader');
const { root, ports, health } = require('../scripts/runtime.cjs');
const definition = grpc.loadPackageDefinition(loader.loadSync(path.join(root, 'src/main/proto/chat.proto'), { longs: String, defaults: true })).chat;
const MAX_FILE = 32 * 1024 * 1024;
// Một instance giữ phiên người dùng, channel dùng lại, cursor lịch sử và counters thực tế.
class Transport extends EventEmitter {
  constructor() {
    super(); this.userId = randomUUID(); this.cursor = 0; this.seen = new Set(); this.sequences = new Set(); this.pending = new Map(); this.generation = 0;
    this.stats = { http: 0, rpc: 0, chunks: 0, messages: 0, errors: 0 }; this.busy = false; this.retryId = 0; this.connectionAttempt = 0;
  }
  statsChanged() { this.emit('stats', { ...this.stats }); }
  async http(route, options = {}) {
    this.stats.http++; this.statsChanged();
    const response = await fetch(`http://${this.config.host}:${this.config.restPort}/demo${route}`, {
      ...options, headers: { Authorization: this.session?.token || '', 'Accept-Encoding': 'identity', ...options.headers },
      signal: options.signal || AbortSignal.timeout(30000)
    });
    if (!response.ok) throw Error((await response.json().catch(() => ({}))).message || `HTTP ${response.status}`);
    return response;
  }
  rpc(method, request) {
    this.stats.rpc++; this.statsChanged();
    return new Promise((resolve, reject) => this.client[method](request, { deadline: Date.now() + 30000 }, (e, r) => e ? reject(e) : resolve(r)));
  }
  // Mỗi lần connect có mã riêng để kết quả từ yêu cầu cũ không ghi đè kết nối mới.
  async connect(config, reconnect = false) {
    if (this.busy) throw Error('Đang truyền file/benchmark; chờ hoàn tất.');
    this.stop(reconnect);
    const attempt = ++this.connectionAttempt;
    if (!reconnect) this.reconnecting = false;
    config = { ...config, ...ports(config) };
    if (!/^[a-zA-Z0-9.-]+$/.test(config.host || '')) throw Error('Nhập IPv4 hoặc hostname server, không gồm http:// hay cổng.');
    if (!Number.isInteger(Number(config.pollMs)) || config.pollMs < 100 || config.pollMs > 10000) throw Error('Polling phải từ 100 đến 10000 ms.');
    if (!['rest', 'grpc'].includes(config.mode)) throw Error('Chọn REST hoặc gRPC.');
    const previousEpoch = this.session?.epoch;
    const sameRoom = this.config?.host === config.host && this.config?.restPort === config.restPort && this.config?.grpcPort === config.grpcPort && this.config?.room === config.room;
    this.config = config; this.closed = false;
    this.emit('status', { connected: false, text: 'Đang kiểm tra REST và gRPC…' });
    this.stats.http++; this.stats.rpc++; this.statsChanged();
    try {
      await health(config.host, config);
      if (attempt !== this.connectionAttempt) throw Error('Yêu cầu kết nối đã được thay thế.');
      this.client = new definition.DemoService(`${config.host}:${config.grpcPort}`, grpc.credentials.createInsecure(), { 'grpc.max_receive_message_length': 1024 * 1024 });
      const request = { username: config.username, room: config.room, userId: this.userId, create: !!config.create };
      const session = config.mode === 'rest'
        ? await (await this.http('/join', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(request) })).json()
        : await this.rpc('join', request);
      if (attempt !== this.connectionAttempt) throw Error('Yêu cầu kết nối đã được thay thế.');
      this.session = session;
      const restarted = sameRoom && previousEpoch && previousEpoch !== this.session.epoch;
      if (!sameRoom || restarted) { this.cursor = 0; this.seen.clear(); this.sequences.clear(); this.emit('reset', { restarted }); }
      this.retries = 0;
      await this.startMode();
      if (attempt !== this.connectionAttempt || !this.stream && config.mode === 'grpc') throw Error('Kết nối đã đóng trong lúc đồng bộ.');
      this.reconnecting = false;
      this.emit('status', { connected: true, text: `${config.mode.toUpperCase()} · ${config.host} · phòng ${config.room}`, restarted });
      return this.session;
    } catch (e) { this.stats.errors++; this.statsChanged(); if (attempt === this.connectionAttempt) this.stop(reconnect); throw e; }
  }
  receive(record) {
    const pending = this.pending.get(record.requestId);
    if (pending) { clearTimeout(pending.timer); this.pending.delete(record.requestId); pending.resolve(record); }
    // ACK của tin vừa gửi có thể tới trước các tin cũ từ polling.
    // Cursor chỉ tiến qua dãy seq liên tiếp để không bỏ sót tin chưa nhận.
    if (Number(record.seq) > this.cursor) this.sequences.add(Number(record.seq));
    while (this.sequences.delete(this.cursor + 1)) this.cursor++;
    if (this.seen.has(record.id)) return;
    this.seen.add(record.id); if (this.seen.size > 2000) this.seen.delete(this.seen.values().next().value);
    this.stats.messages++; this.statsChanged(); this.emit('message', record);
  }
  async history(generation = this.generation) {
    const page = this.config.mode === 'rest' ? await (await this.http(`/history?after=${this.cursor}`)).json()
      : await this.rpc('history', { token: this.session.token, after: this.cursor });
    if (generation !== this.generation) return;
    if (page.epoch !== this.session.epoch) throw Error('Server đã khởi động lại. Kết nối lại để tham gia phiên mới.');
    if (page.truncated || (this.cursor === 0 && Number(page.messages[0]?.seq) > 1)) {
      this.cursor = Number(page.messages[0]?.seq || 1) - 1;
      this.emit('notice', 'Một phần lịch sử đã hết hạn; server chỉ giữ 500 tin gần nhất.');
    }
    for (const message of page.messages) this.receive(message);
  }
  // Mở stream/poll mới rồi đối soát lịch sử; generation vô hiệu hóa callback của chế độ cũ.
  async startMode() {
    const generation = ++this.generation;
    // Giữ tạm tin live trong lúc tải lịch sử để tránh cursor nhảy qua khoảng còn thiếu.
    let syncing = true; const buffered = [];
    if (this.config.mode === 'grpc') {
      this.stats.rpc++; this.statsChanged();
      const stream = this.client.chat(); this.stream = stream;
      stream.on('data', record => {
        if (generation !== this.generation) return;
        this.stats.chunks++;
        if (syncing) { if (buffered.length >= 500) this.lost(Error('Quá nhiều tin trong lúc đồng bộ.')); else buffered.push(record); }
        else this.receive(record);
      });
      stream.on('error', e => { if (generation === this.generation) this.lost(e); });
      stream.on('end', () => { if (generation === this.generation) this.lost(Error('gRPC stream đã đóng.')); });
      stream.write({ token: this.session.token });
    }
    await this.history(generation);
    syncing = false;
    if (generation !== this.generation) return;
    buffered.sort((a,b) => Number(a.seq)-Number(b.seq)).forEach(record => this.receive(record));
    // Chỉ đặt timeout tiếp sau khi request hoàn tất, tránh polling bị chồng lên nhau.
    const tick = async () => {
      if (generation !== this.generation) return;
      try { await this.history(generation); } catch (e) { if (generation === this.generation) this.lost(e); return; }
      if (generation === this.generation) this.timer = setTimeout(tick, this.config.mode === 'rest' ? Number(this.config.pollMs) : 2000);
    };
    this.timer = setTimeout(tick, this.config.mode === 'rest' ? Number(this.config.pollMs) : 2000);
  }
  async switchMode(mode) {
    if (this.busy) throw Error('Không chuyển phương thức khi đang truyền file/benchmark.');
    if (!['rest', 'grpc'].includes(mode)) throw Error('Phương thức không hợp lệ.');
    this.stopActivity(); this.config.mode = mode;
    try { await this.startMode(); this.emit('status', { connected: true, text: `${mode.toUpperCase()} · ${this.config.host} · phòng ${this.config.room}` }); }
    catch (e) { this.lost(e); throw e; }
  }
  // Dừng tác vụ của phương thức cũ nhưng giữ session để đổi REST/gRPC không phải đăng nhập lại.
  stopActivity() {
    ++this.generation; clearTimeout(this.timer); clearTimeout(this.retryTimer);
    if (this.stream) { const stream = this.stream; this.stream = null; stream.cancel(); }
    for (const p of this.pending.values()) { clearTimeout(p.timer); p.reject(Error('Kết nối đã đóng hoặc chuyển phương thức.')); } this.pending.clear();
  }
  stop(preserveRetry = false) {
    this.closed = true; ++this.connectionAttempt;
    if (!preserveRetry) { ++this.retryId; this.reconnecting = false; }
    this.stopActivity(); this.client?.close(); this.client = null;
  }
  // Retry hữu hạn có backoff; retryId giúp lần kết nối thủ công hủy vòng retry cũ.
  lost(error) {
    if (this.closed || this.reconnecting) return;
    this.reconnecting = true; this.stopActivity(); this.stats.errors++; this.statsChanged();
    const retryId = ++this.retryId;
    this.emit('status', { connected: false, text: `Mất kết nối: ${error.message}. Đang thử lại tối đa 3 lần.` });
    const retry = async attempt => {
      if (this.closed || retryId !== this.retryId) return;
      try { await this.connect(this.config, true); this.reconnecting = false; }
      catch (e) {
        if (retryId !== this.retryId) return;
        this.closed = false;
        if (attempt < 3) this.retryTimer = setTimeout(() => retry(attempt + 1), 1000 * 2 ** attempt);
        else { this.reconnecting = false; this.emit('status', { connected: false, text: `Không kết nối được sau 3 lần: ${e.message}. Bấm Kết nối lại.` }); }
      }
    };
    this.retryTimer = setTimeout(() => retry(1), 1000);
  }
  // Đo trên cùng client từ lúc gửi đến ACK; gRPC ghép ACK với requestId trong stream chat.
  async send(text) {
    const begin = performance.now(); const requestId = randomUUID(); let record;
    if (!this.session || this.closed || this.reconnecting) throw Error('Chưa kết nối server.');
    try {
      if (this.config.mode === 'rest') {
        record = await (await this.http('/messages', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ text, requestId }) })).json();
      } else {
        record = await new Promise((resolve, reject) => {
          const timer = setTimeout(() => { this.pending.delete(requestId); reject(Error('Hết thời gian chờ server xác nhận tin nhắn.')); }, 10000);
          this.pending.set(requestId, { resolve, reject, timer });
          this.stream.write({ token: this.session.token, text, requestId }); this.stats.chunks++;
        });
      }
      this.receive(record); const ms = performance.now() - begin; this.emit('measurement', { operation: 'message', mode: this.config.mode, ms }); return ms;
    } catch (e) { this.stats.errors++; this.statsChanged(); throw e; }
  }
  progress(operation, bytes, total) { this.emit('progress', { operation, bytes, total }); }
  // Đọc file theo chunk, hash trong khi truyền; không đưa toàn bộ file vào RAM hoặc IPC.
  async upload(filename) {
    if (this.busy) throw Error('Đang có tác vụ truyền file.');
    this.busy = true;
    const begin = performance.now(); const sourceHash = createHash('sha256'); let bytes = 0;
    try {
      const size = (await fs.promises.stat(filename)).size;
      if (!filename.toLowerCase().endsWith('.txt') || size > MAX_FILE) throw Error('Chọn file .txt tối đa 32 MiB.');
      const name = path.basename(filename); let info, checksum;
      const source = () => fs.createReadStream(filename, { highWaterMark: 65536 });
      if (this.config.mode === 'rest') {
        const boundary = 'relay' + randomUUID(); const self = this;
        const body = Readable.from((async function* () {
          yield Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="upload.txt"; filename*=UTF-8''${encodeURIComponent(name)}\r\nContent-Type: text/plain\r\n\r\n`);
          for await (const chunk of source()) { sourceHash.update(chunk); bytes += chunk.length; self.stats.chunks++; self.progress('upload', bytes, size); yield chunk; }
          // Multipart kết thúc bằng size/hash để REST kiểm tra giống đường upload gRPC.
          checksum = sourceHash.digest('hex');
          yield Buffer.from(`\r\n--${boundary}\r\nContent-Disposition: form-data; name="sha256"\r\n\r\n${checksum}\r\n--${boundary}\r\nContent-Disposition: form-data; name="size"\r\n\r\n${size}\r\n--${boundary}--\r\n`);
        })());
        info = await (await this.http('/files?name=' + encodeURIComponent(name), { method: 'POST', headers: { 'Content-Type': `multipart/form-data; boundary=${boundary}` }, body, duplex: 'half', signal: AbortSignal.timeout(120000) })).json();
      } else {
        this.stats.rpc++;
        let stream, response;
        response = new Promise((resolve, reject) => { stream = this.client.upload({ deadline: Date.now() + 120000 }, (e, r) => e ? reject(e) : resolve(r)); });
        // Attach rejection immediately while producing chunks.
        response.catch(() => {});
        stream.on('error', () => {});
        try {
          stream.write({ token: this.session.token, name, expectedSize: size });
          for await (const chunk of source()) {
            sourceHash.update(chunk); bytes += chunk.length; this.stats.chunks++; this.progress('upload', bytes, size);
            if (!stream.write({ data: chunk })) await once(stream, 'drain');
          }
          checksum = sourceHash.digest('hex');
          // Dấu kết thúc riêng phân biệt file hoàn tất với stream bị ngắt giữa chừng.
          stream.end({ complete: true, sha256: checksum }); info = await response;
        } catch (e) { stream.cancel(); throw e; }
      }
      checksum ||= sourceHash.digest('hex');
      if (Number(info.size) !== bytes || info.sha256 !== checksum) throw Error('Checksum upload không khớp.');
      this.transferMeasurement('upload', begin, bytes); return info;
    } catch (e) { this.stats.errors++; throw e; }
    finally { this.busy = false; this.statsChanged(); }
  }
  // Chỉ đổi tên file .partial thành đích sau khi nhận đủ byte và kiểm tra SHA-256.
  async download(info, destination) {
    if (this.busy) throw Error('Đang có tác vụ truyền file.'); this.busy = true;
    const begin = performance.now(); const temporary = destination + '.' + randomUUID() + '.partial';
    const hash = createHash('sha256'); let bytes = 0;
    try {
      let input;
      if (this.config.mode === 'rest') input = Readable.fromWeb((await this.http('/files/' + encodeURIComponent(info.id), { signal: AbortSignal.timeout(120000) })).body);
      else {
        this.stats.rpc++;
        const call = this.client.download({ token: this.session.token, id: info.id }, { deadline: Date.now() + 120000 });
        input = Readable.from((async function* () { try { for await (const chunk of call) yield chunk.data; } finally { call.cancel(); } })());
      }
      const self = this;
      await pipeline(input, new Transform({ transform(chunk, _, callback) {
        hash.update(chunk); bytes += chunk.length; self.stats.chunks++; self.progress('download', bytes, Number(info.size)); callback(null, chunk);
      } }), fs.createWriteStream(temporary, { flags: 'wx' }));
      if (bytes !== Number(info.size) || hash.digest('hex') !== info.sha256) throw Error('Checksum download không khớp; đã xóa file truyền dở.');
      await fs.promises.rename(temporary, destination);
      this.transferMeasurement('download', begin, bytes); return { bytes };
    } catch (e) { this.stats.errors++; throw e; }
    finally { this.busy = false; await fs.promises.rm(temporary, { force: true }); this.statsChanged(); }
  }
  transferMeasurement(operation, begin, bytes) {
    const ms = performance.now() - begin;
    this.emit('measurement', { operation, mode: this.config.mode, ms, bytes, mibps: bytes / 1048576 / (ms / 1000) });
  }
}
module.exports = { Transport };
