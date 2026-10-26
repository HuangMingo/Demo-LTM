package demo.chat;

import demo.chat.proto.*;
import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;

/** Logic dùng chung cho REST/gRPC desktop; dữ liệu chỉ tồn tại trong phiên server. */
public final class DemoStore implements AutoCloseable {
    public static final long MAX_FILE = 32L * 1024 * 1024;
    public static final long MAX_TOTAL = 256L * 1024 * 1024;
    // Epoch mới sau mỗi lần khởi động để client nhận biết lịch sử phiên cũ không còn.
    public final String epoch = UUID.randomUUID().toString();
    private final Path directory;
    private final Map<String, Room> rooms = new HashMap<>();
    private final Map<String, Session> sessions = new HashMap<>();
    private final Map<String, StoredFile> files = new HashMap<>();
    private long storedBytes;
    private final Set<Upload> pending = ConcurrentHashMap.newKeySet();
    static final class Room {
        long seq;
        final ArrayDeque<demo.chat.proto.Record> messages = new ArrayDeque<>();
        final Set<Consumer<demo.chat.proto.Record>> listeners = ConcurrentHashMap.newKeySet();
    }
    public record StoredFile(FileInfo info, Path path, String room) {}
    public DemoStore() throws IOException { directory = Files.createTempDirectory("relay-demo-"); }
    private static String roomCode(String code) {
        String room = code.trim();
        if (!room.matches("[A-Za-z0-9_-]{1,40}")) throw new IllegalArgumentException("Mã phòng: 1–40 ký tự chữ, số, _ hoặc -.");
        return room;
    }
    /** Host được tạo phòng; client phải nhập mã phòng đã tồn tại. */
    public synchronized Session join(JoinRequest request) {
        String room = roomCode(request.getRoom());
        String name = request.getUsername().trim();
        if (name.isEmpty() || name.length() > 32) throw new IllegalArgumentException("Tên hiển thị phải có 1–32 ký tự.");
        if (!rooms.containsKey(room)) {
            if (!request.getCreate()) throw new IllegalArgumentException("Sai mã phòng hoặc phòng chưa được host tạo.");
            rooms.put(room, new Room());
        }
        Session s = Session.newBuilder().setToken(UUID.randomUUID().toString()).setEpoch(epoch).setRoom(room)
            .setUsername(name).setUserId(request.getUserId().isBlank() ? UUID.randomUUID().toString() : request.getUserId()).build();
        sessions.put(s.getToken(), s); return s;
    }
    public synchronized Session session(String token) {
        Session s = sessions.get(token);
        if (s == null) throw new IllegalArgumentException("Phiên đăng nhập không còn. Kết nối lại và tham gia phòng.");
        return s;
    }
    /** Lấy tin sau cursor; truncated báo các tin còn thiếu đã hết hạn lưu trữ. */
    public synchronized Page history(String token, long after) {
        Room r = rooms.get(session(token).getRoom());
        Page.Builder page = Page.newBuilder().setEpoch(epoch);
        if (!r.messages.isEmpty() && after > 0 && after < r.messages.getFirst().getSeq() - 1) page.setTruncated(true);
        r.messages.stream().filter(m -> m.getSeq() > after).limit(500).forEach(page::addMessages);
        return page.build();
    }
    /** requestId ngăn yêu cầu gửi lặp tạo thêm tin trong phần lịch sử còn lưu. */
    public synchronized demo.chat.proto.Record send(String token, String text, String requestId) {
        Session s = session(token);
        if (text.isBlank() || text.length() > 8192) throw new IllegalArgumentException("Tin nhắn phải có 1–8192 ký tự.");
        Room r = rooms.get(s.getRoom());
        if (!requestId.isBlank()) for (var m : r.messages) if (m.getUserId().equals(s.getUserId()) && m.getRequestId().equals(requestId)) return m;
        return publish(s, demo.chat.proto.Record.newBuilder().setText(text).setRequestId(requestId));
    }
    // Caller giữ khóa store: cấp seq, lưu lịch sử rồi phát cùng record cho client live.
    private demo.chat.proto.Record publish(Session s, demo.chat.proto.Record.Builder builder) {
        Room r = rooms.get(s.getRoom());
        var record = builder.setId(UUID.randomUUID().toString()).setSeq(++r.seq).setUsername(s.getUsername())
            .setUserId(s.getUserId()).setSentAt(System.currentTimeMillis()).build();
        r.messages.add(record); while (r.messages.size() > 500) r.messages.removeFirst();
        for (var listener : List.copyOf(r.listeners)) {
            try { listener.accept(record); } catch (RuntimeException e) { r.listeners.remove(listener); }
        }
        return record;
    }
    public synchronized Runnable listen(String token, Consumer<demo.chat.proto.Record> listener) {
        Room r = rooms.get(session(token).getRoom()); r.listeners.add(listener);
        return () -> r.listeners.remove(listener);
    }
    /** Chỉ được tải file thuộc phòng của phiên đang yêu cầu. */
    public synchronized StoredFile file(String token, String id) {
        Session s = session(token); StoredFile f = files.get(id);
        if (f == null || !f.room.equals(s.getRoom())) throw new IllegalArgumentException("Không tìm thấy file trong phòng này.");
        return f;
    }
    public Upload upload(String token, String name) throws IOException {
        session(token);
        if (name.length() > 200 || !name.toLowerCase(Locale.ROOT).endsWith(".txt") || name.contains("/") || name.contains("\\"))
            throw new IllegalArgumentException("Chỉ nhận tên file .txt, tối đa 200 ký tự, không chứa đường dẫn.");
        Upload u = new Upload(token, name); pending.add(u); return u;
    }
    /** File tạm chưa được công bố cho đến khi finish kiểm tra toàn vẹn thành công. */
    public final class Upload implements AutoCloseable {
        private final String token, name;
        private final Path path;
        private final OutputStream out;
        private final MessageDigest digest;
        private long size;
        private boolean finished;
        Upload(String token, String name) throws IOException {
            this.token = token; this.name = name;
            // Server tự sinh tên lưu, không ghép đường dẫn từ tên client cung cấp.
            path = Files.createTempFile(directory, "partial-", ".tmp");
            out = Files.newOutputStream(path);
            try { digest = MessageDigest.getInstance("SHA-256"); } catch (NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
        }
        public synchronized void write(byte[] bytes) throws IOException {
            if (finished) throw new IOException("Upload đã kết thúc.");
            if (bytes.length > 65536 || size + bytes.length > MAX_FILE) throw new IllegalArgumentException("Giới hạn file 32 MiB, chunk 64 KiB.");
            out.write(bytes); digest.update(bytes); size += bytes.length;
        }
        public synchronized FileInfo finish() throws IOException {
            return finish(-1, null);
        }
        /** Đóng file, đối chiếu size/hash rồi mới công bố metadata và tin thông báo. */
        public synchronized FileInfo finish(long expectedSize, String expectedHash) throws IOException {
            if (finished) throw new IOException("Upload đã kết thúc.");
            out.close();
            String actualHash = HexFormat.of().formatHex(digest.digest());
            if (expectedSize >= 0 && (size != expectedSize || !actualHash.equals(expectedHash)))
                throw new IllegalArgumentException("File chưa đủ byte hoặc SHA-256 không khớp.");
            synchronized (DemoStore.this) {
                Session s = session(token);
                if (storedBytes + size > MAX_TOTAL) throw new IllegalArgumentException("Kho file phiên demo đã đầy (256 MiB).");
                FileInfo info = FileInfo.newBuilder().setId(UUID.randomUUID().toString()).setName(name).setSize(size)
                    .setSha256(actualHash).build();
                files.put(info.getId(), new StoredFile(info, path, s.getRoom())); storedBytes += size;
                publish(s, demo.chat.proto.Record.newBuilder().setFile(info).setText("File: " + name));
                finished = true; pending.remove(this); return info;
            }
        }
        // try-with-resources dọn file chưa hoàn tất khi upload lỗi hoặc bị hủy.
        @Override public synchronized void close() throws IOException {
            out.close(); if (!finished) Files.deleteIfExists(path); pending.remove(this);
        }
    }
    @Override public void close() throws IOException {
        for (Upload u : List.copyOf(pending)) u.close();
        try (var paths = Files.list(directory)) { for (Path p : paths.toList()) Files.deleteIfExists(p); }
        Files.deleteIfExists(directory);
    }
}
