package demo.chat;

import demo.chat.proto.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.*;
import java.io.*;
import java.nio.file.Files;
import java.util.ArrayDeque;

/** Adapter native gRPC gọi thẳng DemoStore, không đi qua WebSocket/gateway. */
public final class DemoRpc extends DemoServiceGrpc.DemoServiceImplBase {
    private final DemoStore store;
    public DemoRpc(DemoStore store) { this.store = store; }
    private static void fail(StreamObserver<?> out, Exception e) {
        out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
    }
    @Override public void health(Empty r, StreamObserver<HealthReply> out) {
        out.onNext(HealthReply.newBuilder().setEpoch(store.epoch).build()); out.onCompleted();
    }
    @Override public void join(JoinRequest r, StreamObserver<Session> out) {
        try { out.onNext(store.join(r)); out.onCompleted(); } catch (Exception e) { fail(out, e); }
    }
    @Override public void history(Query r, StreamObserver<Page> out) {
        try { out.onNext(store.history(r.getToken(), r.getAfter())); out.onCompleted(); } catch (Exception e) { fail(out, e); }
    }
    // Stream hai chiều: nhận Envelope, trả Record đã được server cấp ID/seq.
    @Override public StreamObserver<Envelope> chat(StreamObserver<demo.chat.proto.Record> response) {
        var out = (ServerCallStreamObserver<demo.chat.proto.Record>) response;
        class Peer implements StreamObserver<Envelope> {
            Runnable remove;
            String token;
            boolean closed;
            final ArrayDeque<demo.chat.proto.Record> queue = new ArrayDeque<>();
            synchronized void cleanup() { closed = true; queue.clear(); if (remove != null) { Runnable r = remove; remove = null; r.run(); } }
            // Chỉ đẩy khi gRPC sẵn sàng; hàng đợi giới hạn tránh client chậm làm đầy RAM.
            synchronized void flush() {
                while (!closed && out.isReady() && !queue.isEmpty()) out.onNext(queue.removeFirst());
            }
            synchronized void receive(demo.chat.proto.Record r) {
                if (closed) return;
                if (queue.size() >= 256) { cleanup(); out.onError(Status.RESOURCE_EXHAUSTED.withDescription("Client quá chậm; kết nối lại để đồng bộ lịch sử.").asRuntimeException()); return; }
                queue.add(r); flush();
            }
            public void onNext(Envelope r) {
                try {
                    if (token == null) { token = r.getToken(); remove = store.listen(token, this::receive); }
                    if (!token.equals(r.getToken())) throw new IllegalArgumentException("Không được đổi phiên trong stream.");
                    if (!r.getText().isEmpty()) store.send(token, r.getText(), r.getRequestId());
                } catch (Exception e) { cleanup(); fail(out, e); }
            }
            public void onError(Throwable e) { cleanup(); }
            public void onCompleted() { cleanup(); out.onCompleted(); }
        }
        Peer peer = new Peer(); out.setOnReadyHandler(peer::flush); out.setOnCancelHandler(peer::cleanup); return peer;
    }
    // Client streaming: metadata đầu, các chunk dữ liệu, cuối cùng là complete/checksum.
    @Override public StreamObserver<FileChunk> upload(StreamObserver<FileInfo> out) {
        var serverOut = (ServerCallStreamObserver<FileInfo>) out;
        class Receiver implements StreamObserver<FileChunk> {
            DemoStore.Upload upload;
            boolean ended;
            boolean complete;
            long expectedSize;
            String expectedHash;
            synchronized void cleanup() { ended = true; if (upload != null) try { upload.close(); } catch (IOException ignored) {} }
            public synchronized void onNext(FileChunk chunk) {
                if (ended) return;
                try {
                    if (complete) throw new IllegalArgumentException("Dữ liệu sau dấu kết thúc file.");
                    if (upload == null) { upload = store.upload(chunk.getToken(), chunk.getName()); expectedSize = chunk.getExpectedSize(); }
                    if (chunk.getComplete()) {
                        if (!chunk.getSha256().matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Thiếu SHA-256 file.");
                        complete = true; expectedHash = chunk.getSha256();
                    } else upload.write(chunk.getData().toByteArray());
                    // Xử lý xong chunk hiện tại mới xin chunk tiếp theo (backpressure).
                    serverOut.request(1);
                }
                catch (Exception e) { cleanup(); fail(out, e); }
            }
            public synchronized void onError(Throwable e) { cleanup(); }
            // Đóng stream chưa đủ: phải có complete và size/hash đúng mới công bố file.
            public synchronized void onCompleted() {
                if (ended) return;
                try { if (upload == null || !complete) throw new IllegalArgumentException("File bị gián đoạn: thiếu xác nhận kết thúc."); out.onNext(upload.finish(expectedSize, expectedHash)); out.onCompleted(); }
                catch (Exception e) { fail(out, e); } finally { cleanup(); }
            }
        }
        Receiver receiver = new Receiver(); serverOut.disableAutoRequest(); serverOut.request(1); serverOut.setOnCancelHandler(receiver::cleanup); return receiver;
    }
    // Server streaming: đọc tối đa 64 KiB/lần, ngừng đọc khi client chưa nhận kịp.
    @Override public void download(FileQuery r, StreamObserver<FileChunk> response) {
        var out = (ServerCallStreamObserver<FileChunk>) response;
        try {
            InputStream input = Files.newInputStream(store.file(r.getToken(), r.getId()).path());
            Runnable close = () -> { try { input.close(); } catch (IOException ignored) {} };
            out.setOnCancelHandler(close);
            out.setOnReadyHandler(new Runnable() {
                boolean done;
                public synchronized void run() {
                    if (done) return;
                    try {
                        while (out.isReady() && !out.isCancelled()) {
                            byte[] bytes = input.readNBytes(65536);
                            if (bytes.length == 0) { done = true; close.run(); out.onCompleted(); return; }
                            out.onNext(FileChunk.newBuilder().setData(ByteString.copyFrom(bytes)).build());
                        }
                    } catch (Exception e) { done = true; close.run(); fail(out, e); }
                }
            });
        } catch (Exception e) { fail(out, e); }
    }
}
