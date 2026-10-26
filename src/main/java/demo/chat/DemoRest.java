package demo.chat;

import demo.chat.proto.*;
import io.javalin.Javalin;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.*;

/** Adapter HTTP gọi cùng DemoStore với gRPC; không gọi vòng qua gRPC để đo REST. */
public final class DemoRest {
    private static Map<String,Object> file(FileInfo f) { return Map.of("id", f.getId(), "name", f.getName(), "size", f.getSize(), "sha256", f.getSha256()); }
    private static Map<String,Object> record(demo.chat.proto.Record m) {
        Map<String,Object> v = new LinkedHashMap<>();
        v.put("id",m.getId()); v.put("seq",m.getSeq()); v.put("username",m.getUsername()); v.put("userId",m.getUserId());
        v.put("text",m.getText()); v.put("sentAt",m.getSentAt()); v.put("requestId",m.getRequestId());
        if (m.hasFile()) v.put("file",file(m.getFile())); return v;
    }
    public static void install(Javalin app, DemoStore store) {
        app.exception(IllegalArgumentException.class, (e,ctx) -> ctx.status(400).json(Map.of("message",e.getMessage())));
        app.get("/demo/health", ctx -> ctx.json(Map.of("epoch",store.epoch)));
        app.post("/demo/join", ctx -> {
            JsonNode b = ctx.bodyAsClass(JsonNode.class);
            Session s = store.join(JoinRequest.newBuilder().setRoom(b.path("room").asText()).setUsername(b.path("username").asText())
                .setUserId(b.path("userId").asText()).setCreate(b.path("create").asBoolean()).build());
            ctx.json(Map.of("token",s.getToken(),"epoch",s.getEpoch(),"room",s.getRoom(),"username",s.getUsername(),"userId",s.getUserId()));
        });
        // Polling dùng after để chỉ nhận tin sau cursor, thay vì tải lại toàn bộ lịch sử.
        app.get("/demo/history", ctx -> {
            Page page = store.history(ctx.header("Authorization"), Long.parseLong(Objects.requireNonNullElse(ctx.queryParam("after"),"0")));
            ctx.json(Map.of("epoch",page.getEpoch(),"messages",page.getMessagesList().stream().map(DemoRest::record).toList(),"truncated",page.getTruncated()));
        });
        app.post("/demo/messages", ctx -> {
            JsonNode b = ctx.bodyAsClass(JsonNode.class);
            ctx.json(record(store.send(ctx.header("Authorization"),b.path("text").asText(),b.path("requestId").asText())));
        });
        // Jetty giữ multipart trên đĩa; copy theo chunk rồi kiểm tra size/hash trước khi công bố.
        app.post("/demo/files", ctx -> {
            var source = ctx.uploadedFile("file");
            if (source == null) throw new IllegalArgumentException("Thiếu multipart field file.");
            try (var input = source.content(); var upload = store.upload(ctx.header("Authorization"),Objects.requireNonNullElse(ctx.queryParam("name"), source.filename()))) {
                byte[] chunk; while ((chunk = input.readNBytes(65536)).length != 0) upload.write(chunk);
                String hash = Objects.requireNonNullElse(ctx.formParam("sha256"), "");
                if (!hash.matches("[a-f0-9]{64}")) throw new IllegalArgumentException("Thiếu SHA-256 file.");
                long expectedSize = Long.parseLong(Objects.requireNonNullElse(ctx.formParam("size"), "-1"));
                if (expectedSize < 0 || expectedSize > DemoStore.MAX_FILE) throw new IllegalArgumentException("Thiếu/sai kích thước file.");
                ctx.json(file(upload.finish(expectedSize, hash)));
            }
        });
        // Trả InputStream để Javalin stream file, không đọc toàn bộ thành byte[].
        app.get("/demo/files/{id}", ctx -> {
            var f = store.file(ctx.header("Authorization"),ctx.pathParam("id"));
            ctx.contentType("application/octet-stream"); ctx.header("Content-Length",String.valueOf(f.info().getSize()));
            ctx.result(java.nio.file.Files.newInputStream(f.path()));
        });
    }
}
