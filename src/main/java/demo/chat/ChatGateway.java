package demo.chat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.chat.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.javalin.Javalin;
import io.javalin.http.staticfiles.Location;
import io.javalin.websocket.WsContext;
import io.javalin.websocket.WsMessageContext;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Browser bridge: REST for unary/batch calls and WebSocket for live bidirectional events. */
public final class ChatGateway {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<WsContext, StreamObserver<ChatEvent>> LIVE = new ConcurrentHashMap<>();
    private static ManagedChannel channel;
    private static Javalin app;

    public static void main(String[] args) {
        startGateway();
        Runtime.getRuntime().addShutdownHook(new Thread(ChatGateway::stopGateway));
    }

    public static synchronized void startGateway() {
        startGateway(null);
    }

    public static synchronized void startGateway(DemoStore store) {
        // Giữ endpoint legacy; chỉ thêm REST desktop khi có store dùng chung.
        if (app != null) return;
        String grpcHost = System.getenv().getOrDefault("GRPC_HOST", "localhost");
        int grpcPort = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));
        channel = ManagedChannelBuilder.forAddress(grpcHost, grpcPort).usePlaintext().build();
        ChatServiceGrpc.ChatServiceBlockingStub blocking = ChatServiceGrpc.newBlockingStub(channel);
        ChatServiceGrpc.ChatServiceStub async = ChatServiceGrpc.newStub(channel);

        app = Javalin.create(config -> {
            // So sánh file với gRPC trong cùng chế độ không nén.
            config.compression.none();
            config.staticFiles.add("/web", Location.CLASSPATH);
            config.http.maxRequestSize = 34L * 1024 * 1024;
            config.jetty.multipartConfig.maxFileSize(33, io.javalin.config.SizeUnit.MB);
            config.jetty.multipartConfig.maxTotalRequestSize(34, io.javalin.config.SizeUnit.MB);
            config.jetty.multipartConfig.maxInMemoryFileSize(0, io.javalin.config.SizeUnit.MB);
        });
        if (store != null) DemoRest.install(app, store);
        app.get("/api/health", ctx -> ctx.json(Map.of("ok", true)));
        app.post("/api/login", ctx -> {
            JsonNode body = JSON.readTree(ctx.body());
            LoginReply reply = blocking.login(LoginRequest.newBuilder().setUsername(body.path("username").asText("")).build());
            ctx.json(Map.of("success", reply.getSuccess(), "userId", reply.getUserId(), "username", reply.getUsername(), "message", reply.getMessage()));
        });
        app.get("/api/history", ctx -> {
            String room = ctx.queryParam("room") == null ? "general" : ctx.queryParam("room");
            int limit = ctx.queryParam("limit") == null ? 50 : Integer.parseInt(ctx.queryParam("limit"));
            CountDownLatch done = new CountDownLatch(1);
            var messages = new java.util.concurrent.CopyOnWriteArrayList<Map<String, Object>>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            async.getHistory(HistoryRequest.newBuilder().setRoomId(room).setLimit(limit).build(), new StreamObserver<>() {
                @Override public void onNext(ChatMessage m) { messages.add(messageJson(m)); }
                @Override public void onError(Throwable t) { failure.set(t); done.countDown(); }
                @Override public void onCompleted() { done.countDown(); }
            });
            if (!done.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("History stream timed out");
            if (failure.get() != null) throw new IllegalStateException(failure.get().getMessage());
            ctx.json(messages);
        });
        app.post("/api/batch", ctx -> {
            JsonNode body = JSON.readTree(ctx.body());
            String userId = body.path("userId").asText("");
            String username = body.path("username").asText("Guest");
            String room = body.path("roomId").asText("general");
            JsonNode texts = body.path("messages");
            CountDownLatch done = new CountDownLatch(1);
            AtomicReference<BatchReply> reply = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            StreamObserver<ChatMessage> request = async.sendBatch(new StreamObserver<>() {
                @Override public void onNext(BatchReply value) { reply.set(value); }
                @Override public void onError(Throwable t) { failure.set(t); done.countDown(); }
                @Override public void onCompleted() { done.countDown(); }
            });
            if (texts.isArray()) for (JsonNode text : texts) {
                String content = text.asText("").trim();
                if (!content.isBlank()) request.onNext(ChatMessage.newBuilder().setId(UUID.randomUUID().toString())
                        .setUserId(userId).setUsername(username).setRoomId(room).setText(content).build());
            }
            request.onCompleted();
            if (!done.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("Batch stream timed out");
            if (failure.get() != null) throw new IllegalStateException(failure.get().getMessage());
            BatchReply result = reply.get();
            ctx.json(Map.of("accepted", result.getAccepted(), "roomId", result.getRoomId(), "message", result.getMessage()));
        });
        app.ws("/ws", ws -> {
            ws.onMessage(ctx -> handleLiveMessage(ctx, async));
            ws.onClose(ctx -> closeLive(ctx, false));
        });
        // Nhận qua adapter Radmin; máy khách vẫn kết nối bằng IP cụ thể của host.
        app.start("0.0.0.0", Integer.parseInt(System.getenv().getOrDefault("HTTP_PORT", "8080")));
        System.out.println("Chat web app: http://localhost:" + System.getenv().getOrDefault("HTTP_PORT", "8080"));
    }

    public static synchronized void stopGateway() {
        if (app != null) {
            app.stop();
            app = null;
        }
        if (channel != null) {
            channel.shutdown();
            channel = null;
        }
    }

    private static void handleLiveMessage(WsMessageContext ws, ChatServiceGrpc.ChatServiceStub async) {
        try {
            JsonNode body = JSON.readTree(ws.message());
            String type = body.path("type").asText("");
            if (type.equals("connect")) {
                if (LIVE.containsKey(ws)) return;
                String room = body.path("roomId").asText("general");
                StreamObserver<ChatEvent> request = async.liveChat(new StreamObserver<>() {
                    @Override public void onNext(ChatEvent event) { send(ws, eventJson(event)); }
                    @Override public void onError(Throwable t) { send(ws, Map.of("type", "error", "text", t.getMessage() == null ? "Stream error" : t.getMessage())); LIVE.remove(ws); }
                    @Override public void onCompleted() { send(ws, Map.of("type", "closed")); LIVE.remove(ws); }
                });
                LIVE.put(ws, request);
                request.onNext(ChatEvent.newBuilder().setType(EventType.JOIN).setRoomId(room)
                        .setUserId(body.path("userId").asText("")).setUsername(body.path("username").asText("Guest")).build());
            } else {
                StreamObserver<ChatEvent> request = LIVE.get(ws);
                if (request == null) return;
                EventType eventType = switch (type) {
                    case "message" -> EventType.MESSAGE;
                    case "typing" -> EventType.TYPING;
                    case "leave" -> EventType.LEAVE;
                    default -> EventType.EVENT_UNSPECIFIED;
                };
                if (eventType != EventType.EVENT_UNSPECIFIED) request.onNext(ChatEvent.newBuilder().setType(eventType)
                        .setRoomId(body.path("roomId").asText("general")).setUserId(body.path("userId").asText("") )
                        .setUsername(body.path("username").asText("Guest")).setText(body.path("text").asText("")).build());
                if (eventType == EventType.LEAVE) closeLive(ws, false);
            }
        } catch (Exception e) { send(ws, Map.of("type", "error", "text", e.getMessage() == null ? "Invalid request" : e.getMessage())); }
    }

    private static void closeLive(WsContext ws, boolean error) {
        StreamObserver<ChatEvent> request = LIVE.remove(ws);
        if (request != null) {
            try { if (!error) request.onNext(ChatEvent.newBuilder().setType(EventType.LEAVE).setRoomId("general").build()); } catch (RuntimeException ignored) { }
            try { request.onCompleted(); } catch (RuntimeException ignored) { }
        }
    }
    private static void send(WsContext ws, Object value) { try { ws.send(JSON.writeValueAsString(value)); } catch (Exception ignored) { } }
    private static Map<String, Object> messageJson(ChatMessage m) {
        return Map.of("id", m.getId(), "roomId", m.getRoomId(), "userId", m.getUserId(), "username", m.getUsername(), "text", m.getText(), "sentAt", m.getSentAt());
    }
    private static Map<String, Object> eventJson(ChatEvent e) {
        return Map.of("type", e.getType().name().toLowerCase(), "roomId", e.getRoomId(), "userId", e.getUserId(), "username", e.getUsername(), "text", e.getText(), "sentAt", e.getSentAt());
    }
}
