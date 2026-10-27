package demo.chat;

import demo.chat.proto.*;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/** The four RPC patterns, backed by process-local memory for this demonstration. */
public final class ChatService extends ChatServiceGrpc.ChatServiceImplBase {
    private static final int HISTORY_CAP = 200;
    private final ConcurrentHashMap<String, String> users = new ConcurrentHashMap<>(); // username -> id
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<ChatMessage>> history = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Peer>> rooms = new ConcurrentHashMap<>();

    @Override
    public void login(LoginRequest request, StreamObserver<LoginReply> out) {
        String username = request.getUsername().trim();
        if (username.isBlank() || username.length() > 32) {
            out.onError(Status.INVALID_ARGUMENT.withDescription("Username must contain 1–32 characters.").asRuntimeException());
            return;
        }
        String id = users.computeIfAbsent(username.toLowerCase(), key -> UUID.randomUUID().toString());
        out.onNext(LoginReply.newBuilder().setSuccess(true).setUserId(id).setUsername(username).setMessage("Welcome to the room.").build());
        out.onCompleted();
    }

    @Override
    public void getHistory(HistoryRequest request, StreamObserver<ChatMessage> out) {
        String room = normalizeRoom(request.getRoomId());
        List<ChatMessage> messages = history.getOrDefault(room, new CopyOnWriteArrayList<>());
        int limit = request.getLimit() <= 0 ? 50 : Math.min(request.getLimit(), HISTORY_CAP);
        int start = Math.max(0, messages.size() - limit);
        for (int i = start; i < messages.size(); i++) {
            out.onNext(messages.get(i));
        }
        out.onCompleted();
    }

    @Override
    public StreamObserver<ChatEvent> liveChat(StreamObserver<ChatEvent> response) {
        Peer peer = new Peer(response);
        return new StreamObserver<>() {
            @Override public void onNext(ChatEvent event) {
                String room = normalizeRoom(event.getRoomId());
                switch (event.getType()) {
                    case JOIN -> {
                        peer.room = room;
                        peer.username = event.getUsername();
                        peer.userId = event.getUserId();
                        rooms.computeIfAbsent(room, ignored -> ConcurrentHashMap.newKeySet()).add(peer);
                        broadcast(room, systemEvent(room, peer, "joined the room"), peer);
                    }
                    case MESSAGE -> {
                        ChatMessage message = ChatMessage.newBuilder().setId(UUID.randomUUID().toString())
                                .setRoomId(room).setUserId(event.getUserId()).setUsername(event.getUsername())
                                .setText(event.getText().trim()).setSentAt(Instant.now().toEpochMilli()).build();
                        if (!message.getText().isBlank()) {
                            save(message);
                            broadcast(room, ChatEvent.newBuilder().setType(EventType.MESSAGE).setRoomId(room)
                                    .setUserId(message.getUserId()).setUsername(message.getUsername())
                                    .setText(message.getText()).setSentAt(message.getSentAt()).build(), null);
                        }
                    }
                    case TYPING -> broadcast(room, event.toBuilder().setType(EventType.TYPING).build(), peer);
                    case LEAVE -> removePeer(peer, true);
                    default -> { }
                }
            }
            @Override public void onError(Throwable error) { removePeer(peer, true); }
            @Override public void onCompleted() { removePeer(peer, true); response.onCompleted(); }
        };
    }

    private ChatMessage normalize(ChatMessage m) {
        return m.toBuilder().setId(m.getId().isBlank() ? UUID.randomUUID().toString() : m.getId())
                .setRoomId(normalizeRoom(m.getRoomId())).setSentAt(m.getSentAt() == 0 ? Instant.now().toEpochMilli() : m.getSentAt()).build();
    }
    private void save(ChatMessage m) {
        CopyOnWriteArrayList<ChatMessage> messages = history.computeIfAbsent(normalizeRoom(m.getRoomId()), ignored -> new CopyOnWriteArrayList<>());
        messages.add(m);
        while (messages.size() > HISTORY_CAP) messages.remove(0);
    }
    private void broadcast(String room, ChatEvent event, Peer except) {
        for (Peer peer : rooms.getOrDefault(room, Set.of())) if (peer != except) peer.send(event);
    }
    private ChatEvent systemEvent(String room, Peer peer, String action) {
        return ChatEvent.newBuilder().setType(EventType.SYSTEM).setRoomId(room).setUserId(peer.userId)
                .setUsername(peer.username).setText(peer.username + " " + action).setSentAt(Instant.now().toEpochMilli()).build();
    }
    private void removePeer(Peer peer, boolean announce) {
        String room = peer.room;
        if (room == null) return;
        Set<Peer> peers = rooms.get(room);
        if (peers != null && peers.remove(peer) && announce) broadcast(room, systemEvent(room, peer, "left the room"), null);
        peer.room = null;
    }
    private static String normalizeRoom(String room) { return room == null || room.isBlank() ? "general" : room.trim().toLowerCase(); }

    private static final class Peer {
        private final StreamObserver<ChatEvent> out;
        private volatile String room;
        private volatile String username = "Guest";
        private volatile String userId = "";
        private Peer(StreamObserver<ChatEvent> out) { this.out = out; }
        private synchronized void send(ChatEvent event) { try { out.onNext(event); } catch (RuntimeException ignored) { } }
    }
}
