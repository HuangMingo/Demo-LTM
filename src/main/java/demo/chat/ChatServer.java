package demo.chat;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class ChatServer {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));
        // Một store duy nhất giúp client REST và gRPC trao đổi cùng phòng/tin/file.
        DemoStore store = new DemoStore();
        Server server = ServerBuilder.forPort(port).addService(new ChatService()).addService(new DemoRpc(store)).build().start();
        System.out.println("Chat gRPC server listening on " + port + " (in-memory storage)");
        try { ChatGateway.startGateway(store); }
        catch (Throwable e) { ChatGateway.stopGateway(); server.shutdownNow(); store.close(); throw e; }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ChatGateway.stopGateway();
            server.shutdownNow();
            try { store.close(); } catch (java.io.IOException ignored) { }
        }));
        if ("1".equals(System.getenv("RELAY_MANAGED"))) {
            Thread control = new Thread(() -> {
                try {
                    // Launcher sở hữu pipe; nhận lệnh hoặc EOF thì thoát qua shutdown hook.
                    new java.io.BufferedReader(new java.io.InputStreamReader(System.in)).readLine();
                } catch (java.io.IOException ignored) { }
                System.exit(0);
            }, "launcher-lifecycle");
            control.setDaemon(true); control.start();
        }
        server.awaitTermination();
    }
}
