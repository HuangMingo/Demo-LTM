package demo.chat;

import io.grpc.Server;
import io.grpc.ServerBuilder;

public final class ChatServer {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv().getOrDefault("GRPC_PORT", "50051"));
        Server server = ServerBuilder.forPort(port).addService(new ChatService()).build().start();
        System.out.println("Chat gRPC server listening on " + port + " (in-memory storage)");
        ChatGateway.startGateway();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            ChatGateway.stopGateway();
            server.shutdown();
        }));
        server.awaitTermination();
    }
}
