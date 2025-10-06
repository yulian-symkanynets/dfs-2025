package sk.tuke.dfs.lock;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class LockServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder
                .forPort(Integer.parseInt(args[0]))
                .addService(new LockServiceImpl())
                .addService(io.grpc.protobuf.services.ProtoReflectionService.newInstance())
                .build();
        server.start();
        System.out.println("Lock server started, listening on " + server.getPort());
        server.awaitTermination();
    }
}
