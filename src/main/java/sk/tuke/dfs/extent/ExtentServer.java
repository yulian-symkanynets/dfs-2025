package sk.tuke.dfs.extent;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ExtentServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1 || args.length > 2) {
            System.err.println("Usage: ExtentServer <port> [extentRoot]");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        String extentRoot = (args.length == 2) ? args[1] : ""; // default root folder

        Files.createDirectories(Paths.get(extentRoot));

        Server server = ServerBuilder
                .forPort(port)
                .addService(new ExtentServiceImpl(extentRoot))
                .addService(io.grpc.protobuf.services.ProtoReflectionService.newInstance())
                .build();

        server.start();

        System.out.println("Extent server started, listening on port " + port + ", root = " + extentRoot);
        server.awaitTermination();
    }
}
