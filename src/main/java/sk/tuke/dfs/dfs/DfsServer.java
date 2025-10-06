package sk.tuke.dfs.dfs;

import com.google.protobuf.ByteString;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.nio.charset.StandardCharsets;

public class DfsServer {

    public static void main(String[] args) throws Exception {
        // Args: <dfsPort> <ipAddressExtent:port> <ipAddressLock:port>
        int dfsPort = Integer.parseInt(args[0]);
        String extentHostname = args[1].split(":")[0];
        int extentPort = Integer.parseInt(args[1].split(":")[1]);
        String lockHostname = args[2].split(":")[0];
        int lockPort = Integer.parseInt(args[2].split(":")[1]);


        ManagedChannel extentCh = ManagedChannelBuilder.forAddress(extentHostname, extentPort).usePlaintext().build();
        ManagedChannel lockCh   = ManagedChannelBuilder.forAddress(lockHostname, lockPort).usePlaintext().build();

        LockServiceGrpc.LockServiceBlockingStub lock    = LockServiceGrpc.newBlockingStub(lockCh);
        ExtentServiceGrpc.ExtentServiceBlockingStub ext = ExtentServiceGrpc.newBlockingStub(extentCh);

        Server server = ServerBuilder
                .forPort(dfsPort)
                .addService(new DfsServiceImpl(lock,ext))
                .addService(io.grpc.protobuf.services.ProtoReflectionService.newInstance())
                .build()
                .start();
        System.out.println("[DFS] listening on " + server.getPort());


        server.awaitTermination();
    }
}
