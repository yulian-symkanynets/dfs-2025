package sk.tuke.dfs.dfs;

import dfs.extent.ExtentServiceGrpc;
import dfs.lock.LockServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.net.InetAddress;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DFS Server entry point.
 * Connects ExtentService, LockService, and the local LockCache for coordination.
 */
public class DfsServer {

    public static void main(String[] args) throws Exception {

        // Args: <dfsPort> <extentHost:port> <lockHost:port>
        if (args.length < 3) {
            System.err.println("Usage: DfsServer <dfsPort> <extentHost:port> <lockHost:port>");
            return;
        }

        int dfsPort = Integer.parseInt(args[0]);
        String extentHost = args[1].split(":")[0];
        int extentPort = Integer.parseInt(args[1].split(":")[1]);
        String lockHost = args[2].split(":")[0];
        int lockPort = Integer.parseInt(args[2].split(":")[1]);

        // -------------------- Channels --------------------
        ManagedChannel extentCh = ManagedChannelBuilder
                .forAddress(extentHost, extentPort)
                .usePlaintext()
                .build();

        ManagedChannel lockCh = ManagedChannelBuilder
                .forAddress(lockHost, lockPort)
                .usePlaintext()
                .build();

        // -------------------- Stubs --------------------
        LockServiceGrpc.LockServiceBlockingStub lockStub = LockServiceGrpc.newBlockingStub(lockCh);
        ExtentServiceGrpc.ExtentServiceBlockingStub extentStub = ExtentServiceGrpc.newBlockingStub(extentCh);

        // -------------------- Shared state --------------------
        ConcurrentHashMap<String, LockState> lockStateMap = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Object> waiters = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Long> lockSequences = new ConcurrentHashMap<>();
        BlockingQueue<String> releaseQueue = new LinkedBlockingQueue<>();

        // -------------------- Unique IDs --------------------
        String baseAddr = InetAddress.getLocalHost().getHostAddress() + ":" + dfsPort;
        String dfsOwnerId = baseAddr + ":dfs-" + UUID.randomUUID();
        System.out.println("[DFS] DFS OwnerId   = " + dfsOwnerId);

        // -------------------- Start Releaser Thread ONCE --------------------
        Thread releaserThread = new Thread(
                new Releaser(releaseQueue, lockStateMap, lockCh, dfsOwnerId),
                "ReleaserThread"
        );
        releaserThread.setDaemon(true);
        releaserThread.start();
        System.out.println("[DFS] Releaser thread started");

        // -------------------- gRPC Server setup --------------------
        Server server = ServerBuilder
                .forPort(dfsPort)
                // DFS logic (acquire/put/delete)
                .addService(new DfsServiceImpl(
                        lockStub,
                        extentStub,
                        dfsOwnerId,
                        lockCh,
                        lockSequences,
                        lockStateMap,
                        waiters,
                        releaseQueue
                ))
                // Local lock cache (revoke/retry handling)
                .addService(new LockCacheServiceImpl(
                        lockStateMap,
                        lockCh,
                        dfsOwnerId,
                        lockSequences,
                        waiters,
                        releaseQueue
                ))
                .addService(io.grpc.protobuf.services.ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("[DFS] Listening on port " + server.getPort());

        // -------------------- Graceful shutdown hook --------------------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("[DFS] Shutting down...");
            lockCh.shutdown();
            extentCh.shutdown();
            try {
                server.shutdown();
                if (!server.awaitTermination(3, java.util.concurrent.TimeUnit.SECONDS)) {
                    server.shutdownNow();
                }
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
        }));

        // -------------------- Await termination --------------------
        server.awaitTermination();
    }
}