package sk.tuke.dfs.lock;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class Revoker implements Runnable {

    private static final Logger log = Logger.getLogger(Revoker.class.getName());

    private final BlockingQueue<RevokeRequest> blockingQueue;
    private volatile boolean running;

    public Revoker(BlockingQueue<RevokeRequest> blockingQueue) {
        this.blockingQueue = blockingQueue;
        this.running = true;
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            ManagedChannel dfsCh = null;
            try {
                RevokeRequest revokeRequest = blockingQueue.take();

                // ownerId format: "ip:port:uuid"
                String[] parts = revokeRequest.ownerId.split(":", 3);
                if (parts.length < 2) {
                    log.warning("[Revoker] Invalid ownerId: " + revokeRequest.ownerId);
                    continue;
                }

                String host = parts[0];
                int port;
                try {
                    port = Integer.parseInt(parts[1]);
                } catch (NumberFormatException nfe) {
                    log.warning("[Revoker] Bad port in ownerId: " + revokeRequest.ownerId);
                    continue;
                }

                log.info("[Revoker] Sending revoke for " + revokeRequest.lockId +
                        " → " + host + ":" + port);

                dfsCh = ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext()
                        .build();

                LockCacheServiceGrpc.LockCacheServiceBlockingStub stub =
                        LockCacheServiceGrpc.newBlockingStub(dfsCh)
                                .withDeadline(Deadline.after(3, TimeUnit.SECONDS));

                stub.revoke(LockCacheServiceOuterClass.RevokeRequest.newBuilder()
                        .setLockId(revokeRequest.lockId)
                        .build());

            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                break;
            } catch (io.grpc.StatusRuntimeException sre) {
                log.warning("[Revoker] gRPC error: " + sre.getStatus());
            } catch (Exception e) {
                log.warning("[Revoker] Error: " + e.getMessage());
            } finally {
                if (dfsCh != null) {
                    dfsCh.shutdown();
                    try {
                        if (!dfsCh.awaitTermination(500, TimeUnit.MILLISECONDS)) {
                            dfsCh.shutdownNow();
                        }
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    public void stop() {
        running = false;
        Thread.currentThread().interrupt();
    }
}
