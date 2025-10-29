package sk.tuke.dfs.dfs;

import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.ManagedChannel;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class Releaser implements Runnable {

    private static final Logger logger = Logger.getLogger(Releaser.class.getName());

    private final BlockingQueue<String> blockingQueue;
    private final ConcurrentHashMap<String, LockState> lockStateMap;
    private final ConcurrentHashMap<String, Long> lockSequences;
    private final ManagedChannel lockChannel;
    private final String ownerId;
    private volatile boolean running = true;

    public Releaser(BlockingQueue<String> queue,
                    ConcurrentHashMap<String, LockState> map,
                    ManagedChannel channel,
                    String ownerId,
                    ConcurrentHashMap<String, Long> lockSequences) {
        this.blockingQueue = queue;
        this.lockStateMap = map;
        this.lockChannel = channel;
        this.ownerId = ownerId;
        this.lockSequences = lockSequences;
    }

    @Override
    public void run() {
        LockServiceGrpc.LockServiceBlockingStub lockStub =
                LockServiceGrpc.newBlockingStub(lockChannel);

        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                String lockId = blockingQueue.take();

                logger.info("[Releaser] Processing release for " + lockId);

                // Get current sequence for this lock
                long seq = lockSequences.getOrDefault(lockId, 0L);
                
                try {
                    lockStub.release(
                            LockServiceOuterClass.ReleaseRequest.newBuilder()
                                    .setLockId(lockId)
                                    .setOwnerId(ownerId)
                                    .setSequence(seq)
                                    .build()
                    );

                    lockStateMap.put(lockId, LockState.NONE);
                    logger.info("[Releaser] Successfully released lock " + lockId + " → NONE (seq=" + seq + ")");

                } catch (Exception e) {
                    logger.warning("[Releaser] Error releasing lock " + lockId + ": " + e.getMessage());
                    // Still mark as NONE even if release fails
                    lockStateMap.put(lockId, LockState.NONE);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("[Releaser] Interrupted, stopping thread.");
                break;
            }
        }
        logger.info("[Releaser] Thread stopped");
    }

    public void stop() {
        running = false;
    }
}