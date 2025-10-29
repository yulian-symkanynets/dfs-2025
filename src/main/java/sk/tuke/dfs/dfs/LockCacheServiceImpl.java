package sk.tuke.dfs.dfs;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class LockCacheServiceImpl extends LockCacheServiceGrpc.LockCacheServiceImplBase {

    private static final Logger logger = Logger.getLogger(LockCacheServiceImpl.class.getName());

    private final ConcurrentHashMap<String, LockEntry> lockTable;
    private final ManagedChannel lockChannel;
    private final String ownerId;
    private final ExecutorService threadPool;

    public LockCacheServiceImpl(ConcurrentHashMap<String, LockEntry> lockTable,
                                ManagedChannel lockChannel,
                                String ownerId) {
        this.lockTable = lockTable;
        this.lockChannel = lockChannel;
        this.ownerId = ownerId;
        this.threadPool = Executors.newFixedThreadPool(10);
    }

    @Override
    public void revoke(LockCacheServiceOuterClass.RevokeRequest request,
                       StreamObserver<LockCacheServiceOuterClass.RevokeResponse> responseObserver) {
        String lockId = request.getLockId();
        LockEntry lockEntry = lockTable.get(lockId);

        logger.info("[Revoke] Received revoke for " + lockId);

        if (lockEntry == null) {
            logger.info("[Revoke] Ignored - lock not found");
            responseObserver.onNext(LockCacheServiceOuterClass.RevokeResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        // Capture current status before changing it
        LockState currentStatus = lockEntry.getStatus();
        lockEntry.setStatus(LockState.RELEASING);
        lockEntry.getRevoked().set(true);
        logger.info("[Revoke] Current status was " + currentStatus + ", set to RELEASING and revoked flag");

        threadPool.submit(() -> {
            try {
                logger.info("[Revoke] Async task started");
                // If an operation was in progress when revoke arrived (status was LOCKED),
                // wait for it to finish and signal via freeSignal
                if (currentStatus == LockState.LOCKED) {
                    logger.info("[Revoke] Operation was in progress, waiting for freeSignal");
                    lockEntry.getFreeSignal().acquire();
                    logger.info("[Revoke] freeSignal received, operation completed");
                }
                
                logger.info("[Revoke] Releasing to lock service");
                LockServiceGrpc.LockServiceBlockingStub lockStub = 
                    LockServiceGrpc.newBlockingStub(lockChannel);
                
                lockStub.release(
                    LockServiceOuterClass.ReleaseRequest.newBuilder()
                        .setLockId(lockEntry.getLockId())
                        .setOwnerId(ownerId)
                        .build()
                );

                lockEntry.setStatus(LockState.NONE);
                lockEntry.getRevoked().set(false);
                logger.info("[Revoke] Successfully completed revoke");
            } catch (InterruptedException e) {
                logger.warning("[Revoke] Interrupted while waiting");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.warning("[Revoke] Error releasing lock: " + e.getMessage());
                e.printStackTrace();
            }
        });

        responseObserver.onNext(LockCacheServiceOuterClass.RevokeResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void retry(LockCacheServiceOuterClass.RetryRequest req,
                      StreamObserver<LockCacheServiceOuterClass.RetryResponse> resp) {
        String lockId = req.getLockId();
        long sequence = req.getSequence();

        logger.info("[Retry] Received retry for " + lockId + " seq=" + sequence);

        LockEntry lockEntry = lockTable.get(lockId);
        if (lockEntry == null) {
            logger.info("[Retry] Ignored - lock not found");
            resp.onNext(LockCacheServiceOuterClass.RetryResponse.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        lockEntry.setStatus(LockState.ACQUIRING);
        logger.info("[Retry] Set status to ACQUIRING, submitting async acquire task");

        threadPool.submit(() -> {
            try {
                logger.info("[Retry] Attempting to acquire lock with sequence " + lockEntry.getSequence().get());
                
                LockServiceGrpc.LockServiceBlockingStub lockStub = 
                    LockServiceGrpc.newBlockingStub(lockChannel);
                
                var response = lockStub.acquire(
                    LockServiceOuterClass.AcquireRequest.newBuilder()
                        .setLockId(lockEntry.getLockId())
                        .setOwnerId(ownerId)
                        .setSequence(lockEntry.getSequence().get())
                        .build()
                );

                if (response.getSuccess()) {
                    logger.info("[Retry] Successfully acquired lock, releasing freeSignal");
                    lockEntry.getFreeSignal().release();
                } else {
                    logger.info("[Retry] Failed to acquire lock");
                }
            } catch (Exception e) {
                logger.warning("[Retry] Error acquiring lock: " + e.getMessage());
            }
        });

        resp.onNext(LockCacheServiceOuterClass.RetryResponse.getDefaultInstance());
        resp.onCompleted();
    }
}