package sk.tuke.dfs.dfs;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class LockCacheServiceImpl extends LockCacheServiceGrpc.LockCacheServiceImplBase {

    private static final Logger logger = Logger.getLogger(LockCacheServiceImpl.class.getName());

    private final ConcurrentHashMap<String, LockState> lockStateMap;
    private final BlockingQueue<String> releaseQueue;
    private final ManagedChannel lockChannel;
    private final String ownerId;
    private final ConcurrentHashMap<String, Long> lockSequences;
    private final ConcurrentHashMap<String, Object> waiters;

    private Object waiter(String lockId) {
        return waiters.computeIfAbsent(lockId, k -> new Object());
    }

    public LockCacheServiceImpl(ConcurrentHashMap<String, LockState> lockStateMap,
                                ManagedChannel lockChannel,
                                String baseAddr,
                                ConcurrentHashMap<String, Long> lockSequences,
                                ConcurrentHashMap<String, Object> waiters,
                                BlockingQueue<String> releaseQueue) {
        this.lockStateMap = lockStateMap;
        this.lockChannel = lockChannel;
        this.ownerId = baseAddr;
        this.lockSequences = lockSequences;
        this.releaseQueue = releaseQueue;
        this.waiters = waiters;

        // ❌ DO NOT START RELEASER HERE - it's started in DfsServer.main()
    }

    @Override
    public void revoke(LockCacheServiceOuterClass.RevokeRequest request,
                       StreamObserver<LockCacheServiceOuterClass.RevokeResponse> responseObserver) {
        String lockId = request.getLockId();
        LockState current = lockStateMap.getOrDefault(lockId, LockState.NONE);

        logger.info("[Revoke] Received revoke for " + lockId + " state=" + current);

        // If lock is not held, ignore
        if (current == LockState.NONE) {
            logger.info("[Revoke] Ignored - lock not held");
            responseObserver.onNext(LockCacheServiceOuterClass.RevokeResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        // If already being released, ignore
        if (current == LockState.RELEASING || current == LockState.REVOKE_PENDING) {
            logger.info("[Revoke] Already releasing/pending");
            responseObserver.onNext(LockCacheServiceOuterClass.RevokeResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        // If FREE, release immediately using the async queue
        if (current == LockState.FREE) {
            lockStateMap.put(lockId, LockState.RELEASING);
            releaseQueue.add(lockId);
            logger.info("[Revoke] Released immediately (FREE) via async queue");
        } else {
            // LOCKED or ACQUIRING - mark as revoke pending
            // The operation will release synchronously in its finally block
            lockStateMap.put(lockId, LockState.REVOKE_PENDING);
            logger.info("[Revoke] Marked as REVOKE_PENDING (state was " + current + ")");
        }

        responseObserver.onNext(LockCacheServiceOuterClass.RevokeResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void retry(LockCacheServiceOuterClass.RetryRequest req,
                      StreamObserver<LockCacheServiceOuterClass.RetryResponse> resp) {
        String lid = req.getLockId();
        long seq = req.getSequence();

        logger.info("[Retry] Received retry for " + lid + " seq=" + seq);

        long old = lockSequences.getOrDefault(lid, 0L);
        if (seq <= old) {
            logger.info("[Retry] Stale retry (seq=" + seq + " <= old=" + old + ")");
            resp.onNext(LockCacheServiceOuterClass.RetryResponse.getDefaultInstance());
            resp.onCompleted();
            return;
        }

        lockSequences.put(lid, seq);
        lockStateMap.put(lid, LockState.FREE);
        logger.info("[Retry] Updated sequence to " + seq + " and set state to FREE");

        synchronized (waiter(lid)) {
            waiter(lid).notifyAll();
            logger.info("[Retry] Notified all waiters for " + lid);
        }

        resp.onNext(LockCacheServiceOuterClass.RetryResponse.getDefaultInstance());
        resp.onCompleted();
    }
}