package sk.tuke.dfs.lock;

import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
    Logger logger = Logger.getLogger(LockServiceImpl.class.getName());

    ConcurrentHashMap<String, LockRecord> concurrentHashMap = new ConcurrentHashMap<>();

    BlockingQueue<RevokeRequest> revokeQueue = new LinkedBlockingQueue<>();
    BlockingQueue<RetryRequest>  retryQueue  = new LinkedBlockingQueue<>();

    public LockServiceImpl() {
        new Thread(new Revoker(revokeQueue)).start();
        new Thread(new Retrier(retryQueue)).start();
    }

    @Override
    public void acquire(LockServiceOuterClass.AcquireRequest req,
                        StreamObserver<LockServiceOuterClass.AcquireResponse> respObs) {
        String lid = req.getLockId();
        String cid = req.getOwnerId();
        long reqSeq = req.getSequence();

        LockRecord r = concurrentHashMap.computeIfAbsent(lid, k -> {
            LockRecord lr = new LockRecord();
            lr.lockId = lid;
            return lr;
        });

        synchronized (r) {
            // Check if lock is free → grant it
            if (r.ownerId == null) {
                r.ownerId = cid;
                r.clientSequences.put(cid, reqSeq);  // Remember client's sequence
                logger.info("[Acquire] Granting lock " + lid + " to " + cid + " with seq=" + reqSeq);
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder()
                        .setSuccess(true).build());
                respObs.onCompleted();
                return;
            }
            
            // Same owner → already cached, grant again
            if (r.ownerId.equals(cid)) {
                r.clientSequences.put(cid, reqSeq);  // Update sequence
                logger.info("[Acquire] Lock " + lid + " already owned by " + cid + " (cached) seq=" + reqSeq);
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder()
                        .setSuccess(true).build());
                respObs.onCompleted();
                return;
            }
            
            // Lock held by different client → RETRY
            logger.info("[Acquire] Lock " + lid + " held by " + r.ownerId + ", denying " + cid);
            
            // Remember the waiting client's sequence
            r.clientSequences.put(cid, reqSeq);
            
            // Add to waiting list if not already there
            if (!r.waitingClients.contains(cid)) {
                r.waitingClients.add(cid);
                logger.info("[Acquire] Added " + cid + " to wait queue for " + lid);
            }

            // Send revoke to current owner (only once)
            if (!r.revokeSent) {
                revokeQueue.add(new RevokeRequest(lid, r.ownerId));
                r.revokeSent = true;
                logger.info("[Acquire] Queued revoke for " + lid + " to " + r.ownerId);
            }

            respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder()
                    .setSuccess(false).build());
            respObs.onCompleted();
        }
    }

    @Override
    public void release(LockServiceOuterClass.ReleaseRequest req,
                        StreamObserver<LockServiceOuterClass.ReleaseResponse> respObs) {
        String lid = req.getLockId();
        String cid = req.getOwnerId();
        long reqSeq = req.getSequence();

        LockRecord r = concurrentHashMap.computeIfAbsent(lid, k -> new LockRecord());
        synchronized (r) {
            // Ignore if not owned by this client
            if (r.ownerId == null || !r.ownerId.equals(cid)) {
                logger.info("[Release] Ignoring release of " + lid + " from " + cid + " (not owner)");
                respObs.onNext(LockServiceOuterClass.ReleaseResponse.getDefaultInstance());
                respObs.onCompleted();
                return;
            }
            
            logger.info("[Release] Releasing lock " + lid + " from " + cid + " seq=" + reqSeq);
            
            // Release the lock
            r.ownerId = null;
            r.revokeSent = false;

            // Send retry to next waiting client (if any) with updated sequence
            String next = r.waitingClients.poll();
            if (next != null) {
                long nextSeq = r.clientSequences.getOrDefault(next, 0L) + 1;
                r.clientSequences.put(next, nextSeq);
                retryQueue.add(new RetryRequest(lid, next, nextSeq));
                logger.info("[Release] Queued retry for " + lid + " to " + next + " seq=" + nextSeq);
            }

            respObs.onNext(LockServiceOuterClass.ReleaseResponse.getDefaultInstance());
            respObs.onCompleted();
        }
    }


    @Override
    public void stop(LockServiceOuterClass.StopRequest request, StreamObserver<LockServiceOuterClass.StopResponse> responseObserver) {
        System.exit(0);
    }
}
