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

        LockRecord r = concurrentHashMap.computeIfAbsent(lid, k -> {
            LockRecord lr = new LockRecord();
            lr.lockId = lid;
            return lr;
        });

        synchronized (r) {
            // free → grant
            if (r.ownerId == null) {
                r.ownerId = cid;
                r.sequence++;
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder()
                        .setSuccess(true).build());
                respObs.onCompleted();
                return;
            }
            // same owner → ok (cached)
            if (r.ownerId.equals(cid)) {
                respObs.onNext(LockServiceOuterClass.AcquireResponse.newBuilder()
                        .setSuccess(true).build());
                respObs.onCompleted();
                return;
            }
            // otherwise enqueue + revoke current owner (only once)
            if (!r.waitingClients.contains(cid))
                r.waitingClients.add(cid);

            if (!r.revokeSent) {
                revokeQueue.add(new RevokeRequest(lid, r.ownerId));
                r.revokeSent = true;
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

        LockRecord r = concurrentHashMap.computeIfAbsent(lid, k -> new LockRecord());
        synchronized (r) {
            // ignore wrong owner
            if (r.ownerId == null || !r.ownerId.equals(cid)) {
                respObs.onNext(LockServiceOuterClass.ReleaseResponse.getDefaultInstance());
                respObs.onCompleted();
                return;
            }
            r.ownerId = null;
            r.sequence++;
            r.revokeSent = false;  // ✅ Reset flag

            // notify next waiting client
            String next = r.waitingClients.poll();
            if (next != null)
                retryQueue.add(new RetryRequest(lid, next, r.sequence));

            respObs.onNext(LockServiceOuterClass.ReleaseResponse.getDefaultInstance());
            respObs.onCompleted();
        }
    }


    @Override
    public void stop(LockServiceOuterClass.StopRequest request, StreamObserver<LockServiceOuterClass.StopResponse> responseObserver) {
        System.exit(0);
    }
}
