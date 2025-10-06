package sk.tuke.dfs.lock;

import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LockServiceImpl extends LockServiceGrpc.LockServiceImplBase {
    Logger logger = Logger.getLogger(LockServiceImpl.class.getName());
    ConcurrentHashMap<String, Semaphore> locks = new ConcurrentHashMap<>();

    @Override
    public void acquire(LockServiceOuterClass.AcquireRequest request, StreamObserver<LockServiceOuterClass.AcquireResponse> responseObserver) {
        logger.log(Level.INFO, "Acquire request received");
        final String lockId = request.getLockId();

        Semaphore semaphore = locks.computeIfAbsent(lockId, k -> new Semaphore(1, true));

        try{
            semaphore.acquire();
            responseObserver.onNext(LockServiceOuterClass.AcquireResponse.newBuilder().setSuccess(true).build());
            responseObserver.onCompleted();
            logger.info("Acquired lock with id " + lockId);
        } catch (InterruptedException e) {
            logger.log(Level.WARNING, "Lock acquisition interrupted");
            Thread.currentThread().interrupt();
            responseObserver.onNext(LockServiceOuterClass.AcquireResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
        }
    }

    @Override
    public void release(LockServiceOuterClass.ReleaseRequest request, StreamObserver<LockServiceOuterClass.ReleaseResponse> responseObserver) {
        final String lockId = request.getLockId();
        logger.log(Level.INFO, "Releasing lock with id " + lockId);

        Semaphore semaphore = locks.get(lockId);
        try{
            semaphore.release();
            responseObserver.onNext(LockServiceOuterClass.ReleaseResponse.getDefaultInstance());
            responseObserver.onCompleted();
            logger.info("Released lock with id " + lockId);
        } catch (Exception e){
            logger.log(Level.WARNING, "Lock release interrupted");
            responseObserver.onNext(LockServiceOuterClass.ReleaseResponse.newBuilder().build());
            responseObserver.onCompleted();
        }

    }

    @Override
    public void stop(LockServiceOuterClass.StopRequest request, StreamObserver<LockServiceOuterClass.StopResponse> responseObserver) {
        System.exit(0);
    }
}
