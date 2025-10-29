package sk.tuke.dfs.dfs;

import dfs.dfs.DfsServiceGrpc;
import dfs.dfs.DfsServiceOuterClass;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class DfsServiceImpl extends DfsServiceGrpc.DfsServiceImplBase {

    private static final Logger logger = Logger.getLogger(DfsServiceImpl.class.getName());

    private final ConcurrentHashMap<String, LockState> lockStateMap;
    private final ConcurrentHashMap<String, Object> waiters;
    private final BlockingQueue<String> releaseQueue;
    private final ConcurrentHashMap<String, Long> lockSequences;

    private final LockServiceGrpc.LockServiceBlockingStub lockStub;
    private final ExtentServiceGrpc.ExtentServiceBlockingStub extentStub;
    private final String ownerId;

    public DfsServiceImpl(LockServiceGrpc.LockServiceBlockingStub lockStub,
                          ExtentServiceGrpc.ExtentServiceBlockingStub extentStub,
                          String ownerId,
                          io.grpc.ManagedChannel lockChannel,
                          ConcurrentHashMap<String, Long> lockSequences,
                          ConcurrentHashMap<String, LockState> lockStateMap,
                          ConcurrentHashMap<String, Object> waiters,
                          BlockingQueue<String> releaseQueue) {
        this.lockStub = lockStub;
        this.extentStub = extentStub;
        this.ownerId = ownerId;
        this.lockSequences = lockSequences;
        this.lockStateMap = lockStateMap;
        this.waiters = waiters;
        this.releaseQueue = releaseQueue;
    }

    private Object waiter(String lockId) {
        return waiters.computeIfAbsent(lockId, k -> new Object());
    }

    private boolean tryAcquireLock(String lid) {
        long seq = lockSequences.getOrDefault(lid, 0L);
        logger.info("[TryAcquire] Attempting to acquire " + lid + " with seq=" + seq);

        var res = lockStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder()
                .setLockId(lid)
                .setOwnerId(ownerId)
                .setSequence(seq)
                .build());

        logger.info("[TryAcquire] Result for " + lid + ": " + res.getSuccess());
        return res.getSuccess();
    }

    private void releaseLock(String lockId) {
        LockState state = lockStateMap.getOrDefault(lockId, LockState.NONE);
        logger.info("[ReleaseLock] Releasing " + lockId + " (current state: " + state + ")");

        if (state == LockState.NONE) {
            logger.info("[ReleaseLock] Lock " + lockId + " already NONE, skipping");
            return;
        }

        // If REVOKE_PENDING, release immediately back to server
        if (state == LockState.REVOKE_PENDING) {
            logger.info("[ReleaseLock] REVOKE_PENDING, releasing to server immediately");
            lockStateMap.put(lockId, LockState.RELEASING);
            
            long seq = lockSequences.getOrDefault(lockId, 0L);
            try {
                lockStub.release(
                        LockServiceOuterClass.ReleaseRequest.newBuilder()
                                .setLockId(lockId)
                                .setOwnerId(ownerId)
                                .setSequence(seq)
                                .build()
                );
                logger.info("[ReleaseLock] Successfully released " + lockId + " to server");
            } catch (Exception e) {
                logger.warning("[ReleaseLock] Error releasing " + lockId + ": " + e.getMessage());
            } finally {
                lockStateMap.put(lockId, LockState.NONE);
            }
            return;
        }

        // Otherwise, just cache it locally (set to FREE)
        lockStateMap.put(lockId, LockState.FREE);
        logger.info("[ReleaseLock] Cached lock " + lockId + " locally (FREE)");
    }

    private void waitAndAcquire(String lockId) {
        logger.info("[WaitAndAcquire] Starting for " + lockId);
        
        LockState current = lockStateMap.getOrDefault(lockId, LockState.NONE);
        
        // If already FREE (cached), just mark as LOCKED
        if (current == LockState.FREE) {
            lockStateMap.put(lockId, LockState.LOCKED);
            logger.info("[WaitAndAcquire] Lock " + lockId + " was cached (FREE), now LOCKED");
            return;
        }
        
        lockStateMap.put(lockId, LockState.ACQUIRING);

        while (true) {
            if (tryAcquireLock(lockId)) {
                lockStateMap.put(lockId, LockState.LOCKED);
                logger.info("[WaitAndAcquire] Successfully acquired " + lockId);
                return;
            }

            logger.info("[WaitAndAcquire] Failed to acquire " + lockId + ", waiting for retry...");

            synchronized (waiter(lockId)) {
                LockState state = lockStateMap.get(lockId);
                if (state == LockState.FREE) {
                    logger.info("[WaitAndAcquire] State is FREE, retrying acquire");
                    continue;
                }

                try {
                    logger.info("[WaitAndAcquire] Waiting on monitor for " + lockId);
                    waiter(lockId).wait(5000);
                    logger.info("[WaitAndAcquire] Woke up for " + lockId);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.severe("[WaitAndAcquire] Interrupted while waiting for " + lockId);
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void dir(DfsServiceOuterClass.DirRequest request,
                    StreamObserver<DfsServiceOuterClass.DirResponse> responseObserver) {
        final String dirName = request.getDirectoryName();
        if (dirName == null || !dirName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.DirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        try {
            waitAndAcquire(dirName);

            ExtentServiceOuterClass.GetResponse getResp =
                    extentStub.get(ExtentServiceOuterClass.GetRequest.newBuilder()
                            .setFileName(dirName)
                            .build());

            if (!getResp.hasFileData()) {
                responseObserver.onNext(DfsServiceOuterClass.DirResponse.newBuilder()
                        .setSuccess(false)
                        .build());
                responseObserver.onCompleted();
                return;
            }

            String listing = getResp.getFileData().toStringUtf8();
            var names = listing.isBlank()
                    ? java.util.Collections.<String>emptyList()
                    : java.util.Arrays.stream(listing.split("\n"))
                    .filter(s -> !s.isEmpty())
                    .toList();

            responseObserver.onNext(DfsServiceOuterClass.DirResponse.newBuilder()
                    .setSuccess(true)
                    .addAllDirList(names)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.warning("[DIR] Error: " + e.getMessage());
            responseObserver.onNext(DfsServiceOuterClass.DirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
        } finally {
            releaseLock(dirName);
        }
    }

    @Override
    public void mkdir(DfsServiceOuterClass.MkdirRequest request,
                      StreamObserver<DfsServiceOuterClass.MkdirResponse> responseObserver) {
        final String dirName = request.getDirectoryName();
        if (!dirName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.MkdirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        try {
            waitAndAcquire(dirName);

            var putResp = extentStub.put(ExtentServiceOuterClass.PutRequest.newBuilder()
                    .setFileName(dirName)
                    .setFileData(com.google.protobuf.ByteString.copyFromUtf8("init"))
                    .build());

            responseObserver.onNext(DfsServiceOuterClass.MkdirResponse.newBuilder()
                    .setSuccess(putResp.getSuccess())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(DfsServiceOuterClass.MkdirResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
        } finally {
            releaseLock(dirName);
        }
    }

    @Override
    public void get(DfsServiceOuterClass.GetRequest request,
                    StreamObserver<DfsServiceOuterClass.GetResponse> responseObserver) {
        final String fileName = request.getFileName();
        if (fileName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.GetResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }

        try {
            waitAndAcquire(fileName);

            var getResp = extentStub.get(ExtentServiceOuterClass.GetRequest.newBuilder()
                    .setFileName(fileName)
                    .build());

            responseObserver.onNext(DfsServiceOuterClass.GetResponse.newBuilder()
                    .setFileData(getResp.getFileData())
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(DfsServiceOuterClass.GetResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } finally {
            releaseLock(fileName);
        }
    }

    @Override
    public void put(DfsServiceOuterClass.PutRequest req,
                    StreamObserver<DfsServiceOuterClass.PutResponse> resp) {
        final String file = req.getFileName();
        logger.info("[PUT] ========== START PUT for " + file + " ==========");

        if (file.endsWith("/")) {
            resp.onNext(DfsServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
            resp.onCompleted();
            return;
        }

        try {
            logger.info("[PUT] Calling waitAndAcquire for " + file);
            waitAndAcquire(file);
            logger.info("[PUT] Lock acquired for " + file + ", calling extent service");

            var p = extentStub.put(
                    ExtentServiceOuterClass.PutRequest.newBuilder()
                            .setFileName(file)
                            .setFileData(req.getFileData())
                            .build());

            logger.info("[PUT] Extent service returned success=" + p.getSuccess());
            resp.onNext(DfsServiceOuterClass.PutResponse.newBuilder()
                    .setSuccess(p.getSuccess())
                    .build());
            resp.onCompleted();
            logger.info("[PUT] Response sent to client");
        } catch (Exception e) {
            logger.warning("[PUT] Error: " + e.getMessage());
            e.printStackTrace();
            resp.onNext(DfsServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
            resp.onCompleted();
        } finally {
            logger.info("[PUT] Releasing lock for " + file);
            releaseLock(file);
            logger.info("[PUT] ========== END PUT for " + file + " ==========");
        }
    }

    @Override
    public void delete(DfsServiceOuterClass.DeleteRequest request,
                       StreamObserver<DfsServiceOuterClass.DeleteResponse> responseObserver) {
        final String fileName = request.getFileName();
        logger.info("[DELETE] ========== START DELETE for " + fileName + " ==========");

        if (fileName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.DeleteResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        try {
            logger.info("[DELETE] Calling waitAndAcquire for " + fileName);
            waitAndAcquire(fileName);
            logger.info("[DELETE] Lock acquired for " + fileName);

            var r = extentStub.put(
                    ExtentServiceOuterClass.PutRequest.newBuilder()
                            .setFileName(fileName)
                            .build());

            logger.info("[DELETE] Extent service returned success=" + r.getSuccess());
            responseObserver.onNext(DfsServiceOuterClass.DeleteResponse.newBuilder()
                    .setSuccess(r.getSuccess())
                    .build());
            responseObserver.onCompleted();
            logger.info("[DELETE] Response sent to client");
        } catch (Exception e) {
            logger.warning("[DELETE] Error: " + e.getMessage());
            e.printStackTrace();
            responseObserver.onNext(DfsServiceOuterClass.DeleteResponse.newBuilder()
                    .setSuccess(false).build());
            responseObserver.onCompleted();
        } finally {
            logger.info("[DELETE] Releasing lock for " + fileName);
            releaseLock(fileName);
            logger.info("[DELETE] ========== END DELETE for " + fileName + " ==========");
        }
    }
}