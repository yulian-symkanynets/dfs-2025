package sk.tuke.dfs.dfs;

import dfs.dfs.DfsServiceGrpc;
import dfs.dfs.DfsServiceOuterClass;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class DfsServiceImpl extends DfsServiceGrpc.DfsServiceImplBase {

    private static final Logger logger = Logger.getLogger(DfsServiceImpl.class.getName());

    private final ConcurrentHashMap<String, LockEntry> lockTable;
    private final LockServiceGrpc.LockServiceBlockingStub lockStub;
    private final ExtentServiceGrpc.ExtentServiceBlockingStub extentStub;
    private final String ownerId;

    public DfsServiceImpl(LockServiceGrpc.LockServiceBlockingStub lockStub,
                          ExtentServiceGrpc.ExtentServiceBlockingStub extentStub,
                          String ownerId,
                          ConcurrentHashMap<String, LockEntry> lockTable) {
        this.lockStub = lockStub;
        this.extentStub = extentStub;
        this.ownerId = ownerId;
        this.lockTable = lockTable;
    }

    private boolean acquireLock(String lockId) throws InterruptedException {
        logger.info("[AcquireLock] Acquiring lock for " + lockId);

        LockEntry lockEntry = lockTable.computeIfAbsent(lockId, LockEntry::new);

        // If lock is in NONE state, we need to acquire it from lock service
        if (lockEntry.getStatus() == LockState.NONE) {
            lockEntry.setStatus(LockState.ACQUIRING);
            logger.info("[AcquireLock] Status is NONE, requesting from lock service");

            var response = lockStub.acquire(
                LockServiceOuterClass.AcquireRequest.newBuilder()
                    .setLockId(lockId)
                    .setOwnerId(ownerId)
                    .setSequence(lockEntry.getSequence().incrementAndGet())
                    .build()
            );

            if (!response.getSuccess()) {
                logger.info("[AcquireLock] Lock denied, waiting for retry signal");
                // Wait for retry signal from lock service
                lockEntry.getFreeSignal().acquire();
                logger.info("[AcquireLock] Retry signal received");
            }

            // Acquire the mutex (mutual exclusion for operation)
            logger.info("[AcquireLock] Acquiring mutex");
            lockEntry.getMutex().acquire();
            lockEntry.setStatus(LockState.LOCKED);
            logger.info("[AcquireLock] Mutex acquired, status set to LOCKED");
        } else {
            // Lock already acquired by us (re-entrant case), just get the mutex
            logger.info("[AcquireLock] Lock already held (status=" + lockEntry.getStatus() + "), acquiring mutex");
            lockEntry.getMutex().acquire();
            lockEntry.setStatus(LockState.LOCKED);
            logger.info("[AcquireLock] Mutex acquired");
        }

        return true;
    }

    private void releaseLock(String lockId) {
        logger.info("[ReleaseLock] Releasing lock for " + lockId);

        LockEntry lockEntry = lockTable.get(lockId);
        if (lockEntry == null) {
            logger.info("[ReleaseLock] Lock entry not found, nothing to release");
            return;
        }

        // Release the mutex and update status
        lockEntry.getMutex().release();
        lockEntry.setStatus(LockState.FREE);
        logger.info("[ReleaseLock] Mutex released, status set to FREE");
        
        // If revoked, signal the revoke task that we're done
        if (lockEntry.getRevoked().get()) {
            logger.info("[ReleaseLock] Lock was revoked, releasing freeSignal to wake revoke task");
            lockEntry.getFreeSignal().release();
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
            acquireLock(dirName);

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
            acquireLock(dirName);

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
            acquireLock(fileName);

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
            logger.info("[PUT] Acquiring lock for " + file);
            acquireLock(file);
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
            logger.info("[DELETE] Acquiring lock for " + fileName);
            acquireLock(fileName);
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