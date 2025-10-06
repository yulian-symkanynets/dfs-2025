package sk.tuke.dfs.dfs;

import dfs.dfs.DfsServiceGrpc;
import dfs.dfs.DfsServiceOuterClass;
import dfs.extent.ExtentServiceGrpc;
import dfs.extent.ExtentServiceOuterClass;
import dfs.lock.LockServiceGrpc;
import dfs.lock.LockServiceOuterClass;
import io.grpc.stub.StreamObserver;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;


public class DfsServiceImpl extends DfsServiceGrpc.DfsServiceImplBase {
    //    private final LockServiceImpl lockService;
//    private final ExtentServiceGrpc.ExtentServiceBlockingStub extentService;
//
//    DfsServiceImpl(LockServiceImpl lockService, ExtentServiceImpl extentService) {
//        this.lockService = lockService;
//        this.extentService = extentService;
//    }
    private LockServiceGrpc.LockServiceBlockingStub lockServiceBlockingStub;
    private ExtentServiceGrpc.ExtentServiceBlockingStub extentServiceBlockingStub;

    public DfsServiceImpl(LockServiceGrpc.LockServiceBlockingStub lockServiceBlockingStub, ExtentServiceGrpc.ExtentServiceBlockingStub extentServiceBlockingStub) {
        super();
        this.extentServiceBlockingStub = extentServiceBlockingStub;
        this.lockServiceBlockingStub = lockServiceBlockingStub;
    }

    @Override
    public void stop(DfsServiceOuterClass.StopRequest request, StreamObserver<DfsServiceOuterClass.StopResponse> responseObserver) {
        super.stop(request, responseObserver);
    }

    @Override
    public void dir(DfsServiceOuterClass.DirRequest request,
                    StreamObserver<DfsServiceOuterClass.DirResponse> responseObserver) {
        final String directoryName = request.getDirectoryName();

        // 1) Validate path shape (dirs must end with '/')
        if (directoryName == null || !directoryName.endsWith("/")) {
            responseObserver.onNext(
                    DfsServiceOuterClass.DirResponse.newBuilder()
                            .setSuccess(false)
                            .build()
            );
            responseObserver.onCompleted();
            return;
        }

        boolean acquired = false;
        try {
            // 2) Acquire lock
            lockServiceBlockingStub.acquire(
                    LockServiceOuterClass.AcquireRequest.newBuilder()
                            .setLockId(directoryName)
                            .build()
            );
            acquired = true;

            // 3) Call Extent
            ExtentServiceOuterClass.GetResponse getResponse =
                    extentServiceBlockingStub.get(
                            ExtentServiceOuterClass.GetRequest.newBuilder()
                                    .setFileName(directoryName)
                                    .build()
                    );

            // 4) Handle "null" case (no fileData set)
            if (!getResponse.hasFileData()) {
                responseObserver.onNext(
                        DfsServiceOuterClass.DirResponse.newBuilder()
                                .setSuccess(false)
                                .build()
                );
                responseObserver.onCompleted();
                return;
            }

            // 5) Convert bytes -> UTF-8 string -> split into names
            String listing = String.valueOf(getResponse.getFileData().toStringUtf8());
            java.util.List<String> names = listing.isBlank()
                    ? java.util.Collections.emptyList()
                    : java.util.Arrays.stream(listing.split("\n"))
                    .filter(s -> !s.isEmpty())
                    .toList();

            // 6) Respond once with success + names
            responseObserver.onNext(
                    DfsServiceOuterClass.DirResponse.newBuilder()
                            .setSuccess(true)
                            .addAllDirList(names)
                            .build()
            );
            responseObserver.onCompleted();

        } catch (Exception e) {
            // On any error: single failure response
            responseObserver.onNext(
                    DfsServiceOuterClass.DirResponse.newBuilder()
                            .setSuccess(false)
                            .build()
            );
            responseObserver.onCompleted();

        } finally {
            // 7) Always release ONLY if acquire succeeded
            if (acquired) {
                try {
                    lockServiceBlockingStub.release(
                            LockServiceOuterClass.ReleaseRequest.newBuilder()
                                    .setLockId(directoryName)
                                    .build()
                    );
                } catch (Exception ignore) {
                    // log if you want, but don't send another response
                }
            }
        }
    }


    @Override
    public void mkdir(DfsServiceOuterClass.MkdirRequest request, StreamObserver<DfsServiceOuterClass.MkdirResponse> responseObserver) {
        if (!request.getDirectoryName().endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.MkdirResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }
        final String dirName = request.getDirectoryName();
        boolean aquired = false;
        try {
            lockServiceBlockingStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder().setLockId(dirName).build());
            aquired = true;
            ExtentServiceOuterClass.PutResponse putResponse = extentServiceBlockingStub.put(ExtentServiceOuterClass.PutRequest.newBuilder().setFileName(dirName).setFileData(com.google.protobuf.ByteString.copyFromUtf8("init")).build());
            responseObserver.onNext(DfsServiceOuterClass.MkdirResponse.newBuilder().setSuccess(putResponse.getSuccess()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(
                    DfsServiceOuterClass.MkdirResponse.newBuilder()
                            .setSuccess(false)
                            .build()
            );
            responseObserver.onCompleted();

        } finally {
            if (aquired) {
                try {
                    lockServiceBlockingStub.release(
                            LockServiceOuterClass.ReleaseRequest.newBuilder()
                                    .setLockId(dirName)
                                    .build()
                    );
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public void rmdir(DfsServiceOuterClass.RmdirRequest request, StreamObserver<DfsServiceOuterClass.RmdirResponse> responseObserver) {
        final String dirName = request.getDirectoryName();
        if (!dirName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.RmdirResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }
        boolean aquired = false;
        try {
            lockServiceBlockingStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder().setLockId(dirName).build());
            aquired = true;
            ExtentServiceOuterClass.PutResponse putResponse = extentServiceBlockingStub.put(ExtentServiceOuterClass.PutRequest.newBuilder().setFileName(dirName).build());
            responseObserver.onNext(DfsServiceOuterClass.RmdirResponse.newBuilder().setSuccess(putResponse.getSuccess()).build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onNext(DfsServiceOuterClass.RmdirResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
        } finally {
            if (aquired) {
                try {
                    lockServiceBlockingStub.release(LockServiceOuterClass.ReleaseRequest.newBuilder().setLockId(dirName).build());
                } catch (Exception ignore) {

                }
            }
        }
    }

    @Override
    public void get(DfsServiceOuterClass.GetRequest request, StreamObserver<DfsServiceOuterClass.GetResponse> responseObserver) {
        final String fileName = request.getFileName();
        if (fileName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.GetResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        }
        boolean aquired = false;
        try {
            lockServiceBlockingStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder().setLockId(fileName).build());
            ExtentServiceOuterClass.GetResponse getResponse = extentServiceBlockingStub.get(ExtentServiceOuterClass.GetRequest.newBuilder().setFileName(fileName).build());
            responseObserver.onNext(DfsServiceOuterClass.GetResponse.newBuilder().setFileData(getResponse.getFileData()).build());
            responseObserver.onCompleted();
            aquired = true;
        } catch (Exception e) {
            responseObserver.onNext(DfsServiceOuterClass.GetResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } finally {
            if (aquired) {
                try {
                    lockServiceBlockingStub.release(LockServiceOuterClass.ReleaseRequest.newBuilder().setLockId(fileName).build());
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public void put(DfsServiceOuterClass.PutRequest request, StreamObserver<DfsServiceOuterClass.PutResponse> responseObserver) {
        final String fileName = request.getFileName();
        if (fileName.endsWith("/")) {
            responseObserver.onNext(DfsServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
        }
        boolean aquired = false;
        try {
            lockServiceBlockingStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder().setLockId(fileName).build());
            ExtentServiceOuterClass.PutResponse putResponse = extentServiceBlockingStub.put(ExtentServiceOuterClass.PutRequest.newBuilder().setFileName(fileName).setFileData(request.getFileData()).build());
            responseObserver.onNext(DfsServiceOuterClass.PutResponse.newBuilder().setSuccess(putResponse.getSuccess()).build());
            responseObserver.onCompleted();
            aquired = true;
        } catch (Exception e) {
            responseObserver.onNext(DfsServiceOuterClass.PutResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
        } finally {
            if (aquired) {
                try {
                    lockServiceBlockingStub.release(LockServiceOuterClass.ReleaseRequest.newBuilder().setLockId(fileName).build());
                } catch (Exception ignore) {
                }
            }
        }
    }

    @Override
    public void delete(DfsServiceOuterClass.DeleteRequest request, StreamObserver<DfsServiceOuterClass.DeleteResponse> responseObserver) {
        final String fileName = request.getFileName();

        if(fileName.endsWith("/")){
            responseObserver.onNext(DfsServiceOuterClass.DeleteResponse.newBuilder().setSuccess(false).build());
            responseObserver.onCompleted();
            return;
        }

        boolean acquired = false;

        try{
            lockServiceBlockingStub.acquire(LockServiceOuterClass.AcquireRequest.newBuilder().setLockId(fileName).build());
            acquired = true;
            ExtentServiceOuterClass.PutResponse putResponse = extentServiceBlockingStub.put(ExtentServiceOuterClass.PutRequest.newBuilder().setFileName(fileName).build());
            responseObserver.onNext(DfsServiceOuterClass.DeleteResponse.newBuilder().setSuccess(putResponse.getSuccess()).build());
            responseObserver.onCompleted();
        }catch (Exception e){

        } finally {
            if(acquired){
                try {
                    lockServiceBlockingStub.release(LockServiceOuterClass.ReleaseRequest.newBuilder().setLockId(fileName).build());
                }catch (Exception ignore){}
            }
        }
    }
}
