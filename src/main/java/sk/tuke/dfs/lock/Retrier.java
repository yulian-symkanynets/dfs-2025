package sk.tuke.dfs.lock;

import dfs.dfs.LockCacheServiceGrpc;
import dfs.dfs.LockCacheServiceOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.concurrent.BlockingQueue;

public class Retrier implements Runnable{

    BlockingQueue<RetryRequest> requestBlockingQueue;
    boolean running;

    public Retrier(BlockingQueue<RetryRequest> requestBlockingQueue) {
        this.requestBlockingQueue = requestBlockingQueue;
        running = true;
    }

    @Override
    public void run() {
        while (running && !Thread.interrupted()){
            try {
                RetryRequest retryRequest = requestBlockingQueue.take();
                String[] parts = retryRequest.ownerId.split(":");
                String hostname = parts[0];
                int port = Integer.parseInt(parts[1]);
                ManagedChannel cacheCh = ManagedChannelBuilder.forAddress(hostname, port).usePlaintext().build();

                LockCacheServiceGrpc.LockCacheServiceBlockingStub lockCacheServiceStub = LockCacheServiceGrpc.newBlockingStub(cacheCh);

                lockCacheServiceStub.retry(LockCacheServiceOuterClass.RetryRequest.newBuilder().setLockId(retryRequest.lockId).build());

                cacheCh.shutdown();

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
