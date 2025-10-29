package sk.tuke.dfs.lock;

public class RetryRequest {
    String ownerId;
    String lockId;
    long sequence;

    public RetryRequest(String lockId, String ownerId, long sequence) {
        this.lockId = lockId;
        this.ownerId = ownerId;
        this.sequence = sequence;
    }
}
