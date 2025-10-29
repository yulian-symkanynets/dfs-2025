package sk.tuke.dfs.lock;

public class RevokeRequest {
    String lockId;
    String ownerId;

    RevokeRequest(String lockId, String ownerId) {
        this.lockId = lockId;
        this.ownerId = ownerId;
    }
}
