package sk.tuke.dfs.dfs;

public enum LockState {
    NONE,
    FREE,
    LOCKED,
    ACQUIRING,
    RELEASING,
    REVOKE_PENDING
}
