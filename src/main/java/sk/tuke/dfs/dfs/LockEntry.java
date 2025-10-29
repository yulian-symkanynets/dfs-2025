package sk.tuke.dfs.dfs;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a lock entry with semaphore-based synchronization.
 */
public class LockEntry {
    private final String lockId;
    private final Semaphore mutex;        // Mutual exclusion: 1 permit
    private final Semaphore freeSignal;   // Wait for retry: 0 permits initially
    private final AtomicLong sequence;
    private final AtomicBoolean revoked;
    private volatile LockState status;

    public LockEntry(String lockId) {
        this.lockId = lockId;
        this.mutex = new Semaphore(1, true);  // Fair semaphore
        this.freeSignal = new Semaphore(0);
        this.sequence = new AtomicLong(0);
        this.revoked = new AtomicBoolean(false);
        this.status = LockState.NONE;
    }

    public String getLockId() {
        return lockId;
    }

    public Semaphore getMutex() {
        return mutex;
    }

    public Semaphore getFreeSignal() {
        return freeSignal;
    }

    public AtomicLong getSequence() {
        return sequence;
    }

    public AtomicBoolean getRevoked() {
        return revoked;
    }

    public synchronized LockState getStatus() {
        return status;
    }

    public synchronized void setStatus(LockState status) {
        this.status = status;
    }
}
