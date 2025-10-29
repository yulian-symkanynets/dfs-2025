package sk.tuke.dfs.lock;

import java.util.LinkedList;
import java.util.Queue;

public class LockRecord {
    String lockId;
    String ownerId;                     // current holder, null if free
    long sequence;                      // last known sequence number
    Queue<String> waitingClients = new LinkedList<>();
    boolean needRevoke;
    boolean revokeSent = false;
}
