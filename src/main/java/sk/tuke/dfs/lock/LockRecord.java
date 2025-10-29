package sk.tuke.dfs.lock;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class LockRecord {
    String lockId;
    String ownerId;                     // current holder, null if free
    ConcurrentHashMap<String, Long> clientSequences = new ConcurrentHashMap<>(); // sequence per client
    Queue<String> waitingClients = new LinkedList<>();
    boolean revokeSent = false;
}
