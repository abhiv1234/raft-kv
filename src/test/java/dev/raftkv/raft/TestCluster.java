package dev.raftkv.raft;

import dev.raftkv.kv.KvCommand;
import dev.raftkv.kv.KvResult;
import dev.raftkv.kv.KvStateMachine;
import dev.raftkv.net.InMemoryNetwork;
import dev.raftkv.storage.MemoryStorage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A multi-node Raft cluster wired through an {@link InMemoryNetwork}. Crashing a node stops it but
 * keeps its storage, so a restart recovers exactly what the node had persisted.
 */
public final class TestCluster implements AutoCloseable {
    static {
        Logger.getLogger("").setLevel(Level.WARNING);
        Logger.getLogger("raftkv").setLevel(Level.WARNING);
    }

    public final List<String> ids = new ArrayList<>();
    public final InMemoryNetwork net = new InMemoryNetwork();
    public final RaftConfig cfg;
    private final Map<String, Supplier<dev.raftkv.raft.Storage>> storageFactories = new LinkedHashMap<>();
    private final Map<String, Storage> storages = new ConcurrentHashMap<>();
    private final Map<String, KvStateMachine> machines = new ConcurrentHashMap<>();
    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();

    public TestCluster(int size, RaftConfig cfg) {
        this.cfg = cfg;
        for (int i = 1; i <= size; i++) {
            String id = "n" + i;
            ids.add(id);
            storages.put(id, new MemoryStorage());
        }
    }

    /** Use custom storage (e.g. file-backed) per node. The factory is called on every (re)start. */
    public TestCluster withStorage(java.util.function.Function<String, Storage> factory) {
        for (String id : ids) storageFactories.put(id, () -> factory.apply(id));
        return this;
    }

    public TestCluster startAll() throws Exception {
        for (String id : ids) start(id);
        return this;
    }

    public synchronized void start(String id) throws Exception {
        if (nodes.containsKey(id)) return;
        Supplier<Storage> f = storageFactories.get(id);
        if (f != null) storages.put(id, f.get());
        KvStateMachine sm = new KvStateMachine();
        RaftNode node = new RaftNode(id, ids, cfg, storages.get(id), sm, net.endpoint(id));
        machines.put(id, sm);
        node.start();
        nodes.put(id, node);
    }

    public synchronized void crash(String id) {
        RaftNode n = nodes.remove(id);
        if (n != null) {
            n.close();
            if (storageFactories.containsKey(id)) storages.get(id).close(); // file storage: release handles
        }
    }

    public void restart(String id) throws Exception {
        crash(id);
        start(id);
    }

    public boolean isRunning(String id) { return nodes.containsKey(id); }

    public RaftNode node(String id) { return nodes.get(id); }

    /** The storage object backing {@code id}; lets a test pre-load persisted state before starting the node. */
    public Storage storage(String id) { return storages.get(id); }

    public KvStateMachine machine(String id) { return machines.get(id); }

    public List<String> runningIds() { return new ArrayList<>(nodes.keySet()); }

    public List<RaftNode> running() { return new ArrayList<>(nodes.values()); }

    /** The running node that currently believes it is leader in the highest term, if any. */
    public Optional<RaftNode> leader() {
        return running().stream().filter(RaftNode::isLeader)
                .max(Comparator.comparingLong(n -> n.status().term()));
    }

    /** Wait until some leader exists AND its term is the highest term among the running nodes. */
    public RaftNode awaitLeader() {
        return awaitLeader(10_000);
    }

    public RaftNode awaitLeader(long timeoutMs) {
        final RaftNode[] found = new RaftNode[1];
        waitUntil(() -> {
            Optional<RaftNode> l = leader();
            if (l.isEmpty()) return false;
            long maxTerm = running().stream().mapToLong(n -> n.status().term()).max().orElse(0);
            if (l.get().status().term() != maxTerm) return false;
            found[0] = l.get();
            return true;
        }, timeoutMs, "a leader to be elected");
        return found[0];
    }

    /** Wait for a leader that is NOT one of {@code excluded}. */
    public RaftNode awaitLeaderOtherThan(long timeoutMs, String... excluded) {
        List<String> ex = List.of(excluded);
        final RaftNode[] found = new RaftNode[1];
        waitUntil(() -> {
            for (RaftNode n : running()) {
                if (n.isLeader() && !ex.contains(n.id())) { found[0] = n; return true; }
            }
            return false;
        }, timeoutMs, "a leader other than " + ex);
        return found[0];
    }

    public static void waitUntil(BooleanSupplier cond, long timeoutMs, String what) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        while (System.nanoTime() < deadline) {
            try {
                if (cond.getAsBoolean()) return;
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
        }
        throw new AssertionError("timed out after " + timeoutMs + "ms waiting for " + what);
    }

    /** Wait until every running node has applied the same entries and holds identical data. */
    public void awaitConverged(long timeoutMs) {
        waitUntil(() -> {
            List<RaftNode> ns = running();
            if (ns.isEmpty()) return true;
            RaftNode.Status first = ns.get(0).status();
            Map<String, String> data = machines.get(ns.get(0).id()).copyData();
            for (RaftNode n : ns) {
                RaftNode.Status s = n.status();
                if (s.lastApplied() != first.lastApplied() || s.commitIndex() != first.commitIndex()) return false;
                if (s.lastApplied() != s.commitIndex()) return false;
                if (!machines.get(n.id()).copyData().equals(data)) return false;
            }
            return true;
        }, timeoutMs, "all running nodes to converge");
    }

    public Client client(String clientId) { return new Client(clientId); }

    @Override
    public void close() {
        for (String id : new ArrayList<>(nodes.keySet())) crash(id);
    }

    /**
     * A retrying client with a stable id: each logical operation keeps its sequence number across
     * retries, which is what makes retries after ambiguous failures safe (exactly-once).
     */
    public final class Client {
        private final String clientId;
        private long seq;

        Client(String clientId) { this.clientId = clientId; }

        public KvResult put(String key, String value) { return execute(KvCommand.put(key, value, clientId, ++seq), 15_000); }

        public KvResult get(String key) { return execute(KvCommand.get(key, clientId, ++seq), 15_000); }

        public KvResult delete(String key) { return execute(KvCommand.delete(key, clientId, ++seq), 15_000); }

        public KvResult cas(String key, String expected, String value) {
            return execute(KvCommand.cas(key, expected, value, clientId, ++seq), 15_000);
        }

        /** Re-send the same command (same clientId and seq) as a retry would. */
        public KvResult execute(KvCommand cmd, long timeoutMs) {
            KvResult r = tryExecute(cmd, timeoutMs);
            if (r == null) throw new AssertionError("no result for " + cmd + " within " + timeoutMs + "ms");
            return r;
        }

        /** Like execute, but returns null on timeout instead of throwing. */
        public KvResult tryExecute(KvCommand cmd, long timeoutMs) {
            long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
            while (System.nanoTime() < deadline) {
                Optional<RaftNode> l = leader();
                if (l.isPresent()) {
                    try {
                        byte[] out = l.get().propose(cmd.encode()).get(1_500, java.util.concurrent.TimeUnit.MILLISECONDS);
                        return KvResult.decode(out);
                    } catch (java.util.concurrent.TimeoutException | java.util.concurrent.ExecutionException e) {
                        // ambiguous or not-leader: retry with the SAME (clientId, seq)
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return null;
                    }
                }
                try {
                    Thread.sleep(15);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
            }
            return null;
        }

        public String clientId() { return clientId; }

        public long lastSeq() { return seq; }
    }
}
