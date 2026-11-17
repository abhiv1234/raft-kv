package dev.raftkv.raft;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A single Raft server (Ongaro &amp; Ousterhout, "In Search of an Understandable Consensus
 * Algorithm"), including pre-vote, check-quorum, fast log backtracking, snapshots and
 * InstallSnapshot.
 *
 * <p><b>Concurrency model.</b> All protocol state is guarded by one monitor ({@code lock}). The
 * lock is never held across a network call or a sleep: RPCs are built under the lock, sent from a
 * worker pool without it, and their replies are processed under it again. A single ticker thread
 * drives election timeouts, heartbeats and the leader's check-quorum.
 *
 * <p><b>Durability rule.</b> Term, vote, log entries and snapshots are written to {@link Storage}
 * before the node replies to the RPC or acknowledges the write that caused them. A storage failure
 * is fatal: the node stops rather than risk violating Raft's safety guarantees.
 */
public final class RaftNode implements RpcHandler, AutoCloseable {

    private static final Logger LOG = Logger.getLogger("raftkv.raft");
    private static final AtomicInteger THREAD_IDS = new AtomicInteger();

    public enum Role { FOLLOWER, CANDIDATE, LEADER }

    /** A point-in-time view of the node, for monitoring and tests. */
    public record Status(String id, Role role, long term, String leaderId, long commitIndex,
                         long lastApplied, long lastLogIndex, long snapshotIndex) {}

    private record Pending(long term, CompletableFuture<byte[]> future) {}

    // ---- immutable wiring ---------------------------------------------------------------
    private final String id;
    private final List<String> peers;
    private final RaftConfig cfg;
    private final Storage storage;
    private final StateMachine stateMachine;
    private final Transport transport;
    private final Random random;
    private final Object lock = new Object();

    // ---- persistent state (Raft figure 2) ------------------------------------------------
    private long currentTerm;
    private String votedFor;
    private final RaftLog log = new RaftLog();
    private Snapshot latestSnapshot;

    // ---- volatile state ------------------------------------------------------------------
    private Role role = Role.FOLLOWER;
    private String leaderId;
    private long commitIndex;
    private long lastApplied;

    // ---- leader state --------------------------------------------------------------------
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<String, Long> lastContact = new HashMap<>();
    private final Set<String> inflight = new HashSet<>();
    private long lastBroadcast;
    private final Map<Long, Pending> pending = new HashMap<>();

    // ---- election state ------------------------------------------------------------------
    private long electionDeadline;
    private long lastLeaderContact = Long.MIN_VALUE / 2;
    private int votes;
    private int preVotes;
    private long preVoteRound;

    // ---- lifecycle -----------------------------------------------------------------------
    private volatile boolean running;
    private ScheduledExecutorService ticker;
    private ExecutorService rpcPool;

    /**
     * @param id         this node's id; must appear in {@code clusterIds}
     * @param clusterIds ids of ALL members of the (static) cluster, including this node
     */
    public RaftNode(String id, List<String> clusterIds, RaftConfig cfg, Storage storage,
                    StateMachine stateMachine, Transport transport) {
        if (!clusterIds.contains(id)) throw new IllegalArgumentException("clusterIds must contain " + id);
        this.id = id;
        List<String> others = new ArrayList<>(clusterIds);
        others.remove(id);
        this.peers = List.copyOf(others);
        this.cfg = cfg;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.transport = transport;
        this.random = new Random(id.hashCode() * 31L + System.nanoTime());
    }

    // =========================================================================================
    // Lifecycle
    // =========================================================================================

    /** Recover persisted state, then start timers and begin serving RPCs. */
    public void start() throws IOException {
        synchronized (lock) {
            if (running) throw new IllegalStateException("already started");
            Storage.State s = storage.load();
            currentTerm = s.currentTerm();
            votedFor = s.votedFor();
            if (s.snapshot() != null) {
                latestSnapshot = s.snapshot();
                stateMachine.restore(s.snapshot().data());
                log.reset(s.snapshot().lastIncludedIndex(), s.snapshot().lastIncludedTerm(), s.entries());
                commitIndex = lastApplied = s.snapshot().lastIncludedIndex();
            } else {
                log.reset(0, 0, s.entries());
            }
            resetElectionDeadline();
            running = true;
        }
        int n = THREAD_IDS.incrementAndGet();
        rpcPool = Executors.newCachedThreadPool(r -> daemon(r, "raft-" + id + "-rpc-" + n));
        ticker = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "raft-" + id + "-ticker-" + n));
        transport.start(this);
        ticker.scheduleWithFixedDelay(this::tick, cfg.tickMs(), cfg.tickMs(), TimeUnit.MILLISECONDS);
        LOG.fine(() -> id + " started at term " + currentTerm + ", log up to " + log.lastIndex());
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** Stop the node. Persistent state is retained in storage; this simulates a clean crash. */
    @Override
    public void close() {
        synchronized (lock) {
            if (!running) return;
            running = false;
            failAllPending(new IllegalStateException("node " + id + " stopped"));
        }
        transport.close();
        if (ticker != null) ticker.shutdownNow();
        if (rpcPool != null) rpcPool.shutdownNow();
        try {
            if (ticker != null) ticker.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String id() { return id; }

    // =========================================================================================
    // Client-facing API
    // =========================================================================================

    /**
     * Propose a command for replication. The future completes with the state machine's result once
     * the command is committed and applied. It fails with {@link NotLeaderException} if this node is
     * not (or stops being) the leader.
     *
     * <p>A failure after the entry was appended means the outcome is <em>unknown</em>: the entry may
     * still commit under a new leader. Clients must retry with the same (clientId, seq) so the state
     * machine can de-duplicate; see {@code KvStateMachine}.
     */
    public CompletableFuture<byte[]> propose(byte[] command) {
        synchronized (lock) {
            if (!running) return CompletableFuture.failedFuture(new IllegalStateException("node stopped"));
            if (role != Role.LEADER) return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
            appendLocal(LogEntry.command(currentTerm, command));
            long index = log.lastIndex();
            CompletableFuture<byte[]> f = new CompletableFuture<>();
            pending.put(index, new Pending(currentTerm, f));
            advanceCommit(); // a single-node cluster commits immediately
            for (String p : peers) replicate(p);
            return f;
        }
    }

    public Status status() {
        synchronized (lock) {
            return new Status(id, role, currentTerm, leaderId, commitIndex, lastApplied, log.lastIndex(),
                    log.snapshotIndex());
        }
    }

    public boolean isLeader() {
        synchronized (lock) { return running && role == Role.LEADER; }
    }

    /** Number of log entries currently held in memory (i.e. not compacted). Visible for tests. */
    public int retainedLogEntries() {
        synchronized (lock) { return log.size(); }
    }

    // =========================================================================================
    // Timer: elections, heartbeats, check-quorum
    // =========================================================================================

    private void tick() {
        try {
            synchronized (lock) {
                if (!running) return;
                long now = System.nanoTime();
                if (role == Role.LEADER) {
                    if (!hasQuorumContact(now)) {
                        LOG.info(() -> id + " lost contact with a quorum; stepping down from term " + currentTerm);
                        stepDown(currentTerm, null);
                        return;
                    }
                    if (now - lastBroadcast >= ms(cfg.heartbeatIntervalMs())) broadcast();
                } else if (now >= electionDeadline) {
                    if (cfg.preVote()) startPreVote(); else startElection();
                }
            }
        } catch (Throwable t) {
            LOG.log(Level.SEVERE, id + ": ticker failure", t);
        }
    }

    private static long ms(long millis) { return TimeUnit.MILLISECONDS.toNanos(millis); }

    private int quorum() { return (peers.size() + 1) / 2 + 1; }

    private void resetElectionDeadline() {
        int span = cfg.electionTimeoutMaxMs() - cfg.electionTimeoutMinMs();
        electionDeadline = System.nanoTime() + ms(cfg.electionTimeoutMinMs() + random.nextInt(span));
    }

    /** Check-quorum: a leader that cannot reach a majority must stop believing it is the leader. */
    private boolean hasQuorumContact(long now) {
        int reachable = 1;
        for (String p : peers) {
            Long t = lastContact.get(p);
            if (t != null && now - t < ms(cfg.electionTimeoutMaxMs())) reachable++;
        }
        return reachable >= quorum();
    }

    // =========================================================================================
    // Elections (with pre-vote)
    // =========================================================================================

    private void startPreVote() {
        resetElectionDeadline();
        preVoteRound++;
        preVotes = 1;
        if (preVotes >= quorum()) { startElection(); return; }
        long round = preVoteRound;
        Rpc.RequestVote req = new Rpc.RequestVote(currentTerm + 1, id, log.lastIndex(), log.lastTerm(), true);
        for (String p : peers) rpcPool.execute(() -> askForVote(p, req, round));
    }

    private void startElection() {
        preVoteRound++; // invalidate any outstanding pre-vote round
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = id;
        leaderId = null;
        persistTermAndVote();
        resetElectionDeadline();
        votes = 1;
        LOG.fine(() -> id + " starts election for term " + currentTerm);
        if (votes >= quorum()) { becomeLeader(); return; }
        Rpc.RequestVote req = new Rpc.RequestVote(currentTerm, id, log.lastIndex(), log.lastTerm(), false);
        for (String p : peers) rpcPool.execute(() -> askForVote(p, req, 0));
    }

    private void askForVote(String peer, Rpc.RequestVote req, long round) {
        if (!running) return;
        Rpc resp;
        try {
            resp = transport.call(peer, req);
        } catch (Exception e) {
            return; // unreachable peer: simply no vote
        }
        synchronized (lock) {
            if (!running || !(resp instanceof Rpc.RequestVoteReply reply)) return;
            if (reply.term() > currentTerm) { stepDown(reply.term(), null); return; }
            if (req.preVote()) {
                if (round != preVoteRound || role == Role.LEADER || currentTerm + 1 != req.term()) return;
                if (reply.granted() && ++preVotes >= quorum()) {
                    startElection();
                }
            } else {
                if (role != Role.CANDIDATE || currentTerm != req.term()) return;
                if (reply.granted() && ++votes >= quorum()) becomeLeader();
            }
        }
    }

    private Rpc.RequestVoteReply onRequestVote(Rpc.RequestVote r) {
        synchronized (lock) {
            boolean upToDate = r.lastLogTerm() > log.lastTerm()
                    || (r.lastLogTerm() == log.lastTerm() && r.lastLogIndex() >= log.lastIndex());

            if (r.preVote()) {
                // Grant only if we would plausibly vote for it AND we have not heard from a live leader.
                boolean heardFromLeader = role == Role.LEADER
                        || System.nanoTime() - lastLeaderContact < ms(cfg.electionTimeoutMinMs());
                boolean grant = r.term() > currentTerm && upToDate && !heardFromLeader;
                return new Rpc.RequestVoteReply(currentTerm, grant);
            }

            if (r.term() < currentTerm) return new Rpc.RequestVoteReply(currentTerm, false);
            if (r.term() > currentTerm) stepDown(r.term(), null);

            boolean canVote = votedFor == null || votedFor.equals(r.candidateId());
            boolean grant = canVote && upToDate;
            if (grant) {
                votedFor = r.candidateId();
                persistTermAndVote(); // vote must be durable before it is revealed
                resetElectionDeadline();
            }
            return new Rpc.RequestVoteReply(currentTerm, grant);
        }
    }

    private void becomeLeader() {
        role = Role.LEADER;
        leaderId = id;
        long now = System.nanoTime();
        nextIndex.clear();
        matchIndex.clear();
        lastContact.clear();
        inflight.clear();
        for (String p : peers) {
            nextIndex.put(p, log.lastIndex() + 1);
            matchIndex.put(p, 0L);
            lastContact.put(p, now);
        }
        LOG.info(() -> id + " became leader for term " + currentTerm);
        // A leader may only commit entries of its own term by counting replicas (paper section 5.4.2).
        // Appending a no-op lets it commit everything before it as soon as the no-op commits.
        appendLocal(LogEntry.noop(currentTerm));
        advanceCommit();
        broadcast();
    }

    /** Revert to follower. Pass a higher term to also adopt it. */
    private void stepDown(long term, String newLeader) {
        if (term > currentTerm) {
            currentTerm = term;
            votedFor = null;
            persistTermAndVote();
        }
        boolean wasLeader = role == Role.LEADER;
        role = Role.FOLLOWER;
        leaderId = newLeader;
        preVoteRound++;
        resetElectionDeadline();
        if (wasLeader || !pending.isEmpty()) failAllPending(new NotLeaderException(newLeader));
    }

    // =========================================================================================
    // Replication (leader side)
    // =========================================================================================

    private void broadcast() {
        lastBroadcast = System.nanoTime();
        for (String p : peers) replicate(p);
    }

    /** Send the next AppendEntries (or InstallSnapshot) to {@code peer} unless one is already in flight. */
    private void replicate(String peer) {
        if (!running || role != Role.LEADER || inflight.contains(peer)) return;
        long next = nextIndex.get(peer);
        Rpc req;
        if (next <= log.snapshotIndex()) {
            // The entries this follower needs were compacted away: ship the snapshot instead.
            Snapshot s = latestSnapshot;
            req = new Rpc.InstallSnapshot(currentTerm, id, s.lastIncludedIndex(), s.lastIncludedTerm(), s.data());
        } else {
            long prev = next - 1;
            req = new Rpc.AppendEntries(currentTerm, id, prev, log.termAt(prev),
                    log.slice(next, cfg.maxEntriesPerAppend()), commitIndex);
        }
        inflight.add(peer);
        long termAtSend = currentTerm;
        try {
            rpcPool.execute(() -> sendTo(peer, req, termAtSend));
        } catch (RuntimeException rejected) { // pool shut down
            inflight.remove(peer);
        }
    }

    private void sendTo(String peer, Rpc req, long termAtSend) {
        Rpc resp = null;
        if (running) {
            try {
                resp = transport.call(peer, req);
            } catch (Exception e) {
                // network failure: retried on the next heartbeat
            }
        }
        synchronized (lock) {
            inflight.remove(peer);
            if (!running || resp == null) return;
            if (resp.term() > currentTerm) { stepDown(resp.term(), null); return; }
            if (role != Role.LEADER || currentTerm != termAtSend) return; // stale reply
            lastContact.put(peer, System.nanoTime());

            if (req instanceof Rpc.AppendEntries && resp instanceof Rpc.AppendEntriesReply r) {
                if (r.success()) {
                    long match = Math.max(matchIndex.get(peer), r.matchIndex());
                    matchIndex.put(peer, match);
                    nextIndex.put(peer, match + 1);
                    advanceCommit();
                    if (match < log.lastIndex()) replicate(peer); // keep the pipeline full
                } else {
                    nextIndex.put(peer, backtrack(peer, r));
                    replicate(peer);
                }
            } else if (req instanceof Rpc.InstallSnapshot is && resp instanceof Rpc.InstallSnapshotReply) {
                long match = Math.max(matchIndex.get(peer), is.lastIncludedIndex());
                matchIndex.put(peer, match);
                nextIndex.put(peer, match + 1);
                advanceCommit();
                if (match < log.lastIndex()) replicate(peer);
            }
        }
    }

    /** Compute the follower's new nextIndex after a rejected AppendEntries (accelerated backtracking). */
    private long backtrack(String peer, Rpc.AppendEntriesReply r) {
        long candidate = r.conflictIndex();
        if (r.conflictTerm() > 0) {
            for (long i = log.lastIndex(); i > log.snapshotIndex(); i--) {
                long t = log.termAt(i);
                if (t == r.conflictTerm()) { candidate = i + 1; break; }
                if (t < r.conflictTerm()) break;
            }
        }
        // Always make progress, and never go below 1.
        return Math.max(1, Math.min(candidate, nextIndex.get(peer) - 1));
    }

    /** Advance commitIndex to the highest index replicated on a majority in the current term. */
    private void advanceCommit() {
        if (role != Role.LEADER) return;
        long[] matches = new long[peers.size() + 1];
        int i = 0;
        for (String p : peers) matches[i++] = matchIndex.get(p);
        matches[i] = log.lastIndex();
        Arrays.sort(matches);
        long n = matches[matches.length - quorum()]; // highest index present on >= quorum nodes
        if (n > commitIndex && log.termAt(n) == currentTerm) {
            commitIndex = n;
            applyCommitted();
        }
    }

    // =========================================================================================
    // RPC handlers (follower side)
    // =========================================================================================

    @Override
    public Rpc handle(Rpc request) {
        if (!running) throw new IllegalStateException("node " + id + " is stopped");
        return switch (request) {
            case Rpc.RequestVote r -> onRequestVote(r);
            case Rpc.AppendEntries r -> onAppendEntries(r);
            case Rpc.InstallSnapshot r -> onInstallSnapshot(r);
            default -> throw new IllegalArgumentException("unexpected request " + request.getClass());
        };
    }

    private Rpc.AppendEntriesReply onAppendEntries(Rpc.AppendEntries r) {
        synchronized (lock) {
            if (r.term() < currentTerm) return new Rpc.AppendEntriesReply(currentTerm, false, 0, 0, 0);
            if (r.term() > currentTerm || role != Role.FOLLOWER) stepDown(r.term(), r.leaderId());
            leaderId = r.leaderId();
            lastLeaderContact = System.nanoTime();
            resetElectionDeadline();

            long prev = r.prevLogIndex();
            long prevTerm = r.prevLogTerm();
            List<LogEntry> entries = r.entries();

            // Part of the message may overlap our snapshot; those entries are committed and identical.
            if (prev < log.snapshotIndex()) {
                long skip = log.snapshotIndex() - prev;
                entries = skip >= entries.size() ? List.of() : entries.subList((int) skip, entries.size());
                prev = log.snapshotIndex();
                prevTerm = log.snapshotTerm();
            }

            if (prev > log.lastIndex()) { // we are missing entries
                return new Rpc.AppendEntriesReply(currentTerm, false, 0, log.lastIndex() + 1, 0);
            }
            if (log.termAt(prev) != prevTerm) { // log mismatch: report the first index of the conflicting term
                long conflictTerm = log.termAt(prev);
                long conflictIndex = prev;
                while (conflictIndex - 1 > log.snapshotIndex() && log.termAt(conflictIndex - 1) == conflictTerm) {
                    conflictIndex--;
                }
                return new Rpc.AppendEntriesReply(currentTerm, false, 0, conflictIndex, conflictTerm);
            }

            // Skip entries we already have; truncate at the first conflict; append the rest.
            int i = 0;
            while (i < entries.size()) {
                long index = prev + 1 + i;
                if (index > log.lastIndex()) break;
                if (log.termAt(index) != entries.get(i).term()) {
                    if (index <= commitIndex) {
                        throw new IllegalStateException(id + ": attempt to overwrite committed entry " + index);
                    }
                    log.truncateFrom(index);
                    persist(() -> storage.truncateFrom(index));
                    break;
                }
                i++;
            }
            if (i < entries.size()) {
                List<LogEntry> add = new ArrayList<>(entries.subList(i, entries.size()));
                long start = prev + 1 + i;
                log.appendAll(add);
                persist(() -> storage.append(start, add));
            }

            long lastNew = prev + entries.size();
            long newCommit = Math.min(r.leaderCommit(), lastNew);
            if (newCommit > commitIndex) {
                commitIndex = newCommit;
                applyCommitted();
            }
            return new Rpc.AppendEntriesReply(currentTerm, true, lastNew, 0, 0);
        }
    }

    private Rpc.InstallSnapshotReply onInstallSnapshot(Rpc.InstallSnapshot r) {
        synchronized (lock) {
            if (r.term() < currentTerm) return new Rpc.InstallSnapshotReply(currentTerm);
            if (r.term() > currentTerm || role != Role.FOLLOWER) stepDown(r.term(), r.leaderId());
            leaderId = r.leaderId();
            lastLeaderContact = System.nanoTime();
            resetElectionDeadline();

            if (r.lastIncludedIndex() <= commitIndex) { // we already have all of this
                return new Rpc.InstallSnapshotReply(currentTerm);
            }
            Snapshot snap = new Snapshot(r.lastIncludedIndex(), r.lastIncludedTerm(), r.data());
            // If our log agrees with the snapshot boundary, keep the entries that follow it.
            List<LogEntry> remaining = new ArrayList<>();
            if (log.contains(snap.lastIncludedIndex()) && log.termAt(snap.lastIncludedIndex()) == snap.lastIncludedTerm()) {
                remaining = log.slice(snap.lastIncludedIndex() + 1, Integer.MAX_VALUE);
            }
            List<LogEntry> keep = remaining;
            persist(() -> storage.saveSnapshot(snap, keep));
            log.reset(snap.lastIncludedIndex(), snap.lastIncludedTerm(), keep);
            latestSnapshot = snap;
            stateMachine.restore(snap.data());
            commitIndex = lastApplied = snap.lastIncludedIndex();
            LOG.fine(() -> id + " installed snapshot up to " + snap.lastIncludedIndex());
            return new Rpc.InstallSnapshotReply(currentTerm);
        }
    }

    // =========================================================================================
    // Applying committed entries, snapshots
    // =========================================================================================

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            long index = ++lastApplied;
            LogEntry e = log.get(index);
            byte[] result = null;
            if (e.type() == LogEntry.Type.COMMAND) {
                try {
                    result = stateMachine.apply(e.data());
                } catch (RuntimeException ex) {
                    fatal(new IOException("state machine failed on committed entry " + index, ex));
                    return;
                }
            }
            Pending p = pending.remove(index);
            if (p != null) {
                if (p.term() == e.term()) p.future().complete(result);
                else p.future().completeExceptionally(new NotLeaderException(leaderId));
            }
        }
        maybeSnapshot();
    }

    private void maybeSnapshot() {
        int threshold = cfg.snapshotThreshold();
        if (threshold <= 0 || lastApplied - log.snapshotIndex() < threshold) return;
        long index = lastApplied;
        long term = log.termAt(index);
        // Taken while holding the lock, so the state machine is exactly at 'index'. (Cost: O(state)
        // pause; see docs/DESIGN.md for the copy-on-write alternative.)
        Snapshot snap = new Snapshot(index, term, stateMachine.snapshot());
        List<LogEntry> remaining = log.slice(index + 1, Integer.MAX_VALUE);
        persist(() -> storage.saveSnapshot(snap, remaining));
        log.reset(index, term, remaining);
        latestSnapshot = snap;
        LOG.fine(() -> id + " took snapshot at " + index + ", " + remaining.size() + " entries retained");
    }

    // =========================================================================================
    // Persistence helpers
    // =========================================================================================

    @FunctionalInterface
    private interface IoAction { void run() throws IOException; }

    private void persist(IoAction action) {
        try {
            action.run();
        } catch (IOException e) {
            fatal(e);
            throw new IllegalStateException("storage failure; node halted", e);
        }
    }

    private void persistTermAndVote() {
        persist(() -> storage.saveTermAndVote(currentTerm, votedFor));
    }

    private void appendLocal(LogEntry e) {
        log.append(e);
        long index = log.lastIndex();
        persist(() -> storage.append(index, List.of(e)));
    }

    private void failAllPending(Throwable cause) {
        List<Pending> all = new ArrayList<>(pending.values());
        pending.clear();
        for (Pending p : all) p.future().completeExceptionally(cause);
    }

    /** Fail-stop: a node that cannot persist state must not keep participating. */
    private void fatal(Exception e) {
        LOG.log(Level.SEVERE, id + ": fatal error, halting node", e);
        running = false;
        failAllPending(e);
        if (ticker != null) ticker.shutdownNow();
        if (rpcPool != null) rpcPool.shutdownNow();
    }
}
