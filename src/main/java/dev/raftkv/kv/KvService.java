package dev.raftkv.kv;

import dev.raftkv.raft.RaftNode;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/** Typed client API over a {@link RaftNode} running a {@link KvStateMachine}. */
public final class KvService {
    private final RaftNode node;
    private final KvStateMachine stateMachine;

    public KvService(RaftNode node, KvStateMachine stateMachine) {
        this.node = node;
        this.stateMachine = stateMachine;
    }

    /**
     * Replicate a command and return its result once committed and applied. All operations,
     * including GET, go through the log, which makes them linearizable.
     */
    public CompletableFuture<KvResult> execute(KvCommand command) {
        return node.propose(command.encode()).thenApply(KvResult::decode);
    }

    /** Read this node's local state without consensus. Fast, but may be stale on any node. */
    public Optional<String> readStale(String key) {
        return stateMachine.readLocal(key);
    }

    public RaftNode node() { return node; }
}
