package dev.raftkv.raft;

/** Timing and tuning knobs. */
public record RaftConfig(
        int electionTimeoutMinMs,
        int electionTimeoutMaxMs,
        int heartbeatIntervalMs,
        int tickMs,
        int snapshotThreshold,
        int maxEntriesPerAppend,
        boolean preVote) {

    public RaftConfig {
        if (electionTimeoutMinMs <= heartbeatIntervalMs * 2)
            throw new IllegalArgumentException("election timeout must be well above the heartbeat interval");
        if (electionTimeoutMaxMs <= electionTimeoutMinMs)
            throw new IllegalArgumentException("electionTimeoutMaxMs must exceed electionTimeoutMinMs");
    }

    /** Defaults suited to a LAN / single-host cluster. */
    public static RaftConfig defaults() {
        return new RaftConfig(300, 600, 75, 10, 10_000, 256, true);
    }

    /** Fast timings for tests. */
    public static RaftConfig fast() {
        return new RaftConfig(150, 300, 40, 5, 10_000, 256, true);
    }

    public RaftConfig withSnapshotThreshold(int n) {
        return new RaftConfig(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, tickMs, n,
                maxEntriesPerAppend, preVote);
    }

    public RaftConfig withPreVote(boolean enabled) {
        return new RaftConfig(electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs, tickMs,
                snapshotThreshold, maxEntriesPerAppend, enabled);
    }

    public RaftConfig withElectionTimeout(int minMs, int maxMs, int heartbeatMs) {
        return new RaftConfig(minMs, maxMs, heartbeatMs, tickMs, snapshotThreshold, maxEntriesPerAppend, preVote);
    }
}
