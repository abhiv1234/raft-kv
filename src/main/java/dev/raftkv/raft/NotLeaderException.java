package dev.raftkv.raft;

/** Thrown (or used to fail a future) when a request reached a node that is not the leader. */
public class NotLeaderException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final String leaderHint;

    public NotLeaderException(String leaderHint) {
        super(leaderHint == null ? "not the leader; no leader known" : "not the leader; leader is " + leaderHint);
        this.leaderHint = leaderHint;
    }

    /** The node believed to be the leader, or {@code null} if unknown. */
    public String leaderHint() {
        return leaderHint;
    }
}
