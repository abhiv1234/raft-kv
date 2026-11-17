package dev.raftkv.raft;

import java.io.Serializable;
import java.util.List;

/** The wire messages exchanged between Raft nodes. */
public sealed interface Rpc extends Serializable {

    long term();

    /**
     * RequestVote. When {@code preVote} is true this is a "would you vote for me?" probe that does
     * not change any state on the receiver (Raft thesis, section 9.6). {@code term} is then the
     * term the candidate <em>would</em> start.
     */
    record RequestVote(long term, String candidateId, long lastLogIndex, long lastLogTerm, boolean preVote)
            implements Rpc {}

    record RequestVoteReply(long term, boolean granted) implements Rpc {}

    record AppendEntries(long term, String leaderId, long prevLogIndex, long prevLogTerm,
                         List<LogEntry> entries, long leaderCommit) implements Rpc {}

    /**
     * On failure, {@code conflictIndex}/{@code conflictTerm} let the leader skip back a whole term
     * at a time instead of one entry per round trip.
     */
    record AppendEntriesReply(long term, boolean success, long matchIndex,
                              long conflictIndex, long conflictTerm) implements Rpc {}

    /** Whole-snapshot transfer (single chunk). See docs/DESIGN.md for the trade-off. */
    record InstallSnapshot(long term, String leaderId, long lastIncludedIndex, long lastIncludedTerm,
                           byte[] data) implements Rpc {}

    record InstallSnapshotReply(long term) implements Rpc {}
}
