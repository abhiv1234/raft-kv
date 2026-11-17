package dev.raftkv.raft;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Durable storage for everything Raft requires to survive a crash: current term, vote, the log
 * and the latest snapshot. Every method must be durable when it returns: Raft nodes reply to RPCs
 * only after the corresponding state is persisted.
 */
public interface Storage extends Closeable {

    /** Everything recovered at startup. {@code entries} are the log entries after the snapshot. */
    record State(long currentTerm, String votedFor, Snapshot snapshot, List<LogEntry> entries) {}

    State load() throws IOException;

    void saveTermAndVote(long term, String votedFor) throws IOException;

    /** Remove all log entries with index {@code >= index}. */
    void truncateFrom(long index) throws IOException;

    /** Append entries; the first one gets log index {@code startIndex}. */
    void append(long startIndex, List<LogEntry> entries) throws IOException;

    /**
     * Atomically install {@code snapshot} and compact the log so only {@code remaining} (the
     * entries after the snapshot) are kept.
     */
    void saveSnapshot(Snapshot snapshot, List<LogEntry> remaining) throws IOException;

    @Override
    void close();
}
