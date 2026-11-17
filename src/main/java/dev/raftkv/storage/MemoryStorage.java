package dev.raftkv.storage;

import dev.raftkv.raft.LogEntry;
import dev.raftkv.raft.Snapshot;
import dev.raftkv.raft.Storage;

import java.util.ArrayList;
import java.util.List;

/**
 * Non-durable {@link Storage} that lives as long as the object does. Tests keep the instance
 * across a simulated node crash, so a "restarted" node recovers exactly what it had persisted.
 */
public final class MemoryStorage implements Storage {
    private long term;
    private String votedFor;
    private Snapshot snapshot;
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    private long base() { return snapshot == null ? 0 : snapshot.lastIncludedIndex(); }

    @Override
    public synchronized State load() {
        return new State(term, votedFor, snapshot, new ArrayList<>(entries));
    }

    @Override
    public synchronized void saveTermAndVote(long term, String votedFor) {
        this.term = term;
        this.votedFor = votedFor;
    }

    @Override
    public synchronized void truncateFrom(long index) {
        int from = (int) (index - base() - 1);
        if (from < 0) throw new IllegalArgumentException("truncate into snapshot");
        if (from < entries.size()) entries.subList(from, entries.size()).clear();
    }

    @Override
    public synchronized void append(long startIndex, List<LogEntry> newEntries) {
        if (startIndex != base() + entries.size() + 1) {
            throw new IllegalStateException("non-contiguous append at " + startIndex + ", expected " + (base() + entries.size() + 1));
        }
        entries.addAll(newEntries);
    }

    @Override
    public synchronized void saveSnapshot(Snapshot s, List<LogEntry> remaining) {
        this.snapshot = s;
        this.entries.clear();
        this.entries.addAll(remaining);
    }

    @Override
    public void close() {}
}
