package dev.raftkv.raft;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory view of the Raft log with a compaction offset. Indices are 1-based and absolute;
 * everything at or below {@code snapshotIndex} has been compacted into a snapshot.
 * Not thread-safe: guarded by the owning {@link RaftNode}'s lock.
 */
final class RaftLog {
    private long snapIndex;
    private long snapTerm;
    private final ArrayList<LogEntry> entries = new ArrayList<>();

    long snapshotIndex() { return snapIndex; }

    long snapshotTerm() { return snapTerm; }

    long lastIndex() { return snapIndex + entries.size(); }

    long lastTerm() { return entries.isEmpty() ? snapTerm : entries.get(entries.size() - 1).term(); }

    /** True if the entry at {@code index} is still held in the log (not compacted, not beyond the end). */
    boolean contains(long index) { return index > snapIndex && index <= lastIndex(); }

    LogEntry get(long index) {
        if (!contains(index)) throw new IllegalArgumentException("index " + index + " not in log (" + (snapIndex + 1) + ".." + lastIndex() + ")");
        return entries.get((int) (index - snapIndex - 1));
    }

    /** Term of the entry at {@code index}; also valid for the snapshot boundary and for index 0. */
    long termAt(long index) {
        if (index == snapIndex) return snapTerm;
        return get(index).term();
    }

    void append(LogEntry entry) { entries.add(entry); }

    void appendAll(List<LogEntry> more) { entries.addAll(more); }

    /** Remove entries with index {@code >= index}. */
    void truncateFrom(long index) {
        if (index <= snapIndex) throw new IllegalArgumentException("cannot truncate into snapshot");
        if (index > lastIndex()) return;
        entries.subList((int) (index - snapIndex - 1), entries.size()).clear();
    }

    /** Copy of the entries from {@code from} (inclusive), at most {@code max}. */
    List<LogEntry> slice(long from, int max) {
        if (from > lastIndex()) return new ArrayList<>();
        int start = (int) (from - snapIndex - 1);
        int end = (int) Math.min((long) entries.size(), (long) start + max);
        return new ArrayList<>(entries.subList(start, end));
    }

    /** Replace the log with a snapshot boundary followed by {@code remaining}. */
    void reset(long snapshotIndex, long snapshotTerm, List<LogEntry> remaining) {
        this.snapIndex = snapshotIndex;
        this.snapTerm = snapshotTerm;
        this.entries.clear();
        this.entries.addAll(remaining);
    }

    int size() { return entries.size(); }
}
