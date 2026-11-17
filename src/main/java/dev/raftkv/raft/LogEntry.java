package dev.raftkv.raft;

import java.io.Serializable;

/** A single replicated log entry. The index is implicit (position in the log). */
public record LogEntry(long term, Type type, byte[] data) implements Serializable {

    public enum Type {
        /** Appended by a new leader to commit entries from earlier terms (Raft paper, section 8). */
        NOOP,
        /** An opaque state-machine command. */
        COMMAND
    }

    public static LogEntry noop(long term) {
        return new LogEntry(term, Type.NOOP, new byte[0]);
    }

    public static LogEntry command(long term, byte[] data) {
        return new LogEntry(term, Type.COMMAND, data);
    }
}
