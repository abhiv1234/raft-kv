package dev.raftkv.raft;

/** A point-in-time image of the state machine covering the log up to {@code lastIncludedIndex}. */
public record Snapshot(long lastIncludedIndex, long lastIncludedTerm, byte[] data) {}
