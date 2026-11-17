package dev.raftkv.raft;

/**
 * The replicated state machine. Implementations MUST be deterministic: the same sequence of
 * commands must always produce the same state and results on every node.
 */
public interface StateMachine {

    /** Apply a committed command and return its result. Must not throw for well-formed commands. */
    byte[] apply(byte[] command);

    /** Serialize the full current state. Called with the Raft lock held, so it is a consistent cut. */
    byte[] snapshot();

    /** Replace the full state with a previously produced snapshot. */
    void restore(byte[] snapshot);
}
