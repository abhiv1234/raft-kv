package dev.raftkv.raft;

/** Handles an inbound RPC and returns the reply. Implemented by {@link RaftNode}. */
public interface RpcHandler {
    Rpc handle(Rpc request);
}
