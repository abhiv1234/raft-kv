package dev.raftkv.raft;

import java.io.IOException;

/** Node-to-node messaging. One instance per node. */
public interface Transport extends AutoCloseable {

    /** Begin delivering inbound requests to {@code handler}. */
    void start(RpcHandler handler) throws IOException;

    /** Blocking request/response call. Throws {@link IOException} on any network failure or timeout. */
    Rpc call(String targetId, Rpc request) throws IOException;

    @Override
    void close();
}
