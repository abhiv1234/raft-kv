package dev.raftkv.net;

import dev.raftkv.raft.Rpc;
import dev.raftkv.raft.RpcHandler;
import dev.raftkv.raft.Transport;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

/**
 * A simulated network for tests, with fault injection: network partitions, message loss (requests
 * and replies), and random latency. Calls are delivered synchronously on the caller's thread.
 */
public final class InMemoryNetwork {
    private final Map<String, RpcHandler> handlers = new ConcurrentHashMap<>();
    private final Map<String, Integer> groups = new ConcurrentHashMap<>();
    private volatile double dropRate;
    private volatile int minDelayMs, maxDelayMs;
    private volatile BiFunction<String, Rpc, Rpc> interceptor;

    public Transport endpoint(String nodeId) {
        return new Endpoint(nodeId);
    }

    /** Split the cluster: nodes can only talk within their own group. Unlisted nodes form group 0. */
    @SafeVarargs
    public final void partition(Collection<String>... parts) {
        groups.clear();
        int g = 1;
        for (Collection<String> part : parts) {
            for (String id : part) groups.put(id, g);
            g++;
        }
    }

    /** Cut {@code nodeId} off from everyone else. */
    public void isolate(String nodeId) {
        groups.put(nodeId, 1000 + Math.abs(nodeId.hashCode() % 1000));
    }

    /** Remove all partitions. */
    public void heal() {
        groups.clear();
    }

    /** Probability in [0,1] that any single message (request or reply) is lost. */
    public void setDropRate(double rate) { this.dropRate = rate; }

    public void setLatency(int minMs, int maxMs) {
        this.minDelayMs = minMs;
        this.maxDelayMs = maxMs;
    }

    /**
     * Inspect or rewrite every request before delivery. The function receives (targetId, request) and
     * returns the request to deliver, or {@code null} to drop it. Pass {@code null} to remove it.
     * Lets tests construct exact interleavings that random fault injection would rarely hit.
     */
    public void setInterceptor(BiFunction<String, Rpc, Rpc> interceptor) {
        this.interceptor = interceptor;
    }

    private boolean reachable(String from, String to) {
        return groups.getOrDefault(from, 0).equals(groups.getOrDefault(to, 0));
    }

    private void maybeDrop(String why) throws IOException {
        if (dropRate > 0 && ThreadLocalRandom.current().nextDouble() < dropRate / 2) {
            throw new IOException("message dropped (" + why + ")");
        }
    }

    private void maybeDelay() {
        if (maxDelayMs <= 0) return;
        int d = minDelayMs + ThreadLocalRandom.current().nextInt(Math.max(1, maxDelayMs - minDelayMs + 1));
        try {
            Thread.sleep(d);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private final class Endpoint implements Transport {
        private final String self;
        private volatile boolean closed;

        Endpoint(String self) { this.self = self; }

        @Override
        public void start(RpcHandler handler) {
            handlers.put(self, handler);
        }

        @Override
        public Rpc call(String target, Rpc request) throws IOException {
            if (closed) throw new IOException(self + " is closed");
            if (!reachable(self, target)) throw new IOException("partitioned: " + self + " -> " + target);
            BiFunction<String, Rpc, Rpc> f = interceptor;
            if (f != null) {
                request = f.apply(target, request);
                if (request == null) throw new IOException("message dropped (interceptor)");
            }
            maybeDrop("request");
            maybeDelay();
            RpcHandler h = handlers.get(target);
            if (h == null) throw new IOException(target + " is down");
            Rpc reply;
            try {
                reply = h.handle(request);
            } catch (IllegalStateException e) {
                throw new IOException(target + " failed: " + e.getMessage(), e);
            }
            maybeDelay();
            if (closed || !reachable(self, target)) throw new IOException("reply lost: partition or close");
            maybeDrop("reply");
            return reply;
        }

        @Override
        public void close() {
            closed = true;
            handlers.remove(self);
        }
    }
}
