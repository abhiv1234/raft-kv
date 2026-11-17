package dev.raftkv.net;

import dev.raftkv.raft.Rpc;
import dev.raftkv.raft.RpcHandler;
import dev.raftkv.raft.Transport;

import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Node-to-node transport over plain TCP using Java object serialization, with a strict
 * deserialization allow-list so a malicious peer cannot instantiate arbitrary classes.
 *
 * <p>One short-lived connection per RPC keeps the code simple and robust; a production system
 * would pool connections and use a compact schema (protobuf). This is documented in DESIGN.md.
 * <b>Do not expose the Raft port to untrusted networks:</b> there is no authentication or TLS.
 */
public final class TcpTransport implements Transport {
    private static final Logger LOG = Logger.getLogger("raftkv.net");
    private static final ObjectInputFilter FILTER = ObjectInputFilter.Config.createFilter(
            "maxbytes=134217728;maxdepth=16;maxarray=134217728;maxrefs=1000000;"
                    + "dev.raftkv.raft.*;java.util.*;java.lang.*;!*");

    private final String selfId;
    private final Map<String, InetSocketAddress> addresses;
    private final int connectTimeoutMs;
    private final int readTimeoutMs;

    private volatile boolean closed;
    private ServerSocket server;
    private ExecutorService workers;

    public TcpTransport(String selfId, Map<String, InetSocketAddress> addresses) {
        this(selfId, addresses, 300, 2000);
    }

    public TcpTransport(String selfId, Map<String, InetSocketAddress> addresses, int connectTimeoutMs, int readTimeoutMs) {
        this.selfId = selfId;
        this.addresses = Map.copyOf(addresses);
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
    }

    @Override
    public void start(RpcHandler handler) throws IOException {
        InetSocketAddress me = addresses.get(selfId);
        if (me == null) throw new IOException("no address configured for " + selfId);
        server = new ServerSocket();
        server.setReuseAddress(true);
        server.bind(new InetSocketAddress(me.getPort()));
        workers = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "raft-" + selfId + "-tcp-worker");
            t.setDaemon(true);
            return t;
        });
        Thread acceptor = new Thread(() -> acceptLoop(handler), "raft-" + selfId + "-tcp-acceptor");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    private void acceptLoop(RpcHandler handler) {
        while (!closed) {
            try {
                Socket s = server.accept();
                workers.execute(() -> serve(s, handler));
            } catch (IOException e) {
                if (!closed) LOG.log(Level.WARNING, selfId + ": accept failed", e);
            }
        }
    }

    private void serve(Socket s, RpcHandler handler) {
        try (s) {
            s.setSoTimeout(readTimeoutMs * 5);
            s.setTcpNoDelay(true);
            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            in.setObjectInputFilter(FILTER);
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            Object req = in.readObject();
            if (!(req instanceof Rpc rpc)) throw new IOException("unexpected payload " + req.getClass());
            out.writeObject(handler.handle(rpc));
            out.flush();
        } catch (Exception e) {
            if (!closed) LOG.log(Level.FINE, selfId + ": rpc serve failed: " + e);
        }
    }

    @Override
    public Rpc call(String target, Rpc request) throws IOException {
        if (closed) throw new IOException("transport closed");
        InetSocketAddress addr = addresses.get(target);
        if (addr == null) throw new IOException("unknown peer " + target);
        try (Socket s = new Socket()) {
            s.setTcpNoDelay(true);
            s.connect(addr, connectTimeoutMs);
            s.setSoTimeout(readTimeoutMs);
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            out.writeObject(request);
            out.flush();
            ObjectInputStream in = new ObjectInputStream(s.getInputStream());
            in.setObjectInputFilter(FILTER);
            Object reply = in.readObject();
            if (!(reply instanceof Rpc rpc)) throw new IOException("unexpected reply " + reply.getClass());
            return rpc;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void close() {
        closed = true;
        try {
            if (server != null) server.close();
        } catch (IOException ignored) {
            // closing
        }
        if (workers != null) workers.shutdownNow();
    }
}
