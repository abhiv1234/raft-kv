package dev.raftkv.server;

import dev.raftkv.kv.KvService;
import dev.raftkv.kv.KvStateMachine;
import dev.raftkv.net.TcpTransport;
import dev.raftkv.raft.RaftConfig;
import dev.raftkv.raft.RaftNode;
import dev.raftkv.storage.FileStorage;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

/** Wires together one complete node: storage, state machine, Raft, TCP transport and HTTP API. */
public final class KvServer implements AutoCloseable {
    private final RaftNode node;
    private final HttpApi http;
    private final FileStorage storage;

    public KvServer(NodeConfig cfg) throws IOException {
        Map<String, InetSocketAddress> raftAddrs = new LinkedHashMap<>();
        cfg.members().forEach((id, m) -> raftAddrs.put(id, m.raftAddress()));
        this.storage = new FileStorage(cfg.dataDir(), cfg.fsync());
        KvStateMachine sm = new KvStateMachine();
        RaftConfig raftCfg = RaftConfig.defaults().withSnapshotThreshold(cfg.snapshotThreshold());
        this.node = new RaftNode(cfg.id(), new ArrayList<>(cfg.members().keySet()), raftCfg, storage, sm,
                new TcpTransport(cfg.id(), raftAddrs));
        this.http = new HttpApi(new KvService(node, sm), cfg.members(), cfg.self().httpPort());
    }

    public void start() throws IOException {
        node.start();
        http.start();
    }

    public RaftNode node() { return node; }

    @Override
    public void close() {
        http.stop();
        node.close();
        storage.close();
    }
}
