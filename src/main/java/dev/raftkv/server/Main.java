package dev.raftkv.server;

import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

public final class Main {
    private static final String USAGE = """
            Usage: java -jar raft-kv.jar --id <nodeId> --peers <id=host:raftPort:httpPort[:publicHost],...> --data-dir <dir>
                   [--no-fsync] [--snapshot-threshold <entries>]

            Example (3 nodes on one machine, run each in its own terminal):
              java -jar raft-kv.jar --id n1 --data-dir data/n1 --peers n1=127.0.0.1:7001:8001,n2=127.0.0.1:7002:8002,n3=127.0.0.1:7003:8003
            """;

    public static void main(String[] args) throws Exception {
        NodeConfig cfg;
        try {
            cfg = NodeConfig.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println(USAGE);
            System.exit(2);
            return;
        }
        KvServer server = new KvServer(cfg);
        server.start();
        Logger.getLogger("raftkv").info("node " + cfg.id() + " up: raft=" + cfg.self().raftPort()
                + " http=" + cfg.self().httpPort() + " data=" + cfg.dataDir());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        new CountDownLatch(1).await();
    }
}
