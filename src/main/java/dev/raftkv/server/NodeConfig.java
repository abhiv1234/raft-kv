package dev.raftkv.server;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Static cluster configuration, parsed from command-line flags:
 * <pre>
 *   --id node1
 *   --peers node1=host:raftPort:httpPort[:publicHost],node2=...   (include this node)
 *   --data-dir ./data/node1
 *   [--no-fsync] [--snapshot-threshold 10000]
 * </pre>
 */
public record NodeConfig(String id, Map<String, Member> members, Path dataDir, boolean fsync, int snapshotThreshold) {

    /**
     * @param host       address other nodes use to reach this member's Raft port
     * @param publicHost address CLIENTS use to reach this member's HTTP port (used in redirects to the leader)
     */
    public record Member(String host, int raftPort, int httpPort, String publicHost) {
        public InetSocketAddress raftAddress() { return new InetSocketAddress(host, raftPort); }
        public String httpBase() { return "http://" + publicHost + ":" + httpPort; }
    }

    public Member self() { return members.get(id); }

    public static NodeConfig parse(String[] args) {
        String id = null, peers = null, dir = null;
        boolean fsync = true;
        int snapshotThreshold = 10_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--id" -> id = args[++i];
                case "--peers" -> peers = args[++i];
                case "--data-dir" -> dir = args[++i];
                case "--no-fsync" -> fsync = false;
                case "--snapshot-threshold" -> snapshotThreshold = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown argument: " + args[i]);
            }
        }
        if (id == null || peers == null || dir == null) {
            throw new IllegalArgumentException("--id, --peers and --data-dir are required");
        }
        Map<String, Member> members = new LinkedHashMap<>();
        for (String spec : peers.split(",")) {
            String[] kv = spec.trim().split("=", 2);
            String[] hp = kv.length == 2 ? kv[1].split(":") : new String[0];
            if (hp.length != 3 && hp.length != 4) {
                throw new IllegalArgumentException("bad peer '" + spec + "', expected id=host:raftPort:httpPort[:publicHost]");
            }
            members.put(kv[0], new Member(hp[0], Integer.parseInt(hp[1]), Integer.parseInt(hp[2]),
                    hp.length == 4 ? hp[3] : hp[0]));
        }
        if (!members.containsKey(id)) throw new IllegalArgumentException("--peers must include this node (" + id + ")");
        return new NodeConfig(id, members, Path.of(dir), fsync, snapshotThreshold);
    }
}
