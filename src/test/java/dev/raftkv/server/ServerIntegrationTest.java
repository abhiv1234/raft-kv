package dev.raftkv.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Boots three real nodes (TCP between them, HTTP to clients, files on disk) in one JVM. */
@Timeout(120)
class ServerIntegrationTest {

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(2)).build();

    private HttpResponse<String> send(String method, String url, String body, String... headers) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10));
        b.method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        for (int i = 0; i < headers.length; i += 2) b.header(headers[i], headers[i + 1]);
        return http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    /** Send to {@code base}, following up to 3 leader redirects (re-sending the body, as curl -L --post307 does). */
    private HttpResponse<String> sendFollowingLeader(String method, String base, String path, String body, String... headers) throws Exception {
        String target = base;
        for (int i = 0; i < 20; i++) {
            HttpResponse<String> r = send(method, target + path, body, headers);
            if (r.statusCode() == 307) {
                String loc = r.headers().firstValue("Location").orElseThrow();
                target = loc.substring(0, loc.length() - path.length());
            } else if (r.statusCode() == 503) {
                Thread.sleep(200); // no leader yet
            } else {
                return r;
            }
        }
        throw new AssertionError("no leader reachable");
    }

    @Test
    void threeNodeClusterServesReadsAndWritesAndSurvivesALeaderFailure() throws Exception {
        Path root = Files.createTempDirectory("raftkv-it");
        Map<String, Integer> raftPorts = new LinkedHashMap<>(), httpPorts = new LinkedHashMap<>();
        for (String id : List.of("n1", "n2", "n3")) { raftPorts.put(id, freePort()); httpPorts.put(id, freePort()); }
        StringBuilder peers = new StringBuilder();
        raftPorts.forEach((id, p) -> peers.append(peers.length() == 0 ? "" : ",")
                .append(id).append("=127.0.0.1:").append(p).append(':').append(httpPorts.get(id)));

        Map<String, KvServer> servers = new LinkedHashMap<>();
        try {
            for (String id : raftPorts.keySet()) {
                NodeConfig cfg = NodeConfig.parse(new String[]{"--id", id, "--peers", peers.toString(),
                        "--data-dir", root.resolve(id).toString(), "--no-fsync"});
                KvServer s = new KvServer(cfg);
                s.start();
                servers.put(id, s);
            }
            String n1 = "http://127.0.0.1:" + httpPorts.get("n1");

            // Write through ANY node (followers redirect to the leader), read back through any node.
            assertEquals(200, sendFollowingLeader("PUT", n1, "/kv/greeting", "hello").statusCode());
            for (int port : httpPorts.values()) {
                HttpResponse<String> r = sendFollowingLeader("GET", "http://127.0.0.1:" + port, "/kv/greeting", null);
                assertEquals(200, r.statusCode());
                assertEquals("hello", r.body());
            }
            assertEquals(404, sendFollowingLeader("GET", n1, "/kv/missing", null).statusCode());

            // CAS over HTTP.
            assertEquals(200, sendFollowingLeader("PUT", n1, "/kv/created?ifAbsent=true", "x").statusCode());
            assertEquals(409, sendFollowingLeader("PUT", n1, "/kv/created?ifAbsent=true", "y").statusCode());
            assertEquals(409, sendFollowingLeader("PUT", n1, "/kv/greeting?ifValue=wrong", "nope").statusCode());
            assertEquals(200, sendFollowingLeader("PUT", n1, "/kv/greeting?ifValue=hello", "hi").statusCode());
            assertEquals("hi", sendFollowingLeader("GET", n1, "/kv/greeting", null).body());

            // Idempotent retry: same client id + seq must not execute twice.
            String[] id = {"X-Client-Id", "tester", "X-Request-Seq", "7"};
            assertEquals(200, sendFollowingLeader("PUT", n1, "/kv/once", "first", id).statusCode());
            assertEquals(200, sendFollowingLeader("PUT", n1, "/kv/once", "second", id).statusCode()); // the retry
            assertEquals("first", sendFollowingLeader("GET", n1, "/kv/once", null).body(), "a retried request must not execute twice");
            assertEquals(200, sendFollowingLeader("DELETE", n1, "/kv/greeting", null).statusCode());

            // Status endpoint reports a single leader.
            int leaders = 0;
            String leaderId = null;
            for (var e : httpPorts.entrySet()) {
                String body = send("GET", "http://127.0.0.1:" + e.getValue() + "/status", null).body();
                if (body.contains("\"role\":\"LEADER\"")) { leaders++; leaderId = e.getKey(); }
            }
            assertEquals(1, leaders);

            // Kill the leader; the survivors must elect a new one and keep all data.
            servers.remove(leaderId).close();
            String survivor = servers.keySet().iterator().next();
            String base = "http://127.0.0.1:" + httpPorts.get(survivor);
            long deadline = System.currentTimeMillis() + 20_000;
            HttpResponse<String> r = null;
            while (System.currentTimeMillis() < deadline) {
                try {
                    r = sendFollowingLeader("PUT", base, "/kv/after-failover", "still-works");
                    if (r.statusCode() == 200) break;
                } catch (Exception e) {
                    Thread.sleep(200);
                }
            }
            assertTrue(r != null && r.statusCode() == 200, "cluster did not recover after leader failure");
            assertEquals("still-works", sendFollowingLeader("GET", base, "/kv/after-failover", null).body());
            assertEquals("first", sendFollowingLeader("GET", base, "/kv/once", null).body(), "data must survive the failover");
        } finally {
            servers.values().forEach(KvServer::close);
            try (var walk = Files.walk(root)) {
                walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }
}
