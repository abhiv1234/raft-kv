package dev.raftkv.server;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.raftkv.kv.KvCommand;
import dev.raftkv.kv.KvResult;
import dev.raftkv.kv.KvService;
import dev.raftkv.raft.NotLeaderException;
import dev.raftkv.raft.RaftNode;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * HTTP front door.
 * <pre>
 *   PUT    /kv/{key}               body = value          store a value
 *   PUT    /kv/{key}?ifValue=old   body = new value      compare-and-swap (409 if it does not match)
 *   PUT    /kv/{key}?ifAbsent=true body = value          create only if the key does not exist (409 otherwise)
 *   GET    /kv/{key}                                     linearizable read (goes through the log)
 *   GET    /kv/{key}?stale=true                          local read, no consensus, may be stale
 *   DELETE /kv/{key}
 *   GET    /status                                       role, term, leader, indexes
 * </pre>
 * Writes and linearizable reads sent to a follower are answered with a 307 redirect to the leader.
 * Optional headers {@code X-Client-Id} and {@code X-Request-Seq} enable exactly-once retries.
 */
public final class HttpApi {
    private static final int MAX_VALUE_BYTES = 1 << 20;
    private static final long REQUEST_TIMEOUT_MS = 5_000;

    private final KvService service;
    private final RaftNode node;
    private final Map<String, NodeConfig.Member> members;
    private final HttpServer server;
    private final String anonymousClient = "http-" + UUID.randomUUID();
    private final AtomicLong anonymousSeq = new AtomicLong();

    public HttpApi(KvService service, Map<String, NodeConfig.Member> members, int port) throws IOException {
        this.service = service;
        this.node = service.node();
        this.members = members;
        this.server = HttpServer.create(new InetSocketAddress(port), 128);
        server.setExecutor(Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "http-worker");
            t.setDaemon(true);
            return t;
        }));
        server.createContext("/kv/", this::handleKv);
        server.createContext("/status", this::handleStatus);
        server.createContext("/health", ex -> respond(ex, 200, "ok\n", "text/plain"));
    }

    public void start() { server.start(); }

    public void stop() { server.stop(0); }

    private void handleStatus(HttpExchange ex) throws IOException {
        RaftNode.Status s = node.status();
        respond(ex, 200, "{\"id\":" + json(s.id()) + ",\"role\":\"" + s.role() + "\",\"term\":" + s.term()
                + ",\"leader\":" + json(s.leaderId()) + ",\"commitIndex\":" + s.commitIndex()
                + ",\"lastApplied\":" + s.lastApplied() + ",\"lastLogIndex\":" + s.lastLogIndex()
                + ",\"snapshotIndex\":" + s.snapshotIndex() + "}\n", "application/json");
    }

    private void handleKv(HttpExchange ex) throws IOException {
        try {
            String rawPath = ex.getRequestURI().getRawPath();
            String key = URLDecoder.decode(rawPath.substring("/kv/".length()), StandardCharsets.UTF_8);
            if (key.isEmpty()) { respond(ex, 400, "{\"error\":\"empty key\"}\n", "application/json"); return; }
            Map<String, String> q = parseQuery(ex.getRequestURI().getRawQuery());
            String method = ex.getRequestMethod();

            if (method.equals("GET") && "true".equals(q.get("stale"))) {
                var v = service.readStale(key);
                if (v.isPresent()) respond(ex, 200, v.get(), "text/plain; charset=utf-8");
                else respond(ex, 404, "{\"error\":\"not found\"}\n", "application/json");
                return;
            }

            String clientId = ex.getRequestHeaders().getFirst("X-Client-Id");
            String seqHeader = ex.getRequestHeaders().getFirst("X-Request-Seq");
            if (clientId == null || seqHeader == null) { // anonymous: unique per request, so no de-duplication on retry
                clientId = anonymousClient;
                seqHeader = Long.toString(anonymousSeq.incrementAndGet());
            }
            long seq = Long.parseLong(seqHeader);

            KvCommand cmd;
            switch (method) {
                case "GET" -> cmd = KvCommand.get(key, clientId, seq);
                case "DELETE" -> cmd = KvCommand.delete(key, clientId, seq);
                case "PUT" -> {
                    byte[] body = readBody(ex.getRequestBody());
                    if (body == null) { respond(ex, 413, "{\"error\":\"value too large\"}\n", "application/json"); return; }
                    String value = new String(body, StandardCharsets.UTF_8);
                    if (q.containsKey("ifValue")) cmd = KvCommand.cas(key, q.get("ifValue"), value, clientId, seq);
                    else if ("true".equals(q.get("ifAbsent"))) cmd = KvCommand.cas(key, null, value, clientId, seq);
                    else cmd = KvCommand.put(key, value, clientId, seq);
                }
                default -> { respond(ex, 405, "{\"error\":\"method not allowed\"}\n", "application/json"); return; }
            }

            if (!node.isLeader()) { redirectToLeader(ex, rawPath, ex.getRequestURI().getRawQuery()); return; }

            KvResult r;
            try {
                r = service.execute(cmd).get(REQUEST_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof NotLeaderException) { redirectToLeader(ex, rawPath, ex.getRequestURI().getRawQuery()); return; }
                respond(ex, 503, "{\"error\":" + json(String.valueOf(e.getCause().getMessage())) + "}\n", "application/json");
                return;
            } catch (TimeoutException e) {
                respond(ex, 504, "{\"error\":\"timed out waiting for commit; outcome unknown, retry with the same X-Client-Id and X-Request-Seq\"}\n", "application/json");
                return;
            }
            writeResult(ex, cmd, r);
        } catch (NumberFormatException e) {
            respond(ex, 400, "{\"error\":\"X-Request-Seq must be an integer\"}\n", "application/json");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            respond(ex, 503, "{\"error\":\"interrupted\"}\n", "application/json");
        } catch (RuntimeException e) {
            respond(ex, 500, "{\"error\":" + json(e.toString()) + "}\n", "application/json");
        }
    }

    private void writeResult(HttpExchange ex, KvCommand cmd, KvResult r) throws IOException {
        if (!r.ok() && r.error() != null) { respond(ex, 409, "{\"error\":" + json(r.error()) + "}\n", "application/json"); return; }
        switch (cmd.type()) {
            case PUT -> respond(ex, 200, "{\"ok\":true}\n", "application/json");
            case GET -> {
                if (r.found()) respond(ex, 200, r.value(), "text/plain; charset=utf-8");
                else respond(ex, 404, "{\"error\":\"not found\"}\n", "application/json");
            }
            case DELETE -> respond(ex, 200, "{\"ok\":true,\"existed\":" + r.found() + "}\n", "application/json");
            case CAS -> {
                if (r.ok()) respond(ex, 200, "{\"ok\":true,\"swapped\":true}\n", "application/json");
                else respond(ex, 409, "{\"ok\":false,\"swapped\":false,\"current\":" + json(r.value()) + "}\n", "application/json");
            }
        }
    }

    private void redirectToLeader(HttpExchange ex, String rawPath, String rawQuery) throws IOException {
        String leader = node.status().leaderId();
        NodeConfig.Member m = leader == null ? null : members.get(leader);
        if (m == null) {
            ex.getResponseHeaders().add("Retry-After", "1");
            respond(ex, 503, "{\"error\":\"no leader elected yet\"}\n", "application/json");
            return;
        }
        ex.getResponseHeaders().add("X-Raft-Leader", leader);
        ex.getResponseHeaders().add("Location", m.httpBase() + rawPath + (rawQuery == null ? "" : "?" + rawQuery));
        respond(ex, 307, "", "text/plain");
    }

    private static byte[] readBody(InputStream in) throws IOException {
        byte[] data = in.readNBytes(MAX_VALUE_BYTES + 1);
        return data.length > MAX_VALUE_BYTES ? null : data;
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> m = new HashMap<>();
        if (raw == null || raw.isEmpty()) return m;
        for (String pair : raw.split("&")) {
            String[] kv = pair.split("=", 2);
            m.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8),
                    kv.length == 2 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "");
        }
        return m;
    }

    private static void respond(HttpExchange ex, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", contentType);
        ex.sendResponseHeaders(status, bytes.length == 0 ? -1 : bytes.length);
        if (bytes.length > 0) ex.getResponseBody().write(bytes);
        ex.close();
    }

    static String json(String s) {
        if (s == null) return "null";
        StringBuilder b = new StringBuilder("\"");
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> b.append("\\\"");
                case '\\' -> b.append("\\\\");
                case '\n' -> b.append("\\n");
                case '\r' -> b.append("\\r");
                case '\t' -> b.append("\\t");
                default -> {
                    if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                    else b.append(c);
                }
            }
        }
        return b.append('"').toString();
    }
}
