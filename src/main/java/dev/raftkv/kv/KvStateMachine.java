package dev.raftkv.kv;

import dev.raftkv.raft.StateMachine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The replicated key-value store plus a client-session table used for request de-duplication.
 * Deterministic: replicas applying the same command sequence end in identical states.
 *
 * <p>The session table is bounded (oldest sessions are evicted); eviction is deterministic because
 * it depends only on the order commands were applied, so all replicas evict the same clients.
 */
public final class KvStateMachine implements StateMachine {
    static final int MAX_SESSIONS = 10_000;
    private static final int SNAPSHOT_VERSION = 1;

    private record Session(long seq, byte[] result) {}

    private final HashMap<String, String> data = new HashMap<>();
    private final LinkedHashMap<String, Session> sessions = new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Session> eldest) {
            return size() > MAX_SESSIONS;
        }
    };

    @Override
    public synchronized byte[] apply(byte[] commandBytes) {
        KvCommand c = KvCommand.decode(commandBytes);
        boolean hasSession = c.clientId() != null && !c.clientId().isEmpty();
        if (hasSession) {
            Session s = sessions.get(c.clientId());
            if (s != null) {
                if (c.seq() == s.seq()) return s.result();               // duplicate of the latest request
                if (c.seq() < s.seq()) return KvResult.error("stale request (seq " + c.seq() + " < " + s.seq() + ")").encode();
            }
        }
        byte[] result = execute(c).encode();
        if (hasSession) {
            sessions.remove(c.clientId());                               // re-insert so it counts as most recent
            sessions.put(c.clientId(), new Session(c.seq(), result));
        }
        return result;
    }

    private KvResult execute(KvCommand c) {
        switch (c.type()) {
            case PUT:
                data.put(c.key(), c.value());
                return KvResult.success();
            case GET: {
                String v = data.get(c.key());
                return new KvResult(true, v != null, v, null);
            }
            case DELETE:
                return new KvResult(true, data.remove(c.key()) != null, null, null);
            case CAS: {
                String current = data.get(c.key());
                boolean matches = c.expected() == null ? current == null : c.expected().equals(current);
                if (matches) {
                    data.put(c.key(), c.value());
                    return new KvResult(true, current != null, c.value(), null);
                }
                return new KvResult(false, current != null, current, null);
            }
            default:
                return KvResult.error("unknown command");
        }
    }

    /** Local (possibly stale) read, bypassing consensus. */
    public synchronized Optional<String> readLocal(String key) {
        return Optional.ofNullable(data.get(key));
    }

    public synchronized Map<String, String> copyData() {
        return new HashMap<>(data);
    }

    public synchronized int size() {
        return data.size();
    }

    @Override
    public synchronized byte[] snapshot() {
        try (ByteArrayOutputStream bo = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bo)) {
            out.writeInt(SNAPSHOT_VERSION);
            out.writeInt(data.size());
            for (Map.Entry<String, String> e : data.entrySet()) {
                KvCommand.Bytes.writeString(out, e.getKey());
                KvCommand.Bytes.writeString(out, e.getValue());
            }
            out.writeInt(sessions.size());
            for (Map.Entry<String, Session> e : sessions.entrySet()) {   // insertion order is preserved
                KvCommand.Bytes.writeString(out, e.getKey());
                out.writeLong(e.getValue().seq());
                out.writeInt(e.getValue().result().length);
                out.write(e.getValue().result());
            }
            out.flush();
            return bo.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void restore(byte[] snapshot) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot))) {
            int version = in.readInt();
            if (version != SNAPSHOT_VERSION) throw new IOException("unsupported snapshot version " + version);
            data.clear();
            sessions.clear();
            int n = in.readInt();
            for (int i = 0; i < n; i++) data.put(KvCommand.Bytes.readString(in), KvCommand.Bytes.readString(in));
            int m = in.readInt();
            for (int i = 0; i < m; i++) {
                String client = KvCommand.Bytes.readString(in);
                long seq = in.readLong();
                byte[] result = new byte[in.readInt()];
                in.readFully(result);
                sessions.put(client, new Session(seq, result));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
