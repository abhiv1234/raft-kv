package dev.raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * A command for the key-value state machine.
 *
 * <p>{@code clientId} + {@code seq} identify a logical request. If the same pair is applied twice
 * (a client retried after an ambiguous timeout), the second application returns the first result
 * instead of executing again: this is what turns "at least once" delivery into exactly-once effects.
 */
public record KvCommand(Type type, String key, String value, String expected, String clientId, long seq) {

    public enum Type { PUT, GET, DELETE, CAS }

    public static KvCommand put(String key, String value, String clientId, long seq) {
        return new KvCommand(Type.PUT, key, value, null, clientId, seq);
    }

    public static KvCommand get(String key, String clientId, long seq) {
        return new KvCommand(Type.GET, key, null, null, clientId, seq);
    }

    public static KvCommand delete(String key, String clientId, long seq) {
        return new KvCommand(Type.DELETE, key, null, null, clientId, seq);
    }

    /** Set {@code key} to {@code value} only if its current value equals {@code expected}. */
    public static KvCommand cas(String key, String expected, String value, String clientId, long seq) {
        return new KvCommand(Type.CAS, key, value, expected, clientId, seq);
    }

    public byte[] encode() {
        try (ByteArrayOutputStream bo = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bo)) {
            out.writeByte(type.ordinal());
            Bytes.writeString(out, key);
            Bytes.writeString(out, value);
            Bytes.writeString(out, expected);
            Bytes.writeString(out, clientId);
            out.writeLong(seq);
            out.flush();
            return bo.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static KvCommand decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            Type type = Type.values()[in.readByte()];
            String key = Bytes.readString(in);
            String value = Bytes.readString(in);
            String expected = Bytes.readString(in);
            String clientId = Bytes.readString(in);
            return new KvCommand(type, key, value, expected, clientId, in.readLong());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Length-prefixed, null-aware string encoding (unlike writeUTF there is no 64KB limit). */
    static final class Bytes {
        private Bytes() {}

        static void writeString(DataOutputStream out, String s) throws IOException {
            if (s == null) { out.writeInt(-1); return; }
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            out.writeInt(b.length);
            out.write(b);
        }

        static String readString(DataInputStream in) throws IOException {
            int len = in.readInt();
            if (len < 0) return null;
            byte[] b = new byte[len];
            in.readFully(b);
            return new String(b, StandardCharsets.UTF_8);
        }
    }
}
