package dev.raftkv.kv;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Result of a {@link KvCommand}.
 * <ul>
 *   <li>PUT: {@code ok=true}</li>
 *   <li>GET: {@code ok=true}, {@code found}, {@code value}</li>
 *   <li>DELETE: {@code ok=true}, {@code found} = the key existed</li>
 *   <li>CAS: {@code ok} = the swap happened, {@code found} = key existed, {@code value} = value after the operation</li>
 * </ul>
 */
public record KvResult(boolean ok, boolean found, String value, String error) {

    public static KvResult success() { return new KvResult(true, false, null, null); }

    public static KvResult error(String message) { return new KvResult(false, false, null, message); }

    public byte[] encode() {
        try (ByteArrayOutputStream bo = new ByteArrayOutputStream(); DataOutputStream out = new DataOutputStream(bo)) {
            out.writeBoolean(ok);
            out.writeBoolean(found);
            KvCommand.Bytes.writeString(out, value);
            KvCommand.Bytes.writeString(out, error);
            out.flush();
            return bo.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static KvResult decode(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            boolean ok = in.readBoolean();
            boolean found = in.readBoolean();
            return new KvResult(ok, found, KvCommand.Bytes.readString(in), KvCommand.Bytes.readString(in));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
