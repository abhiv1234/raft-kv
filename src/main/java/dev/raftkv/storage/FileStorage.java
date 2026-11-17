package dev.raftkv.storage;

import dev.raftkv.raft.LogEntry;
import dev.raftkv.raft.Snapshot;
import dev.raftkv.raft.Storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Crash-safe on-disk storage.
 *
 * <pre>
 *   meta.bin      term + votedFor        rewritten atomically (write temp, fsync, rename)
 *   snapshot.bin  latest snapshot        rewritten atomically
 *   log.wal       write-ahead log        append-only records, each with a length and CRC32
 * </pre>
 *
 * WAL records are {@code ENTRY(index, term, type, data)}, {@code TRUNCATE(fromIndex)} and
 * {@code BASE(snapshotIndex, snapshotTerm)} (always the first record after a compaction). On
 * recovery the WAL is replayed; a torn or corrupt tail (a crash mid-write) is detected by the
 * CRC and discarded, which is safe because Raft never acknowledges an entry before its fsync.
 *
 * <p>Compaction order is: (1) atomically write the new snapshot, (2) atomically replace the WAL.
 * A crash between the two leaves an old WAL next to a new snapshot; {@link #load()} reconciles
 * them by dropping WAL entries the snapshot already covers.
 */
public final class FileStorage implements Storage {
    private static final int META_MAGIC = 0x52414654;      // "RAFT"
    private static final int SNAP_MAGIC = 0x534E4150;      // "SNAP"
    private static final byte REC_ENTRY = 1, REC_TRUNCATE = 2, REC_BASE = 3;

    private final Path dir;
    private final boolean fsync;
    private final Path metaFile, snapFile, walFile;
    private FileChannel wal;
    private long lastIndex;     // highest index currently in the WAL (or the snapshot boundary)
    private boolean loaded;

    /** @param fsync set false only in tests/benchmarks; without it a power loss can lose acknowledged writes */
    public FileStorage(Path dir, boolean fsync) throws IOException {
        this.dir = dir;
        this.fsync = fsync;
        Files.createDirectories(dir);
        this.metaFile = dir.resolve("meta.bin");
        this.snapFile = dir.resolve("snapshot.bin");
        this.walFile = dir.resolve("log.wal");
    }

    // ------------------------------------------------------------------ load / recovery

    @Override
    public synchronized State load() throws IOException {
        long term = 0;
        String votedFor = null;
        if (Files.exists(metaFile)) {
            ByteBuffer b = readChecked(metaFile);
            if (b.getInt() != META_MAGIC) throw new IOException("bad meta file magic");
            term = b.getLong();
            int len = b.getInt();
            if (len >= 0) {
                byte[] s = new byte[len];
                b.get(s);
                votedFor = new String(s, StandardCharsets.UTF_8);
            }
        }

        Snapshot snapshot = null;
        if (Files.exists(snapFile)) {
            ByteBuffer b = readChecked(snapFile);
            if (b.getInt() != SNAP_MAGIC) throw new IOException("bad snapshot file magic");
            long idx = b.getLong();
            long t = b.getLong();
            byte[] data = new byte[b.getInt()];
            b.get(data);
            snapshot = new Snapshot(idx, t, data);
        }

        // Replay the WAL.
        long base = 0, baseTerm = 0;
        ArrayList<LogEntry> entries = new ArrayList<>();
        long validLength = 0;
        if (Files.exists(walFile)) {
            byte[] bytes = Files.readAllBytes(walFile);
            ByteBuffer b = ByteBuffer.wrap(bytes);
            while (b.remaining() >= 8) {
                int len = b.getInt();
                int crc = b.getInt();
                if (len < 1 || len > b.remaining()) break;                       // torn tail
                byte[] payload = new byte[len];
                b.get(payload);
                if (crc32(payload) != crc) break;                                // corrupt tail
                ByteBuffer p = ByteBuffer.wrap(payload);
                byte type = p.get();
                if (type == REC_BASE) {
                    base = p.getLong();
                    baseTerm = p.getLong();
                    entries.clear();
                } else if (type == REC_TRUNCATE) {
                    long from = p.getLong();
                    int keep = (int) Math.max(0, Math.min(entries.size(), from - base - 1));
                    entries.subList(keep, entries.size()).clear();
                } else if (type == REC_ENTRY) {
                    long index = p.getLong();
                    if (index != base + entries.size() + 1) break;               // gap: stop at last good record
                    long t = p.getLong();
                    LogEntry.Type et = LogEntry.Type.values()[p.get()];
                    byte[] data = new byte[p.getInt()];
                    p.get(data);
                    entries.add(new LogEntry(t, et, data));
                } else {
                    break;
                }
                validLength = b.position();
            }
            if (validLength < bytes.length) {                                    // drop the torn tail for good
                try (FileChannel ch = FileChannel.open(walFile, StandardOpenOption.WRITE)) {
                    ch.truncate(validLength);
                    if (fsync) ch.force(true);
                }
            }
        }

        // Reconcile WAL with snapshot (see class comment about compaction order).
        long snapIndex = snapshot == null ? 0 : snapshot.lastIncludedIndex();
        boolean needRewrite = false;
        if (base > snapIndex) throw new IOException("log base " + base + " is ahead of snapshot " + snapIndex + ": data loss");
        if (base < snapIndex) {
            int drop = (int) Math.min(entries.size(), snapIndex - base);
            entries.subList(0, drop).clear();
            base = snapIndex;
            baseTerm = snapshot.lastIncludedTerm();
            needRewrite = true;
        }

        if (needRewrite || !Files.exists(walFile)) {
            rewriteWal(base, baseTerm, entries);
        } else {
            openWal();
        }
        lastIndex = base + entries.size();
        loaded = true;
        return new State(term, votedFor, snapshot, entries);
    }

    // ------------------------------------------------------------------ mutations

    @Override
    public synchronized void saveTermAndVote(long term, String votedFor) throws IOException {
        byte[] v = votedFor == null ? null : votedFor.getBytes(StandardCharsets.UTF_8);
        ByteBuffer b = ByteBuffer.allocate(4 + 8 + 4 + (v == null ? 0 : v.length));
        b.putInt(META_MAGIC).putLong(term).putInt(v == null ? -1 : v.length);
        if (v != null) b.put(v);
        writeAtomically(metaFile, withCrc(b.array()));
    }

    @Override
    public synchronized void truncateFrom(long index) throws IOException {
        requireLoaded();
        ByteBuffer p = ByteBuffer.allocate(9).put(REC_TRUNCATE).putLong(index);
        writeWal(List.of(p.array()));
        lastIndex = Math.min(lastIndex, index - 1);
    }

    @Override
    public synchronized void append(long startIndex, List<LogEntry> entries) throws IOException {
        requireLoaded();
        if (startIndex != lastIndex + 1) {
            throw new IllegalStateException("non-contiguous append at " + startIndex + ", expected " + (lastIndex + 1));
        }
        List<byte[]> records = new ArrayList<>(entries.size());
        long index = startIndex;
        for (LogEntry e : entries) {
            ByteBuffer p = ByteBuffer.allocate(1 + 8 + 8 + 1 + 4 + e.data().length);
            p.put(REC_ENTRY).putLong(index++).putLong(e.term()).put((byte) e.type().ordinal())
                    .putInt(e.data().length).put(e.data());
            records.add(p.array());
        }
        writeWal(records);
        lastIndex = index - 1;
    }

    @Override
    public synchronized void saveSnapshot(Snapshot s, List<LogEntry> remaining) throws IOException {
        requireLoaded();
        ByteBuffer b = ByteBuffer.allocate(4 + 8 + 8 + 4 + s.data().length);
        b.putInt(SNAP_MAGIC).putLong(s.lastIncludedIndex()).putLong(s.lastIncludedTerm())
                .putInt(s.data().length).put(s.data());
        writeAtomically(snapFile, withCrc(b.array()));            // step 1: snapshot is now durable
        rewriteWal(s.lastIncludedIndex(), s.lastIncludedTerm(), remaining); // step 2: shrink the log
        lastIndex = s.lastIncludedIndex() + remaining.size();
    }

    @Override
    public synchronized void close() {
        try {
            if (wal != null) wal.close();
        } catch (IOException ignored) {
            // nothing useful to do on close
        }
        wal = null;
    }

    // ------------------------------------------------------------------ helpers

    private void requireLoaded() {
        if (!loaded) throw new IllegalStateException("load() must be called first");
    }

    private void openWal() throws IOException {
        wal = FileChannel.open(walFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    /** Append already-framed payloads as one batch and fsync once. */
    private void writeWal(List<byte[]> payloads) throws IOException {
        int total = 0;
        for (byte[] p : payloads) total += 8 + p.length;
        ByteBuffer buf = ByteBuffer.allocate(total);
        for (byte[] p : payloads) buf.putInt(p.length).putInt(crc32(p)).put(p);
        buf.flip();
        while (buf.hasRemaining()) wal.write(buf);
        if (fsync) wal.force(false);
    }

    private void rewriteWal(long base, long baseTerm, List<LogEntry> entries) throws IOException {
        if (wal != null) { wal.close(); wal = null; }
        Path tmp = dir.resolve("log.wal.tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            List<byte[]> payloads = new ArrayList<>();
            payloads.add(ByteBuffer.allocate(17).put(REC_BASE).putLong(base).putLong(baseTerm).array());
            long index = base + 1;
            for (LogEntry e : entries) {
                payloads.add(ByteBuffer.allocate(1 + 8 + 8 + 1 + 4 + e.data().length)
                        .put(REC_ENTRY).putLong(index++).putLong(e.term()).put((byte) e.type().ordinal())
                        .putInt(e.data().length).put(e.data()).array());
            }
            int total = 0;
            for (byte[] p : payloads) total += 8 + p.length;
            ByteBuffer buf = ByteBuffer.allocate(total);
            for (byte[] p : payloads) buf.putInt(p.length).putInt(crc32(p)).put(p);
            buf.flip();
            while (buf.hasRemaining()) ch.write(buf);
            if (fsync) ch.force(true);
        }
        move(tmp, walFile);
        syncDir();
        openWal();
    }

    private void writeAtomically(Path target, byte[] content) throws IOException {
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(content);
            while (buf.hasRemaining()) ch.write(buf);
            if (fsync) ch.force(true);
        }
        move(tmp, target);
        syncDir();
    }

    private static void move(Path from, Path to) throws IOException {
        try {
            Files.move(from, to, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    /** Make a rename durable by fsyncing the directory (not supported on every platform; best effort). */
    private void syncDir() {
        if (!fsync) return;
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // e.g. Windows
        }
    }

    private static byte[] withCrc(byte[] body) {
        return ByteBuffer.allocate(body.length + 4).put(body).putInt(crc32(body)).array();
    }

    /** Read a file written with {@link #withCrc}, verify the checksum, and return the body. */
    private static ByteBuffer readChecked(Path file) throws IOException {
        byte[] all = Files.readAllBytes(file);
        if (all.length < 4) throw new IOException(file + " is truncated");
        byte[] body = new byte[all.length - 4];
        System.arraycopy(all, 0, body, 0, body.length);
        int expected = ByteBuffer.wrap(all, body.length, 4).getInt();
        if (crc32(body) != expected) throw new IOException(file + " failed its checksum");
        return ByteBuffer.wrap(body);
    }

    private static int crc32(byte[] data) {
        CRC32 c = new CRC32();
        c.update(data);
        return (int) c.getValue();
    }
}
