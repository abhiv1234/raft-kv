package dev.raftkv.storage;

import dev.raftkv.raft.LogEntry;
import dev.raftkv.raft.Snapshot;
import dev.raftkv.raft.Storage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class FileStorageTest {
    private Path dir;

    @BeforeEach
    void setUp() throws IOException {
        dir = Files.createTempDirectory("raftkv-storage");
    }

    @AfterEach
    void tearDown() throws IOException {
        try (var walk = Files.walk(dir)) {
            walk.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }

    private static LogEntry e(long term, String s) {
        return LogEntry.command(term, s.getBytes(StandardCharsets.UTF_8));
    }

    private static List<String> payloads(List<LogEntry> es) {
        List<String> out = new ArrayList<>();
        for (LogEntry x : es) out.add(x.term() + ":" + new String(x.data(), StandardCharsets.UTF_8));
        return out;
    }

    private Storage open() throws IOException {
        return new FileStorage(dir, true);
    }

    @Test
    void freshStorageIsEmpty() throws Exception {
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertEquals(0, st.currentTerm());
            assertNull(st.votedFor());
            assertNull(st.snapshot());
            assertEquals(0, st.entries().size());
        }
    }

    @Test
    void termVoteAndEntriesSurviveReopen() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.saveTermAndVote(7, "n2");
            s.append(1, List.of(e(1, "a"), e(1, "b"), e(2, "c")));
        }
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertEquals(7, st.currentTerm());
            assertEquals("n2", st.votedFor());
            assertEquals(List.of("1:a", "1:b", "2:c"), payloads(st.entries()));
        }
    }

    @Test
    void nullVoteIsPreserved() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.saveTermAndVote(3, "n1");
            s.saveTermAndVote(4, null);
        }
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertEquals(4, st.currentTerm());
            assertNull(st.votedFor());
        }
    }

    @Test
    void truncationThenAppendReplaysCorrectly() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b"), e(1, "c"), e(1, "d")));
            s.truncateFrom(3);
            s.append(3, List.of(e(2, "X")));
        }
        try (Storage s = open()) {
            assertEquals(List.of("1:a", "1:b", "2:X"), payloads(s.load().entries()));
        }
    }

    @Test
    void tornWriteAtTheTailIsIgnoredAndRepaired() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b")));
        }
        // Simulate a crash in the middle of writing the next record: a partial frame at the end.
        Files.write(dir.resolve("log.wal"), new byte[]{0, 0, 0, 40, 1, 2, 3}, StandardOpenOption.APPEND);
        try (Storage s = open()) {
            assertEquals(List.of("1:a", "1:b"), payloads(s.load().entries()));
            s.append(3, List.of(e(1, "c")));        // must be able to keep going afterwards
        }
        try (Storage s = open()) {
            assertEquals(List.of("1:a", "1:b", "1:c"), payloads(s.load().entries()));
        }
    }

    @Test
    void corruptRecordIsDetectedByChecksum() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b"), e(1, "c")));
        }
        Path wal = dir.resolve("log.wal");
        byte[] bytes = Files.readAllBytes(wal);
        bytes[bytes.length - 3] ^= 0x7F;           // flip bits inside the last record
        Files.write(wal, bytes);
        try (Storage s = open()) {
            assertEquals(List.of("1:a", "1:b"), payloads(s.load().entries()), "the corrupt last record must be dropped");
        }
    }

    @Test
    void snapshotCompactsTheLogAndSurvivesReopen() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b"), e(2, "c"), e(2, "d")));
            s.saveSnapshot(new Snapshot(2, 1, "state@2".getBytes(StandardCharsets.UTF_8)), List.of(e(2, "c"), e(2, "d")));
            s.append(5, List.of(e(3, "e")));
        }
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertNotNull(st.snapshot());
            assertEquals(2, st.snapshot().lastIncludedIndex());
            assertEquals(1, st.snapshot().lastIncludedTerm());
            assertArrayEquals("state@2".getBytes(StandardCharsets.UTF_8), st.snapshot().data());
            assertEquals(List.of("2:c", "2:d", "3:e"), payloads(st.entries()));
        }
    }

    /** A crash after the snapshot is written but before the log is rewritten must not lose or duplicate data. */
    @Test
    void crashBetweenSnapshotAndLogCompactionIsRecovered() throws Exception {
        Path wal = dir.resolve("log.wal");
        byte[] oldWal;
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b"), e(1, "c"), e(1, "d"), e(1, "e")));
            oldWal = Files.readAllBytes(wal);
            s.saveSnapshot(new Snapshot(3, 1, new byte[]{9}), List.of(e(1, "d"), e(1, "e")));
        }
        Files.write(wal, oldWal);                  // roll the WAL back: new snapshot + old, uncompacted log
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertEquals(3, st.snapshot().lastIncludedIndex());
            assertEquals(List.of("1:d", "1:e"), payloads(st.entries()), "entries covered by the snapshot must be dropped");
            s.append(6, List.of(e(2, "f")));
        }
        try (Storage s = open()) {
            assertEquals(List.of("1:d", "1:e", "2:f"), payloads(s.load().entries()));
        }
    }

    @Test
    void snapshotBeyondTheEndOfTheLogDiscardsTheLog() throws Exception {
        try (Storage s = open()) {
            s.load();
            s.append(1, List.of(e(1, "a"), e(1, "b")));
            s.saveSnapshot(new Snapshot(10, 4, new byte[]{1, 2}), List.of());
            s.append(11, List.of(e(4, "k")));
        }
        try (Storage s = open()) {
            Storage.State st = s.load();
            assertEquals(10, st.snapshot().lastIncludedIndex());
            assertEquals(List.of("4:k"), payloads(st.entries()));
        }
    }
}
