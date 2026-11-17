# Design notes

This document explains how `raft-kv` works, why it is built the way it is, and where it deliberately stops. Section numbers refer to the Raft paper (Ongaro and Ousterhout) and to Ongaro's thesis.

## 1. Goals and non-goals

**Goals**

- A *correct* Raft implementation whose safety properties are testable under injected faults.
- Linearizable client semantics, including safe retries.
- Small enough to read in one sitting: about 1,300 lines of protocol and storage code (including comments), with no runtime dependencies.

**Non-goals**: peak throughput, dynamic membership, multi-Raft sharding, a production wire format.

## 2. Components

| Component | Responsibility |
|---|---|
| `RaftNode` | The whole protocol: roles, elections, replication, commit, apply, snapshots |
| `RaftLog` | In-memory log with a compaction offset (`snapshotIndex`); 1-based absolute indices |
| `Storage` | Durability boundary: `FileStorage` (WAL + atomic files), `MemoryStorage` (tests) |
| `Transport` | `TcpTransport` (real) and `InMemoryNetwork` (simulated faults) |
| `StateMachine` | Deterministic `apply` / `snapshot` / `restore`; implemented by `KvStateMachine` |
| `KvService`, `HttpApi` | Client-facing layer: typed commands, redirects, timeouts |

The protocol core depends only on the three small interfaces `Storage`, `Transport` and `StateMachine`. That is what makes the fault-injection tests possible: the exact same `RaftNode` runs over a simulated network and in-memory storage in tests and over TCP and files in production.

## 3. Concurrency model

All protocol state lives behind **one monitor** (`lock`). This is deliberate: Raft's invariants span term, vote, log, and commit index together, and fine-grained locking invites subtle races.

- **No network I/O under the lock.** RPCs are *built* under the lock, *sent* from a worker pool, and their replies are processed under the lock again. Two nodes calling each other can therefore never deadlock.
- A reply is only acted on after re-checking state: `running`, `role == LEADER`, and `currentTerm == termAtSend`. This discards stale replies from earlier terms.
- **At most one in-flight RPC per peer** (`inflight` set). Entries that arrive while an RPC is outstanding are batched into the next one, which gives natural pipelining and batching.
- A **single ticker thread** drives election timeouts, heartbeats and check-quorum.
- **Disk I/O does happen under the lock** (appends and fsyncs). It is simple and preserves the "persist before reply" rule trivially, at the cost of serialising disk waits. Group commit (batching fsyncs across concurrent proposals) is the standard next optimisation.
- **A storage failure is fatal.** If persisting fails, the node halts rather than continue with state it cannot guarantee is durable.

## 4. How each Raft safety property is enforced

| Property | Where it is enforced |
|---|---|
| **Election safety** (≤ 1 leader per term) | A node grants at most one vote per term, and `votedFor` is persisted *before* the vote is revealed (`onRequestVote`). `ElectionTest.atMostOneLeaderPerTermAcrossRepeatedFailovers` observes this continuously. |
| **Leader append-only** | A leader only calls `appendLocal`; it never truncates. Only `onAppendEntries` truncates, and only on followers. |
| **Log matching** | `AppendEntries` carries `(prevLogIndex, prevLogTerm)`; a follower rejects unless its log matches there, and truncates only at a genuine conflict. |
| **Leader completeness** | Vote granting requires the candidate's log to be at least as up to date (`lastLogTerm`, then `lastLogIndex`). |
| **State-machine safety** | A leader commits only entries **from its own term** by counting replicas (`advanceCommit`, §5.4.2); earlier-term entries commit indirectly. A new leader appends a **no-op** so that happens promptly. A follower also asserts it never overwrites a committed entry. |

## 5. Elections

- **Randomised timeouts** (default 300–600 ms against a 75 ms heartbeat) make split votes rare and self-correcting.
- **Pre-vote** (thesis §9.6). Before incrementing its term, a node asks peers *"would you vote for me?"* A peer says yes only if the candidate's term would be higher, its log is up to date, and the peer has **not heard from a leader within the minimum election timeout**. Only a majority of yeses lets the node start a real election. Without this, a node that was merely partitioned would return with an inflated term and force a needless leader change on the whole cluster. `PartitionTest` runs both variants as a control experiment: with pre-vote on the term is unchanged after the rejoin, with it off the term jumps.
- **Check-quorum.** A leader that has not heard from a majority within an election timeout steps down on its own. This stops a leader that is cut off from accepting writes it can never commit, and it fails any pending client futures promptly.
- **Term and vote are persisted** before use, so a restarted node can never vote twice in the same term.

## 6. Log replication

- The leader keeps `nextIndex` and `matchIndex` per follower and sends up to `maxEntriesPerAppend` entries per message.
- **Accelerated backtracking.** On a mismatch the follower returns `conflictTerm` and the first index of that term. The leader jumps back past the whole term (or to the follower's `conflictIndex`) instead of one entry per round trip.
- **Snapshot overlap.** If a message overlaps entries a follower has already compacted, the follower skips the overlapping prefix. Those entries are committed, so they are identical.
- **Commit** advances to the median of `matchIndex` values (including the leader's own last index), subject to the current-term rule above.
- **Apply** happens in `applyCommitted`, in order, under the lock. Pending client futures are completed there. A future is completed successfully only if the entry at that index still has the term the proposal was made in; otherwise it fails with `NotLeaderException`.

## 7. Durability

`FileStorage` writes three files:

| File | Contents | Update method |
|---|---|---|
| `meta.bin` | current term, `votedFor`, CRC32 | write temp file, fsync, atomic rename, fsync directory |
| `snapshot.bin` | last included index/term, state bytes, CRC32 | same |
| `log.wal` | append-only records: `ENTRY`, `TRUNCATE`, `BASE`, each `[len][crc32][payload]` | append + fsync per batch |

**Recovery.** The WAL is replayed. A short or CRC-failing tail is the signature of a crash mid-write; it is discarded and the file truncated. This is safe because the node never acknowledged anything that wasn't fully fsynced, so a half-written record was never visible to anyone.

**Compaction crash window.** Compaction is two steps: write the new snapshot (atomic), then replace the WAL (atomic). A crash between them leaves a *new snapshot next to an old, longer WAL*. `load()` reconciles by dropping WAL entries the snapshot already covers and rewriting the WAL. This exact scenario is a unit test (`crashBetweenSnapshotAndLogCompactionIsRecovered`).

**What "durable" means here.** With `fsync` on (the default), an acknowledged write has been forced to disk on a majority of nodes. `--no-fsync` exists for tests and benchmarks only.

## 8. Snapshots and InstallSnapshot

- When `lastApplied - snapshotIndex >= snapshotThreshold` the node serialises the state machine, persists it, and drops the covered log prefix. Because this runs under the lock, the snapshot is an exact cut at `lastApplied`.
- If a follower needs entries the leader has compacted (`nextIndex <= snapshotIndex`), the leader sends the snapshot (`InstallSnapshot`). If the follower's own log already agrees with the snapshot boundary, it keeps the entries after it.
- **Trade-offs taken:** the snapshot pauses the node for O(state), and is sent as one message. At scale you would snapshot asynchronously (copy-on-write or fork) and stream in chunks.
- The session table is part of the snapshot, so de-duplication survives compaction and `InstallSnapshot`.

## 9. Client interaction

**Proposals.** `RaftNode.propose` appends to the leader's log and returns a future that completes when the entry is committed and applied.

**Ambiguous outcomes.** If a leader is deposed after appending, the future fails, but the entry may still be committed by the next leader. The client cannot know. The standard remedy (thesis §6.3) is implemented here: each command carries `(clientId, seq)`; the state machine remembers the last `(seq, result)` per client and, on a duplicate, returns the stored result without re-executing. This converts *at-least-once delivery* into *exactly-once effect*. The unit tests and the chaos test's CAS counter verify it.

**Linearizable reads.** `GET` is replicated as a log entry, so it is ordered with all writes. This is the simplest correct approach. The usual optimisation is **ReadIndex** (leader records `commitIndex`, confirms leadership with a heartbeat round, then serves the read after applying up to that index) or **lease reads** (which rely on bounded clock drift). `?stale=true` offers an explicitly non-linearizable local read.

**Session table bound.** At most 10,000 sessions are retained, evicting the least recently used. Eviction depends only on apply order, so every replica evicts the same clients. A client idle long enough to be evicted loses duplicate-detection. A production system would add session expiry with client keep-alives.

## 10. Transport

`TcpTransport` opens a short-lived connection per RPC and uses Java serialization with an `ObjectInputFilter` allow-list (`dev.raftkv.raft.*`, `java.util.*`, `java.lang.*`; everything else rejected) plus size and depth limits. It is intentionally simple. It has no authentication or TLS, so the Raft port must stay on a trusted network.

`InMemoryNetwork` delivers calls synchronously on the caller's thread and supports partitions (`partition`, `isolate`, `heal`), probabilistic loss of requests and replies, and random latency. It also re-checks reachability *after* the handler ran, so a partition that appears mid-call loses the reply, which is exactly the nasty "request executed, reply lost" case.

## 11. Walkthrough: the leader dies mid-write

Consider a 3-node cluster, leader **L**, followers **A** and **B**, and a client `PUT x=1` in flight when **L** crashes. Where the crash lands decides the outcome, and every case is safe:

1. **Before replicating anywhere.** The entry exists only in L's log. A or B wins the election (neither has the entry). When L restarts it learns the new term and its uncommitted entry is **truncated** (`ReplicationTest.followerRejectsDivergentHistoryAndAdoptsLeadersLog`). The write was never acknowledged, so nothing was promised. The client's future fails, and its retry (same `clientId`/`seq`) applies the write once.
2. **Replicated to A only (a majority of 2 including L), commit not yet announced.** The entry is on a majority, and the vote rule (the candidate must have an up-to-date log) lets only A win. A's new term starts with a no-op that commits everything before it, including `x=1`. The write survives even though the client never got an answer. This is the ambiguous case that sessions exist for: the retry returns the original result instead of executing twice.
3. **Committed and acknowledged.** It is on a majority and in the winner's log by leader completeness. It cannot be lost.

The chaos test generates thousands of such interleavings and verifies the end state.

## 12. Trade-off summary

| Decision | Benefit | Cost / what a production system would change |
|---|---|---|
| One global lock | Easy to reason about, fewer races | Serialises work; shard the lock or pipeline for throughput |
| Reads via the log | Linearizable with no extra machinery | Every read costs a replication round; use ReadIndex or leases |
| fsync under the lock | Persist-before-reply is trivially true | No group commit |
| Snapshot under the lock, single message | Simple, exact cut | Pause is O(state); stream and copy-on-write for large state |
| Java serialization over TCP | Zero dependencies, compact code | Not a stable or secure wire format; use protobuf plus mTLS |
| Static membership | Avoids joint-consensus complexity | No scaling the cluster without downtime |
| Randomised chaos tests, not a history checker | Catches lost writes, double-apply and divergence | Doesn't prove linearizability of every history; add Porcupine/Knossos |

## 13. Roadmap

1. ReadIndex reads with a benchmark against log reads.
2. History recording plus an offline linearizability check on chaos runs.
3. Membership changes (single-server or joint consensus).
4. Asynchronous chunked snapshots.
5. Group commit and per-peer pipelining.
6. Protobuf wire format and mTLS.
