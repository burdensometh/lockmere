# Lockmere

Lockmere is an embedded relational database with a narrow SQL dialect and a single durable writer. It supports integer and UTF-8 string primary keys, ordered range reads, explicit transactions, uniqueness, foreign keys, and deferred checks. The same engine is available as a Rust library, a console, and a loopback TCP service.

## Run on Linux

Rust 1.90 or newer is required. The crate has no external dependencies.

```sh
cargo build --locked --offline
cargo test --locked --offline

target/debug/lockmere init var/store config/sample.schema config/default.conf
target/debug/lockmere console var/store config/default.conf <<'EOF'
HELLO client-a
REQUEST r1 BEGIN SERIALIZABLE
REQUEST r2 INSERT INTO branch (id, code) VALUES (1, 'north')
REQUEST r3 INSERT INTO badge (id, branch_id) VALUES ('A', 1)
REQUEST published COMMIT
REQUEST published COMMIT
REQUEST r4 SHOW HISTORY
EOF
```

TCP uses the same line protocol. Run `lockmere serve var/store 127.0.0.1:5057` and connect each client separately. The first line associates a client name with the connection. Later frames contain a request identity and one statement. The server accepts loopback addresses, processes one request at a time per connection, and rolls back that connection’s active transaction when it closes. The client name scopes durable receipts and is supplied by the caller.

## SQL and transaction behavior

| Statement | Syntax |
| --- | --- |
| Begin | `BEGIN [TRANSACTION] [ISOLATION LEVEL] [SNAPSHOT\|SERIALIZABLE]` |
| Point read | `GET table key` |
| Range read | `SELECT * FROM table [WHERE id comparison key [AND ...]] [LIMIT n]` |
| Insert | `INSERT INTO table (id, col, ...) VALUES (key, value, ...)` |
| Update | `UPDATE table SET col=value [, ...] WHERE id=key` |
| Delete | `DELETE FROM table WHERE id=key` |
| Savepoints | `SAVEPOINT name`, `ROLLBACK TO [SAVEPOINT] name`, `RELEASE [SAVEPOINT] name` |
| Constraint timing | `SET CONSTRAINTS name\|ALL IMMEDIATE\|DEFERRED` |
| End | `COMMIT`, `ROLLBACK` |
| Inspect | `SHOW LOCKS`, `SHOW TRANSACTION`, `SHOW SCHEMA`, `SHOW EVENTS [AFTER n]`, `SHOW HISTORY [AFTER n] [LIMIT n]`, `CHECKPOINT` |

Identifiers are case-insensitive in SQL and lowercase in schema files. Literals include signed 64-bit integers, single-quoted UTF-8 strings with doubled quotes, and `NULL` for nullable non-key fields. String keys use case-sensitive byte order. Scans return keys in ascending order. The comparisons are `=`, `<`, `<=`, `>`, `>=`. One statement can end in a semicolon. An omitted limit uses the configured scan maximum.

Snapshot transactions pin the committed sequence at `BEGIN` and read their own writes. A write waits for an overlapping lock and then checks whether the key changed since its snapshot. Serializable transactions also acquire shared locks on point reads and on the full requested predicate, including missing rows and ranges returned with a limit. A reader whose predicate changed after its snapshot aborts with `SERIALIZATION`. For successful serializable transactions, the committed reads and writes agree with commit order. Snapshot clients can exhibit write skew.

Locks are released at transaction end. `ROLLBACK TO` restores pending writes and constraint modes and retains acquired locks. Waiters follow FIFO order among conflicting requests. A new request that closes a cycle aborts its own transaction. A lock timeout removes the waiting operation and keeps the transaction active. `SHOW LOCKS` distinguishes granted blockers from earlier waiters. `SHOW EVENTS` returns bounded event history with a cursor and reports constraint phases, waiting, readiness, commits, and aborts.

Schema configuration defines tables and named single-column constraints. Unique and foreign-key participants are non-null. A foreign key may reference a primary key or an equally typed unique column. Immediate checks use the latest committed state overlaid with the current transaction’s writes. Every commit rechecks affected constraints against that view. Constraint failure leaves the transaction active for repair or rollback. A durable identity consists of the client and request of a successful `COMMIT`. Repeating that command with the same identity from an idle session returns the original sequence and write count with `replayed=true`.

## Storage v1

`journal.v1` starts with `LOCKMERE<TAB>1`, a store identity, a canonical schema, and a `DATA` separator. Each committed transaction has one newline-terminated frame. Frames store the sequence, transaction identity, optional durable request, sorted row changes, and a footer with the sequence, payload byte length, and CRC-32. Integers are decimal, text uses lowercase UTF-8 hex, and deletion uses a tombstone.

The engine synchronizes each frame before publishing its versions and receipt. An incomplete final frame is truncated on open. A malformed complete frame errors with its byte offset. An exclusive file lease prevents concurrent opens. The optional checkpoint carries current row heads, committed identities, and durable requests. It binds to the journal’s store identity and exact last included frame. Checkpoint creation writes and synchronizes a temporary file, atomically replaces the previous checkpoint, and synchronizes the directory. Open applies the journal suffix after that anchor. The full append-only journal stays available to `SHOW HISTORY`. Checkpointing can collect old in-memory versions once active snapshots no longer require them.

`SHOW HISTORY` is served through a derived, sparse query index in `history.v1`. The sidecar never duplicates frame payloads: each entry records the byte offset at which a span of 246 commit frames begins, and the journal remains the source of truth. A tail read seeks to the newest span and keeps a bounded window of the requested page, while a cursor read seeks to the span owning the first frame after the cursor and stops once the page is full. Either shape decodes at most one span plus the page, so the strict bound is 256 decoded frames for any request, including the last ten rows of a hundred-thousand-commit journal, and every page is returned in ascending commit order. The index is maintained incrementally while appending and refreshed atomically at each span boundary. On open it is validated against the journal; a missing, stale, or corrupt sidecar is rebuilt in one journal scan and persisted again, without rewriting any journal frame, so an existing v1 store gains the index purely by being reopened. A torn journal tail is truncated before the index is built, leaving every indexed frame confirmed durable.

The Rust `FaultInjector` supports three boundaries around append and checkpoint publication. A failed append or interrupted publication makes the open engine unavailable. Reopening reconstructs the last durable commit and its request receipt. The native recovery tests exercise a child process that exits after the journal sync.

## Replay and benchmarks

```sh
target/debug/lockmere demo var/demo
target/debug/lockmere init var/replay config/sample.schema
target/debug/lockmere replay var/replay demo/history.lm
cargo bench --locked --offline --bench hotspots
```

Use a new directory per demo. The schedule parks a write behind a serial range read, checks the wait graph, resumes the writer after commit, rolls back a deferred foreign-key violation to a savepoint, then checkpoints and reopens the store. It asserts durable sequence and request replay at explicit fences.

`fixtures/v1/` contains the journal and checkpoint emitted by the native demo. Tests load this journal, append a new transaction, checkpoint, and reopen. The tests also include a serial-order enumerator independent of the engine, plus real loopback sessions. The benchmark measures ordered index scans, wait-graph construction, and recovery from WAL alone and with a checkpoint. It checks each workload’s expected rows or edges. Set `LOCKMERE_BENCH_SCALE` between 1 and 20 to change the dataset.

## Source map

- `src/tx` owns live transactions, savepoints, validation and the append-before-publish boundary.
- `src/rows` merges MVCC histories with pending changes in primary-key order.
- `src/locks` owns predicate grants, conflicting queues and deadlock evidence.
- `src/constraints` validates canonical schema and evaluates immediate or deferred checks.
- `src/wal` implements v1 framing, receipt persistence, checkpoints, recovery and the sparse `history.v1` query index.
- `src/session` implements the SQL parser, request identity, result encoding and transports.
- `src/scenario.rs` executes deterministic multi-client fences against the session interface.
