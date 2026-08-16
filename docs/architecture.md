# Architecture

This document explains what the platform does, why each part exists, and what
goes wrong without it. It is organised around failures rather than components,
because the components only make sense once the failure they prevent is clear.

---

## 1. The shape of the problem

Moving a live production database to a new engine has four requirements that pull
against each other:

1. **No outage.** The source keeps serving traffic throughout.
2. **No data loss.** Every committed change reaches the target exactly once in
   effect.
3. **Demonstrable correctness.** Not "we believe it worked" — evidence.
4. **No plaintext exposure.** Regulated columns must not appear in clear form
   outside the network that owns them.

Requirements 1 and 2 conflict directly: reading a consistent snapshot of a live
database while changes continue is the entire difficulty. Requirement 3 conflicts
with scale: verifying a billion-row table naively costs more than the migration.
Requirement 4 conflicts with throughput, because the obvious implementation puts
a hardware security module in the path of every row.

Each of the next four sections resolves one of those conflicts.

---

## 2. Snapshot and change capture, without a gap

### The failure

The standard approach is: snapshot first, then start capturing changes. Every
change committed between the snapshot's read and the connector's first captured
event is lost. Nothing errors. The row counts match. The data is wrong.

The usual fixes are all unsatisfying:

| Fix | Why it fails at scale |
|---|---|
| Lock the source tables | Unacceptable on a live system |
| Hold a consistent snapshot for the whole extract | Source must retain undo/WAL for hours or days |
| Start CDC first, then snapshot, and replay everything | Requires the snapshot to be idempotent under reordering — which is really the fence, arrived at the hard way |
| Snapshot, then "catch up" from a recorded timestamp | Timestamps are not a total order; concurrent transactions interleave |

### The resolution: watermarks through the source's own log

The extractor writes marker rows into a signal table on the **source**, around
each chunk read:

```
1. INSERT low watermark for chunk N        ──▶ appears in the source's log
2. SELECT chunk N's rows into memory
3. INSERT high watermark for chunk N       ──▶ appears in the source's log
4. Continue consuming the change stream.
   Between observing the low and the high watermark, every change event
   removes its key from the buffered chunk.
5. On the high watermark, emit whatever survives.
```

The critical property is that the watermarks travel through the **same
transaction log** as the data changes, so they have a defined position in the same
total order. A marker sent out-of-band — over HTTP, on a side channel — would have
no defined position relative to the changes and the protocol would degenerate
into a race.

A row updated while its chunk was in flight is evicted, because the log carries a
fresher version. A row not touched during the window survives, because the
snapshot's copy is current. There is no gap, no lock, and no long-lived snapshot.

This is Netflix's DBLog algorithm, and it is what Debezium implements as
incremental snapshotting. The state machine is
[`internal/snapshot/window.go`](../internal/snapshot/window.go), deliberately free
of I/O so it can be exhaustively tested — its behaviour is otherwise almost
impossible to observe in production.

### The metric that proves it is working

`RowsEvicted`. On a busy table it should be non-zero. **A value of zero on a table
that is receiving writes means the window is not catching concurrent changes and
the protocol is silently not working.** A value approaching the chunk size means
chunks are too large relative to the write rate.

### The file-based alternative

An on-premise bulk unload — an HPC-class export utility writing delimited files —
often cannot be replaced. The platform supports that path as a first-class
strategy: parts are size-rolled with numeric suffixes, sealed with a row count
and a SHA-256, and described by a manifest.

Two things make it safe:

- **A part is only loadable once sealed.** The seal callback fires the moment a
  part is complete, which is what allows the extract and the load to be
  pipelined rather than serialised — roughly halving wall-clock time and
  narrowing the window in which snapshot and stream can disagree.
- **The digest is verified before the part touches the database.** A part
  truncated by a full disk, a partial multipart upload, or a retried extract that
  wrote a shorter file all produce something that loads cleanly and silently omits
  rows. The digest turns each of those into a loud failure.

Because parts are loaded *after* they are extracted, a change event can reach the
target before the part containing that row is merged. That is exactly the
snapshot-clobbers-CDC bug — and it is handled by the fence, below, rather than by
ordering the two.

---

## 3. Effectively-once application

### The failure

Kafka delivers at least once. Two independent things must be true for that to be
safe, and most implementations only get the first:

1. Writes must be idempotent under **repetition**.
2. Writes must be safe under **reordering**, and the offset must not be able to
   disagree with the data.

A separate offset commit fails the second. Crash between the data write and the
offset commit and the batch replays; crash the other way and it is skipped.

### The resolution: a fence and a shared transaction

**The fence.** Every migrated table carries `_mig_lsn`. Every write is
conditional on the incoming LSN being at least as new:

```sql
-- PostgreSQL
INSERT INTO app.accounts (...) VALUES (...)
ON CONFLICT (account_id) DO UPDATE SET ...
WHERE app.accounts._mig_lsn <= EXCLUDED._mig_lsn
```

MySQL has no `WHERE` on `ON DUPLICATE KEY UPDATE`, so the fence becomes
conditional per assignment:

```sql
-- MySQL: same semantics, different shape
... AS new_row ON DUPLICATE KEY UPDATE
  balance = IF(app.accounts._mig_lsn <= new_row._mig_lsn, new_row.balance, app.accounts.balance)
```

The consequence is that arrival order stops mattering. Redelivery is a no-op. A
stale part landing after a fresh change loses. A dead letter replayed forty
minutes late cannot resurrect an old value.

**Deletes tombstone rather than remove.** A removed row loses its LSN, so a
delayed older update would re-insert it — resurrecting a record the source
deleted, with no error anywhere. The tombstone keeps the LSN available to reject
that write. Tombstones are purged after cutover.

**The offset rides along.** `migration_ctl.applied_offset` is written in the same
transaction as the rows, and the consumer positions itself from that table on
startup rather than from the broker's committed offset. No distributed
transaction, no two-phase commit, no window of disagreement.

This is also why the consumer uses explicit partition assignment rather than a
consumer group: a group rebalances at times the platform does not control, and a
rebalance during an apply transaction is precisely when offset and data are most
likely to diverge.

---

## 4. One bad row must not stop the stream

### The failure

A single unmigratable record — a value too long for the target column, a decimal
that overflows, a foreign key that does not exist yet — fails the batch it is in.
Retried, it fails again. The partition stops advancing. Nothing errors loudly,
because from the platform's perspective it is retrying, which is normal. The
migration silently stops making progress.

### The resolution: classify, bisect, dead-letter

**Classify.** [`internal/errclass`](../internal/errclass) separates transient
failures (deadlock, serialisation failure, connection reset, throttling — retry
will succeed) from permanent ones (constraint violation, truncation, decode
failure — retry will fail identically forever). Anything unclassifiable is
treated as transient with a bounded budget, so a novel failure degrades into a
dead letter rather than an infinite loop.

**Bisect.** On a non-transient batch failure the applier halves the batch and
retries each half, recursively. A failing batch of 500 costs about nine extra
round trips to isolate the one broken record; the other 499 are applied normally.

Transient failures are explicitly *not* bisected — the database is unavailable,
not the data invalid, and bisecting would repeat the same failing write against a
struggling target.

**Dead-letter durably.** Failed records go to a table with their original payload
bytes, error class, attempt count and next retry time. Offsets only advance once
every record in the batch has either been applied or durably dead-lettered.

Why a table and not a topic: a dead letter in a topic inherits the topic's
retention, so a record that failed on Friday may not exist on Monday. In a table
it survives until resolved, is queryable by table or error class or age, and is
counted by the same cutover gate as everything else. Payloads are encrypted at
rest, because the table is a durable copy of production data with a longer
retention than the pipeline itself.

**Backoff uses full jitter.** When a target sheds load, every worker fails at
nearly the same instant. Exponential backoff alone makes them all retry at the
same instant too, and the recovering database is knocked over by the retry storm
it just caused. Full jitter spreads retries uniformly across the window.

**Quarantine is terminal.** A record that exhausts its budget stops retrying and
waits for a person, with the reason stored on the row. A record that keeps failing
is a signal; retrying it forever turns that signal into background noise.

---

## 5. Verification that is affordable

### The failure

Row counts catch a dropped row. They do not catch a wrong one — and wrong rows are
what heterogeneous migrations produce, because the two engines disagree about
things nobody thinks to check:

| Disagreement | Effect if unhandled |
|---|---|
| DB2 pads `CHAR` to declared width; the targets do not | Every CHAR column mismatches |
| `DECIMAL` trailing zeros preserved differently | Every decimal column mismatches |
| Timestamp precision and timezone rendering | Every timestamp mismatches |
| CSV cannot distinguish `''` from `NULL` | Every NULL becomes an empty string; `IS NULL` silently returns nothing |
| Concatenation with a separator that occurs in data | `("ab","c")` and `("a","bc")` hash identically |

Row-by-row comparison would catch these, and is infeasible at a billion rows.

### The resolution: hierarchical digests over normalised values

Both databases compute an order-independent digest over a key range, and only the
digests cross the network:

```
digest(range) = ( count(*),
                  Σ first 60 bits of md5(normalised row),
                  Σ second 60 bits of md5(normalised row) )
```

Summation makes it order-independent, so each engine can choose its own access
path and the scan can be parallelised. Two independent projections rather than
one, because a single additive digest can in principle be defeated by two
compensating errors.

When digests disagree, the range is bisected and the comparison repeats. Only at
the leaves — a range small enough to be one indexed scan per side — are rows
actually read, and then the output is exactly which keys differ and how.

```
1 billion rows, correct     →  2 queries,  0 rows transferred
1 billion rows, 1 wrong     →  ~60 queries, ~5,000 rows
```

Confirming correctness is O(1); only incorrectness costs, and it costs
logarithmically. That is what makes it affordable to run continuously during the
migration rather than once at the end, when the only remaining option is to start
over.

**The normalisation is the load-bearing part.** Each dialect generates a digest
expression that trims CHAR padding, rounds decimals to declared scale, forces
timestamps to UTC at fixed precision, maps NULL to a sentinel that cannot occur in
data, and joins with the unit separator control character. Without it every row
mismatches and the exercise is worthless. Both implementations live side by side
in [`internal/dialect`](../internal/dialect) specifically so they can be read
against each other.

**Tokenised columns are compared as tokens.** Deterministic tokenisation means the
same plaintext always yields the same token, so a protected column can be verified
across both databases without ever decrypting anything. Randomly-encrypted and
redacted columns are excluded from the digest, because they differ on the two
sides by design.

---

## 6. The confidentiality boundary

### The failure

The common pattern is: encrypt for transit, decrypt at the loader, insert
plaintext. This protects the wire, which TLS already did, and leaves plaintext at
rest in the target and in the loader's memory. It buys key custody — a real
benefit — while paying the full operational cost of an HSM, and it leaves the
target fully in scope for the regulation the encryption was meant to address.

The second failure is throughput: calling an HSM per row makes HSM
operations-per-second the rate limiter for the entire migration. An HSM does low
thousands of operations per second; a migration needs millions.

### The resolution: tokenise at the source edge, envelope the key

Columns marked `tokenize` are replaced with **deterministic, format-preserving**
tokens before anything leaves the source network. The target stores tokens
permanently:

- it never holds plaintext, so it is out of scope for those columns;
- equality joins and indexes still work, because tokenisation is deterministic;
- reconciliation works without decrypting anything;
- a `CHAR(11)` national identifier column still receives 11 characters with the
  separators in place, so the target schema needs no change.

The token is a PRF over the **complete value**, expanded into the required shape.
Deriving each character independently would be far weaker — an attacker seeing
many tokens could attack one character at a time — so a single-character change
re-randomises the whole token. The domain (`schema.table.column`) is bound into
the PRF, so the same email in two tables tokenises differently and cannot be
correlated across a boundary the data model never intended.

Columns needing reversibility use authenticated encryption, with the column
domain bound as additional authenticated data so a ciphertext lifted from one
column fails rather than decrypts in another.

**The key store is called once per process.** An `Unwrapper` — AWS KMS, GCP KMS,
a PKCS#11 session against a SafeNet HSM, all the same one-method interface —
unwraps a data key at startup; every row after that is a local AES operation.
This is not an optimisation. It is the difference between a migration that
finishes and one that does not.

---

## 7. Cutover

Repointing the application is the one step that is genuinely hard to undo.
Everything before it can be retried, restarted or abandoned at no cost to the
business. Everything after it is running on the new database.

So the decision is a predicate over measured facts, exposed as an endpoint:

```
GET /v1/cutover/readiness  →  200 open  |  409 closed, with every blocker
```

The gate requires lag under threshold **and stably so**, zero open dead letters,
zero reconciliation findings from a **recent** pass, every part loaded, and
reverse replication armed. Each blocker reports the observed and required value,
and all of them are returned at once — an operator planning a window needs the
whole list, not one discovery at a time.

Two details worth calling out:

**Stability, not a snapshot.** A momentary dip below the lag threshold proves
nothing. The gate requires lag to have been under threshold continuously for a
sustained window, because the question is whether the target is keeping up, not
whether it briefly caught its breath.

**Reverse replication is mandatory by default.** Without target-to-source
replication armed, a post-cutover rollback silently discards every write the
business made after cutover. That is not a rollback; it is data loss with a
reassuring name.

**The override is first-class.** It requires a named author, a substantive
reason, acknowledgement of every *currently active* blocker, and is written to an
audit table. Refusing to provide an override does not stop anyone from cutting
over — it just moves the action outside the platform where nothing records it.

---

## 8. Component map

| Component | Responsibility | Key failure it prevents |
|---|---|---|
| `snapshot.PartWriter` | Size-rolled, sealed, digested parts | Loading a half-written or truncated file |
| `snapshot.Window` | Watermark deduplication | Losing or clobbering rows at the snapshot/CDC seam |
| `dialect` | All engine-specific SQL | Cross-engine representation differences becoming phantom drift |
| `sink.Applier` | Transactional, fenced, coalescing apply | Duplicate or skipped batches; one bad row stalling a partition |
| `recon.Reconciler` | Hierarchical digest comparison | Shipping a target nobody verified |
| `dlq` + `repair-worker` | Durable retry with classification | Losing failed records; infinite retry loops |
| `control` | State machine and cutover gate | Cutting over on a green dashboard rather than on evidence |
| `crypto` | Tokenisation and key envelope | Plaintext at rest in the target; HSM as the throughput ceiling |
| `errclass` | Transient vs permanent | Head-of-line blocking on an unretryable record |
| `obs` | Redacted logging, metrics, health | A debug line becoming a reportable incident |

---

## 9. Failure modes and responses

| Failure | Response | Data loss? |
|---|---|---|
| Applier crashes mid-batch | Transaction rolls back; offset never advanced; batch replays; fence makes replay a no-op | None |
| Target failover | Writes fail transiently; backoff with full jitter; resume on the new writer | None |
| Broker unavailable | Poll fails; applier backs off; offsets untouched | None |
| One unmigratable row | Batch bisected; row dead-lettered; rest applied | None (record preserved for repair) |
| Part truncated in transit | Digest verification fails before load | None (part re-staged) |
| Extract dies mid-part | Unsealed part discarded; manifest still consistent; re-extract that part | None |
| Row updated during its chunk read | Window evicts the stale snapshot copy | None |
| Stale part loads after a fresh change | Fence rejects it | None |
| Reconciliation finds drift | Findings recorded; gate closes; repair worker re-applies | None |
| Cutover goes wrong | Reverse replication was armed; roll back to source | None |

---

## 10. Related documents

- [diagrams/](diagrams/) — the deployment, sequence and flow diagrams, also
  embedded in the project README
- [scale.md](scale.md) — capacity math and what breaks first as volume grows
- [reconciliation.md](reconciliation.md) — the digest algorithm in detail
- [security.md](security.md) — threat model and compliance boundary
- [adr/](adr/) — numbered decisions, each with the alternatives considered
- [runbooks/](runbooks/) — cutover, rollback, dead-letter triage, lag incidents
