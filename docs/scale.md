# Scale and capacity

This document exists because the interesting failures in a migration platform are
almost all volume-dependent. A design that is obviously correct at ten thousand
rows can be quietly impossible at ten billion, and the difference is rarely in
the part people spend time on.

The numbers below assume the shape this platform was built for: a multi-terabyte
source, **millions of change events per day**, and a table count in the tens.

---

## 1. What the change stream actually costs

Start with the arithmetic, because intuition is unreliable here.

| Daily changes | Average rate | Realistic peak (8× average) |
|---|---|---|
| 1,000,000 | 12 /s | ~100 /s |
| 10,000,000 | 116 /s | ~1,000 /s |
| 50,000,000 | 580 /s | ~5,000 /s |
| 200,000,000 | 2,300 /s | ~20,000 /s |

At 10M/day the applier is not the constraint. With `batch_size: 1000` and a
250 ms linger, peak traffic produces roughly one transaction per second, each
writing a single multi-row upsert per table. Aurora's write path is nowhere near
stressed by that.

The thing that *is* a constraint at every volume is **partition count**, because
per-key ordering means one partition is one ordering domain.

### Partition sizing

```
partitions ≥ max( peak_events_per_second ÷ 3000 ,  desired_applier_replicas )
```

Three thousand events per second per partition is a conservative figure for a
batched, fenced applier against Aurora; measure yours, but start there.

Two rules that matter more than the number:

- **Partition by primary key, never round-robin.** Round-robin puts two changes
  to the same row on different partitions, they are consumed concurrently, and
  the older one can land last. The LSN fence catches that — but it catches it by
  *discarding* the older write, which means the fence is now doing work that
  correct partitioning would have made unnecessary, and any table you later add
  without a fence is silently broken.
- **Over-provision partitions at the start.** Kafka can add partitions later, but
  adding them changes the key-to-partition mapping, so a row's history splits
  across two partitions and ordering is no longer guaranteed for it. Doubling the
  partition count mid-migration is not a scaling operation; it is a correctness
  event.

### Applier replica sizing

One replica per partition is the ceiling; fewer is fine. Each replica holds one
connection per in-flight transaction, so:

```
target_connections_used ≈ applier_replicas × 2   (one active, one idle)
```

Keep total platform connections — appliers, loaders, reconcilers, repair
workers — under about 40% of the target's `max_connections`. The rest belongs to
the application that is about to be pointed at this database.

---

## 2. The bulk load is the hard part, not the stream

Ten million changes a day is a modest stream. Ten billion existing rows is a
genuinely large load, and this is where designs fall over.

### Part sizing

At the default 2 GiB roll threshold:

| Table size | Approximate parts |
|---|---|
| 100 GB | ~50 |
| 2 TB | ~1,000 |
| 20 TB | ~10,000 |

The threshold is a balance between two failure modes. Parts that are too large
make retries expensive — a failure at 99% of a 50 GB part discards 50 GB of
work. Parts that are too small multiply per-part overhead: an object storage
request, a staging table create and drop, a manifest entry, and a control-schema
row each.

Adjust with `extract.max_part_bytes` when rows are unusually wide or narrow. The
rows-per-part cap (`extract.max_part_rows`) exists so that a table of very narrow
rows still produces parts that load in bounded time.

### Loader concurrency is a hard bound, not a suggestion

`extract.loader_concurrency` is validated as strictly positive and has no
"unlimited" setting. That is deliberate.

The natural cloud-native design here is S3 event notifications triggering a
Lambda per object. It is elegant, it is in every reference architecture, and at
ten thousand parts it is a denial-of-service attack on your own database: a burst
of notifications spawns hundreds of concurrent workers, each opens a connection,
`max_connections` is exhausted, the failures retry into the same wall, and the
retries make it worse. There is no backpressure anywhere in that design because
nothing in it is aware of the queue.

A bounded pool is slower in theory and dramatically faster in practice. Start at
4–8 and raise it while watching target write IOPS and connection count; stop
when either flattens.

### Why the merge is set-based

Each part is imported into a staging table by the target's **native** S3 loader —
`aws_s3.table_import_from_s3` on Postgres, `LOAD DATA FROM S3` on MySQL — so the
bytes travel from object storage to the database directly and never through a
worker process. Then one statement moves the staged rows into the live table
under the LSN fence.

Streaming ten billion rows through an application process, even a fast one, means
ten billion round trips of marshalling, and puts your migration's throughput
ceiling at whatever a single Go process can push through a driver. The native
path removes the process from the data path entirely.

---

## 3. Reconciliation is where naive designs become impossible

This is the section that most justifies the architecture.

Row-by-row comparison of a billion-row table is not slow, it is infeasible: it
means reading both tables in full, transferring both across the network, and
sorting or hashing a billion rows on a machine that has neither the memory nor
the time. Which is why most migrations at this scale do not verify at all — they
compare row counts, declare victory, and find out later.

The hierarchical digest changes the shape of the problem:

| Table state | Digest queries | Rows transferred |
|---|---|---|
| 1 billion rows, correct | **2** | 0 |
| 1 billion rows, 1 wrong | ~60 | ~5,000 |
| 1 billion rows, 100 wrong scattered | ~600 | ~500,000 |
| 1 billion rows, systematically wrong | stops at `max_findings` | bounded |

Confirming correctness is O(1). Only incorrectness costs anything, and it costs
logarithmically.

### Cadence at volume

- **Continuous** (`reconciliation.interval: 5m`): the whole plan, every five
  minutes. On correct data this is two queries per table per pass — cheap enough
  that there is no argument for turning it off.
- The digest queries are full range scans on both sides. On a very large table
  they are the most expensive thing the reconciler does even when everything
  matches, so on tables above roughly a billion rows, widen the interval to
  15–30 minutes and rely on the continuous stream lag metric between passes.
- Run one **full pass immediately before** the cutover gate is consulted. The
  gate's staleness threshold (`cutover.max_reconcile_age`) exists to enforce
  exactly this.

### Tuning `leaf_rows`

`reconciliation.leaf_rows` is where the descent stops bisecting and reads rows.
Too small and the descent makes many round trips; too large and each leaf read is
expensive. 5,000 is a good default: one indexed range scan per side. Raise it if
your key distribution is dense and even; lower it if the tables are very wide.

---

## 4. What breaks first, in order

Ranked by what actually bites as volume grows:

1. **Partition count.** Under-provisioned partitions cap applier parallelism, and
   fixing it mid-migration changes key routing. Get this right before you start.
2. **Target connection limit.** Every component holds connections. Unbounded
   loader concurrency is the usual culprit; a reconciler with too many workers is
   the second.
3. **Dead-letter table growth.** Resolved entries accumulate. The claim query has
   a partial index on pending rows precisely so that a million resolved rows do
   not turn the drain into a table scan — but archive resolved rows older than a
   few days anyway.
4. **Part count per table.** Ten thousand parts means ten thousand
   `part_state` rows and ten thousand manifest entries per table. Workable, but
   check that your object storage listing is prefix-scoped rather than scanning
   the whole bucket.
5. **Reconciliation digest cost on the largest table.** The first thing to widen
   when the source starts feeling the load.
6. **Log volume.** At 20k events/s, one log line per event is 20k lines/s and
   your log bill exceeds your database bill. The applier uses sampled logging on
   the hot path for this reason; keep it that way.

---

## 5. Settings that matter at volume

| Setting | Default | At high volume |
|---|---|---|
| `apply.batch_size` | 1000 | 2000–5000; watch transaction duration, not throughput |
| `apply.batch_timeout` | 250ms | Leave. Lower increases commit rate; higher increases lag |
| `apply.max_rows_per_statement` | 500 | 500–1000. Larger statements make failure bisection costlier |
| `apply.workers` | 4 | One per partition, up to the connection budget |
| `extract.max_part_bytes` | 2 GiB | 2–8 GiB. Larger only if retries are cheap for you |
| `extract.loader_concurrency` | 4 | 8–16, raised while watching target IOPS |
| `reconciliation.interval` | 5m | 15–30m on billion-row tables |
| `reconciliation.leaf_rows` | 5000 | 5000–20000 for dense integer keys |
| `cutover.lag_stable_for` | 15m | Raise it. A busy system should demonstrate more than 15 minutes of stability before you commit |

---

## 6. Two things that do not get faster by tuning

**Per-key ordering is a hard serialisation point.** A single extremely hot row —
a counter, a sequence table, a shared aggregate — is processed by exactly one
partition, one applier, one transaction at a time. No amount of parallelism helps.
Coalescing mitigates it: fifty updates to the same row inside one batch collapse
to one write. If a table is genuinely dominated by one hot key, the migration is
not the problem; the schema is.

**The HSM, if you put it in the row path.** This is the single most common way a
correctly-designed migration ends up impossibly slow. An HSM does low thousands
of operations per second; a migration needs millions. The envelope pattern used
here — unwrap a data key once per process, then do local AES per row — is not an
optimisation, it is the difference between a migration that finishes and one that
does not. If a requirement forces a per-row HSM call, the honest answer is to
change the requirement or the schedule, not to tune around it.
