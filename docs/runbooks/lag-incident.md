# Runbook — Replication lag

Lag is the number the cutover gate watches. Sustained high lag means cutover
cannot happen; growing lag means the target is not keeping up with the source and
will never catch up on its own.

## First: is it growing or just high?

```bash
curl -s $APPLIER/metrics | grep migration_replication_lag_seconds
```

**High but flat** — the target is keeping pace, just behind. Usually a backlog
after an outage, and it will drain.

**Growing** — the target is slower than the source. It will not recover without
intervention. Treat this as urgent.

## Diagnose in this order

1. **Is the applier running and healthy?**
   ```bash
   curl -s $APPLIER/readyz
   ```
   Not ready usually means it lost the target or the broker.

2. **Is it erroring?**
   ```bash
   curl -s $APPLIER/metrics | grep -E 'apply_errors|poll_errors'
   ```
   A rising error rate with flat applied rows means every batch is failing.

3. **Is one partition stuck?** Compare per-partition consumer lag. One partition
   flat while others advance means a record is failing repeatedly on that
   partition — go to [dlq-triage.md](dlq-triage.md).

4. **Is the target the bottleneck?** Check write IOPS, CPU and active
   connections. A target near its connection limit produces failures that look
   like data errors.

5. **Is the connector behind?** Lag is measured from source commit, so connector
   delay shows up here. Check the connector's own metrics — if it is behind, the
   applier is idle and adding appliers will do nothing.

## Remedies, in order of preference

**Add applier replicas**, up to one per partition. Cheap and safe. Watch the
target connection count as you scale.

**Raise `apply.batch_size`** (1000 → 2000–5000). Fewer, larger transactions.
Watch transaction duration: if it approaches the target's lock-wait timeout you
have gone too far.

**Reduce competing load.** The snapshot loader and the applier contend. During a
lag incident, lower `extract.loader_concurrency` — the bulk load can wait, the
stream cannot.

**Widen the reconciliation interval.** Digest queries are full range scans; on
the largest tables they compete with the apply path.

**Add partitions — carefully.** This is the last resort. Adding partitions
changes key routing, so a row's history splits across two partitions and per-key
ordering is no longer guaranteed for it. It is a correctness event, not a scaling
operation. If you must, do it during a write freeze after draining to zero.

## What not to do

**Do not skip records to catch up.** Lag is a symptom; skipping converts it into
silent data loss.

**Do not disable the fence or the offset transaction.** Both are occasionally
suggested as throughput wins. Both trade correctness for a number on a dashboard.

**Do not cut over on non-zero lag** because the window is closing. Reschedule.
