# ADR 0001 — Fence every write on the source change sequence number

**Status:** Accepted

## Context

Records arrive out of order. Kafka redelivers after a rebalance. The repair
worker replays dead letters minutes or hours late. A bulk snapshot part is loaded
after a change event that was captured while the extract was still running. Any
of these can present the target with an older version of a row after a newer one
has already been applied.

## Decision

Every migrated table carries `_mig_lsn`, holding the source change sequence
number of the last write that touched the row. Every write — from the bulk loader
and from the change applier alike — is conditional on the incoming LSN being at
least as new as the stored one.

Deletes write a tombstone rather than removing the row.

## Alternatives considered

**Rely on ordered delivery.** Requires per-key partitioning to be correct
everywhere, forever, including on tables added later by someone who does not know
this. One misconfigured topic silently corrupts data.

**Last-writer-wins on a timestamp.** Timestamps are not a total order.
Concurrent transactions interleave, clocks drift, and two changes inside one
millisecond are indistinguishable.

**Compare-and-swap on a version column the application maintains.** Requires
changing the source application, which is usually the one thing a migration is
not permitted to do.

**Hard deletes.** Rejected specifically: removing the row discards its LSN, so a
delayed older UPDATE re-inserts it and resurrects a record the source deleted,
with no error anywhere.

## Consequences

- Every write is safe under replay and reordering, so no other component needs
  to be careful about arrival order.
- Two extra columns per table, plus a tombstone purge after cutover.
- On MySQL the fence must be expressed per-assignment, because there is no
  `WHERE` on `ON DUPLICATE KEY UPDATE`. Semantics are identical; the affected-rows
  count is not, and monitoring must not read meaning into it.
