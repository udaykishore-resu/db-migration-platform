# ADR 0006 — Dead letters in a table, not a topic

**Status:** Accepted

## Context

Failed records must not be lost, must not block the records behind them, and must
be retryable later without an operator reconstructing them by hand.

## Decision

Dead letters are rows in `migration_ctl.dead_letter` in the target database,
holding the original payload bytes, error class, attempt count and next retry
time. Payloads are encrypted at rest. The repair worker claims work with
`SELECT … FOR UPDATE SKIP LOCKED`.

## Alternatives considered

**A dead-letter topic.** Inherits the topic's retention: a record that failed on
Friday may not exist by Monday. Requires a consumer to inspect. Cannot
participate in the apply transaction, so a record can be both dead-lettered and
re-consumed.

**Tiered retry topics (`retry.5s`, `retry.1m`, …).** Works, but the schedule is
fixed at topic-creation time and the backlog is invisible without consuming it.

**In-memory retry only.** Loses everything on restart, which is when failures
cluster.

## Consequences

- Records survive until somebody resolves them; the backlog is queryable by
  table, error class or age with ordinary SQL.
- The dead-letter write participates in the same transaction as the offset
  commit, so a record is never both dead-lettered and re-consumed.
- The target now holds a durable copy of production data with a longer retention
  than the pipeline, which is why payloads are encrypted.
- Resolved rows accumulate and need archiving; the claim index is partial on
  pending rows so growth does not degrade the drain.
