# ADR 0002 — Commit stream offsets inside the data transaction

**Status:** Accepted

## Context

Kafka provides at-least-once delivery. The broker's committed offset and the
target database's contents cannot be updated atomically, so any design that
relies on the broker's offset has a window in which the two disagree: crash after
the data write and the batch replays; crash after the offset commit and the batch
is skipped.

## Decision

Offsets live in `migration_ctl.applied_offset` in the target database, written in
the same transaction as the rows they account for. On startup the consumer is
positioned from that table, not from the broker.

The consumer uses explicit partition assignment rather than a consumer group.

## Alternatives considered

**Kafka consumer-group offsets with idempotent writes.** Idempotence handles
repetition but not the offset/data divergence window, and a rebalance during an
apply transaction is exactly when divergence is most likely.

**Two-phase commit across broker and database.** Enormous operational cost, poor
failure behaviour, and unsupported by the brokers in question.

**Offsets in a third store (Redis, DynamoDB).** Reintroduces the same
non-atomicity with an extra dependency.

## Consequences

- At-least-once delivery becomes effectively-once application, with no
  distributed transaction anywhere.
- One extra statement per batch, which is negligible against the data writes.
- Consumer-group tooling does not show meaningful lag; lag is published by the
  applier as a metric instead.
- Adding partitions mid-migration changes key routing and is a correctness event,
  not a scaling operation.
