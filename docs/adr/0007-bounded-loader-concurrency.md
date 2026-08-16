# ADR 0007 — Bounded loader concurrency and set-based merges

**Status:** Accepted

## Context

The natural cloud-native design for loading extracted parts is an S3 event
notification triggering a function per object. It appears in most reference
architectures. At ten thousand parts it is a denial-of-service attack on your own
database: a burst of notifications spawns hundreds of concurrent workers, each
opens a connection, `max_connections` is exhausted, and the resulting failures
retry into the same wall. Nothing in the design is aware of the queue, so there
is no backpressure anywhere.

## Decision

Parts are loaded by a worker pool with a configured, strictly-positive bound.
There is no "unlimited" setting; the configuration validator rejects zero.

Each part is imported into a staging table by the target's **native** S3 loader,
then moved into the live table by **one set-based, LSN-fenced statement**.

## Alternatives considered

**Function-per-object (Lambda on S3 events).** See Context. Also carries a
15-minute execution limit that a large part can exceed, and retries duplicate
work unless every write is idempotent — which it is here, but that is not a
reason to rely on it.

**Row-by-row insertion from the application.** Puts the throughput ceiling at what
one process can push through a driver, and moves terabytes through a worker that
has no reason to see them.

**`COPY` / `LOAD DATA` directly into the live table.** Loses the fence: a stale
part would overwrite fresher rows.

## Consequences

- Predictable connection usage and genuine backpressure.
- Bytes travel object-store-to-database directly.
- "A stale part cannot overwrite a fresher row" becomes a property of one SQL
  statement rather than of a loop that has to be trusted.
- A staging table per in-flight part, created and dropped inside the load
  transaction.
