# ADR 0003 — Close the snapshot/CDC seam with watermarks

**Status:** Accepted

## Context

The gap between "snapshot read" and "change capture started" loses every change
in between. Silently: no error, and the row counts still match.

## Decision

The extractor writes low and high watermark rows into a signal table on the
source around each chunk read. Because the markers travel through the source's own
transaction log, they have a defined position in the same total order as the data
changes. While the window is open, every change event evicts its key from the
buffered chunk; on the high watermark, whatever survives is emitted.

## Alternatives considered

**Lock the source tables.** Unacceptable on a live system.

**Hold one consistent snapshot for the whole extract.** Forces the source to
retain undo or WAL for the duration, which on a multi-terabyte table is hours or
days.

**Snapshot, then replay from a recorded timestamp.** Timestamps are not a total
order; concurrent transactions interleave across any timestamp boundary.

**Start CDC first and replay everything over the snapshot.** Works, but only
because the fence makes replay safe — which is this decision arrived at
indirectly, with a much larger replay volume.

**Out-of-band watermarks (HTTP, side channel).** Rejected: a marker that does not
travel through the log has no defined position relative to the changes, and the
protocol becomes a race.

## Consequences

- No source locking, no long-lived snapshot, no gap.
- Requires write access to a signal table on the source, which is occasionally a
  political rather than technical obstacle.
- `RowsEvicted` is the metric that proves the protocol is live. Zero on a table
  receiving writes means it is silently not working.
