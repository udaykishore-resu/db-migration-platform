# ADR 0008 — Cutover is a predicate with an audited override

**Status:** Accepted

## Context

Cutover is the one step that is genuinely hard to undo. In practice the decision
is often made by someone reading a dashboard late at night, reconstructing from
memory which conditions were supposed to hold.

## Decision

Readiness is a pure function over measured state, exposed as
`GET /v1/cutover/readiness`, returning 200 when open and 409 with every blocker
when closed. Each blocker carries a stable machine-readable code and a detail
string stating the observed and required values.

An override requires a named author, a substantive reason, and acknowledgement of
every currently-active blocker. It is written to an audit table.

## Alternatives considered

**A dashboard and a checklist.** Relies on the checklist being current and the
reader being rested.

**A hard block with no override.** Refusing to provide an override does not stop
anyone cutting over — it moves the action outside the platform where nothing
records it. Strictly worse.

**Returning only the first blocker.** Turns one fifteen-minute fix into five
separate discoveries across five attempts.

## Consequences

- The gate can be wired into a deployment pipeline on the status code alone.
- Reverse replication is required by default, because without it a post-cutover
  rollback silently discards every write made after cutover.
- Lag must be *stably* under threshold, not momentarily under it.
- Thresholds are configurable, so a migration with a documented set of
  unmigratable records can proceed deliberately, with the count written down.
