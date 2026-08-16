# ADR 0004 — Verify with hierarchical digests, not row comparison

**Status:** Accepted

## Context

A migration that cannot demonstrate correctness has not finished. Row counts
catch a dropped row but not a wrong one, and wrong rows are what heterogeneous
migrations produce. Row-by-row comparison of a billion-row table is infeasible.

## Decision

Both databases compute an order-independent digest (count plus two independent
60-bit projections of the row hash) over a key range. Only digests cross the
network. When they disagree the range is bisected and the comparison repeats;
rows are read only at the leaves.

Digest expressions are generated per dialect with explicit normalisation: CHAR
padding trimmed, decimals rounded to declared scale, timestamps forced to UTC at
fixed precision, NULL mapped to a sentinel that cannot occur in data, values
joined with the unit separator.

## Alternatives considered

**Row counts only.** Cannot detect a wrong value, which is the failure mode that
actually occurs.

**Full row comparison.** Infeasible above roughly ten million rows.

**Sampling.** Gives a confidence interval, not evidence. A regulator asking
whether the data is correct is not asking for a confidence interval.

**A single additive digest.** Can in principle be defeated by two compensating
errors; two independent projections make that vanishingly unlikely at negligible
extra cost.

**Ordered concatenation instead of summation.** Would force both engines to scan
in the same order and prevent parallelism.

## Consequences

- A correct billion-row table costs two queries to prove.
- Verification is cheap enough to run continuously during the migration rather
  than once at the end.
- The normalisation must be kept in lockstep between the two dialect
  implementations; they are written side by side for exactly that reason.
- Randomly-encrypted and redacted columns cannot participate and are excluded.
