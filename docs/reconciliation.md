# Reconciliation

How the platform proves the target matches the source, and what that proof is
worth.

## The claim being made

At the end of a migration somebody asks "is the data correct?". Three answers are
commonly given and only one of them is evidence:

- *"The row counts match."* Detects a dropped row. Detects nothing about a wrong
  one — and wrong rows are what heterogeneous migrations produce.
- *"We sampled ten thousand rows and they matched."* A confidence interval. A
  regulator asking whether the data is correct is not asking for a confidence
  interval.
- *"Every row on both sides hashes to the same value under a normalisation we can
  show you."* Evidence.

This platform provides the third, at a cost that makes it affordable to run
continuously rather than once.

## The algorithm

### Row digest

Each row is reduced to `md5` over its normalised column values, joined with the
unit separator (`0x1F`) — a character that cannot occur in a text column, so
`("ab","c")` and `("a","bc")` cannot collide.

Normalisation is where the work is:

| Concern | Treatment | Without it |
|---|---|---|
| `NULL` | Mapped to `\x00NULL` | A NULL and a row containing the sentinel string would hash identically |
| `CHAR` padding | `rtrim` | DB2 pads to declared width, targets do not; every CHAR column mismatches |
| `DECIMAL` scale | Rounded to declared scale | `1.50` vs `1.5` mismatches on every row |
| Timestamps | Forced to UTC, fixed precision | Precision and zone rendering differ per engine |
| Floats | Fixed significant digits | Default float-to-text differs in the last place |
| Booleans | `'1'` / `'0'` | `true`/`t`/`1` all appear across engines |
| Bytes | Lowercase hex | Encoding differs |

Columns excluded from the digest: `redact` (they do not exist on the target) and
`encrypt` (randomised ciphertext differs on every write by design). `tokenize`
columns **are** included — deterministic tokenisation means both sides hold the
same token, so a protected column is verified without decrypting anything.

### Range digest

```
digest(range) = ( count(*),
                  Σ  int(md5[0:15],  base 16),
                  Σ  int(md5[16:31], base 16) )
```

Summation is order-independent, so each engine may scan however it prefers and
the scan can be parallelised. An ordered concatenation would force both sides
into the same access path.

Two independent 60-bit projections rather than one: a single additive digest can
in principle be defeated by two compensating errors — one row hashing too high,
another too low by exactly the same amount. Vanishingly unlikely, but the second
projection costs nothing and removes the argument.

### Descent

```
compare(range):
    if digest_source(range) == digest_target(range):   return          # clean
    if range is small enough, or cannot be bisected:   compare rows
    mid = bisect(range)
    compare(low..mid); compare(mid..high)
```

Ranges are half-open `(low, high]` so adjacent ranges tile the key space without
overlapping — an overlap would double-count rows on both sides.

Bisection is type-aware: integers by arithmetic midpoint computed in `big.Int`
(the obvious `(lo+hi)/2` overflows near the extremes and sends the descent
somewhere nonsensical); strings by base-256 midpoint, which keeps making progress
on keys sharing a long prefix like `ACCT-2026-08-000000001`, where
first-differing-character bisection stalls; timestamps by instant.

## Cost

| Table state | Digest queries | Rows transferred |
|---|---|---|
| 1 billion rows, correct | 2 | 0 |
| 1 billion rows, 1 wrong | ~60 | ~5,000 |
| 1 billion rows, 100 wrong scattered | ~600 | ~500,000 |
| Systematically wrong | stops at `max_findings` | bounded |

Confirming correctness is O(1) in table size. Only incorrectness costs, and
logarithmically.

## Findings

| Kind | Meaning | Usual cause |
|---|---|---|
| `missing_in_target` | Source has it, target does not | Dropped event, or a part that failed to load |
| `missing_in_source` | Target has it, source does not | Unreplicated delete, or a row invented by a bad transform |
| `value_mismatch` | Both have it, contents differ | The failure counts can never catch |
| `range_unresolved` | Digests disagree, descent hit its budget | Investigate manually; never reported as clean |

That last row matters. **A verification tool that reports "no differences found"
when it ran out of budget is worse than no tool at all**, so exhausting the depth
limit produces an explicit unresolved finding carrying the range.

Findings carry a key hash for reporting and the real key for the repair worker.
Primary keys in a consumer database are frequently PII, and a findings table is
read by more people than the data is.

## Limits, stated plainly

- **A range that matches is proven equal only under the declared normalisation.**
  If a column's `type` or `scale` in the plan is wrong, both sides are normalised
  wrongly and identically, and a real difference can hide. The plan is part of
  the proof.
- **`encrypt` and `redact` columns are not verified.** By construction. If a
  column must be verified, it must be `tokenize` or unprotected.
- **The comparison is not instantaneous.** On a live source, a row changing
  between the source and target digest queries can produce a transient finding.
  This is why continuous mode tolerates findings that disappear on the next pass,
  and why the cutover gate requires a *recent* clean pass rather than any clean
  pass.
- **Reconciliation reads both databases.** On the largest tables the digest scan
  is real load. Widen the interval rather than turning it off.
