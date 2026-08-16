# Runbook — Dead-letter triage

Records the applier could not write are in `migration_ctl.dead_letter`. The
migration cannot satisfy the cutover gate until every one is resolved or
explicitly accepted.

## Assess

```sql
SELECT status, error_class, source_table, COUNT(*),
       MIN(first_seen_at) AS oldest
  FROM migration_ctl.dead_letter
 WHERE migration_id = :m
 GROUP BY status, error_class, source_table
 ORDER BY COUNT(*) DESC;
```

Read the shape before reading individual rows:

| Shape | Almost always means |
|---|---|
| Many rows, one table, one error class | A schema mismatch on that table |
| Many rows, all tables, transient class | The target was unavailable; they will drain on their own |
| A handful, permanent class | Genuinely bad individual records |
| Steadily growing pending count | The repair worker is not running, or not keeping up |

Check the age of the oldest open record. A growing age is what a raw count hides:
a stalled drain looks identical to a steady trickle until you look at it.

```bash
curl -s $REPAIR/metrics | grep migration_dlq_oldest_open_age_seconds
```

## Common causes

**`value too long`, `numeric out of range`, truncation.** The target column is
narrower than the source. Widen it, then requeue. Do not truncate the data to fit
— that is silent corruption with extra steps.

**Foreign key violations.** The referenced row has not been loaded yet. Usually
resolves on its own as the load progresses; if it persists, the table load order
in the plan is wrong.

**Decode failures.** The connector's output changed, or a message is genuinely
malformed. Inspect the stored payload before assuming.

**Duplicate key.** Should not happen — the fence and coalescing exist to prevent
it. If it does, the primary key declared in the plan does not match the table's
actual key. Stop and fix the plan; do not requeue.

## Requeue

Fix the cause first. Requeueing into an unchanged environment just burns the
retry budget again.

```bash
curl -XPOST $CP/v1/deadletters/requeue \
  -d '{"ids":[101,102,103],"by":"uday"}'
```

Only quarantined records are requeued; pending ones are already scheduled.

## Accepting records that cannot migrate

Occasionally a record genuinely cannot move — corrupt source data, a row that
violates a constraint the new schema legitimately enforces. That is a business
decision, and it should be visible:

1. Document each record and why, outside the platform.
2. Mark them discarded with the reason recorded.
3. Raise `cutover.max_open_dead_letters` to exactly that count — no higher.

Raising the threshold to a round number "for headroom" defeats the purpose. The
number is the point.

## Escalate if

- The pending count grows while the repair worker is running and healthy.
- The same record has been requeued three times.
- Quarantined records exceed roughly 0.01% of migrated rows: that is systemic,
  and requeueing will not fix it.
