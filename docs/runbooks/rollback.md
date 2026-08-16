# Runbook — Rollback after cutover

Returning the application to the source database after cutover.

The whole procedure depends on reverse replication having been running. If it was
not, this is not a rollback — it is a decision about how much data to lose, and
that decision belongs to the business, not to whoever is on call.

## Decide quickly

Rollback is cheapest in the first minutes. The longer the application writes to
the target, the more the two databases have diverged and the more the reverse
replication has to carry. Do not spend an hour debugging before deciding.

Roll back if: data is visibly wrong, the target cannot carry the load, or a
defect is found that would take longer to fix than to reverse.

## Procedure

1. **Announce.** Same channel as the cutover.

2. **Stop application writes to the target.**

3. **Drain reverse replication.** Wait until target → source lag reaches zero.
   Every write made after cutover is in that stream; leaving before it drains is
   how a rollback becomes data loss.

4. **Verify the source caught up.**
   ```bash
   ./bin/reconciler -config config.reverse.json -once
   ```

5. **Repoint the application back to the source.** Reverse whatever change step 7
   of the cutover made.

6. **Record it.**
   ```bash
   curl -XPOST $CP/v1/phase -d '{"phase":"rolled_back","by":"<you>"}'
   ```

7. **Re-arm forward replication** source → target, so the target catches up while
   the defect is investigated. The migration returns to the streaming phase and
   can be retried once the cause is fixed.

## If reverse replication was not running

There is no clean path. Assemble the facts before proposing anything:

- Exactly when did cutover happen?
- How many writes landed on the target since? (`_mig_updated_at` on the target
  gives an upper bound.)
- Can those writes be extracted and replayed against the source?

Then present the options with their data-loss implications and let the business
decide. Do not silently pick one.

## Afterwards

Write the post-incident review before re-attempting. The specific question worth
answering: which gate condition, had it been enforced, would have caught this? If
the answer is "none", the gate needs a new condition.
