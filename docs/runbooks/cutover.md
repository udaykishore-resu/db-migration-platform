# Runbook — Cutover

Repointing the application at the target. This is the step that is hard to undo,
so the procedure is deliberately unhurried.

## Preconditions

Do not start until `GET /v1/cutover/readiness` returns **200**. If it returns
409, the body lists every blocker with observed and required values. Work through
them; do not proceed on the assumption that one of them "is probably fine".

Confirm independently:

- [ ] Reverse replication (target → source) is running and its own lag is healthy.
      Without it, rollback discards every write made after cutover.
- [ ] A full reconciliation pass finished within `cutover.max_reconcile_age` and
      found nothing.
- [ ] `migration_ctl.dead_letter` has zero rows in pending, retrying or
      quarantined.
- [ ] Every part is loaded: `parts_loaded == parts_total` in `/v1/status`.
- [ ] Someone who is not driving the cutover has reviewed this list.

## Procedure

1. **Announce.** Post the window. Freeze unrelated deploys.

2. **Move to the cutting-over phase.**
   ```bash
   curl -XPOST $CP/v1/phase -d '{"phase":"cutting_over","by":"<you>"}'
   ```

3. **Stop application writes to the source.** Read traffic may continue. This is
   the only moment of reduced service and it should last minutes, not hours.

4. **Drain.** Wait until replication lag reaches zero and stays there for two
   consecutive minutes.
   ```bash
   watch -n5 'curl -s $APPLIER/metrics | grep migration_replication_lag_seconds'
   ```
   If lag will not reach zero, stop. Do not cut over on a non-zero lag; go to
   [lag-incident.md](lag-incident.md).

5. **Final reconciliation.** Run a full pass and require zero findings.
   ```bash
   ./bin/reconciler -config config.json -once
   ```
   A non-zero exit means findings. Stop and triage.

6. **Re-check the gate.** It must still return 200.

7. **Repoint the application.** Prefer changing a DNS record or a proxy target
   over editing connection strings in the application: it is one change, it is
   reversible in seconds, and it does not require a redeploy to undo.

8. **Verify from the application side.** Run whatever smoke tests exist. Confirm
   writes are landing on the target and that the source is receiving none.

9. **Confirm reverse replication is flowing** target → source. This is your
   rollback path and it is now load-bearing.

10. **Record the cutover.**
    ```bash
    curl -XPOST $CP/v1/phase -d '{"phase":"cutover","by":"<you>"}'
    ```

## After

- Keep reverse replication running for at least one full business cycle — a week
  is typical. Retiring it is the point of no return, not the cutover itself.
- Keep the source read-only rather than deleting it.
- Purge tombstones only after reverse replication is retired; until then they are
  still carrying LSNs the rollback path may need.

## Stop conditions

Abandon the window and roll back if any of these occur. None of them are worth
pushing through:

- Lag will not drain to zero within the window.
- The final reconciliation finds anything at all.
- Reverse replication is not confirmed flowing.
- Application smoke tests fail against the target.
