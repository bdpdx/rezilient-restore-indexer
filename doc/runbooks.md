# Restore Indexer Runbooks (RS-15)

## 1. Sidecar Lag / Freshness Breach

1. Query freshness status in restore admin:
   - `GET /v1/admin/ops/freshness`
2. Confirm lag pattern:
   - stale partitions growing,
   - stale source count breaching SLO target,
   - execute plans shifting to `preview_only` or `blocked`.
3. Pause non-critical bootstrap/gap backfill work if active.
4. Validate REC artifact ingress and object-store read path.
5. Resume/restart indexer worker loop and verify watermark forward progress.
6. Re-run dry-run plans for affected scopes and confirm executability returns to
   `executable`.

## 2. Backfill Starvation Guardrail Pause

1. Inspect backfill status for `paused_realtime_lag_guardrail`.
2. Confirm realtime lag exceeded guardrail threshold.
3. Keep pause in place until freshness lag drops under threshold.
4. Resume backfill in controlled windows and monitor:
   - stale source count,
   - queue depth,
   - restore execute blocks due to freshness.

## 3. Generation Rewind / Replay Event

1. Verify whether offset rewind occurred in the same generation.
2. If rewind is required, create a new generation ID before replay.
3. Re-index affected partitions under the new generation.
4. Confirm watermark monotonicity for the active generation and update incident
   notes with previous/current generation IDs.

## 4. Unknown Watermark (Fail-Closed)

1. Treat unknown watermark as non-executable restore state.
2. Validate index publication pipeline and per-partition writes.
3. Reconcile missing partition coverage and restore watermark state.
4. Re-run dry-run to verify reason code clears from
   `blocked_freshness_unknown`.

## 5. Integrity / Duplicate Suppression Drift

1. Confirm repeated event IDs are scoped correctly by generation.
2. If duplicate suppression drift is suspected:
   - inspect generation IDs,
   - verify replay mode was intentional,
   - rebuild affected index slice in a fresh generation.
3. Record all corrective actions in incident notes and link to GA gate
   readiness evidence.
