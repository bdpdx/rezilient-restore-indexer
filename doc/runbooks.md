# Restore Indexer Runbooks (RS-15)

## REC -> RRI Polling Contract

1. REC is the producer of canonical `*.manifest.json` objects under the shared
   restore prefix/bucket.
2. RRI is poll-driven (no push callback). It discovers manifests by listing
   object keys and reading manifest/artifact pairs.
3. RRI cursor advancement is fail-closed:
   - cursor advances only when the full batch indexes without failures,
   - any batch failure keeps the previous cursor and replays deterministically
     on the next cycle.
4. Backfill uses the same fail-closed rule: indexing failures pause backfill
   with `paused_indexing_failures` and do not advance cursor.
5. Operators should treat repeated cursor pinning as an indexing incident and
   resolve the bad artifact/metadata before expecting forward progress.

## 0. Cursor Health Checks and Replay Interpretation

1. Query source progress for the affected source scope:

```sql
SELECT
    tenant_id,
    instance_id,
    source,
    cursor,
    last_batch_size,
    last_indexed_event_time,
    last_indexed_offset,
    last_lag_seconds,
    processed_count,
    updated_at
FROM rez_restore_index.source_progress
WHERE tenant_id = '<tenant_id>'
  AND instance_id = '<instance_id>'
  AND source = '<source_uri>';
```

2. Cursor health expectations:
   - `cursor` should be either:
     - legacy plain key string (backward compatibility), or
     - v2 JSON with `scan_cursor` and `replay` fields.
   - `updated_at` and `processed_count` should continue to move during active
     ingest windows.
3. Batch log interpretation (`restore-indexer batch processed`):
   - `fast_path_selected_key_count`: keys selected from scan cursor path.
   - `replay_path_selected_key_count`: keys selected from replay path.
   - `replay_only_hit_count`: keys found only by replay (expected during
     lexically-lower late arrivals).
   - `replay_cycle_ran=true` with non-zero replay hits indicates replay is
     recovering keys that scan cursor alone would miss.
4. Healthy progression signal:
   - `last_indexed_event_time` and `last_indexed_offset` advance over time.
   - replay counters may be zero on normal cycles and non-zero on late-key
     cycles.
   - temporary cursor pinning is acceptable only when failures are present in
     the same batch.

## 0.1 Malformed Cursor Fail-Closed Remediation

1. When cursor parsing fails, worker logs:
   - `restore-indexer source cursor parse failed; failing closed`
   - diagnostic fields include `cursor_kind`, `cursor_preview`,
     and `manual_remediation` guidance.
2. Parse failures are fail-closed by design:
   - no new batch is indexed,
   - source progress cursor is not advanced.
3. Manual intervention is warranted when either condition is true:
   - repeated parse-failure logs for the same source scope, or
   - `source_progress.updated_at` and `processed_count` stall while new REC
     manifests are known to exist.
4. Remediation procedure:
   - snapshot the current `source_progress` row for incident records,
   - replace `cursor` with either a valid legacy key string or valid v2 JSON,
   - restart/recover worker and confirm batch progression resumes.
5. Do not use broad cursor resets as first response if parse errors are not
   present; prefer diagnosing artifact/indexing failures first.

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
