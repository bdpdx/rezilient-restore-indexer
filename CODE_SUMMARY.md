# rezilient-restore-indexer

Purpose:
- RS-06 restore index sidecar that consumes REC artifact metadata, writes
  allowlisted restore index rows, and maintains authoritative watermark/
  coverage/backfill/source-progress state in `rez_restore_index` relational
  tables for freshness-gated restore workflows.

Entrypoints:
- `src/index.ts`: service bootstrap, durable store wiring, continuous worker
  runtime, and graceful shutdown handling.
- `src/indexer.service.ts`: core read/process/write indexing loop logic.
- `src/freshness.ts`: strict fail-closed freshness gate evaluation.
- `src/backfill.ts`: bootstrap and gap-repair orchestration with lag guardrails.
- `src/store.ts`: restore index persistence interfaces plus in-memory and
  Postgres-backed durable state stores for restore-plane tables
  (`index_events`, `partition_watermarks`, `partition_generations`,
  `source_coverage`, `backfill_runs`, `source_progress`,
  `source_leader_leases`).
- `src/worker.ts`: cursor-aware batch worker with continuous polling loop,
  fail-closed cursor advancement, source-scope leader lease gating, and
  persisted source progress checkpoints.
- `src/rec-manifest-source.ts`: production REC manifest object-store source
  adapter with retrying reads, deterministic key cursoring, and manifest-page
  scanning that avoids empty-page starvation.
- `src/rec-manifest-object-store-client.ts`: S3-backed object-store client used
  by runtime REC manifest source wiring.
- `src/runtime.ts`: fail-closed runtime bootstrap that selects production REC
  source by default and allows explicit scaffold-only opt-in.
- `doc/runbooks.md`: RS-15 operator runbooks for lag, replay/generation, and
  freshness fail-closed incidents.

Tests:
- `src/watermark-invariants.test.ts`
- `src/generation-replay.integration.test.ts`
- `src/worker.integration.test.ts`
- `src/runtime-bootstrap.test.ts`
- `src/failure-modes.test.ts`
- `src/durability.integration.test.ts`
- `src/continuous-runtime.integration.test.ts`
- `src/rec-manifest-source.test.ts`
