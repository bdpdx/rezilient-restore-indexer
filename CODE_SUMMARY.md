# rezilient-restore-indexer

Purpose:
- RS-06 restore index sidecar that consumes REC artifact metadata, writes
  allowlisted restore index rows, and maintains authoritative watermark/
  coverage/backfill state for freshness-gated restore workflows.

Entrypoints:
- `src/index.ts`: service bootstrap, durable store wiring, continuous worker
  runtime, and graceful shutdown handling.
- `src/indexer.service.ts`: core read/process/write indexing loop logic.
- `src/freshness.ts`: strict fail-closed freshness gate evaluation.
- `src/backfill.ts`: bootstrap and gap-repair orchestration with lag guardrails.
- `src/store.ts`: restore index persistence interfaces plus in-memory and
  SQLite-backed durable state stores.
- `src/worker.ts`: cursor-aware batch worker with continuous polling loop and
  persisted source progress checkpoints.
- `doc/runbooks.md`: RS-15 operator runbooks for lag, replay/generation, and
  freshness fail-closed incidents.

Tests:
- `src/watermark-invariants.test.ts`
- `src/generation-replay.integration.test.ts`
- `src/worker.integration.test.ts`
- `src/failure-modes.test.ts`
- `src/durability.integration.test.ts`
- `src/continuous-runtime.integration.test.ts`
