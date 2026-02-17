# rezilient-restore-indexer

Purpose:
- RS-06 restore index sidecar that consumes REC artifact metadata, writes
  allowlisted restore index rows, and maintains authoritative watermark/
  coverage/backfill state for freshness-gated restore workflows.

Entrypoints:
- `src/index.ts`: service bootstrap and in-memory worker wiring.
- `src/indexer.service.ts`: core read/process/write indexing loop logic.
- `src/freshness.ts`: strict fail-closed freshness gate evaluation.
- `src/backfill.ts`: bootstrap and gap-repair orchestration with lag guardrails.
- `src/store.ts`: in-memory restore index state store used by tests/dev.

Tests:
- `src/watermark-invariants.test.ts`
- `src/generation-replay.integration.test.ts`
- `src/worker.integration.test.ts`
- `src/failure-modes.test.ts`
