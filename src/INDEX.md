# src Index

- `constants.ts`: RS-06 profile/version constants.
- `types.ts`: indexer input/output model types.
- `env.ts`: environment parsing helpers.
- `metadata.ts`: metadata allowlist validation and normalization.
- `rec-manifest-source.ts`: object-store-backed REC manifest source adapter
  with cursor paging, retries, and allowlisted metadata extraction.
- `rec-manifest-object-store-client.ts`: S3 client wrapper for REC manifest
  list/read operations.
- `runtime.ts`: runtime bootstrap wiring for production REC source with
  fail-closed source configuration.
- `store.ts`: sidecar persistence interface plus in-memory/SQLite durable
  implementations.
- `freshness.ts`: freshness/executability state evaluation.
- `indexer.service.ts`: indexing core with generation-bound watermark logic.
- `worker.ts`: cursor-aware source polling loop with continuous runtime
  controls and source progress checkpointing.
- `backfill.ts`: bootstrap and gap-repair controller.
- `index.ts`: local bootstrap.
- `test-helpers.ts`: deterministic fixtures.
- `watermark-invariants.test.ts`: monotonicity unit tests.
- `generation-replay.integration.test.ts`: replay/rewind generation tests.
- `worker.integration.test.ts`: sidecar worker loop tests.
- `runtime-bootstrap.test.ts`: startup fail-closed and runtime source wiring
  tests.
- `failure-modes.test.ts`: stale gate timeout and backfill starvation tests.
- `durability.integration.test.ts`: restart-survival tests for watermark,
  coverage, backfill state, and source progress checkpoints.
- `continuous-runtime.integration.test.ts`: steady-state and lag/replay
  continuous loop processing tests.
- `rec-manifest-source.test.ts`: REC manifest source adapter pagination,
  retry, duplicate-suppression, and parsing tests.
