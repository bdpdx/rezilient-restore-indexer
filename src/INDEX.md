# src Index

- `constants.ts`: RS-06 profile/version constants.
- `types.ts`: indexer input/output model types.
- `env.ts`: environment parsing helpers, including source cursor mode
  (`mixed|v2_primary`) controls.
- `source-cursor.ts`: cursor-state v3 parsing/serialization helpers with
  legacy plain-string and v2 JSON cursor migration plus replay defaults.
- `metadata.ts`: metadata allowlist validation and normalization.
- `rec-manifest-source.ts`: object-store-backed REC manifest source adapter
  with v1/v2 manifest layout parsing, cursor paging, retries, extended
  manifest-key scanning, mixed/v2-primary replay behavior, and allowlisted
  metadata extraction.
- `rec-manifest-object-store-client.ts`: S3 client wrapper for REC manifest
  list/read operations.
- `runtime.ts`: runtime bootstrap wiring for production REC source with
  fail-closed source configuration and source cursor mode wiring.
- `store.ts`: sidecar persistence interface plus in-memory/Postgres durable
  implementations aligned to restore-plane relational tables including
  `source_progress` checkpoints and source-scope leader leases.
- `freshness.ts`: freshness/executability state evaluation.
- `indexer.service.ts`: indexing core with generation-bound watermark logic.
- `worker.ts`: cursor-aware source polling loop with continuous runtime
  controls, source-scope leader lease gating, fail-closed cursor progression,
  v2 shard-progress (`topic|partition`) cursor tracking, source progress
  checkpointing, and per-batch v1/v2 cutover observability logs.
- `backfill.ts`: bootstrap and gap-repair controller with fail-closed pause
  semantics when indexing failures occur.
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
  retry, duplicate-suppression, parsing, and replay fallback tests.
- `source-cursor.test.ts`: cursor-state v3 migration/serialization tests.
