# src Index

- `constants.ts`: RS-06 profile/version constants.
- `types.ts`: indexer input/output model types.
- `env.ts`: environment parsing helpers.
- `metadata.ts`: metadata allowlist validation and normalization.
- `store.ts`: sidecar persistence interface and in-memory implementation.
- `freshness.ts`: freshness/executability state evaluation.
- `indexer.service.ts`: indexing core with generation-bound watermark logic.
- `worker.ts`: source polling read/process/write loop.
- `backfill.ts`: bootstrap and gap-repair controller.
- `index.ts`: local bootstrap.
- `test-helpers.ts`: deterministic fixtures.
- `watermark-invariants.test.ts`: monotonicity unit tests.
- `generation-replay.integration.test.ts`: replay/rewind generation tests.
- `worker.integration.test.ts`: sidecar worker loop tests.
- `failure-modes.test.ts`: stale gate timeout and backfill starvation tests.
