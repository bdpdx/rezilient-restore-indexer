# rezilient-restore-indexer

Restore index sidecar for RS-06.

Current stage behavior:
- Ingests normalized REC artifact metadata.
- Persists allowlisted index rows and runtime state in durable SQLite
  snapshots.
- Maintains generation-bound partition watermarks.
- Reports source coverage windows.
- Tracks per-source progress checkpoints for restart-safe cursor resume.
- Evaluates strict fail-closed freshness states.
- Runs bootstrap/gap-repair backfill controllers with lag guardrails.
- Runs as a continuous worker service with graceful shutdown.
- Includes RS-15 incident runbooks at `doc/runbooks.md`.

Run local tests:
```bash
NPM_CONFIG_CACHE=/tmp/rez-npm-cache npm test
```
