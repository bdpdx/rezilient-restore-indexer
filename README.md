# rezilient-restore-indexer

Restore index sidecar for RS-06.

Current stage behavior:
- Ingests normalized REC artifact metadata.
- Persists allowlisted index rows.
- Maintains generation-bound partition watermarks.
- Reports source coverage windows.
- Evaluates strict fail-closed freshness states.
- Runs bootstrap/gap-repair backfill controllers with lag guardrails.

Run local tests:
```bash
NPM_CONFIG_CACHE=/tmp/rez-npm-cache npm test
```
# rezilient-restore-indexer
