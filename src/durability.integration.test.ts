import assert from 'node:assert/strict';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { test } from 'node:test';
import {
    BackfillController,
    InMemoryBackfillBatchSource,
} from './backfill';
import { RestoreIndexerService } from './indexer.service';
import { SqliteRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

function createTempDbPath(name: string): string {
    const directory = mkdtempSync(join(tmpdir(), 'rri-stage12-'));

    return join(directory, `${name}.sqlite`);
}

function createFixture(dbPath: string): {
    indexer: RestoreIndexerService;
    store: SqliteRestoreIndexStore;
} {
    const store = new SqliteRestoreIndexStore(dbPath);
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });

    return {
        indexer,
        store,
    };
}

test('restart preserves watermark, coverage, backfill, and source progress',
async () => {
    const dbPath = createTempDbPath('restart-state');
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };

    const first = createFixture(dbPath);
    const initialItems = [
        buildTestInput({
            eventId: 'evt-stage12-r1',
            generationId: 'gen-01',
            offset: 1,
        }),
        buildTestInput({
            eventId: 'evt-stage12-r2',
            generationId: 'gen-01',
            offset: 2,
        }),
        buildTestInput({
            eventId: 'evt-stage12-r3',
            generationId: 'gen-01',
            offset: 3,
        }),
    ];
    const source = new InMemoryArtifactBatchSource(initialItems, 12);
    const worker = new RestoreIndexerWorker(
        source,
        first.indexer,
        2,
        {
            pollIntervalMs: 1,
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T10:00:00.000Z',
        },
    );

    const firstRun = await worker.runOnce();

    assert.equal(firstRun.batchSize, 2);
    assert.equal(firstRun.inserted, 2);

    const backfill = new BackfillController(
        'bootstrap',
        new InMemoryBackfillBatchSource([
            buildTestInput({
                eventId: 'evt-stage12-backfill-1',
                eventTime: '2026-02-18T10:01:00.000Z',
                generationId: 'gen-02',
                ingestionMode: 'bootstrap',
                offset: 1,
            }),
        ], 0),
        first.indexer,
        first.store,
        {
            maxRealtimeLagSeconds: 180,
            runId: 'run-stage12-bootstrap',
            throttleBatchSize: 10,
            timeProvider: () => '2026-02-18T10:01:00.000Z',
        },
    );
    const backfillState = await backfill.tick();

    assert.equal(backfillState.status, 'completed');
    assert.equal(backfillState.processedCount, 1);

    const restarted = createFixture(dbPath);
    const coverage = await restarted.indexer.getSourceCoverageWindow(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );
    const backfillAfterRestart = await restarted.store.getBackfillRun(
        'run-stage12-bootstrap',
    );
    const progressAfterRestart = await restarted.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(coverage, null);
    assert.equal(coverage?.generation_span['rez.cdc:0'], 'gen-02');
    assert.notEqual(backfillAfterRestart, null);
    assert.equal(backfillAfterRestart?.status, 'completed');
    assert.equal(backfillAfterRestart?.processedCount, 1);
    assert.notEqual(progressAfterRestart, null);
    assert.equal(progressAfterRestart?.cursor, '2');
    assert.equal(progressAfterRestart?.processed_count, 2);

    const resumedWorker = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource(initialItems, 4),
        restarted.indexer,
        2,
        {
            pollIntervalMs: 1,
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T10:03:00.000Z',
        },
    );
    const resumedRun = await resumedWorker.runOnce();

    assert.equal(resumedRun.batchSize, 1);
    assert.equal(resumedRun.inserted, 1);

    const progressAfterResume = await restarted.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.equal(progressAfterResume?.cursor, '3');
    assert.equal(progressAfterResume?.processed_count, 3);

    const resumedBackfill = new BackfillController(
        'bootstrap',
        new InMemoryBackfillBatchSource([], 0),
        restarted.indexer,
        restarted.store,
        {
            maxRealtimeLagSeconds: 180,
            runId: 'run-stage12-bootstrap',
            throttleBatchSize: 10,
            timeProvider: () => '2026-02-18T10:04:00.000Z',
        },
    );
    const resumedBackfillState = await resumedBackfill.tick();

    assert.equal(resumedBackfillState.status, 'completed');
    assert.equal(resumedBackfillState.processedCount, 1);
});
