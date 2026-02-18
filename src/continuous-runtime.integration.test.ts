import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

function createFixture() {
    const store = new InMemoryRestoreIndexStore();
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

test('continuous loop processes steady-state batches until idle', async () => {
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const fixture = createFixture();
    const source = new InMemoryArtifactBatchSource([
        buildTestInput({
            eventId: 'evt-loop-steady-1',
            offset: 1,
        }),
        buildTestInput({
            eventId: 'evt-loop-steady-2',
            offset: 2,
        }),
        buildTestInput({
            eventId: 'evt-loop-steady-3',
            offset: 3,
        }),
    ], 0);
    const worker = new RestoreIndexerWorker(
        source,
        fixture.indexer,
        2,
        {
            pollIntervalMs: 1,
            sleep: async () => Promise.resolve(),
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T11:00:00.000Z',
        },
    );
    const summary = await worker.runContinuously({
        maxCycles: 3,
    });

    assert.equal(summary.cycles, 3);
    assert.equal(summary.inserted, 3);
    assert.equal(summary.existing, 0);
    assert.equal(summary.failures, 0);
    assert.equal(summary.emptyBatches, 1);
    assert.equal(fixture.store.getIndexedEventCount(), 3);

    const progress = await fixture.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(progress, null);
    assert.equal(progress?.cursor, '3');
    assert.equal(progress?.processed_count, 3);
    assert.equal(progress?.last_batch_size, 0);
});

test('continuous loop tracks lag and replayed events without rewinds',
async () => {
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const fixture = createFixture();
    const source = new InMemoryArtifactBatchSource([
        buildTestInput({
            eventId: 'evt-loop-lag-1',
            generationId: 'gen-loop',
            offset: 1,
        }),
        buildTestInput({
            eventId: 'evt-loop-lag-2',
            generationId: 'gen-loop',
            offset: 2,
        }),
        buildTestInput({
            eventId: 'evt-loop-lag-2',
            generationId: 'gen-loop',
            offset: 2,
        }),
    ], 420);
    const worker = new RestoreIndexerWorker(
        source,
        fixture.indexer,
        2,
        {
            pollIntervalMs: 1,
            sleep: async () => Promise.resolve(),
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T11:05:00.000Z',
        },
    );
    const summary = await worker.runContinuously({
        maxCycles: 2,
    });

    assert.equal(summary.cycles, 2);
    assert.equal(summary.inserted, 2);
    assert.equal(summary.existing, 1);
    assert.equal(summary.failures, 0);

    const watermark = await fixture.indexer.getPartitionWatermarkStatus({
        instanceId: scope.instanceId,
        partition: 0,
        source: scope.source,
        tenantId: scope.tenantId,
        topic: 'rez.cdc',
    }, {
        now: '2026-02-18T11:06:00.000Z',
    });

    assert.equal(watermark.watermark?.indexed_through_offset, 2);

    const progress = await fixture.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(progress, null);
    assert.equal(progress?.processed_count, 3);
    assert.equal(progress?.last_lag_seconds, 420);
});
