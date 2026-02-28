import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreIndexerService } from './indexer.service';
import {
    parseSourceCursorState,
    serializeSourceCursorState,
    SOURCE_CURSOR_VERSION,
} from './source-cursor';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import {
    type ArtifactBatchSource,
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

    assert.equal(watermark.watermark?.indexed_through_offset, '2');

    const progress = await fixture.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(progress, null);
    assert.equal(progress?.processed_count, 3);
    assert.equal(progress?.last_lag_seconds, 420);
});

test('continuous loop advances with replay-only batches and v2 cursor state',
async () => {
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const fixture = createFixture();
    const afterFastCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T11:20:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/900.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const afterReplayCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T11:21:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/900.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const observedCursors: Array<string | null> = [];
    let readCalls = 0;
    const source: ArtifactBatchSource = {
        async readBatch(input) {
            observedCursors.push(input.cursor);
            readCalls += 1;

            if (readCalls === 1) {
                return {
                    items: [buildTestInput({
                        eventId: 'evt-loop-replay-fast-1',
                        offset: 300,
                    })],
                    nextCursor: afterFastCursor,
                    realtimeLagSeconds: 90,
                    scanCounters: {
                        fastPathSelectedKeyCount: 1,
                        replayCycleRan: false,
                        replayOnlyHitCount: 0,
                        replayPathSelectedKeyCount: 0,
                    },
                };
            }

            return {
                items: [buildTestInput({
                    eventId: 'evt-loop-replay-hit-1',
                    offset: 301,
                })],
                nextCursor: afterReplayCursor,
                realtimeLagSeconds: 30,
                scanCounters: {
                    fastPathSelectedKeyCount: 0,
                    replayCycleRan: true,
                    replayOnlyHitCount: 1,
                    replayPathSelectedKeyCount: 1,
                },
            };
        },
    };
    const worker = new RestoreIndexerWorker(
        source,
        fixture.indexer,
        1,
        {
            pollIntervalMs: 1,
            sleep: async () => Promise.resolve(),
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T11:21:30.000Z',
        },
    );
    const summary = await worker.runContinuously({
        maxCycles: 2,
    });

    assert.equal(summary.cycles, 2);
    assert.equal(summary.inserted, 2);
    assert.equal(summary.failures, 0);
    assert.deepEqual(observedCursors, [null, afterFastCursor]);

    const progress = await fixture.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(progress, null);
    assert.equal(progress?.cursor, afterReplayCursor);
    assert.equal(progress?.processed_count, 2);

    const cursorState = parseSourceCursorState(progress?.cursor || null, {
        enabled: true,
        lowerBound: 'rez/restore-artifacts',
    });

    assert.equal(
        cursorState.scan_cursor,
        'rez/restore-artifacts/kind=schema/900.manifest.json',
    );
    assert.equal(
        cursorState.replay.last_replay_at,
        '2026-02-18T11:21:00.000Z',
    );
});

test('continuous loop normalizes source progress event time to millis',
async () => {
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const fixture = createFixture();
    const source = new InMemoryArtifactBatchSource([
        buildTestInput({
            eventId: 'evt-loop-seconds-1',
            eventTime: '2026-02-18T11:04:05Z',
            metadata: {
                __time: undefined,
            },
            offset: 1,
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
            timeProvider: () => '2026-02-18T11:05:00Z',
        },
    );

    const result = await worker.runOnce();

    assert.equal(result.batchSize, 1);
    assert.equal(result.inserted, 1);
    assert.equal(result.failures, 0);

    const progress = await fixture.indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.notEqual(progress, null);
    assert.equal(
        progress?.last_indexed_event_time,
        '2026-02-18T11:04:05.000Z',
    );
    assert.equal(progress?.updated_at, '2026-02-18T11:05:00.000Z');
});

test('continuous loop does not double-count processed items on retry',
async () => {
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const fixture = createFixture();
    const validItem = buildTestInput({
        eventId: 'evt-loop-retry-ok',
        offset: 1,
    });
    const invalidItem = buildTestInput({
        eventId: 'evt-loop-retry-bad',
        metadata: {
            short_description: 'plaintext not allowed',
        },
        offset: 2,
    });
    const source = new InMemoryArtifactBatchSource(
        [validItem, invalidItem],
        0,
    );
    const worker = new RestoreIndexerWorker(
        source,
        fixture.indexer,
        10,
        {
            pollIntervalMs: 1,
            sleep: async () => Promise.resolve(),
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T11:10:00.000Z',
        },
    );

    await worker.runOnce();

    const progressAfterFirstAttempt =
        await fixture.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );

    assert.equal(progressAfterFirstAttempt?.processed_count, 1);

    await worker.runOnce();

    const progressAfterSecondAttempt =
        await fixture.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );

    assert.equal(progressAfterSecondAttempt?.processed_count, 2);
});
