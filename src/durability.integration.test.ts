import assert from 'node:assert/strict';
import { test } from 'node:test';
import { newDb } from 'pg-mem';
import {
    BackfillController,
    InMemoryBackfillBatchSource,
} from './backfill';
import { RestoreIndexerService } from './indexer.service';
import { PostgresRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import {
    parseSourceCursorState,
    serializeSourceCursorState,
    SOURCE_CURSOR_VERSION,
} from './source-cursor';
import {
    type ArtifactBatchSource,
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

type Fixture = {
    close: () => Promise<void>;
    indexer: RestoreIndexerService;
    store: PostgresRestoreIndexStore;
};

function createFixture(
    db: ReturnType<typeof newDb>,
): Fixture {
    const pgAdapter = db.adapters.createPg();
    const pool = new pgAdapter.Pool();
    const store = new PostgresRestoreIndexStore('postgres://unused', {
        pool: pool as any,
    });
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });

    return {
        close: async () => {
            await pool.end();
        },
        indexer,
        store,
    };
}

test('restart preserves watermark, coverage, backfill, and source progress',
async () => {
    const db = newDb();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const first = createFixture(db);
    let restarted: Fixture | null = null;

    try {
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

        restarted = createFixture(db);

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
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }
    }
});

test('restart preserves large offset watermark and source progress',
async () => {
    const db = newDb();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const partitionScope = {
        ...scope,
        partition: 0,
        topic: 'rez.cdc',
    };
    const secondLargeOffset = '9007199254740995';
    const thirdLargeOffset = '9007199254740996';
    const first = createFixture(db);
    let restarted: Fixture | null = null;

    try {
        const source = new InMemoryArtifactBatchSource([
            buildTestInput({
                eventId: 'evt-large-durable-1',
                generationId: 'gen-large',
                offset: '9007199254740994',
            }),
            buildTestInput({
                eventId: 'evt-large-durable-2',
                generationId: 'gen-large',
                offset: secondLargeOffset,
            }),
        ], 5);
        const worker = new RestoreIndexerWorker(
            source,
            first.indexer,
            2,
            {
                pollIntervalMs: 1,
                sourceProgressScope: scope,
                timeProvider: () => '2026-02-18T10:10:00.000Z',
            },
        );

        const firstRun = await worker.runOnce();

        assert.equal(firstRun.inserted, 2);

        const progressBeforeRestart = await first.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        const watermarkBeforeRestart =
            await first.indexer.getPartitionWatermarkStatus(
                partitionScope,
                {
                    now: '2026-02-18T10:11:00.000Z',
                },
            );

        assert.equal(
            progressBeforeRestart?.last_indexed_offset,
            secondLargeOffset,
        );
        assert.equal(
            watermarkBeforeRestart.watermark?.indexed_through_offset,
            secondLargeOffset,
        );

        restarted = createFixture(db);

        const progressAfterRestart = await restarted.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        const watermarkAfterRestart =
            await restarted.indexer.getPartitionWatermarkStatus(
                partitionScope,
                {
                    now: '2026-02-18T10:12:00.000Z',
                },
            );

        assert.equal(
            progressAfterRestart?.last_indexed_offset,
            secondLargeOffset,
        );
        assert.equal(
            watermarkAfterRestart.watermark?.indexed_through_offset,
            secondLargeOffset,
        );

        const resumedWorker = new RestoreIndexerWorker(
            new InMemoryArtifactBatchSource([
                buildTestInput({
                    eventId: 'evt-large-durable-1',
                    generationId: 'gen-large',
                    offset: '9007199254740994',
                }),
                buildTestInput({
                    eventId: 'evt-large-durable-2',
                    generationId: 'gen-large',
                    offset: secondLargeOffset,
                }),
                buildTestInput({
                    eventId: 'evt-large-durable-3',
                    generationId: 'gen-large',
                    offset: thirdLargeOffset,
                }),
            ], 5),
            restarted.indexer,
            2,
            {
                pollIntervalMs: 1,
                sourceProgressScope: scope,
                timeProvider: () => '2026-02-18T10:13:00.000Z',
            },
        );
        const resumedRun = await resumedWorker.runOnce();

        assert.equal(resumedRun.inserted, 1);

        const progressAfterResume = await restarted.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        const watermarkAfterResume =
            await restarted.indexer.getPartitionWatermarkStatus(
                partitionScope,
                {
                    now: '2026-02-18T10:14:00.000Z',
                },
            );

        assert.equal(progressAfterResume?.last_indexed_offset, thirdLargeOffset);
        assert.equal(
            watermarkAfterResume.watermark?.indexed_through_offset,
            thirdLargeOffset,
        );
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }
    }
});

test('restart preserves v2 replay cursor state across worker continuity',
async () => {
    const db = newDb();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const first = createFixture(db);
    let restarted: Fixture | null = null;
    const firstNextCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T13:00:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/900.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const resumedNextCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T13:05:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/900.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });

    try {
        const firstSource: ArtifactBatchSource = {
            async readBatch() {
                return {
                    items: [buildTestInput({
                        eventId: 'evt-durable-replay-1',
                        offset: 200,
                    })],
                    nextCursor: firstNextCursor,
                    realtimeLagSeconds: 25,
                    scanCounters: {
                        fastPathSelectedKeyCount: 1,
                        replayCycleRan: false,
                        replayOnlyHitCount: 0,
                        replayPathSelectedKeyCount: 0,
                    },
                };
            },
        };
        const worker = new RestoreIndexerWorker(
            firstSource,
            first.indexer,
            10,
            {
                sourceProgressScope: scope,
                timeProvider: () => '2026-02-18T13:00:30.000Z',
            },
        );
        const firstRun = await worker.runOnce();

        assert.equal(firstRun.inserted, 1);
        assert.equal(firstRun.cursor, firstNextCursor);

        restarted = createFixture(db);

        let resumedCursor: string | null = null;
        const resumedSource: ArtifactBatchSource = {
            async readBatch(input) {
                resumedCursor = input.cursor;

                return {
                    items: [buildTestInput({
                        eventId: 'evt-durable-replay-2',
                        offset: 201,
                    })],
                    nextCursor: resumedNextCursor,
                    realtimeLagSeconds: 10,
                    scanCounters: {
                        fastPathSelectedKeyCount: 0,
                        replayCycleRan: true,
                        replayOnlyHitCount: 1,
                        replayPathSelectedKeyCount: 1,
                    },
                };
            },
        };
        const resumedWorker = new RestoreIndexerWorker(
            resumedSource,
            restarted.indexer,
            10,
            {
                sourceProgressScope: scope,
                timeProvider: () => '2026-02-18T13:05:30.000Z',
            },
        );
        const resumedRun = await resumedWorker.runOnce();

        assert.equal(resumedRun.inserted, 1);
        assert.equal(resumedCursor, firstNextCursor);

        const progress = await restarted.indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );

        assert.equal(progress?.cursor, resumedNextCursor);
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
            '2026-02-18T13:05:00.000Z',
        );
    } finally {
        await first.close();

        if (restarted) {
            await restarted.close();
        }
    }
});

test('postgres store source leader lease enforces single active holder',
async () => {
    const db = newDb();
    const fixture = createFixture(db);
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };

    try {
        const first = await fixture.store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: scope.instanceId,
            leaseDurationSeconds: 30,
            source: scope.source,
            tenantId: scope.tenantId,
        });
        const second = await fixture.store.acquireSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: scope.instanceId,
            leaseDurationSeconds: 30,
            source: scope.source,
            tenantId: scope.tenantId,
        });

        assert.equal(first, true);
        assert.equal(second, false);

        await fixture.store.releaseSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: scope.instanceId,
            source: scope.source,
            tenantId: scope.tenantId,
        });

        const recovered = await fixture.store.acquireSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: scope.instanceId,
            leaseDurationSeconds: 30,
            source: scope.source,
            tenantId: scope.tenantId,
        });

        assert.equal(recovered, true);
    } finally {
        await fixture.close();
    }
});

test('postgres store watermark lookup ignores source filter in scope',
async () => {
    const db = newDb();
    const fixture = createFixture(db);

    try {
        const worker = new RestoreIndexerWorker(
            new InMemoryArtifactBatchSource([
                buildTestInput({
                    eventId: 'evt-source-pg-1',
                    generationId: 'gen-source-pg',
                    offset: 7,
                    source: 'sn://pg-source-a.service-now.com',
                }),
            ], 0),
            fixture.indexer,
            10,
            {
                pollIntervalMs: 1,
            },
        );

        const run = await worker.runOnce();

        assert.equal(run.inserted, 1);

        const withOriginalSource =
            await fixture.indexer.getPartitionWatermarkStatus({
                instanceId: 'sn-dev-01',
                partition: 0,
                source: 'sn://pg-source-a.service-now.com',
                tenantId: 'tenant-acme',
                topic: 'rez.cdc',
            }, {
                now: '2026-02-18T10:05:00.000Z',
            });

        assert.notEqual(withOriginalSource.watermark, null);
        assert.equal(
            withOriginalSource.watermark?.indexed_through_offset,
            '7',
        );

        const withDifferentSource =
            await fixture.indexer.getPartitionWatermarkStatus({
                instanceId: 'sn-dev-01',
                partition: 0,
                source: 'sn://pg-source-b.service-now.com',
                tenantId: 'tenant-acme',
                topic: 'rez.cdc',
            }, {
                now: '2026-02-18T10:05:00.000Z',
            });

        assert.notEqual(withDifferentSource.watermark, null);
        assert.equal(
            withDifferentSource.watermark?.indexed_through_offset,
            '7',
        );
    } finally {
        await fixture.close();
    }
});
