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

test('worker polls source and writes indexed metadata batches', async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const source = new InMemoryArtifactBatchSource([
        buildTestInput({
            eventId: 'evt-worker-1',
            offset: 1,
        }),
        buildTestInput({
            eventId: 'evt-worker-2',
            offset: 2,
        }),
    ]);
    const worker = new RestoreIndexerWorker(source, indexer, 10);

    const first = await worker.runOnce();

    assert.equal(first.batchSize, 2);
    assert.equal(first.inserted, 2);
    assert.equal(first.failures, 0);

    const second = await worker.runOnce();

    assert.equal(second.batchSize, 0);

    worker.pause();
    source.enqueue([buildTestInput({
        eventId: 'evt-worker-3',
        offset: 3,
    })]);

    const paused = await worker.runOnce();

    assert.equal(paused.batchSize, 0);
    assert.equal(store.getIndexedEventCount(), 2);

    worker.resume();

    const resumed = await worker.runOnce();

    assert.equal(resumed.batchSize, 1);
    assert.equal(store.getIndexedEventCount(), 3);
});

test('worker keeps cursor pinned when a batch includes failures', async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const valid = buildTestInput({
        eventId: 'evt-worker-failure-ok',
        offset: 1,
    });
    const invalid = buildTestInput({
        eventId: 'evt-worker-failure-bad',
        offset: 2,
        metadata: {
            short_description: 'plaintext not allowed',
        },
    });
    const worker = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([valid, invalid]),
        indexer,
        10,
    );

    const first = await worker.runOnce();

    assert.equal(first.batchSize, 2);
    assert.equal(first.inserted, 1);
    assert.equal(first.failures, 1);
    assert.equal(first.cursor, null);
    assert.equal(store.getIndexedEventCount(), 1);

    const recoveryWorker = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([
            valid,
            buildTestInput({
                eventId: 'evt-worker-failure-bad',
                offset: 2,
            }),
        ]),
        indexer,
        10,
    );
    const recovered = await recoveryWorker.runOnce();

    assert.equal(recovered.batchSize, 2);
    assert.equal(recovered.inserted, 1);
    assert.equal(recovered.existing, 1);
    assert.equal(recovered.failures, 0);
    assert.equal(recovered.cursor, '2');
    assert.equal(store.getIndexedEventCount(), 2);
});

test('worker fails closed when canonical tenant identity is missing',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const invalid = buildTestInput({
        eventId: 'evt-worker-no-tenant',
        offset: 1,
        tenantId: 'tenant-from-config',
        metadata: {
            tenant_id: undefined,
        },
    });

    invalid.manifest.tenant_id = undefined;

    const worker = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([invalid]),
        indexer,
        10,
    );
    const originalError = console.error;

    console.error = () => {};

    try {
        const result = await worker.runOnce();

        assert.equal(result.batchSize, 1);
        assert.equal(result.inserted, 0);
        assert.equal(result.failures, 1);
        assert.equal(result.cursor, null);
        assert.equal(store.getIndexedEventCount(), 0);
    } finally {
        console.error = originalError;
    }
});

test('worker processes batches only while holding source leader lease',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const workerA = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([
            buildTestInput({
                eventId: 'evt-worker-lease-a',
                offset: 1,
            }),
        ]),
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-a',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
        },
    );
    const workerB = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([
            buildTestInput({
                eventId: 'evt-worker-lease-a',
                offset: 1,
            }),
            buildTestInput({
                eventId: 'evt-worker-lease-b',
                offset: 2,
            }),
        ]),
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-b',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
        },
    );

    const first = await workerA.runOnce();

    assert.equal(first.batchSize, 1);
    assert.equal(first.inserted, 1);

    const blocked = await workerB.runOnce();

    assert.equal(blocked.batchSize, 0);
    assert.equal(blocked.inserted, 0);

    await store.releaseSourceLeaderLease({
        holderId: 'holder-a',
        instanceId: scope.instanceId,
        source: scope.source,
        tenantId: scope.tenantId,
    });

    const recovered = await workerB.runOnce();

    assert.equal(recovered.batchSize, 1);
    assert.equal(recovered.inserted, 1);
    assert.equal(store.getIndexedEventCount(), 2);
});

test('continuous worker run releases source leader lease on shutdown',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const workerA = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([
            buildTestInput({
                eventId: 'evt-worker-release-a',
                offset: 1,
            }),
        ]),
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-a',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
        },
    );

    const summary = await workerA.runContinuously({
        maxCycles: 1,
    });

    assert.equal(summary.inserted, 1);

    const workerB = new RestoreIndexerWorker(
        new InMemoryArtifactBatchSource([
            buildTestInput({
                eventId: 'evt-worker-release-a',
                offset: 1,
            }),
            buildTestInput({
                eventId: 'evt-worker-release-b',
                offset: 2,
            }),
        ]),
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-b',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
        },
    );

    const recovered = await workerB.runOnce();

    assert.equal(recovered.batchSize, 1);
    assert.equal(recovered.inserted, 1);
});

test('worker restart loads persisted v2 cursor payload from source progress',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const persistedCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T12:00:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/300.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const nextCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T12:01:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/320.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });

    await indexer.recordSourceProgress({
        cursor: persistedCursor,
        instanceId: scope.instanceId,
        lastBatchSize: 0,
        lastIndexedEventTime: null,
        lastIndexedOffset: null,
        lastLagSeconds: null,
        measuredAt: '2026-02-18T11:59:00.000Z',
        processedDelta: 0,
        source: scope.source,
        tenantId: scope.tenantId,
    });

    let receivedCursor: string | null = null;
    const source: ArtifactBatchSource = {
        async readBatch(input) {
            receivedCursor = input.cursor;

            return {
                items: [buildTestInput({
                    eventId: 'evt-worker-v2-restart',
                    offset: 11,
                })],
                nextCursor,
                realtimeLagSeconds: 15,
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
        source,
        indexer,
        10,
        {
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T12:01:30.000Z',
        },
    );
    const run = await worker.runOnce();

    assert.equal(run.inserted, 1);
    assert.equal(receivedCursor, persistedCursor);
    assert.equal(run.cursor, nextCursor);

    const progress = await indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.equal(progress?.cursor, nextCursor);
});

test('leader handoff resumes from latest persisted v2 cursor payload',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const scope = {
        instanceId: 'sn-dev-01',
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
    };
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const initialCursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T12:10:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/400.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const cursorAfterWorkerA = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T12:11:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/420.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });
    const cursorAfterWorkerB = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: '2026-02-18T12:12:00.000Z',
            lower_bound: 'rez/restore-artifacts',
        },
        scan_cursor: 'rez/restore-artifacts/kind=schema/420.manifest.json',
        v: SOURCE_CURSOR_VERSION,
    });

    await indexer.recordSourceProgress({
        cursor: initialCursor,
        instanceId: scope.instanceId,
        lastBatchSize: 0,
        lastIndexedEventTime: null,
        lastIndexedOffset: null,
        lastLagSeconds: null,
        measuredAt: '2026-02-18T12:09:59.000Z',
        processedDelta: 0,
        source: scope.source,
        tenantId: scope.tenantId,
    });

    let workerACursor: string | null = null;
    let workerBReadCalls = 0;
    let workerBCursor: string | null = null;
    const sourceA: ArtifactBatchSource = {
        async readBatch(input) {
            workerACursor = input.cursor;

            return {
                items: [buildTestInput({
                    eventId: 'evt-worker-handoff-a',
                    offset: 21,
                })],
                nextCursor: cursorAfterWorkerA,
                realtimeLagSeconds: 5,
                scanCounters: {
                    fastPathSelectedKeyCount: 1,
                    replayCycleRan: false,
                    replayOnlyHitCount: 0,
                    replayPathSelectedKeyCount: 0,
                },
            };
        },
    };
    const sourceB: ArtifactBatchSource = {
        async readBatch(input) {
            workerBReadCalls += 1;
            workerBCursor = input.cursor;

            return {
                items: [buildTestInput({
                    eventId: 'evt-worker-handoff-b',
                    offset: 22,
                })],
                nextCursor: cursorAfterWorkerB,
                realtimeLagSeconds: 6,
                scanCounters: {
                    fastPathSelectedKeyCount: 0,
                    replayCycleRan: true,
                    replayOnlyHitCount: 1,
                    replayPathSelectedKeyCount: 1,
                },
            };
        },
    };
    const workerA = new RestoreIndexerWorker(
        sourceA,
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-a',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T12:11:30.000Z',
        },
    );
    const workerB = new RestoreIndexerWorker(
        sourceB,
        indexer,
        10,
        {
            leaderLease: {
                holderId: 'holder-b',
                leaseDurationSeconds: 30,
                manager: store,
            },
            sourceProgressScope: scope,
            timeProvider: () => '2026-02-18T12:12:30.000Z',
        },
    );

    const first = await workerA.runOnce();

    assert.equal(first.inserted, 1);
    assert.equal(workerACursor, initialCursor);

    const blocked = await workerB.runOnce();

    assert.equal(blocked.batchSize, 0);
    assert.equal(workerBReadCalls, 0);

    await store.releaseSourceLeaderLease({
        holderId: 'holder-a',
        instanceId: scope.instanceId,
        source: scope.source,
        tenantId: scope.tenantId,
    });

    const handedOff = await workerB.runOnce();

    assert.equal(handedOff.inserted, 1);
    assert.equal(workerBReadCalls, 1);
    assert.equal(workerBCursor, cursorAfterWorkerA);

    const progress = await indexer.getSourceProgress(
        scope.tenantId,
        scope.instanceId,
        scope.source,
    );

    assert.equal(progress?.cursor, cursorAfterWorkerB);
    const parsed = parseSourceCursorState(progress?.cursor || null, {
        enabled: true,
        lowerBound: 'rez/restore-artifacts',
    });

    assert.equal(
        parsed.scan_cursor,
        'rez/restore-artifacts/kind=schema/420.manifest.json',
    );
    assert.equal(
        parsed.replay.last_replay_at,
        '2026-02-18T12:12:00.000Z',
    );
});

test('worker logs replay scan counters for per-batch observability',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const source: ArtifactBatchSource = {
        async readBatch() {
            return {
                items: [buildTestInput({
                    eventId: 'evt-worker-observability',
                    offset: 31,
                })],
                nextCursor: '1',
                realtimeLagSeconds: 9,
                scanCounters: {
                    fastPathSelectedKeyCount: 2,
                    replayCycleRan: true,
                    replayOnlyHitCount: 1,
                    replayPathSelectedKeyCount: 1,
                },
            };
        },
    };
    const worker = new RestoreIndexerWorker(source, indexer, 10);
    const originalLog = console.log;
    const observedLogs: unknown[][] = [];

    console.log = (...args: unknown[]) => {
        observedLogs.push(args);
    };

    try {
        await worker.runOnce();
    } finally {
        console.log = originalLog;
    }

    const batchLog = observedLogs.find((entry) => {
        return entry[0] === 'restore-indexer batch processed';
    });

    assert.notEqual(batchLog, undefined);
    const payload = batchLog?.[1] as Record<string, unknown>;

    assert.equal(payload.fast_path_selected_key_count, 2);
    assert.equal(payload.replay_path_selected_key_count, 1);
    assert.equal(payload.replay_only_hit_count, 1);
    assert.equal(payload.replay_cycle_ran, true);
});
