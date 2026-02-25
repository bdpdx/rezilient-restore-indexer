import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { RestoreIndexerService } from './indexer.service.js';
import { InMemoryRestoreIndexStore } from './store.js';
import { buildTestInput } from './test-helpers.js';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
    type SourceLeaderLeaseOptions,
    type SourceProgressScope,
} from './worker.js';

const FIXED_TIME = '2026-02-16T12:00:00.000Z';

function createWorkerFixture(overrides?: {
    batchSize?: number;
    items?: ReturnType<typeof buildTestInput>[];
    lagSeconds?: number | null;
    leaderLease?: SourceLeaderLeaseOptions;
    pollIntervalMs?: number;
    sourceProgressScope?: SourceProgressScope;
}) {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const items = overrides?.items ?? [
        buildTestInput({ eventId: 'evt-w-1', offset: 1 }),
        buildTestInput({ eventId: 'evt-w-2', offset: 2 }),
    ];
    const source = new InMemoryArtifactBatchSource(
        items,
        overrides?.lagSeconds ?? 5,
    );
    const sleepCalls: number[] = [];
    const worker = new RestoreIndexerWorker(
        source,
        indexer,
        overrides?.batchSize ?? 10,
        {
            leaderLease: overrides?.leaderLease,
            pollIntervalMs: overrides?.pollIntervalMs ?? 100,
            sleep: async (ms: number) => {
                sleepCalls.push(ms);
            },
            sourceProgressScope: overrides?.sourceProgressScope,
            timeProvider: () => FIXED_TIME,
        },
    );

    return { indexer, sleepCalls, source, store, worker };
}

const defaultScope: SourceProgressScope = {
    instanceId: 'sn-dev-01',
    source: 'sn://acme-dev.service-now.com',
    tenantId: 'tenant-acme',
};

function createLeaseManager(grants = true) {
    let leased = false;
    const released: boolean[] = [];

    return {
        lease: {
            holderId: 'holder-test',
            leaseDurationSeconds: 30,
            manager: {
                acquireSourceLeaderLease: async () => {
                    leased = grants;
                    return grants;
                },
                releaseSourceLeaderLease: async () => {
                    leased = false;
                    released.push(true);
                },
            },
        } satisfies SourceLeaderLeaseOptions,
        released,
        get isLeased() {
            return leased;
        },
    };
}

describe('RestoreIndexerWorker', () => {
    it('pause sets paused flag', async () => {
        const { worker } = createWorkerFixture();
        worker.pause();
        const result = await worker.runOnce();
        assert.equal(result.batchSize, 0);
    });

    it('resume clears paused flag', async () => {
        const { worker } = createWorkerFixture();
        worker.pause();
        worker.resume();
        const result = await worker.runOnce();
        assert.ok(result.batchSize > 0);
    });

    it('requestStop sets stopRequested flag', async () => {
        const { worker } = createWorkerFixture();
        worker.requestStop();
        const summary = await worker.runContinuously();
        assert.equal(summary.cycles, 0);
    });

    it('runOnce returns skipped=true when paused', async () => {
        const { worker } = createWorkerFixture();
        worker.pause();
        const result = await worker.runOnce();
        assert.equal(result.batchSize, 0);
        assert.equal(result.inserted, 0);
        assert.equal(result.existing, 0);
        assert.equal(result.failures, 0);
    });

    it('runOnce returns skipped when no lease', async () => {
        const leaseMgr = createLeaseManager(false);
        const originalLog = console.log;

        console.log = () => {};

        try {
            const { worker } = createWorkerFixture({
                leaderLease: leaseMgr.lease,
                sourceProgressScope: defaultScope,
            });
            const result = await worker.runOnce();
            assert.equal(result.batchSize, 0);
        } finally {
            console.log = originalLog;
        }
    });

    it('runOnce loads cursor from persisted progress on first run',
    async () => {
        const scope = defaultScope;
        const store = new InMemoryRestoreIndexStore();
        const indexer = new RestoreIndexerService(store, {
            freshnessPolicy: {
                staleAfterSeconds: 120,
                timeoutSeconds: 60,
            },
        });

        await indexer.recordSourceProgress({
            cursor: '1',
            instanceId: scope.instanceId,
            lastBatchSize: 10,
            lastIndexedEventTime: null,
            lastIndexedOffset: null,
            lastLagSeconds: null,
            measuredAt: FIXED_TIME,
            processedDelta: 0,
            source: scope.source,
            tenantId: scope.tenantId,
        });

        const items = [
            buildTestInput({
                eventId: 'evt-lc-1',
                offset: 1,
            }),
            buildTestInput({
                eventId: 'evt-lc-2',
                offset: 2,
            }),
            buildTestInput({
                eventId: 'evt-lc-3',
                offset: 3,
            }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const worker = new RestoreIndexerWorker(
            source,
            indexer,
            10,
            {
                sleep: async () => {},
                sourceProgressScope: scope,
                timeProvider: () => FIXED_TIME,
            },
        );
        const result = await worker.runOnce();
        assert.equal(result.batchSize, 2);
    });

    it('runOnce does not reload cursor on subsequent runs',
    async () => {
        const scope = defaultScope;
        const { worker } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-nr-1',
                    offset: 1,
                }),
                buildTestInput({
                    eventId: 'evt-nr-2',
                    offset: 2,
                }),
            ],
            batchSize: 1,
            sourceProgressScope: scope,
        });
        const run1 = await worker.runOnce();
        assert.equal(run1.batchSize, 1);
        const run2 = await worker.runOnce();
        assert.equal(run2.batchSize, 1);
    });

    it('runOnce does not advance cursor when failures > 0',
    async () => {
        const { worker } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-fail-cur-1',
                    offset: 1,
                    metadata: {
                        short_description: 'not allowed',
                    },
                }),
            ],
            sourceProgressScope: defaultScope,
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            const run1 = await worker.runOnce();
            assert.equal(run1.failures, 1);
            assert.equal(run1.cursor, null);
        } finally {
            console.error = originalError;
        }
    });

    it('runOnce advances cursor when failures === 0', async () => {
        const { worker } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-adv-cur-1',
                    offset: 1,
                }),
            ],
            sourceProgressScope: defaultScope,
        });
        const result = await worker.runOnce();
        assert.equal(result.failures, 0);
        assert.notEqual(result.cursor, null);
    });

    it('runOnce persists progress with lag_seconds', async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            lagSeconds: 42,
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.notEqual(progress, null);
        assert.equal(progress?.last_lag_seconds, 42);
    });

    it('runOnce persists progress without lag when null',
    async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-nolag-fail-1',
                    offset: 1,
                    metadata: {
                        short_description: 'not allowed',
                    },
                }),
            ],
            lagSeconds: 10,
            sourceProgressScope: scope,
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            await worker.runOnce();
            const progress = await indexer.getSourceProgress(
                scope.tenantId,
                scope.instanceId,
                scope.source,
            );
            assert.equal(progress?.last_lag_seconds, null);
        } finally {
            console.error = originalError;
        }
    });

    it('runContinuously stops when requestStop called',
    async () => {
        const { worker, source } = createWorkerFixture({
            items: [],
        });
        source.enqueue([
            buildTestInput({
                eventId: 'evt-stop-1',
                offset: 1,
            }),
            buildTestInput({
                eventId: 'evt-stop-2',
                offset: 2,
            }),
        ]);
        worker.requestStop();
        const summary = await worker.runContinuously();
        assert.equal(summary.cycles, 0);
    });

    it('runContinuously respects maxCycles limit', async () => {
        const { worker } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-cyc-1',
                    offset: 1,
                }),
                buildTestInput({
                    eventId: 'evt-cyc-2',
                    offset: 2,
                }),
            ],
            batchSize: 1,
        });
        const summary = await worker.runContinuously({
            maxCycles: 2,
        });
        assert.equal(summary.cycles, 2);
    });

    it('runContinuously sleeps on empty batch', async () => {
        const sleepCalls: number[] = [];
        const store = new InMemoryRestoreIndexStore();
        const indexer = new RestoreIndexerService(store, {
            freshnessPolicy: {
                staleAfterSeconds: 120,
                timeoutSeconds: 60,
            },
        });
        const source = new InMemoryArtifactBatchSource([], 0);
        const worker = new RestoreIndexerWorker(
            source,
            indexer,
            10,
            {
                pollIntervalMs: 100,
                sleep: async (ms: number) => {
                    sleepCalls.push(ms);
                    worker.requestStop();
                },
                timeProvider: () => FIXED_TIME,
            },
        );
        await worker.runContinuously();
        assert.ok(sleepCalls.length >= 1);
        assert.equal(sleepCalls[0], 100);
    });

    it('runContinuously releases leadership on exit',
    async () => {
        const leaseMgr = createLeaseManager(true);
        const originalLog = console.log;

        console.log = () => {};

        try {
            const { worker } = createWorkerFixture({
                items: [
                    buildTestInput({
                        eventId: 'evt-rel-1',
                        offset: 1,
                    }),
                ],
                leaderLease: leaseMgr.lease,
                sourceProgressScope: defaultScope,
            });
            await worker.runContinuously({ maxCycles: 1 });
            assert.equal(leaseMgr.released.length, 1);
        } finally {
            console.log = originalLog;
        }
    });

    it('runContinuously accumulates counts across cycles',
    async () => {
        const { worker } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-acc-1',
                    offset: 1,
                }),
                buildTestInput({
                    eventId: 'evt-acc-2',
                    offset: 2,
                }),
            ],
            batchSize: 1,
        });
        const summary = await worker.runContinuously({
            maxCycles: 2,
        });
        assert.equal(summary.inserted, 2);
        assert.equal(summary.cycles, 2);
    });

    it('constructor throws when leaderLease without scope',
    () => {
        const leaseMgr = createLeaseManager(true);
        assert.throws(() => {
            const store = new InMemoryRestoreIndexStore();
            const indexer = new RestoreIndexerService(store, {
                freshnessPolicy: {
                    staleAfterSeconds: 120,
                    timeoutSeconds: 60,
                },
            });
            const source = new InMemoryArtifactBatchSource([], 0);

            new RestoreIndexerWorker(source, indexer, 10, {
                leaderLease: leaseMgr.lease,
            });
        }, /leader lease requires sourceProgressScope/);
    });

    it('empty batch returns zero counts', async () => {
        const { worker } = createWorkerFixture({ items: [] });
        const result = await worker.runOnce();
        assert.equal(result.batchSize, 0);
        assert.equal(result.inserted, 0);
        assert.equal(result.existing, 0);
        assert.equal(result.failures, 0);
    });

    it('runOnce with leader lease acquires before processing',
    async () => {
        const leaseMgr = createLeaseManager(true);
        const originalLog = console.log;

        console.log = () => {};

        try {
            const { worker } = createWorkerFixture({
                leaderLease: leaseMgr.lease,
                sourceProgressScope: defaultScope,
            });
            const result = await worker.runOnce();
            assert.ok(leaseMgr.isLeased);
            assert.ok(result.batchSize > 0);
        } finally {
            console.log = originalLog;
        }
    });

    it('runOnce skips processing when lease not acquired',
    async () => {
        const leaseMgr = createLeaseManager(false);
        const originalLog = console.log;

        console.log = () => {};

        try {
            const { worker } = createWorkerFixture({
                leaderLease: leaseMgr.lease,
                sourceProgressScope: defaultScope,
            });
            const result = await worker.runOnce();
            assert.equal(result.batchSize, 0);
        } finally {
            console.log = originalLog;
        }
    });
});

describe('InMemoryArtifactBatchSource', () => {
    it('readBatch returns items from queue', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-src-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-src-2', offset: 2 }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 2);
    });

    it('readBatch respects cursor position', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-rc-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-rc-2', offset: 2 }),
            buildTestInput({ eventId: 'evt-rc-3', offset: 3 }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: '1',
            limit: 10,
        });
        assert.equal(batch.items.length, 2);
    });

    it('readBatch respects limit', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-rl-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-rl-2', offset: 2 }),
            buildTestInput({ eventId: 'evt-rl-3', offset: 3 }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 2,
        });
        assert.equal(batch.items.length, 2);
    });

    it('readBatch returns nextCursor as string offset',
    async () => {
        const items = [
            buildTestInput({ eventId: 'evt-nc-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-nc-2', offset: 2 }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.nextCursor, '2');
    });

    it('readBatch with null cursor starts from beginning',
    async () => {
        const items = [
            buildTestInput({
                eventId: 'evt-null-1',
                offset: 1,
            }),
        ];
        const source = new InMemoryArtifactBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 1);
    });

    it('enqueue adds items to queue', async () => {
        const source = new InMemoryArtifactBatchSource([], 0);
        source.enqueue([
            buildTestInput({
                eventId: 'evt-enq-1',
                offset: 1,
            }),
        ]);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 1);
    });

    it('setRealtimeLagSeconds updates lag value', async () => {
        const source = new InMemoryArtifactBatchSource([], 0);
        source.setRealtimeLagSeconds(99);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, 99);
    });

    it('readBatch includes realtimeLagSeconds', async () => {
        const source = new InMemoryArtifactBatchSource([], 42);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, 42);
    });
});

describe('worker helper functions (tested via worker)', () => {
    it('normalizeOffset: valid string returns canonical',
    async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-norm-1',
                    offset: '42',
                }),
            ],
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.equal(progress?.last_indexed_offset, '42');
    });

    it('normalizeOffset: null returns null for empty batch',
    async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [],
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.equal(progress?.last_indexed_offset, null);
    });

    it('latestEventTime: returns latest from items',
    async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-let-1',
                    eventTime: '2026-02-16T10:00:00.000Z',
                    offset: 1,
                }),
                buildTestInput({
                    eventId: 'evt-let-2',
                    eventTime: '2026-02-16T12:00:00.000Z',
                    offset: 2,
                }),
            ],
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.equal(
            progress?.last_indexed_event_time,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('latestEventTime: returns null for empty array',
    async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [],
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.equal(progress?.last_indexed_event_time, null);
    });

    it('latestOffset: returns largest from items', async () => {
        const scope = defaultScope;
        const { worker, indexer } = createWorkerFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-lo-1',
                    offset: '5',
                }),
                buildTestInput({
                    eventId: 'evt-lo-2',
                    offset: '100',
                }),
            ],
            sourceProgressScope: scope,
        });
        await worker.runOnce();
        const progress = await indexer.getSourceProgress(
            scope.tenantId,
            scope.instanceId,
            scope.source,
        );
        assert.equal(progress?.last_indexed_offset, '100');
    });
});
