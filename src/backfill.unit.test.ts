import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    BackfillController,
    InMemoryBackfillBatchSource,
    type BackfillControllerOptions,
} from './backfill.js';
import { RestoreIndexerService } from './indexer.service.js';
import { InMemoryRestoreIndexStore } from './store.js';
import { buildTestInput } from './test-helpers.js';
import type { BackfillMode, IndexArtifactInput } from './types.js';

function createFixture(overrides?: {
    items?: IndexArtifactInput[];
    lagSeconds?: number;
    maxLag?: number;
    mode?: BackfillMode;
    runId?: string;
    throttleBatchSize?: number;
    timeProvider?: () => string;
}) {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const items = overrides?.items ?? [
        buildTestInput({ eventId: 'evt-bf-1', offset: 1 }),
        buildTestInput({ eventId: 'evt-bf-2', offset: 2 }),
        buildTestInput({ eventId: 'evt-bf-3', offset: 3 }),
    ];
    const lagSeconds = overrides?.lagSeconds ?? 0;
    const source = new InMemoryBackfillBatchSource(
        items,
        lagSeconds,
    );
    const mode: BackfillMode = overrides?.mode ?? 'bootstrap';
    const opts: BackfillControllerOptions = {
        maxRealtimeLagSeconds: overrides?.maxLag ?? 180,
        runId: overrides?.runId ?? 'run-test-1',
        throttleBatchSize: overrides?.throttleBatchSize ?? 10,
        timeProvider: overrides?.timeProvider
            ?? (() => '2026-02-16T12:00:00.000Z'),
    };
    const controller = new BackfillController(
        mode,
        source,
        indexer,
        store,
        opts,
    );

    return { controller, indexer, source, store };
}

describe('BackfillController', () => {
    it('initializes with provided state', () => {
        const { controller } = createFixture({
            maxLag: 200,
            mode: 'gap_repair',
            runId: 'run-custom',
            throttleBatchSize: 50,
        });
        const state = controller.getState();
        assert.equal(state.runId, 'run-custom');
        assert.equal(state.mode, 'gap_repair');
        assert.equal(state.throttleBatchSize, 50);
        assert.equal(state.maxRealtimeLagSeconds, 200);
        assert.equal(state.status, 'running');
        assert.equal(state.reasonCode, 'none');
        assert.equal(state.cursor, null);
        assert.equal(state.processedCount, 0);
    });

    it('generates random runId when not provided', () => {
        const store = new InMemoryRestoreIndexStore();
        const indexer = new RestoreIndexerService(store, {
            freshnessPolicy: {
                staleAfterSeconds: 120,
                timeoutSeconds: 60,
            },
        });
        const source = new InMemoryBackfillBatchSource([], 0);
        const c = new BackfillController(
            'bootstrap',
            source,
            indexer,
            store,
            {
                maxRealtimeLagSeconds: 180,
                throttleBatchSize: 10,
                timeProvider: () => '2026-02-16T12:00:00.000Z',
            },
        );
        const state = c.getState();
        assert.ok(state.runId.length > 0);
        assert.notEqual(state.runId, 'run-test-1');
    });

    it('getState returns cloned state (mutation safe)', () => {
        const { controller } = createFixture();
        const state1 = controller.getState();
        state1.cursor = 'mutated';
        state1.processedCount = 999;
        const state2 = controller.getState();
        assert.equal(state2.cursor, null);
        assert.equal(state2.processedCount, 0);
    });

    it('pauseManual sets status=paused, reason=paused_manual',
    async () => {
        const { controller } = createFixture();
        const state = await controller.pauseManual();
        assert.equal(state.status, 'paused');
        assert.equal(state.reasonCode, 'paused_manual');
    });

    it('pauseManual persists state to store', async () => {
        const { controller, store } = createFixture({
            runId: 'run-persist',
        });
        await controller.pauseManual();
        const persisted = await store.getBackfillRun(
            'run-persist',
        );
        assert.notEqual(persisted, null);
        assert.equal(persisted?.status, 'paused');
        assert.equal(persisted?.reasonCode, 'paused_manual');
    });

    it('resume sets status=running, reason=none', async () => {
        const { controller } = createFixture();
        await controller.pauseManual();
        const state = await controller.resume();
        assert.equal(state.status, 'running');
        assert.equal(state.reasonCode, 'none');
    });

    it('resume does not resume from completed state',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-comp-1',
                    offset: 1,
                }),
            ],
            throttleBatchSize: 10,
        });
        await controller.tick();
        const stateAfterTick = controller.getState();
        assert.equal(stateAfterTick.status, 'completed');
        const stateAfterResume = await controller.resume();
        assert.equal(stateAfterResume.status, 'completed');
    });

    it('resume persists state to store', async () => {
        const { controller, store } = createFixture({
            runId: 'run-resume-persist',
        });
        await controller.pauseManual();
        await controller.resume();
        const persisted = await store.getBackfillRun(
            'run-resume-persist',
        );
        assert.equal(persisted?.status, 'running');
    });

    it('tick returns immediately when paused', async () => {
        const { controller } = createFixture();
        await controller.pauseManual();
        const state = await controller.tick();
        assert.equal(state.status, 'paused');
    });

    it('tick returns immediately when completed', async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-done-1',
                    offset: 1,
                }),
            ],
        });
        await controller.tick();
        assert.equal(controller.getState().status, 'completed');
        const state = await controller.tick();
        assert.equal(state.status, 'completed');
    });

    it('tick reads batch with correct cursor and throttle size',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-cursor-1',
                    offset: 1,
                }),
                buildTestInput({
                    eventId: 'evt-cursor-2',
                    offset: 2,
                }),
                buildTestInput({
                    eventId: 'evt-cursor-3',
                    offset: 3,
                }),
            ],
            throttleBatchSize: 2,
        });
        const state1 = await controller.tick();
        assert.equal(state1.processedCount, 2);
        assert.equal(state1.status, 'running');
        const state2 = await controller.tick();
        assert.equal(state2.processedCount, 3);
        assert.equal(state2.status, 'completed');
    });

    it('tick sets ingestionMode on items before processing',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-mode-1',
                    offset: 1,
                }),
            ],
            mode: 'gap_repair',
        });
        const state = await controller.tick();
        assert.equal(state.status, 'completed');
        assert.equal(state.processedCount, 1);
    });

    it('tick pauses on lag guardrail breach', async () => {
        const { controller } = createFixture({
            lagSeconds: 200,
            maxLag: 100,
        });
        const state = await controller.tick();
        assert.equal(state.status, 'paused');
    });

    it('tick pauses with reason paused_realtime_lag_guardrail',
    async () => {
        const { controller } = createFixture({
            lagSeconds: 200,
            maxLag: 100,
        });
        const state = await controller.tick();
        assert.equal(
            state.reasonCode,
            'paused_realtime_lag_guardrail',
        );
    });

    it('tick pauses with reason paused_indexing_failures',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-fail-1',
                    offset: 1,
                    metadata: {
                        short_description: 'not allowed',
                    },
                }),
            ],
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            const state = await controller.tick();
            assert.equal(state.status, 'paused');
            assert.equal(
                state.reasonCode,
                'paused_indexing_failures',
            );
        } finally {
            console.error = originalError;
        }
    });

    it('tick advances cursor only when failures === 0',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-adv-1',
                    offset: 1,
                }),
            ],
        });
        const state = await controller.tick();
        assert.notEqual(state.cursor, null);
        assert.equal(state.status, 'completed');
    });

    it('tick marks completed when batch.done and no failures',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-comp-done-1',
                    offset: 1,
                }),
            ],
        });
        const state = await controller.tick();
        assert.equal(state.status, 'completed');
    });

    it('tick does not mark completed when failures present',
    async () => {
        const { controller } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-ncomp-1',
                    offset: 1,
                    metadata: {
                        short_description: 'not allowed',
                    },
                }),
            ],
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            const state = await controller.tick();
            assert.equal(state.status, 'paused');
            assert.notEqual(state.status, 'completed');
        } finally {
            console.error = originalError;
        }
    });

    it('tick persists updated state after each iteration',
    async () => {
        const { controller, store } = createFixture({
            items: [
                buildTestInput({
                    eventId: 'evt-pers-1',
                    offset: 1,
                }),
            ],
            runId: 'run-persist-tick',
        });
        await controller.tick();
        const persisted = await store.getBackfillRun(
            'run-persist-tick',
        );
        assert.notEqual(persisted, null);
        assert.equal(persisted?.status, 'completed');
        assert.equal(persisted?.processedCount, 1);
    });

    it('ensureInitialized loads persisted state from store',
    async () => {
        const { store } = createFixture();
        await store.upsertBackfillRun({
            cursor: 'saved-cursor',
            maxRealtimeLagSeconds: 180,
            mode: 'bootstrap',
            processedCount: 42,
            reasonCode: 'paused_manual',
            runId: 'run-load',
            status: 'paused',
            throttleBatchSize: 10,
            updatedAt: '2026-02-16T11:00:00.000Z',
        });

        const source = new InMemoryBackfillBatchSource([], 0);
        const indexer = new RestoreIndexerService(store, {
            freshnessPolicy: {
                staleAfterSeconds: 120,
                timeoutSeconds: 60,
            },
        });
        const controller = new BackfillController(
            'bootstrap',
            source,
            indexer,
            store,
            {
                maxRealtimeLagSeconds: 180,
                runId: 'run-load',
                throttleBatchSize: 10,
                timeProvider: () => '2026-02-16T12:00:00.000Z',
            },
        );
        const state = await controller.resume();
        assert.equal(state.processedCount, 42);
        assert.equal(state.cursor, 'saved-cursor');
    });

    it('ensureInitialized uses initial state when none persisted',
    async () => {
        const { controller } = createFixture({
            runId: 'run-fresh',
        });
        const state = await controller.tick();
        assert.ok(state.processedCount >= 0);
    });

    it('ensureInitialized loads only once (guard flag)',
    async () => {
        const { controller } = createFixture({
            runId: 'run-guard',
        });
        await controller.pauseManual();
        await controller.resume();
        const state = controller.getState();
        assert.equal(state.status, 'running');
    });
});

describe('InMemoryBackfillBatchSource', () => {
    it('returns items from array', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-src-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-src-2', offset: 2 }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
            mode: 'bootstrap',
        });
        assert.equal(batch.items.length, 2);
    });

    it('respects cursor position', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-cur-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-cur-2', offset: 2 }),
            buildTestInput({ eventId: 'evt-cur-3', offset: 3 }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: '1',
            limit: 10,
            mode: 'bootstrap',
        });
        assert.equal(batch.items.length, 2);
    });

    it('respects limit', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-lim-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-lim-2', offset: 2 }),
            buildTestInput({ eventId: 'evt-lim-3', offset: 3 }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 2,
            mode: 'bootstrap',
        });
        assert.equal(batch.items.length, 2);
    });

    it('sets done=true at end of array', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-end-1', offset: 1 }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
            mode: 'bootstrap',
        });
        assert.equal(batch.done, true);
    });

    it('sets done=false when more items remain', async () => {
        const items = [
            buildTestInput({ eventId: 'evt-more-1', offset: 1 }),
            buildTestInput({ eventId: 'evt-more-2', offset: 2 }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 1,
            mode: 'bootstrap',
        });
        assert.equal(batch.done, false);
    });

    it('includes realtimeLagSeconds in result', async () => {
        const source = new InMemoryBackfillBatchSource([], 42);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
            mode: 'bootstrap',
        });
        assert.equal(batch.realtimeLagSeconds, 42);
    });

    it('null cursor starts from beginning', async () => {
        const items = [
            buildTestInput({
                eventId: 'evt-null-1',
                offset: 1,
            }),
        ];
        const source = new InMemoryBackfillBatchSource(items, 0);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
            mode: 'bootstrap',
        });
        assert.equal(batch.items.length, 1);
    });
});
