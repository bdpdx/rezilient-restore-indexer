import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    BackfillController,
    InMemoryBackfillBatchSource,
} from './backfill';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';

test('freshness gate transitions stale blocked to preview_only on timeout',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 30,
            timeoutSeconds: 60,
        },
    });

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-stale-1',
        eventTime: '2026-02-16T12:00:00.000Z',
        offset: 1,
    }));

    const blocked = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:02:00.000Z',
        waitStartedAt: '2026-02-16T12:01:30.000Z',
    });

    assert.equal(blocked.freshness, 'stale');
    assert.equal(blocked.executability, 'blocked');
    assert.equal(blocked.reason_code, 'blocked_freshness_stale');

    const previewOnly = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:03:00.000Z',
        waitStartedAt: '2026-02-16T12:01:30.000Z',
    });

    assert.equal(previewOnly.freshness, 'stale');
    assert.equal(previewOnly.executability, 'preview_only');
    assert.equal(previewOnly.gate_timed_out, true);
    assert.equal(previewOnly.reason_code, 'blocked_freshness_stale');
});

test('unknown watermark state remains fail-closed and non-executable',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 30,
            timeoutSeconds: 60,
        },
    });

    const status = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 9,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:00:30.000Z',
    });

    assert.equal(status.freshness, 'unknown');
    assert.equal(status.executability, 'blocked');
    assert.equal(status.reason_code, 'blocked_freshness_unknown');
    assert.equal(status.watermark, null);
});

test('backfill controller auto-pauses when lag guardrail is exceeded',
async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const source = new InMemoryBackfillBatchSource([
        buildTestInput({
            eventId: 'evt-backfill-1',
            ingestionMode: 'bootstrap',
            offset: 1,
        }),
    ], 301);
    const backfill = new BackfillController(
        'bootstrap',
        source,
        indexer,
        store,
        {
            maxRealtimeLagSeconds: 180,
            runId: 'run-backfill-guard',
            throttleBatchSize: 100,
            timeProvider: () => '2026-02-16T12:20:00.000Z',
        },
    );

    const state = await backfill.tick();

    assert.equal(state.status, 'paused');
    assert.equal(state.reasonCode, 'paused_realtime_lag_guardrail');
    assert.equal(state.processedCount, 0);
    assert.equal(store.getIndexedEventCount(), 0);
});

test('backfill controller pauses fail-closed on indexing failures', async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });
    const source = new InMemoryBackfillBatchSource([
        buildTestInput({
            eventId: 'evt-backfill-idx-ok',
            ingestionMode: 'bootstrap',
            offset: 1,
        }),
        buildTestInput({
            eventId: 'evt-backfill-idx-bad',
            ingestionMode: 'bootstrap',
            offset: 2,
            metadata: {
                short_description: 'plaintext not allowed',
            },
        }),
    ], 0);
    const backfill = new BackfillController(
        'bootstrap',
        source,
        indexer,
        store,
        {
            maxRealtimeLagSeconds: 180,
            runId: 'run-backfill-indexing-failure',
            throttleBatchSize: 100,
            timeProvider: () => '2026-02-16T12:21:00.000Z',
        },
    );

    const state = await backfill.tick();

    assert.equal(state.status, 'paused');
    assert.equal(state.reasonCode, 'paused_indexing_failures');
    assert.equal(state.cursor, null);
    assert.equal(state.processedCount, 1);
    assert.equal(store.getIndexedEventCount(), 1);
});
