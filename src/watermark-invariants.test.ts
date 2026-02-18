import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';

function createIndexer() {
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

test('rejects offset rewind inside the same generation', async () => {
    const { indexer } = createIndexer();

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-1',
        generationId: 'gen-a',
        offset: 10,
    }));

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-2',
            generationId: 'gen-a',
            offset: 9,
        })),
        {
            name: 'WatermarkInvariantError',
        },
    );

    const status = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:06:00.000Z',
    });

    assert.equal(status.watermark?.generation_id, 'gen-a');
    assert.equal(status.watermark?.indexed_through_offset, '10');
});

test('new generation accepts rewind and resets active coverage state', async () => {
    const { indexer } = createIndexer();

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-11',
        eventTime: '2026-02-16T12:10:00.000Z',
        generationId: 'gen-a',
        offset: 15,
    }));

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-12',
        eventTime: '2026-02-16T12:05:00.000Z',
        generationId: 'gen-b',
        offset: 3,
    }));

    const status = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:06:00.000Z',
    });

    assert.equal(status.watermark?.generation_id, 'gen-b');
    assert.equal(status.watermark?.indexed_through_offset, '3');

    const coverage = await indexer.getSourceCoverageWindow(
        'tenant-acme',
        'sn-dev-01',
        'sn://acme-dev.service-now.com',
    );

    assert.equal(coverage?.earliest_indexed_time, '2026-02-16T12:05:00.000Z');
    assert.equal(coverage?.latest_indexed_time, '2026-02-16T12:05:00.000Z');
});

test('rejects metadata outside rrs.metadata.allowlist.v1', async () => {
    const { indexer } = createIndexer();

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-sec',
            metadata: {
                short_description: 'plaintext not allowed',
            },
        })),
    );
});

test('rejects unsafe offset beyond runtime compatibility range', async () => {
    const { indexer } = createIndexer();

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-unsafe-offset',
            metadata: {
                offset: '9007199254740993',
            },
        })),
        /Operational metadata offset exceeds safe integer runtime/,
    );
});

test('normalizes second-precision manifest event_time to canonical millis',
async () => {
    const { indexer } = createIndexer();

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-seconds-manifest',
        eventTime: '2026-02-16T12:07:09Z',
        metadata: {
            __time: undefined,
        },
    }));

    const status = await indexer.getPartitionWatermarkStatus({
        instanceId: 'sn-dev-01',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
    }, {
        now: '2026-02-16T12:08:00Z',
    });

    assert.equal(
        status.watermark?.coverage_start,
        '2026-02-16T12:07:09.000Z',
    );
    assert.equal(
        status.watermark?.coverage_end,
        '2026-02-16T12:07:09.000Z',
    );
    assert.equal(
        status.watermark?.indexed_through_time,
        '2026-02-16T12:07:09.000Z',
    );
    assert.equal(status.watermark?.measured_at, '2026-02-16T12:08:00.000Z');

    const coverage = await indexer.getSourceCoverageWindow(
        'tenant-acme',
        'sn-dev-01',
        'sn://acme-dev.service-now.com',
    );

    assert.equal(
        coverage?.earliest_indexed_time,
        '2026-02-16T12:07:09.000Z',
    );
    assert.equal(
        coverage?.latest_indexed_time,
        '2026-02-16T12:07:09.000Z',
    );
});

test('rejects invalid manifest fallback timestamp with field-specific error',
async () => {
    const { indexer } = createIndexer();

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-invalid-manifest-time',
            eventTime: '2026-02-16T12:07:09+00:00',
            metadata: {
                __time: undefined,
            },
        })),
        /Invalid manifest\.event_time value/,
    );
});

test('rejects invalid metadata timestamp with field-specific error',
async () => {
    const { indexer } = createIndexer();

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-invalid-metadata-time',
            metadata: {
                __time: '2026-02-16T12:07:09+00:00',
            },
        })),
        /Invalid metadata\.__time value/,
    );
});
