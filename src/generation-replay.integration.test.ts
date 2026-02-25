import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';

test('replay is idempotent and rewind requires a new generation', async () => {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-a1',
        generationId: 'gen-1',
        offset: 1,
    }));
    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-a2',
        generationId: 'gen-1',
        offset: 2,
    }));

    const replay = await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-a2',
        generationId: 'gen-1',
        offset: 2,
    }));

    assert.equal(replay.eventWriteState, 'existing');

    await assert.rejects(
        indexer.indexArtifact(buildTestInput({
            eventId: 'evt-a3',
            generationId: 'gen-1',
            offset: 1,
        })),
        {
            name: 'WatermarkInvariantError',
        },
    );

    await indexer.indexArtifact(buildTestInput({
        eventId: 'evt-b1',
        generationId: 'gen-2',
        offset: 1,
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

    assert.equal(status.watermark?.generation_id, 'gen-2');
    assert.equal(status.watermark?.indexed_through_offset, '1');
    assert.equal(store.getIndexedEventCount(), 4);

    const coverage = await indexer.getSourceCoverageWindow(
        'tenant-acme',
        'sn-dev-01',
        'sn://acme-dev.service-now.com',
    );

    assert.equal(coverage?.generation_span['rez.cdc:0'], 'gen-2');
});
