import assert from 'node:assert/strict';
import { test } from 'node:test';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import {
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
