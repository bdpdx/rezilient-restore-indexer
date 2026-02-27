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
