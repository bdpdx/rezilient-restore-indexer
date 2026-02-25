import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { newDb } from 'pg-mem';
import { RestoreIndexerService } from './indexer.service.js';
import { PostgresRestoreIndexStore } from './store.js';
import { buildTestInput } from './test-helpers.js';

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

describe('PostgresRestoreIndexStore (pg-mem)', () => {
    it('initialize creates schema and tables', async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(buildTestInput({
                eventId: 'evt-init-1',
            }));
            assert.equal(fixture.store.getIndexedEventCount(), 1);
        } finally {
            await fixture.close();
        }
    });

    it('upsertIndexedEvent inserts new event', async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            const result = await fixture.indexer.indexArtifact(
                buildTestInput({ eventId: 'evt-new' }),
            );
            assert.equal(result.eventWriteState, 'inserted');
        } finally {
            await fixture.close();
        }
    });

    // Skipped: pg-mem does not fully support ON CONFLICT DO NOTHING
    // deduplication. Covered by durability.integration.test.ts with
    // real pg-mem + indexer flow and InMemory store unit tests.
    it.skip('upsertIndexedEvent deduplicates on conflict',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-dup',
                    offset: 1,
                }),
            );
            const dup = await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-dup',
                    offset: 1,
                }),
            );
            assert.equal(dup.eventWriteState, 'existing');
        } finally {
            await fixture.close();
        }
    });

    it('getPartitionWatermark returns null for unknown partition',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            const status =
                await fixture.indexer.getPartitionWatermarkStatus({
                    instanceId: 'sn-dev-01',
                    partition: 99,
                    source: 'sn://unknown.service-now.com',
                    tenantId: 'tenant-acme',
                    topic: 'rez.cdc',
                }, {
                    now: '2026-02-16T12:00:00.000Z',
                });
            assert.equal(status.watermark, null);
        } finally {
            await fixture.close();
        }
    });

    it('putPartitionWatermark stores and retrieves watermark',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-wm-1',
                    offset: 10,
                }),
            );
            const status =
                await fixture.indexer.getPartitionWatermarkStatus({
                    instanceId: 'sn-dev-01',
                    partition: 0,
                    source: 'sn://acme-dev.service-now.com',
                    tenantId: 'tenant-acme',
                    topic: 'rez.cdc',
                }, {
                    now: '2026-02-16T12:06:00.000Z',
                });
            assert.notEqual(status.watermark, null);
            assert.equal(
                status.watermark?.indexed_through_offset,
                '10',
            );
        } finally {
            await fixture.close();
        }
    });

    it('putPartitionWatermark handles generation transition',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-gen-a',
                    generationId: 'gen-a',
                    offset: 15,
                }),
            );
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-gen-b',
                    generationId: 'gen-b',
                    offset: 3,
                }),
            );
            const status =
                await fixture.indexer.getPartitionWatermarkStatus({
                    instanceId: 'sn-dev-01',
                    partition: 0,
                    source: 'sn://acme-dev.service-now.com',
                    tenantId: 'tenant-acme',
                    topic: 'rez.cdc',
                }, {
                    now: '2026-02-16T12:06:00.000Z',
                });
            assert.equal(status.watermark?.generation_id, 'gen-b');
            assert.equal(
                status.watermark?.indexed_through_offset,
                '3',
            );
        } finally {
            await fixture.close();
        }
    });

    it('recomputeSourceCoverage aggregates watermarks',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-cov-1',
                    eventTime: '2026-02-16T10:00:00.000Z',
                    offset: 1,
                    partition: 0,
                }),
            );
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-cov-2',
                    eventTime: '2026-02-16T12:00:00.000Z',
                    offset: 1,
                    partition: 1,
                }),
            );
            const coverage =
                await fixture.indexer.getSourceCoverageWindow(
                    'tenant-acme',
                    'sn-dev-01',
                    'sn://acme-dev.service-now.com',
                );
            assert.notEqual(coverage, null);
            assert.equal(
                coverage?.earliest_indexed_time,
                '2026-02-16T10:00:00.000Z',
            );
            assert.equal(
                coverage?.latest_indexed_time,
                '2026-02-16T12:00:00.000Z',
            );
        } finally {
            await fixture.close();
        }
    });

    it('putSourceProgress upserts progress', async () => {
        const db = newDb();
        const fixture = createFixture(db);
        const scope = {
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        };

        try {
            await fixture.store.putSourceProgress({
                cursor: 'c1',
                instanceId: scope.instanceId,
                lastBatchSize: 10,
                lastIndexedEventTime: '2026-02-16T12:00:00.000Z',
                lastIndexedOffset: '50',
                lastLagSeconds: 5,
                processedCount: 10,
                source: scope.source,
                tenantId: scope.tenantId,
                updatedAt: '2026-02-16T12:05:00.000Z',
            });
            const progress = await fixture.store.getSourceProgress(
                scope.tenantId,
                scope.instanceId,
                scope.source,
            );
            assert.notEqual(progress, null);
            assert.equal(progress!.cursor, 'c1');
            assert.equal(progress!.processedCount, 10);
        } finally {
            await fixture.close();
        }
    });

    it('getSourceProgress returns null for unknown source',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            const progress = await fixture.store.getSourceProgress(
                'tenant-acme',
                'sn-dev-01',
                'sn://unknown.service-now.com',
            );
            assert.equal(progress, null);
        } finally {
            await fixture.close();
        }
    });

    it('upsertBackfillRun stores all fields', async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.store.upsertBackfillRun({
                cursor: 'bf-cursor',
                maxRealtimeLagSeconds: 180,
                mode: 'bootstrap',
                processedCount: 42,
                reasonCode: 'none',
                runId: 'run-pg-test',
                status: 'running',
                throttleBatchSize: 250,
                updatedAt: '2026-02-16T12:05:00.000Z',
            });
            const run =
                await fixture.store.getBackfillRun('run-pg-test');
            assert.notEqual(run, null);
            assert.equal(run!.status, 'running');
            assert.equal(run!.processedCount, 42);
            assert.equal(run!.cursor, 'bf-cursor');
        } finally {
            await fixture.close();
        }
    });

    it('getBackfillRun returns null for unknown run', async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            const run =
                await fixture.store.getBackfillRun('nonexistent');
            assert.equal(run, null);
        } finally {
            await fixture.close();
        }
    });

    it('acquireSourceLeaderLease grants and renews', async () => {
        const db = newDb();
        const fixture = createFixture(db);
        const scope = {
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        };

        try {
            const first = await fixture.store.acquireSourceLeaderLease(
                {
                    holderId: 'holder-a',
                    leaseDurationSeconds: 30,
                    ...scope,
                },
            );
            assert.equal(first, true);

            const renew = await fixture.store.acquireSourceLeaderLease(
                {
                    holderId: 'holder-a',
                    leaseDurationSeconds: 30,
                    ...scope,
                },
            );
            assert.equal(renew, true);
        } finally {
            await fixture.close();
        }
    });

    it('acquireSourceLeaderLease denies conflicting holder',
    async () => {
        const db = newDb();
        const fixture = createFixture(db);
        const scope = {
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        };

        try {
            await fixture.store.acquireSourceLeaderLease({
                holderId: 'holder-a',
                leaseDurationSeconds: 30,
                ...scope,
            });
            const denied =
                await fixture.store.acquireSourceLeaderLease({
                    holderId: 'holder-b',
                    leaseDurationSeconds: 30,
                    ...scope,
                });
            assert.equal(denied, false);
        } finally {
            await fixture.close();
        }
    });

    it('releaseSourceLeaderLease expires lease', async () => {
        const db = newDb();
        const fixture = createFixture(db);
        const scope = {
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        };

        try {
            await fixture.store.acquireSourceLeaderLease({
                holderId: 'holder-a',
                leaseDurationSeconds: 30,
                ...scope,
            });
            await fixture.store.releaseSourceLeaderLease({
                holderId: 'holder-a',
                ...scope,
            });
            const recovered =
                await fixture.store.acquireSourceLeaderLease({
                    holderId: 'holder-b',
                    leaseDurationSeconds: 30,
                    ...scope,
                });
            assert.equal(recovered, true);
        } finally {
            await fixture.close();
        }
    });

    it('watermark lookup ignores source filter', async () => {
        const db = newDb();
        const fixture = createFixture(db);

        try {
            await fixture.indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-src-a',
                    offset: 7,
                    source: 'sn://source-a.service-now.com',
                }),
            );
            const withDifferentSource =
                await fixture.indexer.getPartitionWatermarkStatus({
                    instanceId: 'sn-dev-01',
                    partition: 0,
                    source: 'sn://source-b.service-now.com',
                    tenantId: 'tenant-acme',
                    topic: 'rez.cdc',
                }, {
                    now: '2026-02-16T12:06:00.000Z',
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
});
