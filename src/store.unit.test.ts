import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { newDb } from 'pg-mem';
import {
    InMemoryRestoreIndexStore,
    PostgresRestoreIndexStore,
} from './store.js';
import type {
    BackfillRunState,
    IndexedEventRecord,
    PartitionWatermarkState,
    SourceProgressState,
} from './types.js';

function buildRecord(
    overrides: Partial<IndexedEventRecord> = {},
): IndexedEventRecord {
    return {
        artifactKey: 'rez/restore/event=evt-0001.artifact.json',
        artifactKind: 'cdc',
        app: 'x_app',
        generationId: 'gen-01',
        indexedAt: '2026-02-16T12:05:00.000Z',
        ingestionMode: 'realtime',
        instanceId: 'sn-dev-01',
        manifestVersion: 'rec.artifact-manifest.v1',
        metadata: {
            __time: '2026-02-16T12:00:00.000Z',
            event_id: 'evt-0001',
            event_type: 'cdc.write',
            instance_id: 'sn-dev-01',
            offset: '1',
            operation: 'U',
            partition: 0,
            record_sys_id: 'abc123',
            schema_version: 1,
            source: 'sn://acme-dev.service-now.com',
            sys_mod_count: 1,
            table: 'x_app.ticket',
            tenant_id: 'tenant-acme',
            topic: 'rez.cdc',
        },
        objectKeyLayoutVersion: 'rec.object-key-layout.v1',
        partitionScope: {
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        },
        table: 'x_app.ticket',
        tenantId: 'tenant-acme',
        ...overrides,
    };
}

function buildWatermark(
    overrides: Partial<PartitionWatermarkState> = {},
): PartitionWatermarkState {
    return {
        coverageEnd: '2026-02-16T12:00:00.000Z',
        coverageStart: '2026-02-16T11:00:00.000Z',
        generationId: 'gen-01',
        indexedThroughOffset: '100',
        indexedThroughTime: '2026-02-16T12:00:00.000Z',
        instanceId: 'sn-dev-01',
        measuredAt: '2026-02-16T12:01:00.000Z',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
        ...overrides,
    };
}

function buildProgress(
    overrides: Partial<SourceProgressState> = {},
): SourceProgressState {
    return {
        cursor: 'cursor-1',
        instanceId: 'sn-dev-01',
        lastBatchSize: 10,
        lastIndexedEventTime: '2026-02-16T12:00:00.000Z',
        lastIndexedOffset: '100',
        lastLagSeconds: 5,
        processedCount: 10,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        updatedAt: '2026-02-16T12:05:00.000Z',
        ...overrides,
    };
}

function buildBackfillRun(
    overrides: Partial<BackfillRunState> = {},
): BackfillRunState {
    return {
        cursor: null,
        maxRealtimeLagSeconds: 180,
        mode: 'bootstrap',
        processedCount: 0,
        reasonCode: 'none',
        runId: 'run-01',
        status: 'running',
        throttleBatchSize: 250,
        updatedAt: '2026-02-16T12:05:00.000Z',
        ...overrides,
    };
}

describe('InMemoryRestoreIndexStore', () => {
    it('upsertIndexedEvent returns inserted for new event',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.upsertIndexedEvent(buildRecord());
        assert.equal(result, 'inserted');
    });

    it('upsertIndexedEvent returns existing for duplicate',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.upsertIndexedEvent(buildRecord());
        const result = await store.upsertIndexedEvent(buildRecord());
        assert.equal(result, 'existing');
    });

    it('upsertIndexedEvent clones record (mutation safe)',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const record = buildRecord();
        await store.upsertIndexedEvent(record);
        record.generationId = 'mutated';
        const second = buildRecord({
            metadata: {
                ...buildRecord().metadata,
                event_id: 'evt-0002',
            },
        });
        await store.upsertIndexedEvent(second);
        assert.equal(store.getIndexedEventCount(), 2);
    });

    it('getIndexedEventCount tracks insertions', async () => {
        const store = new InMemoryRestoreIndexStore();
        assert.equal(store.getIndexedEventCount(), 0);
        await store.upsertIndexedEvent(buildRecord());
        assert.equal(store.getIndexedEventCount(), 1);
        await store.upsertIndexedEvent(buildRecord({
            metadata: {
                ...buildRecord().metadata,
                event_id: 'evt-0002',
            },
        }));
        assert.equal(store.getIndexedEventCount(), 2);
    });

    it('getPartitionWatermark returns null for unknown partition',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.getPartitionWatermark({
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        assert.equal(result, null);
    });

    it('getPartitionWatermark returns stored watermark',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const wm = buildWatermark();
        await store.putPartitionWatermark(wm);
        const result = await store.getPartitionWatermark({
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        assert.notEqual(result, null);
        assert.equal(result!.generationId, 'gen-01');
        assert.equal(result!.indexedThroughOffset, '100');
    });

    it('getPartitionWatermark ignores source in scope match',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            source: 'sn://source-a.service-now.com',
        }));
        const result = await store.getPartitionWatermark({
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://source-b.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        assert.notEqual(result, null);
        assert.equal(result!.indexedThroughOffset, '100');
    });

    it('putPartitionWatermark stores watermark', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark());
        const result = await store.getPartitionWatermark({
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        assert.notEqual(result, null);
    });

    it('putPartitionWatermark overwrites existing', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            indexedThroughOffset: '50',
        }));
        await store.putPartitionWatermark(buildWatermark({
            indexedThroughOffset: '100',
        }));
        const result = await store.getPartitionWatermark({
            instanceId: 'sn-dev-01',
            partition: 0,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        assert.equal(result!.indexedThroughOffset, '100');
    });

    it('recomputeSourceCoverage aggregates across partitions',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            coverageStart: '2026-02-16T10:00:00.000Z',
            coverageEnd: '2026-02-16T11:00:00.000Z',
            partition: 0,
        }));
        await store.putPartitionWatermark(buildWatermark({
            coverageStart: '2026-02-16T09:00:00.000Z',
            coverageEnd: '2026-02-16T12:00:00.000Z',
            partition: 1,
        }));
        const result = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.notEqual(result, null);
        assert.equal(
            result!.earliestIndexedTime,
            '2026-02-16T09:00:00.000Z',
        );
        assert.equal(
            result!.latestIndexedTime,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('recomputeSourceCoverage returns null when no watermarks',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(result, null);
    });

    it('recomputeSourceCoverage deletes stale coverage',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            source: 'sn://source-a.service-now.com',
        }));
        await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://source-a.service-now.com',
            tenantId: 'tenant-acme',
        });
        let result = await store.getSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://source-a.service-now.com',
        );
        assert.notEqual(result, null);

        const nullResult = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:06:00.000Z',
            source: 'sn://no-watermarks.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(nullResult, null);

        result = await store.getSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://no-watermarks.service-now.com',
        );
        assert.equal(result, null);
    });

    it('getSourceCoverage returns null for unknown source',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.getSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://unknown.service-now.com',
        );
        assert.equal(result, null);
    });

    it('getSourceCoverage returns stored coverage', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark());
        await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const result = await store.getSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
        );
        assert.notEqual(result, null);
        assert.equal(result!.tenantId, 'tenant-acme');
    });

    it('putSourceProgress stores progress', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putSourceProgress(buildProgress());
        const result = await store.getSourceProgress(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
        );
        assert.notEqual(result, null);
        assert.equal(result!.cursor, 'cursor-1');
    });

    it('getSourceProgress returns null for unknown source',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.getSourceProgress(
            'tenant-acme',
            'sn-dev-01',
            'sn://unknown.service-now.com',
        );
        assert.equal(result, null);
    });

    it('getSourceProgress returns stored progress', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putSourceProgress(buildProgress());
        const result = await store.getSourceProgress(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
        );
        assert.notEqual(result, null);
        assert.equal(result!.processedCount, 10);
    });

    it('upsertBackfillRun stores state', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.upsertBackfillRun(buildBackfillRun());
        const result = await store.getBackfillRun('run-01');
        assert.notEqual(result, null);
        assert.equal(result!.status, 'running');
    });

    it('getBackfillRun returns null for unknown runId', async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.getBackfillRun('unknown');
        assert.equal(result, null);
    });

    it('getBackfillRun returns stored state', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.upsertBackfillRun(buildBackfillRun({
            cursor: 'cursor-5',
            processedCount: 500,
        }));
        const result = await store.getBackfillRun('run-01');
        assert.equal(result!.cursor, 'cursor-5');
        assert.equal(result!.processedCount, 500);
    });

    it('acquireSourceLeaderLease grants to first requester',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const granted = await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(granted, true);
    });

    it('acquireSourceLeaderLease denies different holder when active',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const denied = await store.acquireSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(denied, false);
    });

    it('acquireSourceLeaderLease renews for same holder',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const renewed = await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(renewed, true);
    });

    it('releaseSourceLeaderLease clears for matching holder',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        await store.releaseSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const granted = await store.acquireSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(granted, true);
    });

    it('releaseSourceLeaderLease no-ops for non-matching holder',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.acquireSourceLeaderLease({
            holderId: 'holder-a',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        await store.releaseSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: 'sn-dev-01',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        const denied = await store.acquireSourceLeaderLease({
            holderId: 'holder-b',
            instanceId: 'sn-dev-01',
            leaseDurationSeconds: 30,
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(denied, false);
    });
});

describe('recomputeCoverageFromWatermarks (via store)', () => {
    it('computes earliest/latest from multiple watermarks',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            coverageStart: '2026-02-16T08:00:00.000Z',
            coverageEnd: '2026-02-16T10:00:00.000Z',
            partition: 0,
        }));
        await store.putPartitionWatermark(buildWatermark({
            coverageStart: '2026-02-16T09:00:00.000Z',
            coverageEnd: '2026-02-16T12:00:00.000Z',
            partition: 1,
        }));
        const result = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(
            result!.earliestIndexedTime,
            '2026-02-16T08:00:00.000Z',
        );
        assert.equal(
            result!.latestIndexedTime,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('builds generationSpan map', async () => {
        const store = new InMemoryRestoreIndexStore();
        await store.putPartitionWatermark(buildWatermark({
            generationId: 'gen-a',
            partition: 0,
            topic: 'rez.cdc',
        }));
        await store.putPartitionWatermark(buildWatermark({
            generationId: 'gen-b',
            partition: 1,
            topic: 'rez.cdc',
        }));
        const result = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(result!.generationSpan['rez.cdc:0'], 'gen-a');
        assert.equal(result!.generationSpan['rez.cdc:1'], 'gen-b');
    });

    it('returns null for empty watermark set', async () => {
        const store = new InMemoryRestoreIndexStore();
        const result = await store.recomputeSourceCoverage({
            instanceId: 'sn-dev-01',
            measuredAt: '2026-02-16T12:05:00.000Z',
            source: 'sn://acme-dev.service-now.com',
            tenantId: 'tenant-acme',
        });
        assert.equal(result, null);
    });
});

describe('store helper functions (tested via PostgresRestoreIndexStore)', () => {
    it('validateSqlIdentifier accepts valid identifiers', () => {
        const db = newDb();
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();

        try {
            const store = new PostgresRestoreIndexStore(
                'postgres://unused',
                {
                    pool: pool as any,
                    schemaName: 'valid_schema',
                },
            );
            assert.ok(store);
        } finally {
            pool.end();
        }
    });

    it('validateSqlIdentifier accepts underscore-prefixed', () => {
        const db = newDb();
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();

        try {
            const store = new PostgresRestoreIndexStore(
                'postgres://unused',
                {
                    pool: pool as any,
                    schemaName: '_private_schema',
                },
            );
            assert.ok(store);
        } finally {
            pool.end();
        }
    });

    it('validateSqlIdentifier throws on SQL injection', () => {
        assert.throws(
            () => new PostgresRestoreIndexStore(
                'postgres://unused',
                { schemaName: '"; DROP TABLE' },
            ),
            /identifier format/,
        );
    });

    it('validateSqlIdentifier throws on spaces', () => {
        assert.throws(
            () => new PostgresRestoreIndexStore(
                'postgres://unused',
                { schemaName: 'foo bar' },
            ),
            /identifier format/,
        );
    });

    it('validateSqlIdentifier throws on leading digit', () => {
        assert.throws(
            () => new PostgresRestoreIndexStore(
                'postgres://unused',
                { schemaName: '123start' },
            ),
            /identifier format/,
        );
    });

    it('empty schemaName falls back to default', () => {
        const db = newDb();
        const pgAdapter = db.adapters.createPg();
        const pool = new pgAdapter.Pool();

        try {
            const store = new PostgresRestoreIndexStore(
                'postgres://unused',
                {
                    pool: pool as any,
                    schemaName: '',
                },
            );
            assert.ok(store);
        } finally {
            pool.end();
        }
    });

    it('constructor throws when pgUrl is empty and no pool', () => {
        assert.throws(
            () => new PostgresRestoreIndexStore(''),
            /REZ_RESTORE_PG_URL is required/,
        );
    });
});
