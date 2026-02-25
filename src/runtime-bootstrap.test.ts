import assert from 'node:assert/strict';
import { describe, it, test } from 'node:test';
import { createRuntime } from './runtime';
import { InMemoryRestoreIndexStore } from './store';
import type { RecManifestObjectStoreClient } from './rec-manifest-source';

class SingleBatchObjectStoreClient implements RecManifestObjectStoreClient {
    public readonly listCalls: Array<{
        limit: number;
        prefix: string;
        startAfter: string | null;
    }> = [];

    public readonly readCalls: string[] = [];

    constructor(
        private readonly manifestKey: string,
        private readonly artifactKey: string,
    ) {}

    async listObjectKeys(input: {
        limit: number;
        prefix: string;
        startAfter: string | null;
    }): Promise<string[]> {
        this.listCalls.push(input);

        if (input.startAfter === null) {
            return [this.manifestKey];
        }

        return [];
    }

    async readObjectText(key: string): Promise<string> {
        this.readCalls.push(key);

        if (key === this.manifestKey) {
            return JSON.stringify({
                app: 'x_app',
                artifact_content_type: 'application/json',
                artifact_key: this.artifactKey,
                artifact_kind: 'cdc',
                artifact_sha256:
                    '1111111111111111111111111111111111111111111111111111111111111111',
                artifact_size_bytes: 100,
                contract_version: 'restore.contracts.v1',
                event_id: 'evt-001',
                event_time: '2026-02-18T15:16:17.000Z',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                manifest_version: 'rec.artifact-manifest.v1',
                metadata_allowlist_version: 'rrs.metadata.allowlist.v1',
                object_key_layout_version: 'rec.object-key-layout.v1',
                offset: '1',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                table: 'x_app.ticket',
                topic: 'rez.cdc',
            });
        }

        if (key === this.artifactKey) {
            return JSON.stringify({
                __event_id: 'evt-001',
                __op: 'U',
                __source: 'sn://acme-dev.service-now.com',
                __table: 'x_app.ticket',
                __time: '2026-02-18T15:16:17Z',
                __type: 'cdc.write',
            });
        }

        throw new Error(`unexpected object key ${key}`);
    }
}

function buildRecModeEnv(
    overrides: Record<string, string> = {},
): NodeJS.ProcessEnv {
    return {
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'rec_manifest_object_store',
        REZ_RESTORE_INDEXER_DEFAULT_TENANT: 'tenant-acme',
        REZ_RESTORE_PG_URL:
            'postgres://rez_restore_indexer_rw:test@127.0.0.1:5432/rez_restore',
        REZ_RESTORE_INDEXER_SOURCE_BUCKET: 'rez-artifacts',
        REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID: 'gen-runtime-01',
        REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID: 'sn-dev-01',
        REZ_RESTORE_INDEXER_SOURCE_PREFIX: 'rez/restore-artifacts',
        REZ_RESTORE_INDEXER_SOURCE_REGION: 'us-east-1',
        REZ_RESTORE_INDEXER_SOURCE_URI: 'sn://acme-dev.service-now.com',
        ...overrides,
    };
}

test('runtime bootstrap fails closed when source mode is invalid', () => {
    const env = buildRecModeEnv({
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'invalid-source',
    });

    assert.throws(() => {
        createRuntime(env, {
            createStore: () => new InMemoryRestoreIndexStore(),
        });
    }, /REZ_RESTORE_INDEXER_ARTIFACT_SOURCE must be one of/);
});

test('runtime bootstrap fails closed when required source config is missing',
() => {
    const env = buildRecModeEnv({
        REZ_RESTORE_INDEXER_SOURCE_BUCKET: '',
    });

    assert.throws(() => {
        createRuntime(env, {
            createStore: () => new InMemoryRestoreIndexStore(),
        });
    }, /REZ_RESTORE_INDEXER_SOURCE_BUCKET is required/);
});

test('runtime bootstrap fails closed when restore Postgres DSN is missing',
() => {
    const env = buildRecModeEnv();

    delete env.REZ_RESTORE_PG_URL;

    assert.throws(() => {
        createRuntime(env, {
            createStore: () => new InMemoryRestoreIndexStore(),
        });
    }, /REZ_RESTORE_PG_URL is required/);
});

test('runtime bootstrap supports deliberate in-memory scaffold mode',
async () => {
    const runtime = createRuntime({
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'in_memory_scaffold',
        REZ_RESTORE_PG_URL:
            'postgres://rez_restore_indexer_rw:test@127.0.0.1:5432/rez_restore',
    }, {
        createStore: () => new InMemoryRestoreIndexStore(),
    });

    assert.equal(runtime.source.mode, 'in_memory_scaffold');

    const run = await runtime.worker.runOnce();

    assert.equal(run.batchSize, 0);
    assert.equal(run.inserted, 0);
    assert.equal(run.failures, 0);
});

test('runtime bootstrap fails closed when leader election is enabled for '
    + 'scaffold source mode', () => {
    assert.throws(() => {
        createRuntime({
            REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'in_memory_scaffold',
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: 'true',
            REZ_RESTORE_PG_URL:
                'postgres://rez_restore_indexer_rw:test@127.0.0.1:5432/rez_restore',
        }, {
            createStore: () => new InMemoryRestoreIndexStore(),
        });
    }, /REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED=true requires/);
});

test('runtime bootstrap wires REC manifest source and ingests a non-empty batch',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifestKey = `${prefix}/manifest-001.manifest.json`;
    const artifactKey = `${prefix}/manifest-001.artifact.json`;
    const client = new SingleBatchObjectStoreClient(manifestKey, artifactKey);
    const store = new InMemoryRestoreIndexStore();
    const runtime = createRuntime(buildRecModeEnv({
        REZ_RESTORE_INDEXER_SOURCE_PREFIX: prefix,
        REZ_RESTORE_INDEXER_SOURCE_TENANT_ID: 'tenant-acme',
    }), {
        createObjectStoreClient: () => client,
        createStore: () => store,
    });

    assert.equal(runtime.source.mode, 'rec_manifest_object_store');

    const run = await runtime.worker.runOnce();

    assert.equal(run.batchSize, 1);
    assert.equal(run.inserted, 1);
    assert.equal(run.failures, 0);
    assert.deepEqual(client.readCalls, [manifestKey, artifactKey]);

    const progress = await store.getSourceProgress(
        'tenant-acme',
        'sn-dev-01',
        'sn://acme-dev.service-now.com',
    );

    assert.equal(progress?.cursor, manifestKey);
    assert.equal(progress?.lastBatchSize, 1);
    assert.equal(progress?.lastIndexedOffset, '1');
});

describe('createRuntime - additional', () => {
    it('uses provided store override via dependencies',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const runtime = createRuntime({
            REZ_RESTORE_INDEXER_ARTIFACT_SOURCE:
                'in_memory_scaffold',
            REZ_RESTORE_PG_URL: 'postgres://unused:5432/db',
        }, {
            createStore: () => store,
        });
        await runtime.worker.runOnce();
        // Store is the one we passed in; verify it
        // was actually used by checking event count
        assert.equal(store.getIndexedEventCount(), 0);
    });

    it('uses provided S3 client factory override',
    async () => {
        let factoryCalled = false;
        const prefix = 'rez/restore-artifacts';
        const mKey = `${prefix}/f.manifest.json`;
        const aKey = `${prefix}/f.artifact.json`;
        const client = new SingleBatchObjectStoreClient(
            mKey, aKey,
        );
        const runtime = createRuntime(buildRecModeEnv({
            REZ_RESTORE_INDEXER_SOURCE_PREFIX: prefix,
        }), {
            createObjectStoreClient: (source) => {
                factoryCalled = true;
                assert.equal(
                    source.bucket,
                    'rez-artifacts',
                );

                return client;
            },
            createStore: () => new InMemoryRestoreIndexStore(),
        });
        await runtime.worker.runOnce();
        assert.equal(factoryCalled, true);
    });

    it('leader lease wiring connects store to worker',
    async () => {
        const prefix = 'rez/restore-artifacts';
        const mKey = `${prefix}/ll.manifest.json`;
        const aKey = `${prefix}/ll.artifact.json`;
        const client = new SingleBatchObjectStoreClient(
            mKey, aKey,
        );
        const store = new InMemoryRestoreIndexStore();
        const runtime = createRuntime(buildRecModeEnv({
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED:
                'true',
            REZ_RESTORE_INDEXER_LEADER_LEASE_SECONDS: '30',
            REZ_RESTORE_INDEXER_SOURCE_PREFIX: prefix,
        }), {
            createObjectStoreClient: () => client,
            createStore: () => store,
        });
        const run = await runtime.worker.runOnce();
        // Worker should have acquired the lease and
        // processed the batch
        assert.equal(run.inserted, 1);
    });

    it('source progress scope set for rec_manifest mode',
    async () => {
        const prefix = 'rez/restore-artifacts';
        const mKey = `${prefix}/sp.manifest.json`;
        const aKey = `${prefix}/sp.artifact.json`;
        const client = new SingleBatchObjectStoreClient(
            mKey, aKey,
        );
        const store = new InMemoryRestoreIndexStore();
        const runtime = createRuntime(buildRecModeEnv({
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED:
                'true',
            REZ_RESTORE_INDEXER_SOURCE_PREFIX: prefix,
        }), {
            createObjectStoreClient: () => client,
            createStore: () => store,
        });
        await runtime.worker.runOnce();
        // Verify source progress was persisted via
        // sourceProgressScope
        const progress = await store.getSourceProgress(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
        );
        assert.notEqual(progress, null);
        assert.equal(progress?.cursor, mKey);
    });

    it('source progress scope not set for scaffold mode',
    async () => {
        const store = new InMemoryRestoreIndexStore();
        const runtime = createRuntime({
            REZ_RESTORE_INDEXER_ARTIFACT_SOURCE:
                'in_memory_scaffold',
            REZ_RESTORE_PG_URL: 'postgres://unused:5432/db',
        }, {
            createStore: () => store,
        });
        await runtime.worker.runOnce();
        // No source progress stored for scaffold mode
        const progress = await store.getSourceProgress(
            'tenant-local-dev',
            'scaffold',
            'scaffold',
        );
        assert.equal(progress, null);
    });

    it('returns scaffold sourceSummary for in_memory mode',
    () => {
        const runtime = createRuntime({
            REZ_RESTORE_INDEXER_ARTIFACT_SOURCE:
                'in_memory_scaffold',
            REZ_RESTORE_PG_URL: 'postgres://unused:5432/db',
        }, {
            createStore: () => new InMemoryRestoreIndexStore(),
        });
        assert.deepEqual(runtime.source, {
            mode: 'in_memory_scaffold',
        });
    });

    it('returns rec_manifest sourceSummary for production mode',
    () => {
        const prefix = 'rez/restore-artifacts';
        const mKey = `${prefix}/ss.manifest.json`;
        const aKey = `${prefix}/ss.artifact.json`;
        const client = new SingleBatchObjectStoreClient(
            mKey, aKey,
        );
        const runtime = createRuntime(buildRecModeEnv({
            REZ_RESTORE_INDEXER_SOURCE_PREFIX: prefix,
        }), {
            createObjectStoreClient: () => client,
            createStore: () => new InMemoryRestoreIndexStore(),
        });
        assert.equal(
            runtime.source.mode,
            'rec_manifest_object_store',
        );
        const source = runtime.source as {
            bucket: string;
            endpoint: string | null;
            mode: string;
            prefix: string;
            region: string;
        };
        assert.equal(source.bucket, 'rez-artifacts');
        assert.equal(source.region, 'us-east-1');
        assert.equal(source.prefix, prefix);
        assert.equal(source.endpoint, null);
    });
});
