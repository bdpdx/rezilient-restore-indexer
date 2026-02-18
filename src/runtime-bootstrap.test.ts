import assert from 'node:assert/strict';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
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

function buildStateDbPath(
    suffix: string,
): string {
    return path.join(
        tmpdir(),
        `rri-runtime-bootstrap-${process.pid}-${suffix}.sqlite`,
    );
}

function buildRecModeEnv(
    overrides: Record<string, string> = {},
): NodeJS.ProcessEnv {
    return {
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'rec_manifest_object_store',
        REZ_RESTORE_INDEXER_DEFAULT_TENANT: 'tenant-acme',
        REZ_RESTORE_INDEXER_SOURCE_BUCKET: 'rez-artifacts',
        REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID: 'gen-runtime-01',
        REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID: 'sn-dev-01',
        REZ_RESTORE_INDEXER_SOURCE_PREFIX: 'rez/restore-artifacts',
        REZ_RESTORE_INDEXER_SOURCE_REGION: 'us-east-1',
        REZ_RESTORE_INDEXER_SOURCE_URI: 'sn://acme-dev.service-now.com',
        REZ_RESTORE_INDEXER_STATE_DB_PATH: buildStateDbPath('rec'),
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

test('runtime bootstrap supports deliberate in-memory scaffold mode',
async () => {
    const runtime = createRuntime({
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'in_memory_scaffold',
        REZ_RESTORE_INDEXER_STATE_DB_PATH: buildStateDbPath('scaffold'),
    }, {
        createStore: () => new InMemoryRestoreIndexStore(),
    });

    assert.equal(runtime.source.mode, 'in_memory_scaffold');

    const run = await runtime.worker.runOnce();

    assert.equal(run.batchSize, 0);
    assert.equal(run.inserted, 0);
    assert.equal(run.failures, 0);
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
