import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    RecManifestArtifactBatchSource,
    type RecManifestObjectStoreClient,
} from './rec-manifest-source.js';

const CONTRACT_VERSION = 'restore.contracts.v1';
const MANIFEST_VERSION = 'rec.artifact-manifest.v1';
const ALLOWLIST_VERSION = 'rrs.metadata.allowlist.v1';
const KEY_LAYOUT_VERSION = 'rec.object-key-layout.v1';
const KEY_LAYOUT_VERSION_V2 = 'rec.object-key-layout.v2';

function buildValidManifest(overrides?: Record<string, unknown>) {
    return {
        artifact_key: 'rez/restore/event=evt-1.artifact.json',
        artifact_kind: 'cdc',
        app: null,
        contract_version: CONTRACT_VERSION,
        event_id: 'evt-1',
        event_time: '2026-02-16T12:00:00.000Z',
        event_type: 'cdc.insert',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        manifest_version: MANIFEST_VERSION,
        metadata_allowlist_version: ALLOWLIST_VERSION,
        object_key_layout_version: KEY_LAYOUT_VERSION,
        offset: '42',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        table: 'incident',
        topic: 'rez.cdc',
        ...overrides,
    };
}

function buildValidArtifact(overrides?: Record<string, unknown>) {
    return {
        __event_id: 'evt-1',
        __source: 'sn://acme-dev.service-now.com',
        __time: '2026-02-16T12:00:00.000Z',
        __type: 'cdc.insert',
        __op: 'I',
        __record_sys_id: 'sys-rec-1',
        __table: 'incident',
        tenant_id: 'tenant-acme',
        instance_id: 'sn-dev-01',
        topic: 'rez.cdc',
        partition: 0,
        offset: '42',
        ...overrides,
    };
}

function createMockClient(
    manifests: Record<string, unknown>,
    artifacts: Record<string, unknown>,
    keyList?: string[],
): RecManifestObjectStoreClient {
    const allKeys = keyList ?? Object.keys(manifests);

    return {
        listObjectKeys: async (input) => {
            let filtered = allKeys.filter((k) => {
                return k.startsWith(input.prefix);
            });

            if (input.startAfter !== null) {
                filtered = filtered.filter((k) => {
                    return k > input.startAfter!;
                });
            }

            return filtered.sort().slice(0, input.limit);
        },
        readObjectText: async (key) => {
            if (key in manifests) {
                return JSON.stringify(manifests[key]);
            }

            if (key in artifacts) {
                return JSON.stringify(artifacts[key]);
            }

            throw new Error(`key not found: ${key}`);
        },
    };
}

function createSource(
    client: RecManifestObjectStoreClient,
    overrides?: Partial<{
        generationId: string;
        ingestionMode: 'realtime' | 'bootstrap' | 'gap_repair';
        maxAttempts: number;
        prefix: string;
        retryBaseMs: number;
        tenantId: string;
        timeProvider: () => string;
    }>,
) {
    return new RecManifestArtifactBatchSource(client, {
        generationId: overrides?.generationId ?? 'gen-1',
        ingestionMode: overrides?.ingestionMode,
        maxAttempts: overrides?.maxAttempts ?? 1,
        prefix: overrides?.prefix ?? 'rez/restore',
        retryBaseMs: overrides?.retryBaseMs ?? 10,
        sleep: async () => {},
        tenantId: overrides?.tenantId ?? 'tenant-acme',
        timeProvider: overrides?.timeProvider
            ?? (() => '2026-02-16T12:01:00.000Z'),
    });
}

describe('parseManifest (tested via readBatch)', () => {
    it('parses valid manifest with all fields', async () => {
        const manifest = buildValidManifest();
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/evt-1.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 1);
        assert.equal(batch.items[0].manifest.event_id, 'evt-1');
        assert.equal(batch.items[0].manifest.offset, '42');
    });

    it('accepts rec.object-key-layout.v2', async () => {
        const manifest = buildValidManifest({
            object_key_layout_version: KEY_LAYOUT_VERSION_V2,
        });
        const artifact = buildValidArtifact();
        const manifestKey = 'rez/restore/evt-v2.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });

        assert.equal(batch.items.length, 1);
        assert.equal(
            batch.items[0].manifest.object_key_layout_version,
            KEY_LAYOUT_VERSION_V2,
        );
    });

    it('throws on non-object input', async () => {
        const manifestKey =
            'rez/restore/bad.manifest.json';
        const client = createMockClient(
            {},
            {},
            [manifestKey],
        );

        client.readObjectText = async (key) => {
            if (key === manifestKey) {
                return '"not an object"';
            }

            throw new Error(`key not found: ${key}`);
        };
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /manifest root must be an object/,
        );
    });

    it('throws on missing contract_version', async () => {
        const manifest = buildValidManifest();
        delete (manifest as Record<string, unknown>).contract_version;
        const manifestKey =
            'rez/restore/missing-cv.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /contract_version is required/,
        );
    });

    it('throws on unsupported contract_version', async () => {
        const manifest = buildValidManifest({
            contract_version: 'bad.version',
        });
        const manifestKey =
            'rez/restore/bad-cv.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /unsupported manifest\.contract_version/,
        );
    });

    it('throws on missing manifest_version', async () => {
        const manifest = buildValidManifest();
        delete (manifest as Record<string, unknown>).manifest_version;
        const manifestKey =
            'rez/restore/missing-mv.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /manifest_version is required/,
        );
    });

    it('throws on unsupported manifest_version', async () => {
        const manifest = buildValidManifest({
            manifest_version: 'bad.version',
        });
        const manifestKey =
            'rez/restore/bad-mv.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /unsupported manifest\.manifest_version/,
        );
    });

    it('throws on unsupported metadata_allowlist_version',
    async () => {
        const manifest = buildValidManifest({
            metadata_allowlist_version: 'bad.version',
        });
        const manifestKey =
            'rez/restore/bad-alv.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /unsupported manifest\.metadata_allowlist_version/,
        );
    });

    it('throws on unsupported object_key_layout_version',
    async () => {
        const manifest = buildValidManifest({
            object_key_layout_version: 'bad.version',
        });
        const manifestKey =
            'rez/restore/bad-okl.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /unsupported manifest\.object_key_layout_version/,
        );
    });

    it('throws on missing kafka topic', async () => {
        const manifest = buildValidManifest();
        delete (manifest as Record<string, unknown>).topic;
        const manifestKey =
            'rez/restore/missing-topic.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /manifest missing kafka coordinates/,
        );
    });

    it('throws on missing kafka partition', async () => {
        const manifest = buildValidManifest();
        delete (manifest as Record<string, unknown>).partition;
        const manifestKey =
            'rez/restore/missing-part.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /manifest missing kafka coordinates/,
        );
    });

    it('throws on missing kafka offset', async () => {
        const manifest = buildValidManifest();
        delete (manifest as Record<string, unknown>).offset;
        const manifestKey =
            'rez/restore/missing-off.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            {},
        );
        const source = createSource(client);
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /manifest missing kafka coordinates/,
        );
    });

    it('canonicalizes offset', async () => {
        const manifest = buildValidManifest({ offset: '00042' });
        const artifact = buildValidArtifact({ offset: '42' });
        const manifestKey =
            'rez/restore/canon-off.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items[0].manifest.offset, '42');
    });

    it('canonicalizes event_time', async () => {
        const manifest = buildValidManifest({
            event_time: '2026-02-16T12:00:00Z',
        });
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/canon-time.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].manifest.event_time,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('parses optional fields', async () => {
        const manifest = buildValidManifest({
            source: 'sn://test.com',
            instance_id: 'inst-1',
            event_type: 'cdc.update',
        });
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/opt-fields.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items[0].manifest.source, 'sn://test.com');
        assert.equal(batch.items[0].manifest.instance_id, 'inst-1');
        assert.equal(
            batch.items[0].manifest.event_type,
            'cdc.update',
        );
    });
});

describe('extractMetadataFromArtifact (tested via readBatch)',
() => {
    it('extracts allowlisted fields', async () => {
        const manifest = buildValidManifest();
        const artifact = buildValidArtifact({
            tenant_id: 'tenant-acme',
            instance_id: 'sn-dev-01',
            size_bytes: 1024,
        });
        const manifestKey =
            'rez/restore/allow.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.tenant_id,
            'tenant-acme',
        );
        assert.equal(
            batch.items[0].metadata.instance_id,
            'sn-dev-01',
        );
        assert.equal(batch.items[0].metadata.size_bytes, 1024);
    });

    it('maps __event_id to event_id', async () => {
        const manifest = buildValidManifest();
        const artifact = buildValidArtifact({
            __event_id: 'mapped-evt-id',
        });
        const manifestKey =
            'rez/restore/eid.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.event_id,
            'mapped-evt-id',
        );
    });

    it('maps __time correctly', async () => {
        const manifest = buildValidManifest();
        const artifact = buildValidArtifact({
            __time: '2026-02-16T13:00:00.000Z',
        });
        const manifestKey =
            'rez/restore/time.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.__time,
            '2026-02-16T13:00:00.000Z',
        );
    });

    it('handles event.sys_id -> record_sys_id mapping',
    async () => {
        const manifest = buildValidManifest();
        const artifact = {
            event: {
                id: 'event-id-123',
                source: 'sn://test.com',
                time: '2026-02-16T12:00:00.000Z',
                type: 'cdc.insert',
            },
        };
        const manifestKey =
            'rez/restore/event-map.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.event_id,
            'event-id-123',
        );
        assert.equal(
            batch.items[0].metadata.source,
            'sn://test.com',
        );
    });

    it('handles media object fields', async () => {
        const manifest = buildValidManifest();
        const artifact = {
            media: {
                op: 'I',
                size_bytes: 2048,
                content_type: 'image/png',
            },
        };
        const manifestKey =
            'rez/restore/media.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items[0].metadata.operation, 'I');
        assert.equal(batch.items[0].metadata.size_bytes, 2048);
        assert.equal(
            batch.items[0].metadata.content_type,
            'image/png',
        );
    });

    it('returns empty metadata for minimal artifact',
    async () => {
        const manifest = buildValidManifest();
        const artifact = {};
        const manifestKey =
            'rez/restore/empty.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.tenant_id,
            'tenant-acme',
        );
        assert.equal(
            batch.items[0].metadata.event_id,
            manifest.event_id,
        );
    });

    it('does not fallback to configured tenant when canonical tenant is absent',
    async () => {
        const manifest = buildValidManifest({
            tenant_id: undefined,
        });
        const artifact = buildValidArtifact({
            tenant_id: undefined,
        });
        const manifestKey =
            'rez/restore/no-tenant.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client, {
            tenantId: 'tenant-from-source-config',
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            batch.items[0].metadata.tenant_id,
            undefined,
        );
        assert.equal(
            batch.items[0].tenantId,
            'tenant-from-source-config',
        );
    });

    it('ignores non-allowlisted fields', async () => {
        const manifest = buildValidManifest();
        const artifact = buildValidArtifact({
            secret_field: 'should-be-excluded',
            password: 'also-excluded',
        });
        const manifestKey =
            'rez/restore/nonallow.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(
            (batch.items[0].metadata as Record<string, unknown>)
                .secret_field,
            undefined,
        );
        assert.equal(
            (batch.items[0].metadata as Record<string, unknown>)
                .password,
            undefined,
        );
    });
});

describe('isTransientObjectStoreError (tested via retry)',
() => {
    it('classifies 429 as transient', async () => {
        let attempts = 0;
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                attempts += 1;

                if (attempts === 1) {
                    const err = new Error('Too Many Requests');

                    (err as any).$metadata = {
                        httpStatusCode: 429,
                    };

                    throw err;
                }

                return [
                    'rez/restore/retry.manifest.json',
                ];
            },
            readObjectText: async (key) => {
                if (key.endsWith('.manifest.json')) {
                    return JSON.stringify(buildValidManifest());
                }

                return JSON.stringify(buildValidArtifact());
            },
        };
        const source = createSource(client, {
            maxAttempts: 3,
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(attempts, 2);
        assert.equal(batch.items.length, 1);
    });

    it('classifies 500 as transient', async () => {
        let attempts = 0;
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                attempts += 1;

                if (attempts === 1) {
                    const err = new Error('Internal Server Error');

                    (err as any).$metadata = {
                        httpStatusCode: 500,
                    };

                    throw err;
                }

                return [
                    'rez/restore/s500.manifest.json',
                ];
            },
            readObjectText: async (key) => {
                if (key.endsWith('.manifest.json')) {
                    return JSON.stringify(buildValidManifest());
                }

                return JSON.stringify(buildValidArtifact());
            },
        };
        const source = createSource(client, {
            maxAttempts: 3,
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(attempts, 2);
    });

    it('classifies ECONNRESET as transient', async () => {
        let attempts = 0;
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                attempts += 1;

                if (attempts === 1) {
                    const err = new Error('Connection reset');

                    (err as any).code = 'ECONNRESET';

                    throw err;
                }

                return [
                    'rez/restore/econn.manifest.json',
                ];
            },
            readObjectText: async (key) => {
                if (key.endsWith('.manifest.json')) {
                    return JSON.stringify(buildValidManifest());
                }

                return JSON.stringify(buildValidArtifact());
            },
        };
        const source = createSource(client, {
            maxAttempts: 3,
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(attempts, 2);
    });

    it('classifies 403 as non-transient', async () => {
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                const err = new Error('Forbidden');

                (err as any).$metadata = {
                    httpStatusCode: 403,
                };

                throw err;
            },
            readObjectText: async () => {
                throw new Error('unreachable');
            },
        };
        const source = createSource(client, {
            maxAttempts: 3,
        });
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /listObjectKeys failed after attempt 1/,
        );
    });

    it('classifies unknown error as non-transient', async () => {
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                throw new Error('something unexpected');
            },
            readObjectText: async () => {
                throw new Error('unreachable');
            },
        };
        const source = createSource(client, {
            maxAttempts: 3,
        });
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /listObjectKeys failed after attempt 1/,
        );
    });
});

describe('RecManifestArtifactBatchSource constructor', () => {
    it('throws when prefix is empty', () => {
        const client = createMockClient({}, {});
        assert.throws(() => {
            new RecManifestArtifactBatchSource(client, {
                generationId: 'gen-1',
                prefix: '',
                tenantId: 'tenant-acme',
            });
        }, /prefix must not be empty/);
    });

    it('throws when tenantId is empty', () => {
        const client = createMockClient({}, {});
        assert.throws(() => {
            new RecManifestArtifactBatchSource(client, {
                generationId: 'gen-1',
                prefix: 'rez/restore',
                tenantId: '',
            });
        }, /tenantId must not be empty/);
    });

    it('throws when generationId is empty', () => {
        const client = createMockClient({}, {});
        assert.throws(() => {
            new RecManifestArtifactBatchSource(client, {
                generationId: '',
                prefix: 'rez/restore',
                tenantId: 'tenant-acme',
            });
        }, /generationId must not be empty/);
    });
});

describe('RecManifestArtifactBatchSource.readBatch', () => {
    it('returns empty batch when limit <= 0', async () => {
        const client = createMockClient({}, {});
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 0,
        });
        assert.equal(batch.items.length, 0);
    });

    it('returns empty batch when no manifests found',
    async () => {
        const client = createMockClient({}, {});
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 0);
    });

    it('calculates realtimeLagSeconds', async () => {
        const manifest = buildValidManifest({
            event_time: '2026-02-16T12:00:00.000Z',
        });
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/lag.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client, {
            timeProvider: () => '2026-02-16T12:01:00.000Z',
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, 60);
    });

    it('returns nextCursor for pagination', async () => {
        const m1 = buildValidManifest({
            event_id: 'evt-pag-1',
            artifact_key:
                'rez/restore/event=evt-pag-1.artifact.json',
        });
        const m2 = buildValidManifest({
            event_id: 'evt-pag-2',
            artifact_key:
                'rez/restore/event=evt-pag-2.artifact.json',
        });
        const k1 = 'rez/restore/evt-pag-1.manifest.json';
        const k2 = 'rez/restore/evt-pag-2.manifest.json';
        const client = createMockClient(
            { [k1]: m1, [k2]: m2 },
            {
                [m1.artifact_key]: buildValidArtifact({
                    __event_id: 'evt-pag-1',
                }),
                [m2.artifact_key]: buildValidArtifact({
                    __event_id: 'evt-pag-2',
                }),
            },
        );
        const source = createSource(client);
        const batch1 = await source.readBatch({
            cursor: null,
            limit: 1,
        });
        assert.equal(batch1.items.length, 1);
        assert.notEqual(batch1.nextCursor, null);
        const batch2 = await source.readBatch({
            cursor: batch1.nextCursor,
            limit: 1,
        });
        assert.equal(batch2.items.length, 1);
    });
});

describe('computeRetryDelayMs (tested via retry)', () => {
    it('retries with exponential backoff', async () => {
        const delays: number[] = [];
        let attempts = 0;
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                attempts += 1;

                if (attempts <= 2) {
                    const err = new Error('timeout');

                    (err as any).code = 'ETIMEDOUT';

                    throw err;
                }

                return [
                    'rez/restore/backoff.manifest.json',
                ];
            },
            readObjectText: async (key) => {
                if (key.endsWith('.manifest.json')) {
                    return JSON.stringify(buildValidManifest());
                }

                return JSON.stringify(buildValidArtifact());
            },
        };
        const source = new RecManifestArtifactBatchSource(client, {
            generationId: 'gen-1',
            maxAttempts: 5,
            prefix: 'rez/restore',
            retryBaseMs: 100,
            sleep: async (ms: number) => {
                delays.push(ms);
            },
            tenantId: 'tenant-acme',
            timeProvider: () => '2026-02-16T12:01:00.000Z',
        });
        await source.readBatch({ cursor: null, limit: 10 });
        assert.equal(delays.length, 2);
        assert.equal(delays[0], 100);
        assert.equal(delays[1], 200);
    });

    it('throws after maxAttempts exhausted', async () => {
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                const err = new Error('timeout');

                (err as any).code = 'ETIMEDOUT';

                throw err;
            },
            readObjectText: async () => {
                throw new Error('unreachable');
            },
        };
        const source = createSource(client, {
            maxAttempts: 2,
        });
        await assert.rejects(
            source.readBatch({ cursor: null, limit: 10 }),
            /listObjectKeys failed after attempt 2/,
        );
    });

    it('caps delay at 2^7 multiplier', async () => {
        const delays: number[] = [];
        let attempts = 0;
        const client: RecManifestObjectStoreClient = {
            listObjectKeys: async () => {
                attempts += 1;

                if (attempts <= 9) {
                    const err = new Error('timeout');

                    (err as any).code = 'ETIMEDOUT';

                    throw err;
                }

                return [
                    'rez/restore/cap.manifest.json',
                ];
            },
            readObjectText: async (key) => {
                if (key.endsWith('.manifest.json')) {
                    return JSON.stringify(buildValidManifest());
                }

                return JSON.stringify(buildValidArtifact());
            },
        };
        const source = new RecManifestArtifactBatchSource(client, {
            generationId: 'gen-1',
            maxAttempts: 12,
            prefix: 'rez/restore',
            retryBaseMs: 10,
            sleep: async (ms: number) => {
                delays.push(ms);
            },
            tenantId: 'tenant-acme',
            timeProvider: () => '2026-02-16T12:01:00.000Z',
        });
        await source.readBatch({ cursor: null, limit: 10 });
        assert.equal(delays.length, 9);
        // attempt 1: 10*2^0=10, attempt 2: 10*2^1=20, ...
        // attempt 8: 10*2^7=1280, attempt 9: capped at 10*2^7=1280
        assert.equal(delays[6], 10 * (2 ** 6));  // 640
        assert.equal(delays[7], 10 * (2 ** 7));  // 1280
        assert.equal(delays[8], 10 * (2 ** 7));  // capped at 1280
    });
});

describe('computeRealtimeLagSeconds (tested via readBatch)', () => {
    it('returns null when no items have event_time', async () => {
        const client = createMockClient({}, {});
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, null);
    });

    it('returns 0 for future eventTime', async () => {
        const manifest = buildValidManifest({
            event_time: '2026-02-16T13:00:00.000Z',
        });
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/future.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client, {
            timeProvider: () => '2026-02-16T12:00:00.000Z',
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, 0);
    });

    it('returns correct seconds for past eventTime', async () => {
        const manifest = buildValidManifest({
            event_time: '2026-02-16T11:58:30.000Z',
        });
        const artifact = buildValidArtifact();
        const manifestKey =
            'rez/restore/past.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client, {
            timeProvider: () => '2026-02-16T12:00:00.000Z',
        });
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.realtimeLagSeconds, 90);
    });

});

describe('readBatch merge behavior', () => {
    it('merges manifest and artifact metadata', async () => {
        const manifest = buildValidManifest({
            source: 'sn://merge-test.service-now.com',
            instance_id: 'merge-inst',
        });
        const artifact = buildValidArtifact({
            tenant_id: 'tenant-merge',
            __table: 'change_request',
            __op: 'U',
        });
        const manifestKey =
            'rez/restore/merge.manifest.json';
        const client = createMockClient(
            { [manifestKey]: manifest },
            { [manifest.artifact_key]: artifact },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        const item = batch.items[0];
        // Manifest-level fields
        assert.equal(
            item.manifest.source,
            'sn://merge-test.service-now.com',
        );
        assert.equal(item.manifest.instance_id, 'merge-inst');
        assert.equal(item.manifest.event_id, 'evt-1');
        // Artifact-level metadata
        assert.equal(
            item.metadata.tenant_id,
            'tenant-merge',
        );
        assert.equal(item.metadata.table, 'change_request');
        assert.equal(item.metadata.operation, 'U');
    });

    it('readBatch with null cursor starts from beginning',
    async () => {
        const m1 = buildValidManifest({
            event_id: 'evt-start-1',
            artifact_key:
                'rez/restore/event=evt-start-1.artifact.json',
        });
        const k1 = 'rez/restore/evt-start-1.manifest.json';
        const client = createMockClient(
            { [k1]: m1 },
            {
                [m1.artifact_key]: buildValidArtifact({
                    __event_id: 'evt-start-1',
                }),
            },
        );
        const source = createSource(client);
        const batch = await source.readBatch({
            cursor: null,
            limit: 10,
        });
        assert.equal(batch.items.length, 1);
        assert.equal(
            batch.items[0].manifest.event_id,
            'evt-start-1',
        );
    });
});
