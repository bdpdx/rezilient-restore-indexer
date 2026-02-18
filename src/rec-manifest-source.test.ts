import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    RecManifestArtifactBatchSource,
    type RecManifestObjectStoreClient,
} from './rec-manifest-source';

type ListStep = {
    error?: Error;
    keys?: string[];
};

type ReadStep = {
    body?: string;
    error?: Error;
};

class ScriptedObjectStoreClient implements RecManifestObjectStoreClient {
    private readonly readStepIndex = new Map<string, number>();

    private listStepIndex = 0;

    public readonly listCalls: Array<{
        limit: number;
        prefix: string;
        startAfter: string | null;
    }> = [];

    public readonly readCalls: string[] = [];

    constructor(
        private readonly listSteps: ListStep[],
        private readonly readSteps: Map<string, ReadStep[]>,
    ) {}

    async listObjectKeys(input: {
        limit: number;
        prefix: string;
        startAfter: string | null;
    }): Promise<string[]> {
        this.listCalls.push(input);
        const step = this.nextListStep();

        if (step.error) {
            throw step.error;
        }

        return step.keys || [];
    }

    async readObjectText(key: string): Promise<string> {
        this.readCalls.push(key);

        const scripted = this.readSteps.get(key);

        if (!scripted || scripted.length === 0) {
            throw new Error(`missing scripted object for key ${key}`);
        }

        const stepIndex = this.readStepIndex.get(key) || 0;
        const boundedIndex = Math.min(stepIndex, scripted.length - 1);
        const step = scripted[boundedIndex];

        this.readStepIndex.set(key, stepIndex + 1);

        if (step.error) {
            throw step.error;
        }

        if (step.body === undefined) {
            throw new Error(`missing scripted body for key ${key}`);
        }

        return step.body;
    }

    private nextListStep(): ListStep {
        if (this.listSteps.length === 0) {
            return {
                keys: [],
            };
        }

        const boundedIndex = Math.min(
            this.listStepIndex,
            this.listSteps.length - 1,
        );
        const step = this.listSteps[boundedIndex];

        this.listStepIndex += 1;

        return step;
    }
}

function transientError(
    code: string,
    message: string,
): Error & {
    code: string;
} {
    const error = new Error(message) as Error & {
        code: string;
    };

    error.code = code;

    return error;
}

function manifestKey(
    prefix: string,
    id: string,
): string {
    return `${prefix}/manifest-${id}.manifest.json`;
}

function artifactKey(
    prefix: string,
    id: string,
): string {
    return `${prefix}/manifest-${id}.artifact.json`;
}

function buildManifest(params: {
    app?: string | null;
    artifactKey: string;
    eventId: string;
    eventTime: string;
    eventType?: string;
    instanceId?: string;
    kind?: 'cdc' | 'schema' | 'media' | 'error' | 'repair';
    offset: string;
    partition?: number;
    source?: string;
    table?: string | null;
    topic?: string;
}): Record<string, unknown> {
    return {
        app: params.app === undefined ? 'x_app' : params.app,
        artifact_content_type: 'application/json',
        artifact_key: params.artifactKey,
        artifact_kind: params.kind || 'cdc',
        artifact_sha256:
            '1111111111111111111111111111111111111111111111111111111111111111',
        artifact_size_bytes: 100,
        contract_version: 'restore.contracts.v1',
        event_id: params.eventId,
        event_time: params.eventTime,
        event_type: params.eventType || 'cdc.write',
        instance_id: params.instanceId || 'sn-dev-01',
        manifest_version: 'rec.artifact-manifest.v1',
        metadata_allowlist_version: 'rrs.metadata.allowlist.v1',
        object_key_layout_version: 'rec.object-key-layout.v1',
        offset: params.offset,
        partition: params.partition ?? 0,
        source: params.source || 'sn://acme-dev.service-now.com',
        table: params.table === undefined ? 'x_app.ticket' : params.table,
        topic: params.topic || 'rez.cdc',
    };
}

function buildCdcArtifact(params: {
    eventId: string;
    eventTime: string;
    source?: string;
    table?: string;
}): Record<string, unknown> {
    return {
        __event_id: params.eventId,
        __op: 'U',
        __record_sys_id: `record-${params.eventId}`,
        __schema_version: 4,
        __source: params.source || 'sn://acme-dev.service-now.com',
        __sys_mod_count: 3,
        __sys_updated_on: '2026-02-18 09:00:00',
        __table: params.table || 'x_app.ticket',
        __time: params.eventTime,
        __type: 'cdc.write',
        non_allowlisted: 'drop-me',
        row: {
            plaintext: true,
        },
    };
}

test('rec manifest source paginates deterministically with cursor progress',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifest001 = manifestKey(prefix, '001');
    const artifact001 = artifactKey(prefix, '001');
    const manifest002 = manifestKey(prefix, '002');
    const artifact002 = artifactKey(prefix, '002');
    const manifest003 = manifestKey(prefix, '003');
    const artifact003 = artifactKey(prefix, '003');

    const client = new ScriptedObjectStoreClient([
        {
            keys: [
                manifest003,
                artifact003,
                manifest001,
                `${prefix}/ignore.txt`,
                manifest002,
            ],
        },
        {
            keys: [manifest003, manifest001, manifest002],
        },
        {
            keys: [manifest003, manifest002],
        },
    ], new Map([
        [
            manifest001,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact001,
                    eventId: 'evt-001',
                    eventTime: '2026-02-18T10:00:00.000Z',
                    offset: '1',
                })),
            }],
        ],
        [
            artifact001,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-001',
                    eventTime: '2026-02-18T10:00:00.000Z',
                })),
            }],
        ],
        [
            manifest002,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact002,
                    eventId: 'evt-002',
                    eventTime: '2026-02-18T10:00:10.000Z',
                    offset: '2',
                })),
            }],
        ],
        [
            artifact002,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-002',
                    eventTime: '2026-02-18T10:00:10.000Z',
                })),
            }],
        ],
        [
            manifest003,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact003,
                    eventId: 'evt-003',
                    eventTime: '2026-02-18T10:00:20.000Z',
                    offset: '3',
                })),
            }],
        ],
        [
            artifact003,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-003',
                    eventTime: '2026-02-18T10:00:20.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T10:01:20.000Z',
    });

    const first = await source.readBatch({
        cursor: null,
        limit: 2,
    });

    assert.equal(first.items.length, 2);
    assert.equal(first.items[0]?.manifest.event_id, 'evt-001');
    assert.equal(first.items[1]?.manifest.event_id, 'evt-002');
    assert.equal(first.nextCursor, manifest002);
    assert.equal(first.realtimeLagSeconds, 70);

    const second = await source.readBatch({
        cursor: first.nextCursor,
        limit: 2,
    });

    assert.equal(second.items.length, 1);
    assert.equal(second.items[0]?.manifest.event_id, 'evt-003');
    assert.equal(second.nextCursor, manifest003);

    const third = await source.readBatch({
        cursor: second.nextCursor,
        limit: 2,
    });

    assert.equal(third.items.length, 0);
    assert.equal(third.nextCursor, manifest003);
    assert.equal(third.realtimeLagSeconds, null);

    assert.deepEqual(
        client.listCalls.map((call) => {
            return call.startAfter;
        }),
        [
            null,
            manifest002,
            manifest003,
        ],
    );
});

test('rec manifest source retries transient errors and suppresses duplicates',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifest101 = manifestKey(prefix, '101');
    const artifact101 = artifactKey(prefix, '101');
    const manifest102 = manifestKey(prefix, '102');
    const artifact102 = artifactKey(prefix, '102');
    const manifest103 = manifestKey(prefix, '103');
    const artifact103 = artifactKey(prefix, '103');
    const sleepCalls: number[] = [];

    const client = new ScriptedObjectStoreClient([
        {
            error: transientError('TimeoutError', 'list timeout'),
        },
        {
            keys: [manifest102, manifest101, manifest101],
        },
        {
            keys: [manifest102, manifest102, manifest103],
        },
    ], new Map([
        [
            manifest101,
            [
                {
                    error: transientError('TimeoutError', 'manifest timeout'),
                },
                {
                    body: JSON.stringify(buildManifest({
                        artifactKey: artifact101,
                        eventId: 'evt-101',
                        eventTime: '2026-02-18T11:00:00.000Z',
                        offset: '101',
                    })),
                },
            ],
        ],
        [
            artifact101,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-101',
                    eventTime: '2026-02-18T11:00:00.000Z',
                })),
            }],
        ],
        [
            manifest102,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact102,
                    eventId: 'evt-102',
                    eventTime: '2026-02-18T11:00:10.000Z',
                    offset: '102',
                })),
            }],
        ],
        [
            artifact102,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-102',
                    eventTime: '2026-02-18T11:00:10.000Z',
                })),
            }],
        ],
        [
            manifest103,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact103,
                    eventId: 'evt-103',
                    eventTime: '2026-02-18T11:00:20.000Z',
                    offset: '103',
                })),
            }],
        ],
        [
            artifact103,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-103',
                    eventTime: '2026-02-18T11:00:20.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        generationId: 'gen-rec',
        maxAttempts: 3,
        prefix,
        retryBaseMs: 1,
        sleep: async (delayMs: number) => {
            sleepCalls.push(delayMs);
        },
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T11:01:20.000Z',
    });

    const first = await source.readBatch({
        cursor: null,
        limit: 2,
    });

    assert.equal(first.items.length, 2);
    assert.deepEqual(
        first.items.map((item) => {
            return item.manifest.event_id;
        }),
        [
            'evt-101',
            'evt-102',
        ],
    );

    const second = await source.readBatch({
        cursor: first.nextCursor,
        limit: 2,
    });

    assert.equal(second.items.length, 1);
    assert.equal(second.items[0]?.manifest.event_id, 'evt-103');
    assert.ok(sleepCalls.length >= 2);

    const manifest101Reads = client.readCalls.filter((key) => {
        return key === manifest101;
    });

    assert.equal(manifest101Reads.length, 2);
});

test('rec manifest source parses REC manifest and keeps allowlisted metadata',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifest201 = manifestKey(prefix, '201');
    const artifact201 = artifactKey(prefix, '201');

    const client = new ScriptedObjectStoreClient([
        {
            keys: [manifest201],
        },
    ], new Map([
        [
            manifest201,
            [{
                body: JSON.stringify(buildManifest({
                    app: 'x_app',
                    artifactKey: artifact201,
                    eventId: 'evt-schema-201',
                    eventTime: '2026-02-18T15:16:17Z',
                    eventType: 'schema.snapshot',
                    kind: 'schema',
                    offset: '000201',
                    table: 'x_app.ticket',
                    topic: 'rez.schema',
                })),
            }],
        ],
        [
            artifact201,
            [{
                body: JSON.stringify({
                    event: {
                        id: 'evt-schema-201',
                        source: 'sn://acme-dev.service-now.com',
                        time: '2026-02-18T15:16:17Z',
                        type: 'schema.snapshot',
                    },
                    non_allowlisted: 'drop-me',
                    row: {
                        plaintext: true,
                    },
                    snapshot: {
                        plaintext: true,
                    },
                }),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-blue',
        timeProvider: () => '2026-02-18T15:16:30.000Z',
    });

    const batch = await source.readBatch({
        cursor: null,
        limit: 1,
    });

    assert.equal(batch.items.length, 1);

    const item = batch.items[0];

    assert.equal(item?.manifest.event_time, '2026-02-18T15:16:17.000Z');
    assert.equal(item?.manifest.offset, '201');
    assert.equal(item?.metadata.event_id, 'evt-schema-201');
    assert.equal(item?.metadata.event_type, 'schema.snapshot');
    assert.equal(item?.metadata.source, 'sn://acme-dev.service-now.com');
    assert.equal(item?.metadata.topic, 'rez.schema');
    assert.equal(item?.metadata.partition, 0);
    assert.equal(item?.metadata.offset, '201');
    assert.equal(item?.metadata.tenant_id, 'tenant-blue');
    assert.equal(item?.tenantId, 'tenant-blue');
    assert.equal(item?.generationId, 'gen-rec');
    assert.equal(item?.ingestionMode, 'realtime');

    assert.equal('non_allowlisted' in (item?.metadata || {}), false);
    assert.equal('row' in (item?.metadata || {}), false);
    assert.equal('snapshot' in (item?.metadata || {}), false);
});
