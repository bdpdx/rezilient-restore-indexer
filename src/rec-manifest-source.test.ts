import assert from 'node:assert/strict';
import { test } from 'node:test';
import {
    RecManifestArtifactBatchSource,
    type RecManifestObjectStoreClient,
} from './rec-manifest-source';
import {
    parseSourceCursorState,
    serializeSourceCursorState,
    SOURCE_CURSOR_VERSION,
} from './source-cursor';

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
    tenantId?: string;
    topic?: string;
    objectKeyLayoutVersion?: string;
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
        object_key_layout_version: params.objectKeyLayoutVersion
            || 'rec.object-key-layout.v1',
        offset: params.offset,
        partition: params.partition ?? 0,
        source: params.source || 'sn://acme-dev.service-now.com',
        table: params.table === undefined ? 'x_app.ticket' : params.table,
        tenant_id: params.tenantId,
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

const CURSOR_DEFAULTS = {
    enabled: false,
    lowerBound: null,
};

function readCursorState(
    cursor: string | null,
) {
    assert.notEqual(cursor, null);

    return parseSourceCursorState(cursor, CURSOR_DEFAULTS);
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
    assert.equal(
        readCursorState(first.nextCursor).scan_cursor,
        manifest002,
    );
    assert.equal(first.realtimeLagSeconds, 70);

    const second = await source.readBatch({
        cursor: first.nextCursor,
        limit: 2,
    });

    assert.equal(second.items.length, 1);
    assert.equal(second.items[0]?.manifest.event_id, 'evt-003');
    assert.equal(
        readCursorState(second.nextCursor).scan_cursor,
        manifest003,
    );

    const third = await source.readBatch({
        cursor: second.nextCursor,
        limit: 2,
    });

    assert.equal(third.items.length, 0);
    assert.equal(
        readCursorState(third.nextCursor).scan_cursor,
        manifest003,
    );
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

test('rec manifest source accepts rec.object-key-layout.v2 manifests',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifest = manifestKey(prefix, 'v2-001');
    const artifact = artifactKey(prefix, 'v2-001');
    const client = new ScriptedObjectStoreClient([
        {
            keys: [manifest],
        },
    ], new Map([
        [
            manifest,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact,
                    eventId: 'evt-v2-001',
                    eventTime: '2026-02-18T11:00:00.000Z',
                    objectKeyLayoutVersion: 'rec.object-key-layout.v2',
                    offset: '42',
                    partition: 7,
                    topic: 'rez.schema',
                })),
            }],
        ],
        [
            artifact,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-v2-001',
                    eventTime: '2026-02-18T11:00:00.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
    });
    const batch = await source.readBatch({
        cursor: null,
        limit: 1,
    });

    assert.equal(batch.items.length, 1);
    assert.equal(batch.items[0]?.manifest.event_id, 'evt-v2-001');
    assert.equal(
        batch.items[0]?.manifest.object_key_layout_version,
        'rec.object-key-layout.v2',
    );
    assert.equal(batch.items[0]?.manifest.topic, 'rez.schema');
    assert.equal(batch.items[0]?.manifest.partition, 7);
    assert.equal(batch.items[0]?.manifest.offset, '42');
});

test('rec manifest source keeps scanning when early pages contain no manifests',
async () => {
    const prefix = 'rez/restore-artifacts';
    const manifestKey001 = manifestKey(prefix, 'scan-001');
    const artifactKey001 = artifactKey(prefix, 'scan-001');
    const noisyPage = Array.from({
        length: 100,
    }, (_, index) => {
        const padded = String(index).padStart(3, '0');

        return `${prefix}/noise-${padded}.txt`;
    });
    const client = new ScriptedObjectStoreClient([
        {
            keys: noisyPage,
        },
        {
            keys: [manifestKey001],
        },
    ], new Map([
        [
            manifestKey001,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifactKey001,
                    eventId: 'evt-scan-001',
                    eventTime: '2026-02-18T12:00:00.000Z',
                    offset: '1',
                })),
            }],
        ],
        [
            artifactKey001,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-scan-001',
                    eventTime: '2026-02-18T12:00:00.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T12:00:30.000Z',
    });

    const batch = await source.readBatch({
        cursor: null,
        limit: 1,
    });

    assert.equal(batch.items.length, 1);
    assert.equal(batch.items[0]?.manifest.event_id, 'evt-scan-001');
    assert.equal(
        readCursorState(batch.nextCursor).scan_cursor,
        manifestKey001,
    );
    assert.equal(client.listCalls.length, 2);
    assert.equal(client.listCalls[0]?.startAfter, null);
    assert.equal(
        client.listCalls[1]?.startAfter,
        `${prefix}/noise-099.txt`,
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
                    tenantId: 'tenant-blue',
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

test('rec manifest source discovers late lexically-lower keys via replay',
async () => {
    const prefix = 'rez/restore-artifacts';
    const replayLowerBound = manifestKey(prefix, '090');
    const scanCursor = manifestKey(prefix, '300');
    const lateManifest = manifestKey(prefix, '120');
    const lateArtifact = artifactKey(prefix, '120');
    const client = new ScriptedObjectStoreClient([
        {
            keys: [],
        },
        {
            keys: [lateManifest],
        },
    ], new Map([
        [
            lateManifest,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: lateArtifact,
                    eventId: 'evt-late-120',
                    eventTime: '2026-02-18T14:00:00.000Z',
                    offset: '120',
                })),
            }],
        ],
        [
            lateArtifact,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-late-120',
                    eventTime: '2026-02-18T14:00:00.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        cursorReplay: {
            enabled: true,
            intervalSeconds: 60,
            lowerBound: replayLowerBound,
            maxKeysPerCycle: 100,
            maxPagesPerCycle: 2,
        },
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T14:01:00.000Z',
    });
    const cursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: null,
            lower_bound: replayLowerBound,
        },
        scan_cursor: scanCursor,
        v: SOURCE_CURSOR_VERSION,
    });
    const batch = await source.readBatch({
        cursor,
        limit: 2,
    });

    assert.equal(batch.items.length, 1);
    assert.equal(batch.items[0]?.manifest.event_id, 'evt-late-120');
    assert.equal(batch.scanCounters?.fastPathSelectedKeyCount, 0);
    assert.equal(batch.scanCounters?.replayPathSelectedKeyCount, 1);
    assert.equal(batch.scanCounters?.replayOnlyHitCount, 1);
    assert.equal(batch.scanCounters?.replayCycleRan, true);
    assert.equal(client.listCalls.length, 2);
    assert.equal(client.listCalls[0]?.startAfter, scanCursor);
    assert.equal(client.listCalls[1]?.startAfter, replayLowerBound);

    const nextCursor = readCursorState(batch.nextCursor);

    assert.equal(nextCursor.scan_cursor, scanCursor);
    assert.equal(nextCursor.replay.lower_bound, replayLowerBound);
    assert.equal(
        nextCursor.replay.last_replay_at,
        '2026-02-18T14:01:00.000Z',
    );
});

test('rec manifest source disables replay in v2_primary cursor mode',
async () => {
    const prefix = 'rez/restore-artifacts';
    const replayLowerBound = manifestKey(prefix, '090');
    const scanCursor = manifestKey(prefix, '300');
    const lateManifest = manifestKey(prefix, '120');
    const lateArtifact = artifactKey(prefix, '120');
    const client = new ScriptedObjectStoreClient([
        {
            keys: [],
        },
        {
            keys: [lateManifest],
        },
    ], new Map([
        [
            lateManifest,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: lateArtifact,
                    eventId: 'evt-late-120',
                    eventTime: '2026-02-18T14:00:00.000Z',
                    offset: '120',
                })),
            }],
        ],
        [
            lateArtifact,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-late-120',
                    eventTime: '2026-02-18T14:00:00.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        cursorMode: 'v2_primary',
        cursorReplay: {
            enabled: true,
            intervalSeconds: 60,
            lowerBound: replayLowerBound,
            maxKeysPerCycle: 100,
            maxPagesPerCycle: 2,
        },
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T14:01:00.000Z',
    });
    const cursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: null,
            lower_bound: replayLowerBound,
        },
        scan_cursor: scanCursor,
        v: SOURCE_CURSOR_VERSION,
    });
    const batch = await source.readBatch({
        cursor,
        limit: 2,
    });

    assert.equal(batch.items.length, 0);
    assert.equal(batch.scanCounters?.fastPathSelectedKeyCount, 0);
    assert.equal(batch.scanCounters?.replayPathSelectedKeyCount, 0);
    assert.equal(batch.scanCounters?.replayOnlyHitCount, 0);
    assert.equal(batch.scanCounters?.replayCycleRan, false);
    assert.equal(client.listCalls.length, 1);
    assert.equal(client.listCalls[0]?.startAfter, scanCursor);

    const nextCursor = readCursorState(batch.nextCursor);

    assert.equal(nextCursor.scan_cursor, scanCursor);
    assert.equal(nextCursor.replay.lower_bound, replayLowerBound);
    assert.equal(nextCursor.replay.last_replay_at, null);
});

test('rec manifest source preserves v2 shard state during legacy replay fallback',
async () => {
    const prefix = 'rez/restore-artifacts';
    const replayLowerBound = manifestKey(prefix, '090');
    const scanCursor = manifestKey(prefix, '300');
    const seededV2State = {
        by_shard: {
            'rez.cdc|0': {
                last_event_time: '2026-02-18T13:59:59.000Z',
                last_key: `${prefix}/seed.artifact.json`,
                next_offset: '301',
            },
        },
        last_reconcile_at: '2026-02-18T14:00:30.000Z',
    };
    const client = new ScriptedObjectStoreClient([
        {
            keys: [],
        },
        {
            keys: [],
        },
    ], new Map());
    const source = new RecManifestArtifactBatchSource(client, {
        cursorReplay: {
            enabled: true,
            intervalSeconds: 60,
            lowerBound: replayLowerBound,
            maxKeysPerCycle: 100,
            maxPagesPerCycle: 2,
        },
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
        timeProvider: () => '2026-02-18T14:01:00.000Z',
    });
    const cursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: null,
            lower_bound: replayLowerBound,
        },
        scan_cursor: scanCursor,
        v2: seededV2State,
        v: SOURCE_CURSOR_VERSION,
    });
    const batch = await source.readBatch({
        cursor,
        limit: 2,
    });
    const nextCursor = readCursorState(batch.nextCursor);

    assert.equal(batch.items.length, 0);
    assert.equal(nextCursor.scan_cursor, scanCursor);
    assert.deepEqual(nextCursor.v2, seededV2State);
    assert.equal(
        nextCursor.replay.last_replay_at,
        '2026-02-18T14:01:00.000Z',
    );
});

test('rec manifest source enforces replay page/key caps', async () => {
    const prefix = 'rez/restore-artifacts';
    const replayLowerBound = manifestKey(prefix, '050');
    const scanCursor = manifestKey(prefix, '500');
    const manifest110 = manifestKey(prefix, '110');
    const artifact110 = artifactKey(prefix, '110');
    const manifest120 = manifestKey(prefix, '120');
    const artifact120 = artifactKey(prefix, '120');
    const manifest130 = manifestKey(prefix, '130');
    const manifest140 = manifestKey(prefix, '140');
    const client = new ScriptedObjectStoreClient([
        {
            keys: [],
        },
        {
            keys: [
                manifest140,
                manifest120,
                manifest110,
                manifest130,
            ],
        },
    ], new Map([
        [
            manifest110,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact110,
                    eventId: 'evt-110',
                    eventTime: '2026-02-18T15:00:00.000Z',
                    offset: '110',
                })),
            }],
        ],
        [
            artifact110,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-110',
                    eventTime: '2026-02-18T15:00:00.000Z',
                })),
            }],
        ],
        [
            manifest120,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: artifact120,
                    eventId: 'evt-120',
                    eventTime: '2026-02-18T15:00:10.000Z',
                    offset: '120',
                })),
            }],
        ],
        [
            artifact120,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-120',
                    eventTime: '2026-02-18T15:00:10.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        cursorReplay: {
            enabled: true,
            intervalSeconds: 60,
            lowerBound: replayLowerBound,
            maxKeysPerCycle: 2,
            maxPagesPerCycle: 1,
        },
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
    });
    const cursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: null,
            lower_bound: replayLowerBound,
        },
        scan_cursor: scanCursor,
        v: SOURCE_CURSOR_VERSION,
    });
    const batch = await source.readBatch({
        cursor,
        limit: 10,
    });

    assert.equal(batch.items.length, 2);
    assert.deepEqual(
        batch.items.map((item) => {
            return item.manifest.event_id;
        }),
        [
            'evt-110',
            'evt-120',
        ],
    );
    assert.equal(client.listCalls.length, 2);
    assert.equal(client.listCalls[0]?.startAfter, scanCursor);
    assert.equal(client.listCalls[1]?.startAfter, replayLowerBound);
});

test('rec manifest source merges fast and replay keys deterministically',
async () => {
    const prefix = 'rez/restore-artifacts';
    const replayLowerBound = manifestKey(prefix, '050');
    const scanCursor = manifestKey(prefix, '100');
    const replayManifest = manifestKey(prefix, '090');
    const replayArtifact = artifactKey(prefix, '090');
    const fastManifest120 = manifestKey(prefix, '120');
    const fastArtifact120 = artifactKey(prefix, '120');
    const fastManifest130 = manifestKey(prefix, '130');
    const fastArtifact130 = artifactKey(prefix, '130');
    const client = new ScriptedObjectStoreClient([
        {
            keys: [fastManifest130, fastManifest120],
        },
        {
            keys: [replayManifest],
        },
    ], new Map([
        [
            replayManifest,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: replayArtifact,
                    eventId: 'evt-replay-090',
                    eventTime: '2026-02-18T16:00:00.000Z',
                    offset: '90',
                })),
            }],
        ],
        [
            replayArtifact,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-replay-090',
                    eventTime: '2026-02-18T16:00:00.000Z',
                })),
            }],
        ],
        [
            fastManifest120,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: fastArtifact120,
                    eventId: 'evt-fast-120',
                    eventTime: '2026-02-18T16:00:10.000Z',
                    offset: '120',
                })),
            }],
        ],
        [
            fastArtifact120,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-fast-120',
                    eventTime: '2026-02-18T16:00:10.000Z',
                })),
            }],
        ],
        [
            fastManifest130,
            [{
                body: JSON.stringify(buildManifest({
                    artifactKey: fastArtifact130,
                    eventId: 'evt-fast-130',
                    eventTime: '2026-02-18T16:00:20.000Z',
                    offset: '130',
                })),
            }],
        ],
        [
            fastArtifact130,
            [{
                body: JSON.stringify(buildCdcArtifact({
                    eventId: 'evt-fast-130',
                    eventTime: '2026-02-18T16:00:20.000Z',
                })),
            }],
        ],
    ]));
    const source = new RecManifestArtifactBatchSource(client, {
        cursorReplay: {
            enabled: true,
            intervalSeconds: 60,
            lowerBound: replayLowerBound,
            maxKeysPerCycle: 100,
            maxPagesPerCycle: 2,
        },
        generationId: 'gen-rec',
        prefix,
        tenantId: 'tenant-acme',
    });
    const cursor = serializeSourceCursorState({
        replay: {
            enabled: true,
            last_replay_at: null,
            lower_bound: replayLowerBound,
        },
        scan_cursor: scanCursor,
        v: SOURCE_CURSOR_VERSION,
    });
    const batch = await source.readBatch({
        cursor,
        limit: 2,
    });

    assert.deepEqual(
        batch.items.map((item) => {
            return item.manifest.event_id;
        }),
        [
            'evt-replay-090',
            'evt-fast-120',
        ],
    );
    assert.equal(batch.scanCounters?.fastPathSelectedKeyCount, 1);
    assert.equal(batch.scanCounters?.replayPathSelectedKeyCount, 1);
    assert.equal(batch.scanCounters?.replayOnlyHitCount, 1);
    assert.equal(batch.scanCounters?.replayCycleRan, true);

    const nextCursor = readCursorState(batch.nextCursor);

    assert.equal(nextCursor.scan_cursor, fastManifest120);
});

test('rec manifest source upgrades legacy cursor to v3 payload', async () => {
    const legacyCursor = 'rez/restore-artifacts/legacy-scan.manifest.json';
    const source = new RecManifestArtifactBatchSource(
        new ScriptedObjectStoreClient([], new Map()),
        {
            generationId: 'gen-rec',
            prefix: 'rez/restore-artifacts',
            tenantId: 'tenant-acme',
        },
    );
    const batch = await source.readBatch({
        cursor: legacyCursor,
        limit: 1,
    });
    const nextCursor = readCursorState(batch.nextCursor);

    assert.equal(nextCursor.scan_cursor, legacyCursor);
    assert.equal(nextCursor.replay.enabled, false);
});

test('rec manifest source fails closed on malformed JSON cursor with diagnostics',
async () => {
    const source = new RecManifestArtifactBatchSource(
        new ScriptedObjectStoreClient([], new Map()),
        {
            generationId: 'gen-rec',
            prefix: 'rez/restore-artifacts',
            tenantId: 'tenant-acme',
        },
    );

    await assert.rejects(async () => {
        await source.readBatch({
            cursor: '{"v":2,"scan_cursor":"scan-key","replay":',
            limit: 1,
        });
    }, (error: unknown) => {
        const message = String((error as Error)?.message || error);

        assert.match(
            message,
            /source cursor parse failure \(fail-closed\):/,
        );
        assert.match(message, /cursor_kind=json_like/);
        assert.match(
            message,
            /manual_remediation=update rez_restore_index\.source_progress\.cursor/,
        );

        return true;
    });
});
