import {
    canonicalizeIsoDateTimeWithMillis,
    canonicalizeRestoreOffsetDecimalString,
    RRS_METADATA_ALLOWLIST_FIELDS,
} from '@rezilient/types';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from './constants';
import type {
    ArtifactManifestInput,
    IndexArtifactInput,
    IngestionMode,
    RestoreArtifactKind,
} from './types';
import type {
    ArtifactBatch,
    ArtifactBatchSource,
} from './worker';

const REC_ARTIFACT_MANIFEST_VERSION = 'rec.artifact-manifest.v1';
const REC_OBJECT_KEY_LAYOUT_VERSION = 'rec.object-key-layout.v1';
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_BASE_MS = 200;
const MANIFEST_SUFFIX = '.manifest.json';

const TRANSIENT_ERROR_CODES = new Set([
    'ECONNRESET',
    'EPIPE',
    'ETIMEDOUT',
    'InternalError',
    'NetworkingError',
    'RequestTimeout',
    'ServiceUnavailable',
    'SlowDown',
    'TimeoutError',
    'TooManyRequests',
    'Throttling',
]);

export interface RecManifestObjectStoreClient {
    listObjectKeys(input: {
        limit: number;
        prefix: string;
        startAfter: string | null;
    }): Promise<string[]>;
    readObjectText(key: string): Promise<string>;
}

export type RecManifestArtifactBatchSourceOptions = {
    generationId: string;
    ingestionMode?: IngestionMode;
    maxAttempts?: number;
    prefix: string;
    retryBaseMs?: number;
    sleep?: (ms: number) => Promise<void>;
    tenantId: string;
    timeProvider?: () => string;
};

function nowIso(): string {
    return new Date().toISOString();
}

function sleepMs(ms: number): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function normalizePrefix(prefix: string): string {
    return String(prefix || '').replace(/^\/+|\/+$/g, '');
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value)
        && typeof value === 'object'
        && !Array.isArray(value);
}

function readOptionalString(
    value: unknown,
    fieldPath: string,
): string | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldPath} must be a string when provided`);
    }

    const trimmed = value.trim();

    if (!trimmed) {
        throw new Error(`${fieldPath} must not be empty`);
    }

    return trimmed;
}

function readRequiredString(
    value: unknown,
    fieldPath: string,
): string {
    const parsed = readOptionalString(value, fieldPath);

    if (parsed === undefined) {
        throw new Error(`${fieldPath} is required`);
    }

    return parsed;
}

function readNullableString(
    value: unknown,
    fieldPath: string,
): string | null {
    if (value === null) {
        return null;
    }

    return readRequiredString(value, fieldPath);
}

function readOptionalInteger(
    value: unknown,
    fieldPath: string,
): number | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (typeof value !== 'number') {
        throw new Error(
            `${fieldPath} must be a non-negative integer when provided`,
        );
    }

    if (!Number.isInteger(value) || value < 0) {
        throw new Error(
            `${fieldPath} must be a non-negative integer when provided`,
        );
    }

    return value;
}

function parseArtifactKind(
    value: unknown,
    fieldPath: string,
): RestoreArtifactKind {
    if (
        value === 'cdc'
        || value === 'schema'
        || value === 'media'
        || value === 'error'
        || value === 'repair'
    ) {
        return value;
    }

    throw new Error(
        `${fieldPath} must be one of cdc|schema|media|error|repair`,
    );
}

function parseManifest(
    manifestKey: string,
    value: unknown,
): ArtifactManifestInput {
    if (!isRecord(value)) {
        throw new Error('manifest root must be an object');
    }

    const contractVersion = readRequiredString(
        value.contract_version,
        'manifest.contract_version',
    );

    if (contractVersion !== RESTORE_CONTRACT_VERSION) {
        throw new Error(
            `unsupported manifest.contract_version: ${contractVersion}`,
        );
    }

    const manifestVersion = readRequiredString(
        value.manifest_version,
        'manifest.manifest_version',
    );

    if (manifestVersion !== REC_ARTIFACT_MANIFEST_VERSION) {
        throw new Error(
            `unsupported manifest.manifest_version: ${manifestVersion}`,
        );
    }

    const allowlistVersion = readRequiredString(
        value.metadata_allowlist_version,
        'manifest.metadata_allowlist_version',
    );

    if (allowlistVersion !== RESTORE_METADATA_ALLOWLIST_VERSION) {
        throw new Error(
            'unsupported manifest.metadata_allowlist_version: '
            + allowlistVersion,
        );
    }

    const objectKeyLayoutVersion = readRequiredString(
        value.object_key_layout_version,
        'manifest.object_key_layout_version',
    );

    if (objectKeyLayoutVersion !== REC_OBJECT_KEY_LAYOUT_VERSION) {
        throw new Error(
            'unsupported manifest.object_key_layout_version: '
            + objectKeyLayoutVersion,
        );
    }

    const topic = readOptionalString(value.topic, 'manifest.topic');
    const partition = readOptionalInteger(value.partition, 'manifest.partition');
    const rawOffset = readOptionalString(value.offset, 'manifest.offset');

    if (
        topic === undefined
        || partition === undefined
        || rawOffset === undefined
    ) {
        throw new Error(
            `manifest missing kafka coordinates for ${manifestKey}`,
        );
    }

    const offset = canonicalizeOffset(rawOffset, 'manifest.offset');

    return {
        artifact_key: readRequiredString(
            value.artifact_key,
            'manifest.artifact_key',
        ),
        artifact_kind: parseArtifactKind(
            value.artifact_kind,
            'manifest.artifact_kind',
        ),
        app: readNullableString(value.app, 'manifest.app'),
        contract_version: contractVersion,
        event_id: readRequiredString(value.event_id, 'manifest.event_id'),
        event_time: canonicalizeTimestamp(
            readRequiredString(value.event_time, 'manifest.event_time'),
            'manifest.event_time',
        ),
        event_type: readRequiredString(value.event_type, 'manifest.event_type'),
        instance_id: readRequiredString(
            value.instance_id,
            'manifest.instance_id',
        ),
        manifest_version: manifestVersion,
        metadata_allowlist_version: allowlistVersion,
        object_key_layout_version: objectKeyLayoutVersion,
        offset,
        partition,
        source: readRequiredString(value.source, 'manifest.source'),
        table: readNullableString(value.table, 'manifest.table'),
        topic,
    };
}

function canonicalizeTimestamp(
    value: string,
    fieldPath: string,
): string {
    try {
        return canonicalizeIsoDateTimeWithMillis(value);
    } catch {
        throw new Error(`Invalid ${fieldPath} value: ${value}`);
    }
}

function canonicalizeOffset(
    value: string,
    fieldPath: string,
): string {
    try {
        return canonicalizeRestoreOffsetDecimalString(value);
    } catch {
        throw new Error(`Invalid ${fieldPath} value: ${value}`);
    }
}

function copyAllowlistedMetadata(
    source: Record<string, unknown>,
    destination: Record<string, unknown>,
): void {
    for (const key of RRS_METADATA_ALLOWLIST_FIELDS) {
        const raw = source[key];

        if (raw !== undefined) {
            destination[key] = raw;
        }
    }
}

function copyMappedField(
    source: Record<string, unknown>,
    sourceKey: string,
    destination: Record<string, unknown>,
    destinationKey: string,
): void {
    const value = source[sourceKey];

    if (value !== undefined) {
        destination[destinationKey] = value;
    }
}

function normalizeOperation(
    operation: unknown,
): 'I' | 'U' | 'D' | undefined {
    if (operation === 'I' || operation === 'U' || operation === 'D') {
        return operation;
    }

    return undefined;
}

function extractMetadataFromArtifact(
    artifactPayload: unknown,
): Record<string, unknown> {
    if (!isRecord(artifactPayload)) {
        throw new Error('artifact payload root must be an object');
    }

    const metadata: Record<string, unknown> = {};

    copyAllowlistedMetadata(artifactPayload, metadata);

    copyMappedField(artifactPayload, '__event_id', metadata, 'event_id');
    copyMappedField(artifactPayload, '__source', metadata, 'source');
    copyMappedField(artifactPayload, '__table', metadata, 'table');
    copyMappedField(artifactPayload, '__time', metadata, '__time');
    copyMappedField(artifactPayload, '__type', metadata, 'event_type');
    copyMappedField(
        artifactPayload,
        '__record_sys_id',
        metadata,
        'record_sys_id',
    );
    copyMappedField(
        artifactPayload,
        '__attachment_sys_id',
        metadata,
        'attachment_sys_id',
    );
    copyMappedField(
        artifactPayload,
        '__schema_version',
        metadata,
        'schema_version',
    );
    copyMappedField(
        artifactPayload,
        '__sys_mod_count',
        metadata,
        'sys_mod_count',
    );
    copyMappedField(
        artifactPayload,
        '__sys_updated_on',
        metadata,
        'sys_updated_on',
    );

    const operation = normalizeOperation(artifactPayload.__op);

    if (operation !== undefined) {
        metadata.operation = operation;
    }

    const event = artifactPayload.event;

    if (isRecord(event)) {
        copyMappedField(event, 'id', metadata, 'event_id');
        copyMappedField(event, 'source', metadata, 'source');
        copyMappedField(event, 'time', metadata, '__time');
        copyMappedField(event, 'type', metadata, 'event_type');
    }

    const media = artifactPayload.media;

    if (isRecord(media)) {
        copyAllowlistedMetadata(media, metadata);

        const mediaOp = normalizeOperation(media.op);

        if (mediaOp !== undefined) {
            metadata.operation = mediaOp;
        }
    }

    const errorPayload = artifactPayload.error;

    if (isRecord(errorPayload)) {
        copyAllowlistedMetadata(errorPayload, metadata);
    }

    const repairPayload = artifactPayload.repair;

    if (isRecord(repairPayload)) {
        copyAllowlistedMetadata(repairPayload, metadata);
    }

    return metadata;
}

function computeRetryDelayMs(
    attempt: number,
    retryBaseMs: number,
): number {
    const cappedAttempt = Math.min(Math.max(attempt, 1), 8);

    return retryBaseMs * (2 ** (cappedAttempt - 1));
}

function isTransientObjectStoreError(error: unknown): boolean {
    const typedError = error as {
        $metadata?: {
            httpStatusCode?: number;
        };
        Code?: unknown;
        code?: unknown;
        name?: unknown;
        retryable?: unknown;
        statusCode?: number;
    };

    if (typedError.retryable === true) {
        return true;
    }

    const statusCode = Number(
        typedError.$metadata?.httpStatusCode || typedError.statusCode || 0,
    );

    if (statusCode === 429 || statusCode >= 500) {
        return true;
    }

    const code = String(
        typedError.code || typedError.Code || typedError.name || '',
    );

    return TRANSIENT_ERROR_CODES.has(code);
}

function extractManifestKeys(
    keys: string[],
    prefix: string,
    cursor: string | null,
): string[] {
    const filtered = new Set<string>();
    const prefixWithSlash = `${prefix}/`;

    for (const key of keys) {
        if (!key.endsWith(MANIFEST_SUFFIX)) {
            continue;
        }

        if (key !== prefix && !key.startsWith(prefixWithSlash)) {
            continue;
        }

        if (cursor !== null && key <= cursor) {
            continue;
        }

        filtered.add(key);
    }

    return Array.from(filtered).sort();
}

function latestManifestEventTime(
    items: IndexArtifactInput[],
): string | null {
    let latest: string | null = null;

    for (const item of items) {
        const eventTime = item.manifest.event_time;

        if (latest === null || eventTime > latest) {
            latest = eventTime;
        }
    }

    return latest;
}

function computeRealtimeLagSeconds(
    eventTime: string | null,
    nowIsoValue: string,
): number | null {
    if (eventTime === null) {
        return null;
    }

    const latestMillis = Date.parse(eventTime);
    const nowMillis = Date.parse(nowIsoValue);

    if (!Number.isFinite(latestMillis) || !Number.isFinite(nowMillis)) {
        return null;
    }

    const lagSeconds = Math.floor((nowMillis - latestMillis) / 1000);

    return Math.max(0, lagSeconds);
}

function parseJson(
    value: string,
    fieldPath: string,
): unknown {
    try {
        return JSON.parse(value);
    } catch {
        throw new Error(`Invalid JSON in ${fieldPath}`);
    }
}

export class RecManifestArtifactBatchSource implements ArtifactBatchSource {
    private readonly generationId: string;

    private readonly ingestionMode: IngestionMode;

    private readonly maxAttempts: number;

    private readonly prefix: string;

    private readonly retryBaseMs: number;

    private readonly sleep: (ms: number) => Promise<void>;

    private readonly tenantId: string;

    private readonly timeProvider: () => string;

    constructor(
        private readonly client: RecManifestObjectStoreClient,
        options: RecManifestArtifactBatchSourceOptions,
    ) {
        this.prefix = normalizePrefix(options.prefix);

        if (!this.prefix) {
            throw new Error('rec manifest source prefix must not be empty');
        }

        this.tenantId = String(options.tenantId || '').trim();

        if (!this.tenantId) {
            throw new Error('rec manifest source tenantId must not be empty');
        }

        this.generationId = String(options.generationId || '').trim();

        if (!this.generationId) {
            throw new Error(
                'rec manifest source generationId must not be empty',
            );
        }

        this.ingestionMode = options.ingestionMode || 'realtime';
        this.maxAttempts = Math.max(
            1,
            options.maxAttempts || DEFAULT_MAX_ATTEMPTS,
        );
        this.retryBaseMs = Math.max(
            10,
            options.retryBaseMs || DEFAULT_RETRY_BASE_MS,
        );
        this.sleep = options.sleep || sleepMs;
        this.timeProvider = options.timeProvider || nowIso;
    }

    async readBatch(input: {
        cursor: string | null;
        limit: number;
    }): Promise<ArtifactBatch> {
        if (input.limit <= 0) {
            return {
                items: [],
                nextCursor: input.cursor,
                realtimeLagSeconds: null,
            };
        }

        const listedKeys = await this.withRetry('listObjectKeys', undefined,
            async () => {
                return this.client.listObjectKeys({
                    limit: input.limit,
                    prefix: this.prefix,
                    startAfter: input.cursor,
                });
            });
        const sortedKeys = extractManifestKeys(
            listedKeys,
            this.prefix,
            input.cursor,
        );
        const selectedKeys = sortedKeys.slice(0, input.limit);
        const items: IndexArtifactInput[] = [];

        for (const manifestKey of selectedKeys) {
            items.push(await this.readManifestItem(manifestKey));
        }

        const latestEventTime = latestManifestEventTime(items);

        return {
            items,
            nextCursor: selectedKeys.length > 0
                ? selectedKeys[selectedKeys.length - 1]
                : input.cursor,
            realtimeLagSeconds: computeRealtimeLagSeconds(
                latestEventTime,
                this.timeProvider(),
            ),
        };
    }

    private async readManifestItem(
        manifestKey: string,
    ): Promise<IndexArtifactInput> {
        const manifestBody = await this.withRetry(
            'readObjectText.manifest',
            manifestKey,
            async () => {
                return this.client.readObjectText(manifestKey);
            },
        );
        const manifest = parseManifest(
            manifestKey,
            parseJson(manifestBody, `manifest ${manifestKey}`),
        );
        const artifactBody = await this.withRetry(
            'readObjectText.artifact',
            manifest.artifact_key,
            async () => {
                return this.client.readObjectText(manifest.artifact_key);
            },
        );
        const artifactPayload = parseJson(
            artifactBody,
            `artifact ${manifest.artifact_key}`,
        );
        const metadata = extractMetadataFromArtifact(artifactPayload);

        if (metadata.tenant_id === undefined) {
            metadata.tenant_id = this.tenantId;
        }

        if (metadata.instance_id === undefined) {
            metadata.instance_id = manifest.instance_id;
        }

        if (metadata.source === undefined) {
            metadata.source = manifest.source;
        }

        if (metadata.event_id === undefined) {
            metadata.event_id = manifest.event_id;
        }

        if (metadata.event_type === undefined) {
            metadata.event_type = manifest.event_type;
        }

        if (metadata.__time === undefined) {
            metadata.__time = manifest.event_time;
        }

        if (metadata.topic === undefined) {
            metadata.topic = manifest.topic;
        }

        if (metadata.partition === undefined) {
            metadata.partition = manifest.partition;
        }

        if (metadata.offset === undefined) {
            metadata.offset = manifest.offset;
        }

        if (metadata.table === undefined && manifest.table !== null) {
            metadata.table = manifest.table;
        }

        return {
            generationId: this.generationId,
            ingestionMode: this.ingestionMode,
            manifest,
            metadata,
            tenantId: this.tenantId,
        };
    }

    private async withRetry<T>(
        operation: string,
        key: string | undefined,
        task: () => Promise<T>,
    ): Promise<T> {
        for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
            try {
                return await task();
            } catch (error) {
                if (
                    attempt >= this.maxAttempts
                    || !isTransientObjectStoreError(error)
                ) {
                    throw new Error(
                        this.buildRetryErrorMessage(operation, key, attempt, error),
                    );
                }

                await this.sleep(computeRetryDelayMs(attempt, this.retryBaseMs));
            }
        }

        throw new Error('unreachable retry state');
    }

    private buildRetryErrorMessage(
        operation: string,
        key: string | undefined,
        attempt: number,
        error: unknown,
    ): string {
        const suffix = key ? ` key=${key}` : '';

        return `${operation} failed after attempt ${attempt}${suffix}: `
            + `${String((error as Error)?.message || error)}`;
    }
}
