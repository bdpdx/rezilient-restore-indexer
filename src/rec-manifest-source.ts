import {
    canonicalizeIsoDateTimeWithMillis,
    canonicalizeRestoreOffsetDecimalString,
    RRS_METADATA_ALLOWLIST_FIELDS,
} from '@rezilient/types';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from './constants';
import {
    parseSourceCursorState,
    serializeSourceCursorState,
    type SourceCursorReplayDefaults,
    type SourceCursorV3State,
} from './source-cursor';
import type { SourceCursorMode } from './env';
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
const REC_OBJECT_KEY_LAYOUT_VERSIONS = new Set([
    'rec.object-key-layout.v1',
    'rec.object-key-layout.v2',
]);
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RETRY_BASE_MS = 200;
const DEFAULT_LIST_PAGE_MIN_LIMIT = 100;
const DEFAULT_REPLAY_INTERVAL_SECONDS = 60;
const DEFAULT_REPLAY_MAX_KEYS_PER_CYCLE = 1000;
const DEFAULT_REPLAY_MAX_PAGES_PER_CYCLE = 2;
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
    cursorMode?: SourceCursorMode;
    cursorReplay?: Partial<{
        enabled: boolean;
        intervalSeconds: number;
        lowerBound: string | null;
        maxKeysPerCycle: number;
        maxPagesPerCycle: number;
    }>;
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

function readOptionalBoolean(
    value: unknown,
    fieldPath: string,
): boolean | undefined {
    if (value === undefined) {
        return undefined;
    }

    if (typeof value !== 'boolean') {
        throw new Error(`${fieldPath} must be a boolean when provided`);
    }

    return value;
}

function readPositiveInteger(
    value: unknown,
    fallback: number,
    fieldPath: string,
): number {
    const parsed = readOptionalInteger(value, fieldPath);

    if (parsed === undefined) {
        return fallback;
    }

    if (parsed <= 0) {
        throw new Error(`${fieldPath} must be a positive integer`);
    }

    return parsed;
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

    if (!REC_OBJECT_KEY_LAYOUT_VERSIONS.has(objectKeyLayoutVersion)) {
        throw new Error(
            'unsupported manifest.object_key_layout_version: '
            + objectKeyLayoutVersion,
        );
    }

    const topic = readOptionalString(value.topic, 'manifest.topic');
    const partition = readOptionalInteger(value.partition, 'manifest.partition');
    const rawOffset = readOptionalString(value.offset, 'manifest.offset');
    const tenantId = readOptionalString(value.tenant_id, 'manifest.tenant_id');

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
        tenant_id: tenantId,
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

type RecManifestCursorReplayConfig = {
    enabled: boolean;
    intervalSeconds: number;
    lowerBound: string | null;
    maxKeysPerCycle: number;
    maxPagesPerCycle: number;
};

type ManifestKeyScanOptions = {
    lowerExclusive: string | null;
    upperExclusive: string | null;
};

type MergedManifestSelection = {
    selectedFastKeys: string[];
    selectedReplayKeys: string[];
    selectedKeys: string[];
};

function normalizeCursorReplayConfig(
    value: RecManifestArtifactBatchSourceOptions['cursorReplay'],
): RecManifestCursorReplayConfig {
    const enabled = readOptionalBoolean(
        value?.enabled,
        'source.cursorReplay.enabled',
    ) ?? false;

    return {
        enabled,
        intervalSeconds: readPositiveInteger(
            value?.intervalSeconds,
            DEFAULT_REPLAY_INTERVAL_SECONDS,
            'source.cursorReplay.intervalSeconds',
        ),
        lowerBound: value?.lowerBound === undefined
            ? null
            : readNullableString(
                value.lowerBound,
                'source.cursorReplay.lowerBound',
            ),
        maxKeysPerCycle: readPositiveInteger(
            value?.maxKeysPerCycle,
            DEFAULT_REPLAY_MAX_KEYS_PER_CYCLE,
            'source.cursorReplay.maxKeysPerCycle',
        ),
        maxPagesPerCycle: readPositiveInteger(
            value?.maxPagesPerCycle,
            DEFAULT_REPLAY_MAX_PAGES_PER_CYCLE,
            'source.cursorReplay.maxPagesPerCycle',
        ),
    };
}

function parseIsoTimeMillis(
    value: string,
    fieldPath: string,
): number {
    const parsed = Date.parse(value);

    if (!Number.isFinite(parsed)) {
        throw new Error(`${fieldPath} must be a valid ISO datetime`);
    }

    return parsed;
}

function shouldRunReplay(
    lastReplayAt: string | null,
    nowIsoValue: string,
    intervalSeconds: number,
): boolean {
    if (lastReplayAt === null) {
        return true;
    }

    const nowMillis = parseIsoTimeMillis(nowIsoValue, 'source now');
    const lastReplayMillis = parseIsoTimeMillis(
        lastReplayAt,
        'source cursor replay.last_replay_at',
    );

    return nowMillis - lastReplayMillis >= intervalSeconds * 1000;
}

function extractManifestKeys(
    keys: string[],
    prefix: string,
    scan: ManifestKeyScanOptions,
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

        if (scan.lowerExclusive !== null && key <= scan.lowerExclusive) {
            continue;
        }

        if (scan.upperExclusive !== null && key >= scan.upperExclusive) {
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

function mergeManifestSelections(input: {
    fastKeys: string[];
    limit: number;
    replayKeys: string[];
}): MergedManifestSelection {
    const fastSelected = Array.from(new Set(input.fastKeys))
        .sort()
        .slice(0, input.limit);
    const replaySorted = Array.from(new Set(input.replayKeys))
        .sort();
    let selectedFastKeys = fastSelected;
    const selectedFastSet = new Set(selectedFastKeys);
    const replayOnly = replaySorted.filter((key) => {
        return !selectedFastSet.has(key);
    });
    let selectedReplayKeys: string[] = [];

    if (input.limit > 0 && replayOnly.length > 0) {
        const availableSlots = input.limit - selectedFastKeys.length;

        if (availableSlots > 0) {
            selectedReplayKeys = replayOnly.slice(-availableSlots);
        } else if (selectedFastKeys.length > 0) {
            selectedFastKeys = selectedFastKeys.slice(
                0,
                selectedFastKeys.length - 1,
            );
            selectedReplayKeys = [replayOnly[replayOnly.length - 1]];
        } else {
            selectedReplayKeys = [replayOnly[replayOnly.length - 1]];
        }
    }

    const selectedKeys = Array.from(new Set([
        ...selectedFastKeys,
        ...selectedReplayKeys,
    ]))
        .sort()
        .slice(0, input.limit);
    const selectedKeySet = new Set(selectedKeys);

    return {
        selectedFastKeys: selectedFastKeys.filter((key) => {
            return selectedKeySet.has(key);
        }),
        selectedReplayKeys: selectedReplayKeys.filter((key) => {
            return selectedKeySet.has(key);
        }),
        selectedKeys,
    };
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

function classifyCursorKind(
    cursor: string | null,
): 'null' | 'empty' | 'json_like' | 'legacy_string' {
    if (cursor === null) {
        return 'null';
    }

    const trimmedStart = cursor.trimStart();

    if (!trimmedStart) {
        return 'empty';
    }

    return trimmedStart.startsWith('{')
        ? 'json_like'
        : 'legacy_string';
}

function summarizeCursorForLog(
    cursor: string | null,
): string {
    if (cursor === null) {
        return 'null';
    }

    const compact = cursor.replace(/\s+/g, ' ').trim();

    if (!compact) {
        return 'empty';
    }

    const maxLength = 180;

    if (compact.length <= maxLength) {
        return compact;
    }

    return `${compact.slice(0, maxLength)}...`;
}

function wrapCursorParseError(
    cursor: string | null,
    error: unknown,
): Error {
    const errorMessage = String((error as Error)?.message || error);

    return new Error(
        'source cursor parse failure (fail-closed): '
        + `${errorMessage}; cursor_kind=${classifyCursorKind(cursor)}; `
        + `cursor_preview=${summarizeCursorForLog(cursor)}; `
        + 'manual_remediation='
        + 'update rez_restore_index.source_progress.cursor to a valid '
        + 'legacy string or v2/v3 JSON payload',
    );
}

export class RecManifestArtifactBatchSource implements ArtifactBatchSource {
    private readonly cursorMode: SourceCursorMode;

    private readonly cursorReplay: RecManifestCursorReplayConfig;

    private readonly generationId: string;

    private readonly ingestionMode: IngestionMode;

    private readonly maxAttempts: number;

    private readonly prefix: string;

    private readonly retryBaseMs: number;

    private readonly sleep: (ms: number) => Promise<void>;

    private readonly tenantId: string;

    private readonly timeProvider: () => string;

    private readonly cursorReplayDefaults: SourceCursorReplayDefaults;

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
        this.cursorMode = options.cursorMode || 'mixed';
        this.cursorReplay = normalizeCursorReplayConfig(options.cursorReplay);
        this.cursorReplayDefaults = {
            enabled: this.cursorReplay.enabled,
            lowerBound: this.cursorReplay.lowerBound,
        };
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
        let cursorState: SourceCursorV3State;

        try {
            cursorState = parseSourceCursorState(
                input.cursor,
                this.cursorReplayDefaults,
            );
        } catch (error) {
            throw wrapCursorParseError(input.cursor, error);
        }

        if (input.limit <= 0) {
            return {
                items: [],
                nextCursor: serializeSourceCursorState(cursorState),
                realtimeLagSeconds: null,
                scanCounters: {
                    fastPathSelectedKeyCount: 0,
                    replayCycleRan: false,
                    replayOnlyHitCount: 0,
                    replayPathSelectedKeyCount: 0,
                },
            };
        }

        const nowIsoValue = this.timeProvider();
        const selected = await this.collectManifestKeys({
            cursorState,
            limit: input.limit,
            nowIsoValue,
        });
        const items: IndexArtifactInput[] = [];

        for (const manifestKey of selected.selectedKeys) {
            items.push(await this.readManifestItem(manifestKey));
        }

        const latestEventTime = latestManifestEventTime(items);
        const nextReplayState = {
            enabled: cursorState.legacy.replay.enabled,
            last_replay_at: selected.replayRan
                ? nowIsoValue
                : cursorState.legacy.replay.last_replay_at,
            lower_bound: selected.replayLowerBound,
        };
        const nextCursorState: SourceCursorV3State = {
            legacy: {
                replay: {
                    enabled: nextReplayState.enabled,
                    last_replay_at: nextReplayState.last_replay_at,
                    lower_bound: nextReplayState.lower_bound,
                },
                scan_cursor: selected.nextScanCursor,
            },
            replay: {
                enabled: cursorState.legacy.replay.enabled,
                last_replay_at: nextReplayState.last_replay_at,
                lower_bound: nextReplayState.lower_bound,
            },
            scan_cursor: selected.nextScanCursor,
            v2: cursorState.v2,
            v: cursorState.v,
        };

        return {
            items,
            nextCursor: serializeSourceCursorState(nextCursorState),
            realtimeLagSeconds: computeRealtimeLagSeconds(
                latestEventTime,
                nowIsoValue,
            ),
            scanCounters: {
                fastPathSelectedKeyCount: selected.fastPathSelectedKeyCount,
                replayCycleRan: selected.replayRan,
                replayOnlyHitCount: selected.replayOnlyHitCount,
                replayPathSelectedKeyCount: selected.replayPathSelectedKeyCount,
            },
        };
    }

    private async collectManifestKeys(input: {
        cursorState: SourceCursorV3State;
        limit: number;
        nowIsoValue: string;
    }): Promise<{
        fastPathSelectedKeyCount: number;
        nextScanCursor: string | null;
        replayOnlyHitCount: number;
        replayLowerBound: string | null;
        replayRan: boolean;
        replayPathSelectedKeyCount: number;
        selectedKeys: string[];
    }> {
        const fastKeys = await this.listManifestKeys({
            lowerExclusive: input.cursorState.legacy.scan_cursor,
            maxKeys: input.limit,
            startAfter: input.cursorState.legacy.scan_cursor,
            upperExclusive: null,
        });
        const replayLowerBound = this.resolveReplayLowerBound(
            input.cursorState,
            fastKeys[0] || null,
        );
        let replayKeys: string[] = [];
        let replayRan = false;

        if (this.shouldRunReplayCycle(
            input.cursorState,
            replayLowerBound,
            input.nowIsoValue,
        )) {
            replayKeys = await this.listManifestKeys({
                lowerExclusive: replayLowerBound,
                maxKeys: this.cursorReplay.maxKeysPerCycle,
                maxPages: this.cursorReplay.maxPagesPerCycle,
                startAfter: replayLowerBound,
                upperExclusive: input.cursorState.scan_cursor,
            });
            replayRan = true;
        }

        const merged = mergeManifestSelections({
            fastKeys,
            limit: input.limit,
            replayKeys,
        });
        const nextScanCursor = merged.selectedFastKeys.length > 0
            ? merged.selectedFastKeys[merged.selectedFastKeys.length - 1]
            : input.cursorState.scan_cursor;

        return {
            fastPathSelectedKeyCount: merged.selectedFastKeys.length,
            nextScanCursor,
            replayOnlyHitCount: merged.selectedReplayKeys.length,
            replayLowerBound,
            replayRan,
            replayPathSelectedKeyCount: merged.selectedReplayKeys.length,
            selectedKeys: merged.selectedKeys,
        };
    }

    private async listManifestKeys(input: {
        lowerExclusive: string | null;
        maxKeys: number;
        maxPages?: number;
        startAfter: string | null;
        upperExclusive: string | null;
    }): Promise<string[]> {
        const selected = new Set<string>();
        const pageLimit = Math.max(
            DEFAULT_LIST_PAGE_MIN_LIMIT,
            Math.min(input.maxKeys * 4, 5000),
        );
        let searchCursor = input.startAfter;
        let pagesRead = 0;

        while (selected.size < input.maxKeys) {
            if (
                input.maxPages !== undefined
                && pagesRead >= input.maxPages
            ) {
                break;
            }

            const listedKeys = await this.withRetry(
                'listObjectKeys',
                undefined,
                async () => {
                    return this.client.listObjectKeys({
                        limit: pageLimit,
                        prefix: this.prefix,
                        startAfter: searchCursor,
                    });
                },
            );

            pagesRead += 1;

            if (listedKeys.length === 0) {
                break;
            }

            const uniqueSortedPageKeys = Array.from(new Set(listedKeys))
                .sort();
            const pageManifestKeys = extractManifestKeys(
                uniqueSortedPageKeys,
                this.prefix,
                {
                    lowerExclusive: input.lowerExclusive,
                    upperExclusive: input.upperExclusive,
                },
            );

            for (const manifestKey of pageManifestKeys) {
                selected.add(manifestKey);

                if (selected.size >= input.maxKeys) {
                    break;
                }
            }

            if (uniqueSortedPageKeys.length < pageLimit) {
                break;
            }

            const lastPageKey =
                uniqueSortedPageKeys[uniqueSortedPageKeys.length - 1] || null;

            if (lastPageKey === null) {
                break;
            }

            if (searchCursor !== null && lastPageKey <= searchCursor) {
                break;
            }

            searchCursor = lastPageKey;
        }

        return Array.from(selected)
            .sort()
            .slice(0, input.maxKeys);
    }

    private resolveReplayLowerBound(
        cursorState: SourceCursorV3State,
        firstFastKey: string | null,
    ): string | null {
        if (cursorState.legacy.replay.lower_bound !== null) {
            return cursorState.legacy.replay.lower_bound;
        }

        if (cursorState.legacy.scan_cursor !== null) {
            return this.prefix;
        }

        return firstFastKey;
    }

    private shouldRunReplayCycle(
        cursorState: SourceCursorV3State,
        replayLowerBound: string | null,
        nowIsoValue: string,
    ): boolean {
        if (
            !cursorState.legacy.replay.enabled
            || !this.cursorReplay.enabled
            || this.cursorMode !== 'mixed'
        ) {
            return false;
        }

        if (
            cursorState.legacy.scan_cursor === null
            || replayLowerBound === null
        ) {
            return false;
        }

        if (replayLowerBound >= cursorState.legacy.scan_cursor) {
            return false;
        }

        return shouldRunReplay(
            cursorState.legacy.replay.last_replay_at,
            nowIsoValue,
            this.cursorReplay.intervalSeconds,
        );
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

        if (
            metadata.tenant_id === undefined
            && manifest.tenant_id !== undefined
        ) {
            metadata.tenant_id = manifest.tenant_id;
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
