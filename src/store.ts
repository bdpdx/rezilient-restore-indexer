import { Pool, type PoolConfig } from 'pg';
import {
    canonicalizeIsoDateTimeWithMillis,
    canonicalizeRestoreOffsetDecimalString,
} from '@rezilient/types';
import type {
    BackfillRunState,
    IndexedEventLookupCandidate,
    IndexedEventLookupInput,
    IndexedEventLookupResult,
    IndexedEventRecord,
    PartitionScope,
    PartitionWatermarkState,
    SourceCoverageState,
    SourceProgressState,
} from './types';

function partitionKey(scope: PartitionScope): string {
    return [
        scope.tenantId,
        scope.instanceId,
        scope.topic,
        String(scope.partition),
    ].join('|');
}

function sourceKey(
    tenantId: string,
    instanceId: string,
    source: string,
): string {
    return [tenantId, instanceId, source].join('|');
}

function runtimeIngestScopeKey(
    ingestScopeId: string,
): string {
    return ingestScopeId;
}

type LegacySourceScopeInput = {
    instanceId: string;
    source: string;
    tenantId: string;
};

type IngestSourceScopeInput = {
    ingestScopeId: string;
    sourceUri: string;
};

export type SourceProgressScopeInput =
    | LegacySourceScopeInput
    | IngestSourceScopeInput;

export type SourceLeaderLeaseAcquireInput = {
    holderId: string;
    leaseDurationSeconds: number;
} & SourceProgressScopeInput;

export type SourceLeaderLeaseReleaseInput = {
    holderId: string;
} & SourceProgressScopeInput;

export type SourceProgressPutInput = {
    state: SourceProgressState;
} & SourceProgressScopeInput;

type ResolvedRuntimeScope = {
    ingestScopeId: string;
    sourceUri: string;
};

function readRequiredRuntimeText(
    value: string,
    field: string,
): string {
    const trimmed = String(value || '').trim();

    if (!trimmed) {
        throw new Error(`${field} is required`);
    }

    return trimmed;
}

function buildLegacyIngestScopeId(
    scope: LegacySourceScopeInput,
): string {
    const tenantId = readRequiredRuntimeText(
        scope.tenantId,
        'source scope tenantId',
    );
    const instanceId = readRequiredRuntimeText(
        scope.instanceId,
        'source scope instanceId',
    );
    const source = readRequiredRuntimeText(
        scope.source,
        'source scope source',
    );

    return `legacy:${JSON.stringify([tenantId, instanceId, source])}`;
}

function resolveRuntimeScopeFromInput(
    input: SourceProgressScopeInput,
): ResolvedRuntimeScope {
    if ('ingestScopeId' in input) {
        return {
            ingestScopeId: readRequiredRuntimeText(
                input.ingestScopeId,
                'ingest_scope_id',
            ),
            sourceUri: readRequiredRuntimeText(
                input.sourceUri,
                'source_uri',
            ),
        };
    }

    return {
        ingestScopeId: buildLegacyIngestScopeId(input),
        sourceUri: readRequiredRuntimeText(
            input.source,
            'source scope source',
        ),
    };
}

function resolveSourceProgressScope(
    scopeOrTenantId: SourceProgressScopeInput | string,
    instanceId?: string,
    source?: string,
): ResolvedRuntimeScope {
    if (typeof scopeOrTenantId === 'string') {
        return resolveRuntimeScopeFromInput({
            instanceId: readRequiredRuntimeText(
                instanceId || '',
                'source scope instanceId',
            ),
            source: readRequiredRuntimeText(
                source || '',
                'source scope source',
            ),
            tenantId: readRequiredRuntimeText(
                scopeOrTenantId,
                'source scope tenantId',
            ),
        });
    }

    return resolveRuntimeScopeFromInput(scopeOrTenantId);
}

function resolveLegacyScopeHint(
    scopeOrTenantId: SourceProgressScopeInput | string,
    instanceId?: string,
    source?: string,
): LegacySourceScopeInput | null {
    if (typeof scopeOrTenantId === 'string') {
        return {
            instanceId: readRequiredRuntimeText(
                instanceId || '',
                'source scope instanceId',
            ),
            source: readRequiredRuntimeText(
                source || '',
                'source scope source',
            ),
            tenantId: readRequiredRuntimeText(
                scopeOrTenantId,
                'source scope tenantId',
            ),
        };
    }

    if ('ingestScopeId' in scopeOrTenantId) {
        return null;
    }

    return {
        instanceId: readRequiredRuntimeText(
            scopeOrTenantId.instanceId,
            'source scope instanceId',
        ),
        source: readRequiredRuntimeText(
            scopeOrTenantId.source,
            'source scope source',
        ),
        tenantId: readRequiredRuntimeText(
            scopeOrTenantId.tenantId,
            'source scope tenantId',
        ),
    };
}

function resolveSourceProgressPutInput(
    input: SourceProgressState | SourceProgressPutInput,
): {
    scope: ResolvedRuntimeScope;
    state: SourceProgressState;
} {
    if ('state' in input) {
        return {
            scope: resolveRuntimeScopeFromInput(input),
            state: input.state,
        };
    }

    return {
        scope: resolveRuntimeScopeFromInput({
            instanceId: input.instanceId,
            source: input.source,
            tenantId: input.tenantId,
        }),
        state: input,
    };
}

function buildRuntimeScopeSourceConflictError(
    ingestScopeId: string,
    sourceUri: string,
    existingSourceUri: string,
): Error {
    return new Error(
        `ingest_scope_id/source_uri conflict: ingest_scope_id=` +
        `"${ingestScopeId}" source_uri="${sourceUri}" ` +
        `existing_source_uri="${existingSourceUri}"`,
    );
}

function eventKey(record: IndexedEventRecord): string {
    return [
        record.tenantId,
        record.partitionScope.instanceId,
        record.partitionScope.source,
        record.partitionScope.topic,
        String(record.partitionScope.partition),
        record.generationId,
        record.metadata.event_id || record.artifactKey,
    ].join('|');
}

function cloneState<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

function recomputeCoverageFromWatermarks(
    input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    },
    watermarks: Iterable<PartitionWatermarkState>,
): SourceCoverageState | null {
    let earliest: string | null = null;
    let latest: string | null = null;
    const generationSpan: Record<string, string> = {};

    for (const watermark of watermarks) {
        if (watermark.tenantId !== input.tenantId) {
            continue;
        }

        if (watermark.instanceId !== input.instanceId) {
            continue;
        }

        if (watermark.source !== input.source) {
            continue;
        }

        if (earliest === null || watermark.coverageStart < earliest) {
            earliest = watermark.coverageStart;
        }

        if (latest === null || watermark.coverageEnd > latest) {
            latest = watermark.coverageEnd;
        }

        generationSpan[
            `${watermark.topic}:${watermark.partition}`
        ] = watermark.generationId;
    }

    if (earliest === null || latest === null) {
        return null;
    }

    return {
        earliestIndexedTime: earliest,
        generationSpan,
        instanceId: input.instanceId,
        latestIndexedTime: latest,
        measuredAt: input.measuredAt,
        source: input.source,
        tenantId: input.tenantId,
    };
}

type NormalizedIndexedEventLookupInput = {
    instanceId: string;
    pitCutoff: string;
    recordSysIds: string[];
    source: string;
    tables: string[];
    tenantId: string;
};

function canonicalizeLookupTimestamp(
    value: string,
    field: string,
): string {
    try {
        return canonicalizeIsoDateTimeWithMillis(String(value));
    } catch {
        throw new Error(`invalid ${field} timestamp`);
    }
}

function normalizeLookupList(
    values: string[],
    field: string,
): string[] {
    if (!Array.isArray(values)) {
        throw new Error(`${field} must contain at least one value`);
    }

    const unique = new Set<string>();

    for (const value of values) {
        const normalized = String(value || '').trim();

        if (!normalized) {
            continue;
        }

        unique.add(normalized);
    }

    if (unique.size === 0) {
        throw new Error(`${field} must contain at least one value`);
    }

    return Array.from(unique.values()).sort((left, right) => {
        return left.localeCompare(right);
    });
}

function normalizeIndexedEventLookupInput(
    input: IndexedEventLookupInput,
): NormalizedIndexedEventLookupInput {
    return {
        instanceId: readRequiredRuntimeText(
            input.instanceId,
            'indexed-event lookup instanceId',
        ),
        pitCutoff: canonicalizeLookupTimestamp(
            input.pitCutoff,
            'indexed-event lookup pitCutoff',
        ),
        recordSysIds: Array.isArray(input.recordSysIds)
            ? normalizeLookupList(
                input.recordSysIds,
                'indexed-event lookup recordSysIds',
            )
            : [],
        source: readRequiredRuntimeText(
            input.source,
            'indexed-event lookup source',
        ),
        tables: normalizeLookupList(
            input.tables,
            'indexed-event lookup tables',
        ),
        tenantId: readRequiredRuntimeText(
            input.tenantId,
            'indexed-event lookup tenantId',
        ),
    };
}

export interface RestoreIndexStore {
    acquireSourceLeaderLease(
        input: SourceLeaderLeaseAcquireInput,
    ): Promise<boolean>;
    getBackfillRun(runId: string): Promise<BackfillRunState | null>;
    getIndexedEventCount(): number;
    getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null>;
    lookupIndexedEventCandidates(
        input: IndexedEventLookupInput,
    ): Promise<IndexedEventLookupResult>;
    getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null>;
    getSourceProgress(
        scopeOrTenantId: SourceProgressScopeInput | string,
        instanceId?: string,
        source?: string,
    ): Promise<SourceProgressState | null>;
    putPartitionWatermark(state: PartitionWatermarkState): Promise<void>;
    putSourceProgress(
        input: SourceProgressState | SourceProgressPutInput,
    ): Promise<void>;
    recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null>;
    releaseSourceLeaderLease(
        input: SourceLeaderLeaseReleaseInput,
    ): Promise<void>;
    upsertBackfillRun(state: BackfillRunState): Promise<void>;
    upsertIndexedEvent(
        record: IndexedEventRecord,
    ): Promise<'inserted' | 'existing'>;
}

export class InMemoryRestoreIndexStore implements RestoreIndexStore {
    private readonly backfillRuns = new Map<string, BackfillRunState>();

    private readonly indexedEvents = new Map<string, IndexedEventRecord>();

    private readonly partitionWatermarks =
        new Map<string, PartitionWatermarkState>();

    private readonly sourceCoverage = new Map<string, SourceCoverageState>();

    private readonly sourceProgress = new Map<string, SourceProgressState>();

    private readonly sourceLeaderLeases = new Map<string, {
        holderId: string;
        leaseExpiresAtMs: number;
    }>();

    private readonly runtimeScopeSourceUris = new Map<string, string>();

    getIndexedEventCount(): number {
        return this.indexedEvents.size;
    }

    async acquireSourceLeaderLease(
        input: SourceLeaderLeaseAcquireInput,
    ): Promise<boolean> {
        const scope = resolveRuntimeScopeFromInput(input);
        this.assertRuntimeScopeSourceUri(
            scope.ingestScopeId,
            scope.sourceUri,
        );
        const key = runtimeIngestScopeKey(scope.ingestScopeId);
        const existing = this.sourceLeaderLeases.get(key);
        const nowMs = Date.now();

        if (
            existing
            && existing.holderId !== input.holderId
            && existing.leaseExpiresAtMs > nowMs
        ) {
            return false;
        }

        const leaseDurationSeconds = Math.max(
            1,
            Math.floor(input.leaseDurationSeconds),
        );

        this.sourceLeaderLeases.set(key, {
            holderId: input.holderId,
            leaseExpiresAtMs: nowMs + (leaseDurationSeconds * 1000),
        });

        return true;
    }

    async releaseSourceLeaderLease(
        input: SourceLeaderLeaseReleaseInput,
    ): Promise<void> {
        const scope = resolveRuntimeScopeFromInput(input);
        this.assertRuntimeScopeSourceUri(
            scope.ingestScopeId,
            scope.sourceUri,
            { registerIfMissing: false },
        );
        const key = runtimeIngestScopeKey(scope.ingestScopeId);
        const existing = this.sourceLeaderLeases.get(key);

        if (!existing || existing.holderId !== input.holderId) {
            return;
        }

        this.sourceLeaderLeases.delete(key);
    }

    async upsertIndexedEvent(
        record: IndexedEventRecord,
    ): Promise<'inserted' | 'existing'> {
        const key = eventKey(record);

        if (this.indexedEvents.has(key)) {
            return 'existing';
        }

        this.indexedEvents.set(key, cloneState(record));

        return 'inserted';
    }

    async lookupIndexedEventCandidates(
        input: IndexedEventLookupInput,
    ): Promise<IndexedEventLookupResult> {
        const normalized = normalizeIndexedEventLookupInput(input);
        const tableSet = new Set(normalized.tables);
        const recordSysIdSet = normalized.recordSysIds.length > 0
            ? new Set(normalized.recordSysIds)
            : null;
        const pitCutoffMillis = parseIsoUtcMillis(normalized.pitCutoff);
        const candidates: IndexedEventLookupCandidate[] = [];

        for (const record of this.indexedEvents.values()) {
            if (record.tenantId !== normalized.tenantId) {
                continue;
            }

            if (record.partitionScope.instanceId !== normalized.instanceId) {
                continue;
            }

            if (record.partitionScope.source !== normalized.source) {
                continue;
            }

            const candidate = buildLookupCandidateFromIndexedEvent(record);

            if (candidate === null) {
                continue;
            }

            if (!tableSet.has(candidate.table)) {
                continue;
            }

            if (recordSysIdSet && !recordSysIdSet.has(candidate.recordSysId)) {
                continue;
            }

            if (parseIsoUtcMillis(candidate.eventTime) > pitCutoffMillis) {
                continue;
            }

            candidates.push(candidate);
        }

        return buildIndexedEventLookupResult(candidates);
    }

    async getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null> {
        const state = this.partitionWatermarks.get(partitionKey(scope));

        return state ? cloneState(state) : null;
    }

    async putPartitionWatermark(state: PartitionWatermarkState): Promise<void> {
        this.partitionWatermarks.set(
            partitionKey({
                instanceId: state.instanceId,
                partition: state.partition,
                source: state.source,
                tenantId: state.tenantId,
                topic: state.topic,
            }),
            cloneState(state),
        );
    }

    async recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null> {
        const state = recomputeCoverageFromWatermarks(
            input,
            this.partitionWatermarks.values(),
        );
        const key = sourceKey(
            input.tenantId,
            input.instanceId,
            input.source,
        );

        if (state === null) {
            this.sourceCoverage.delete(key);
            return null;
        }

        this.sourceCoverage.set(key, cloneState(state));

        return cloneState(state);
    }

    async getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null> {
        const state = this.sourceCoverage.get(
            sourceKey(tenantId, instanceId, source),
        );

        return state ? cloneState(state) : null;
    }

    async putSourceProgress(
        input: SourceProgressState | SourceProgressPutInput,
    ): Promise<void> {
        const resolved = resolveSourceProgressPutInput(input);
        this.assertRuntimeScopeSourceUri(
            resolved.scope.ingestScopeId,
            resolved.scope.sourceUri,
        );
        this.sourceProgress.set(
            runtimeIngestScopeKey(resolved.scope.ingestScopeId),
            cloneState(resolved.state),
        );
    }

    async getSourceProgress(
        scopeOrTenantId: SourceProgressScopeInput | string,
        instanceId?: string,
        source?: string,
    ): Promise<SourceProgressState | null> {
        const scope = resolveSourceProgressScope(
            scopeOrTenantId,
            instanceId,
            source,
        );
        this.assertRuntimeScopeSourceUri(
            scope.ingestScopeId,
            scope.sourceUri,
            { registerIfMissing: false },
        );
        const state = this.sourceProgress.get(
            runtimeIngestScopeKey(scope.ingestScopeId),
        );

        return state ? cloneState(state) : null;
    }

    async upsertBackfillRun(state: BackfillRunState): Promise<void> {
        this.backfillRuns.set(state.runId, cloneState(state));
    }

    async getBackfillRun(runId: string): Promise<BackfillRunState | null> {
        const state = this.backfillRuns.get(runId);

        return state ? cloneState(state) : null;
    }

    private assertRuntimeScopeSourceUri(
        ingestScopeId: string,
        sourceUri: string,
        options: {
            registerIfMissing?: boolean;
        } = {},
    ): void {
        const registerIfMissing = options.registerIfMissing !== false;
        const existingSourceUri = this.runtimeScopeSourceUris.get(ingestScopeId);

        if (existingSourceUri && existingSourceUri !== sourceUri) {
            throw buildRuntimeScopeSourceConflictError(
                ingestScopeId,
                sourceUri,
                existingSourceUri,
            );
        }

        if (!existingSourceUri && registerIfMissing) {
            this.runtimeScopeSourceUris.set(ingestScopeId, sourceUri);
        }
    }
}

type PartitionWatermarkRow = {
    coverage_end: Date | string;
    coverage_start: Date | string;
    generation_id: string;
    indexed_through_offset: number | string;
    indexed_through_time: Date | string;
    instance_id: string;
    kafka_partition: number;
    measured_at: Date | string;
    source: string;
    tenant_id: string;
    topic: string;
};

type IndexedEventLookupRow = {
    artifact_key: string | null;
    event_id: string | null;
    event_time: Date | string;
    kafka_offset: number | string;
    kafka_partition: number;
    manifest_key: string | null;
    record_sys_id: string | null;
    sys_mod_count: number | null;
    sys_updated_on: string | null;
    table_name: string | null;
    topic: string;
};

type SourceCoverageRow = {
    earliest_indexed_time: Date | string;
    generation_span: unknown;
    instance_id: string;
    latest_indexed_time: Date | string;
    measured_at: Date | string;
    source: string;
    tenant_id: string;
};

type SourceProgressRow = {
    cursor: string | null;
    ingest_scope_id: string;
    last_batch_size: number;
    last_indexed_event_time: Date | string | null;
    last_indexed_offset: number | string | null;
    last_lag_seconds: number | null;
    last_observed_instance_id: string | null;
    last_observed_source: string | null;
    last_observed_tenant_id: string | null;
    processed_count: number | string;
    source_uri: string;
    updated_at: Date | string;
};

type SourceLeaderLeaseRow = {
    holder_id: string;
    lease_expires_at: Date | string;
};

type RuntimeScopeSourceUriRow = {
    source_uri: string;
};

type BackfillRunRow = {
    last_cursor: string | null;
    max_realtime_lag_seconds: number;
    mode: string;
    pause_reason_code: string;
    rows_processed: number | string;
    run_id: string;
    status: string;
    throttle_batch_size: number;
    updated_at: Date | string;
};

export type PostgresRestoreIndexStoreOptions = {
    pool?: Pool;
    poolConfig?: Omit<PoolConfig, 'connectionString'>;
    schemaName?: string;
    sourceLeaderLeaseTableName?: string;
    sourceProgressTableName?: string;
    tableName?: string;
};

const DEFAULT_SCHEMA = 'rez_restore_index';
const DEFAULT_SOURCE_PROGRESS_TABLE = 'source_progress_v2';
const DEFAULT_SOURCE_LEADER_LEASE_TABLE = 'source_leader_leases_v2';
const PG_BIGINT_MAX = BigInt('9223372036854775807');
const UNKNOWN_SCOPE = {
    instanceId: 'unknown_instance',
    source: 'unknown_source',
    tenantId: 'unknown_tenant',
} as const;

function canonicalizeTimestamp(
    value: string | Date,
    field: string,
): string {
    const input = value instanceof Date ? value.toISOString() : value;

    try {
        return canonicalizeIsoDateTimeWithMillis(String(input));
    } catch {
        throw new Error(`invalid ${field} timestamp`);
    }
}

function toIsoTimestamp(
    value: Date | string | null,
    field: string,
): string | null {
    if (value === null) {
        return null;
    }

    const input = value instanceof Date ? value.toISOString() : String(value);

    return canonicalizeTimestamp(input, field);
}

function toPgBigIntOffset(
    value: string | number,
    field: string,
): string {
    const canonical = canonicalizeRestoreOffsetDecimalString(value);
    const parsed = BigInt(canonical);

    if (parsed > PG_BIGINT_MAX) {
        throw new Error(`${field} exceeds PostgreSQL BIGINT range`);
    }

    return canonical;
}

function parseJsonObject(
    value: unknown,
    field: string,
): Record<string, string> {
    let parsed = value;

    if (typeof parsed === 'string') {
        parsed = JSON.parse(parsed) as unknown;
    }

    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
        throw new Error(`invalid ${field} payload`);
    }

    const record = parsed as Record<string, unknown>;
    const out: Record<string, string> = {};

    for (const [key, rawValue] of Object.entries(record)) {
        out[String(key)] = String(rawValue);
    }

    return out;
}

function parseInteger(
    value: unknown,
    field: string,
): number {
    const parsed = Number(value);

    if (!Number.isInteger(parsed)) {
        throw new Error(`invalid ${field} integer`);
    }

    return parsed;
}

function parseNonNegativeInteger(
    value: unknown,
    field: string,
): number {
    const parsed = parseInteger(value, field);

    if (parsed < 0) {
        throw new Error(`invalid ${field} integer`);
    }

    return parsed;
}

function readOptionalString(
    value: unknown,
): string | null {
    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();

    return trimmed.length > 0 ? trimmed : null;
}

function readOptionalInteger(
    value: unknown,
): number | null {
    if (typeof value !== 'number') {
        return null;
    }

    if (!Number.isInteger(value)) {
        return null;
    }

    return value;
}

function sanitizeOperation(
    value: unknown,
): 'I' | 'U' | 'D' | null {
    if (value === 'I' || value === 'U' || value === 'D') {
        return value;
    }

    return null;
}

function toOptionalNonNegativeInteger(
    value: unknown,
): number | null {
    if (value === null || value === undefined) {
        return null;
    }

    const parsed = Number(value);

    if (!Number.isInteger(parsed) || parsed < 0) {
        return null;
    }

    return parsed;
}

function parseServiceNowUtcMillis(
    value: string | null,
): number {
    if (value === null) {
        return Number.NEGATIVE_INFINITY;
    }

    const parsed = Date.parse(
        value.replace(' ', 'T') + '.000Z',
    );

    if (!Number.isFinite(parsed)) {
        return Number.NEGATIVE_INFINITY;
    }

    return parsed;
}

function parseIsoUtcMillis(
    value: string,
): number {
    const parsed = Date.parse(value);

    if (!Number.isFinite(parsed)) {
        return Number.NEGATIVE_INFINITY;
    }

    return parsed;
}

function parseOffsetBigInt(
    value: string,
): bigint {
    try {
        return BigInt(canonicalizeRestoreOffsetDecimalString(value));
    } catch {
        return BigInt(0);
    }
}

function compareLookupCandidates(
    left: IndexedEventLookupCandidate,
    right: IndexedEventLookupCandidate,
): number {
    const tableCompare = left.table.localeCompare(right.table);

    if (tableCompare !== 0) {
        return tableCompare;
    }

    const recordCompare = left.recordSysId.localeCompare(right.recordSysId);

    if (recordCompare !== 0) {
        return recordCompare;
    }

    const sysUpdatedCompare = parseServiceNowUtcMillis(right.sysUpdatedOn)
        - parseServiceNowUtcMillis(left.sysUpdatedOn);

    if (sysUpdatedCompare !== 0) {
        return sysUpdatedCompare;
    }

    const leftSysModCount = left.sysModCount === null
        ? Number.NEGATIVE_INFINITY
        : left.sysModCount;
    const rightSysModCount = right.sysModCount === null
        ? Number.NEGATIVE_INFINITY
        : right.sysModCount;
    const sysModCompare = rightSysModCount - leftSysModCount;

    if (sysModCompare !== 0) {
        return sysModCompare;
    }

    const eventTimeCompare = parseIsoUtcMillis(right.eventTime)
        - parseIsoUtcMillis(left.eventTime);

    if (eventTimeCompare !== 0) {
        return eventTimeCompare;
    }

    const eventIdCompare = right.eventId.localeCompare(left.eventId);

    if (eventIdCompare !== 0) {
        return eventIdCompare;
    }

    const topicCompare = left.topic.localeCompare(right.topic);

    if (topicCompare !== 0) {
        return topicCompare;
    }

    if (left.partition !== right.partition) {
        return left.partition - right.partition;
    }

    const leftOffset = parseOffsetBigInt(left.offset);
    const rightOffset = parseOffsetBigInt(right.offset);

    if (leftOffset !== rightOffset) {
        return rightOffset > leftOffset ? 1 : -1;
    }

    const artifactCompare = left.artifactKey.localeCompare(right.artifactKey);

    if (artifactCompare !== 0) {
        return artifactCompare;
    }

    return left.manifestKey.localeCompare(right.manifestKey);
}

function buildIndexedEventLookupResult(
    candidates: IndexedEventLookupCandidate[],
): IndexedEventLookupResult {
    const ordered = [...candidates].sort(compareLookupCandidates);

    return {
        candidates: ordered,
        coverage: ordered.length > 0 ? 'covered' : 'no_indexed_coverage',
    };
}

function buildLookupCandidateFromIndexedEvent(
    record: IndexedEventRecord,
): IndexedEventLookupCandidate | null {
    const table = readOptionalString(record.table)
        || readOptionalString(record.metadata.table);
    const recordSysId = readOptionalString(record.metadata.record_sys_id);
    const eventId = readOptionalString(record.metadata.event_id)
        || record.artifactKey;
    const eventTimeInput = readOptionalString(record.metadata.__time)
        || record.indexedAt;
    const kafkaOffsetRaw = record.metadata.offset;
    const metadataRecord = record.metadata as Record<string, unknown>;
    const manifestKey =
        readOptionalString(metadataRecord.manifest_key)
        || readOptionalString(metadataRecord.manifestKey)
        || record.artifactKey.replace(/\.artifact\.json$/u, '.manifest.json');

    if (!table || !recordSysId) {
        return null;
    }

    if (typeof kafkaOffsetRaw !== 'string' && typeof kafkaOffsetRaw !== 'number') {
        return null;
    }

    return {
        artifactKey: record.artifactKey,
        eventId,
        eventTime: canonicalizeLookupTimestamp(
            eventTimeInput,
            'indexed-event lookup event_time',
        ),
        manifestKey,
        offset: canonicalizeRestoreOffsetDecimalString(kafkaOffsetRaw),
        partition: parseNonNegativeInteger(
            record.partitionScope.partition,
            'indexed-event lookup partition',
        ),
        recordSysId,
        sysModCount: toOptionalNonNegativeInteger(record.metadata.sys_mod_count),
        sysUpdatedOn: readOptionalString(record.metadata.sys_updated_on),
        table,
        topic: readRequiredRuntimeText(
            record.partitionScope.topic,
            'indexed-event lookup topic',
        ),
    };
}

function validateSqlIdentifier(
    value: string,
    field: string,
): string {
    const trimmed = String(value || '').trim();

    if (!trimmed) {
        throw new Error(`${field} must not be empty`);
    }

    if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
        throw new Error(
            `${field} must use [A-Za-z_][A-Za-z0-9_]* identifier format`,
        );
    }

    return trimmed;
}

export class PostgresRestoreIndexStore implements RestoreIndexStore {
    private readonly ownsPool: boolean;

    private readonly pool: Pool;

    private readonly ready: Promise<void>;

    private readonly schemaName: string;

    private readonly indexEventsTableQualified: string;

    private readonly partitionGenerationsTableQualified: string;

    private readonly partitionWatermarksTableQualified: string;

    private readonly sourceCoverageTableQualified: string;

    private readonly sourceProgressTableQualified: string;

    private readonly sourceLeaderLeasesTableQualified: string;

    private readonly backfillRunsTableQualified: string;

    private readonly runtimeScopeSourceUris = new Map<string, string>();

    private indexedEventCount = 0;

    constructor(
        pgUrl: string,
        options: PostgresRestoreIndexStoreOptions = {},
    ) {
        const connectionString = String(pgUrl || '').trim();

        if (!connectionString && !options.pool) {
            throw new Error('REZ_RESTORE_PG_URL is required');
        }

        this.schemaName = validateSqlIdentifier(
            options.schemaName || DEFAULT_SCHEMA,
            'restore-index schema name',
        );
        const sourceProgressTableName = validateSqlIdentifier(
            options.sourceProgressTableName
                || options.tableName
                || DEFAULT_SOURCE_PROGRESS_TABLE,
            'source progress table name',
        );
        const sourceLeaderLeaseTableName = validateSqlIdentifier(
            options.sourceLeaderLeaseTableName
                || DEFAULT_SOURCE_LEADER_LEASE_TABLE,
            'source leader lease table name',
        );

        this.indexEventsTableQualified = `"${this.schemaName}"."index_events"`;
        this.partitionGenerationsTableQualified =
            `"${this.schemaName}"."partition_generations"`;
        this.partitionWatermarksTableQualified =
            `"${this.schemaName}"."partition_watermarks"`;
        this.sourceCoverageTableQualified =
            `"${this.schemaName}"."source_coverage"`;
        this.backfillRunsTableQualified = `"${this.schemaName}"."backfill_runs"`;
        this.sourceProgressTableQualified =
            `"${this.schemaName}"."${sourceProgressTableName}"`;
        this.sourceLeaderLeasesTableQualified =
            `"${this.schemaName}"."${sourceLeaderLeaseTableName}"`;

        if (options.pool) {
            this.pool = options.pool;
            this.ownsPool = false;
        } else {
            this.pool = new Pool({
                allowExitOnIdle: true,
                connectionString,
                idleTimeoutMillis: options.poolConfig?.idleTimeoutMillis ?? 30000,
                max: options.poolConfig?.max ?? 10,
                ...options.poolConfig,
            });
            this.ownsPool = true;
        }

        this.ready = this.initialize();
    }

    getIndexedEventCount(): number {
        return this.indexedEventCount;
    }

    async close(): Promise<void> {
        if (!this.ownsPool) {
            return;
        }

        await this.pool.end();
    }

    async acquireSourceLeaderLease(
        input: SourceLeaderLeaseAcquireInput,
    ): Promise<boolean> {
        await this.ensureReady();
        const scope = resolveRuntimeScopeFromInput(input);
        await this.assertRuntimeScopeSourceUri(scope);

        const leaseDurationSeconds = Math.max(
            1,
            Math.floor(input.leaseDurationSeconds),
        );
        const result = await this.pool.query<SourceLeaderLeaseRow>(
            `INSERT INTO ${this.sourceLeaderLeasesTableQualified} AS leases (
                ingest_scope_id,
                holder_id,
                lease_expires_at,
                created_at,
                updated_at
            ) VALUES (
                $1,
                $2,
                now() + (($3::text || ' seconds')::interval),
                now(),
                now()
            )
            ON CONFLICT (ingest_scope_id) DO UPDATE SET
                holder_id = CASE
                    WHEN leases.holder_id = EXCLUDED.holder_id
                        OR leases.lease_expires_at <= now()
                    THEN EXCLUDED.holder_id
                    ELSE leases.holder_id
                END,
                lease_expires_at = CASE
                    WHEN leases.holder_id = EXCLUDED.holder_id
                        OR leases.lease_expires_at <= now()
                    THEN EXCLUDED.lease_expires_at
                    ELSE leases.lease_expires_at
                END,
                updated_at = CASE
                    WHEN leases.holder_id = EXCLUDED.holder_id
                        OR leases.lease_expires_at <= now()
                    THEN now()
                    ELSE leases.updated_at
                END
            RETURNING
                holder_id,
                lease_expires_at`,
            [
                scope.ingestScopeId,
                input.holderId,
                leaseDurationSeconds,
            ],
        );

        if (result.rowCount !== 1) {
            return false;
        }

        return result.rows[0].holder_id === input.holderId;
    }

    async releaseSourceLeaderLease(
        input: SourceLeaderLeaseReleaseInput,
    ): Promise<void> {
        await this.ensureReady();
        const scope = resolveRuntimeScopeFromInput(input);
        await this.assertRuntimeScopeSourceUri(scope);
        await this.pool.query(
            `UPDATE ${this.sourceLeaderLeasesTableQualified}
            SET lease_expires_at = now(),
                updated_at = now()
            WHERE ingest_scope_id = $1
              AND holder_id = $2`,
            [
                scope.ingestScopeId,
                input.holderId,
            ],
        );
    }

    async upsertIndexedEvent(
        record: IndexedEventRecord,
    ): Promise<'inserted' | 'existing'> {
        await this.ensureReady();

        const eventId = readOptionalString(record.metadata.event_id)
            || record.artifactKey;
        const eventType = readOptionalString(record.metadata.event_type)
            || 'unknown';
        const eventTime = canonicalizeTimestamp(
            readOptionalString(record.metadata.__time) || record.indexedAt,
            'index event_time',
        );
        const kafkaOffsetRaw = record.metadata.offset;

        if (typeof kafkaOffsetRaw !== 'string' && typeof kafkaOffsetRaw !== 'number') {
            throw new Error('index event metadata.offset is required');
        }

        const kafkaOffset = toPgBigIntOffset(
            kafkaOffsetRaw,
            'index event kafka_offset',
        );
        const metadataRecord = record.metadata as Record<string, unknown>;
        const manifestKey =
            readOptionalString(metadataRecord.manifest_key)
            || readOptionalString(metadataRecord.manifestKey)
            || record.artifactKey.replace(/\.artifact\.json$/u, '.manifest.json');
        const result = await this.pool.query(
            `INSERT INTO ${this.indexEventsTableQualified} (
                tenant_id,
                instance_id,
                source,
                app,
                table_name,
                record_sys_id,
                attachment_sys_id,
                media_id,
                event_id,
                event_type,
                operation,
                schema_version,
                sys_updated_on,
                sys_mod_count,
                event_time,
                topic,
                kafka_partition,
                kafka_offset,
                content_type,
                size_bytes,
                sha256_plain,
                artifact_key,
                manifest_key,
                artifact_kind,
                generation_id,
                indexed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15::timestamptz, $16, $17, $18,
                $19, $20, $21, $22, $23, $24, $25, $26::timestamptz
            )
            ON CONFLICT (
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                event_id
            ) DO NOTHING
            RETURNING id`,
            [
                record.tenantId,
                record.partitionScope.instanceId,
                record.partitionScope.source,
                record.app,
                record.table,
                readOptionalString(record.metadata.record_sys_id),
                readOptionalString(record.metadata.attachment_sys_id),
                readOptionalString(record.metadata.media_id),
                eventId,
                eventType,
                sanitizeOperation(record.metadata.operation),
                readOptionalInteger(record.metadata.schema_version),
                readOptionalString(record.metadata.sys_updated_on),
                readOptionalInteger(record.metadata.sys_mod_count),
                eventTime,
                record.partitionScope.topic,
                record.partitionScope.partition,
                kafkaOffset,
                readOptionalString(record.metadata.content_type),
                readOptionalInteger(record.metadata.size_bytes),
                readOptionalString(record.metadata.sha256_plain),
                record.artifactKey,
                manifestKey,
                record.artifactKind,
                record.generationId,
                canonicalizeTimestamp(record.indexedAt, 'index indexed_at'),
            ],
        );

        if (result.rowCount === 1) {
            this.indexedEventCount += 1;
            return 'inserted';
        }

        return 'existing';
    }

    async lookupIndexedEventCandidates(
        input: IndexedEventLookupInput,
    ): Promise<IndexedEventLookupResult> {
        await this.ensureReady();

        const normalized = normalizeIndexedEventLookupInput(input);
        const params: Array<string | string[]> = [
            normalized.tenantId,
            normalized.instanceId,
            normalized.source,
            normalized.pitCutoff,
            normalized.tables,
        ];
        let recordSysIdClause = '';

        if (normalized.recordSysIds.length > 0) {
            const recordSysIdParam = params.length + 1;
            params.push(normalized.recordSysIds);
            recordSysIdClause =
                `AND record_sys_id = ANY($${recordSysIdParam}::text[])`;
        }

        const result = await this.pool.query<IndexedEventLookupRow>(
            `SELECT
                table_name,
                record_sys_id,
                event_id,
                sys_updated_on,
                sys_mod_count,
                event_time,
                topic,
                kafka_partition,
                kafka_offset,
                artifact_key,
                manifest_key
            FROM ${this.indexEventsTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3
              AND event_time <= $4::timestamptz
              AND table_name = ANY($5::text[])
              AND table_name IS NOT NULL
              AND record_sys_id IS NOT NULL
              ${recordSysIdClause}
            ORDER BY
                table_name ASC,
                record_sys_id ASC,
                sys_updated_on DESC NULLS LAST,
                sys_mod_count DESC NULLS LAST,
                event_time DESC,
                event_id DESC,
                topic ASC,
                kafka_partition ASC,
                kafka_offset::numeric DESC,
                artifact_key ASC,
                manifest_key ASC`,
            params,
        );
        const candidates: IndexedEventLookupCandidate[] = [];

        for (const row of result.rows) {
            const table = readOptionalString(row.table_name);
            const recordSysId = readOptionalString(row.record_sys_id);
            const eventId = readOptionalString(row.event_id);
            const artifactKey = readOptionalString(row.artifact_key);
            const manifestKey = readOptionalString(row.manifest_key);

            if (
                !table
                || !recordSysId
                || !eventId
                || !artifactKey
                || !manifestKey
            ) {
                continue;
            }

            candidates.push({
                artifactKey,
                eventId,
                eventTime: canonicalizeTimestamp(
                    row.event_time,
                    'indexed-event lookup event_time',
                ),
                manifestKey,
                offset: canonicalizeRestoreOffsetDecimalString(row.kafka_offset),
                partition: parseNonNegativeInteger(
                    row.kafka_partition,
                    'indexed-event lookup kafka_partition',
                ),
                recordSysId,
                sysModCount: toOptionalNonNegativeInteger(row.sys_mod_count),
                sysUpdatedOn: readOptionalString(row.sys_updated_on),
                table,
                topic: readRequiredRuntimeText(
                    row.topic,
                    'indexed-event lookup topic',
                ),
            });
        }

        return buildIndexedEventLookupResult(candidates);
    }

    async getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null> {
        await this.ensureReady();
        const result = await this.pool.query<PartitionWatermarkRow>(
            `SELECT
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                indexed_through_offset::text AS indexed_through_offset,
                indexed_through_time,
                coverage_start,
                coverage_end,
                measured_at
            FROM ${this.partitionWatermarksTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND topic = $3
              AND kafka_partition = $4
            LIMIT 1`,
            [
                scope.tenantId,
                scope.instanceId,
                scope.topic,
                scope.partition,
            ],
        );

        if (result.rowCount !== 1) {
            return null;
        }

        const row = result.rows[0];

        return {
            coverageEnd: toIsoTimestamp(
                row.coverage_end,
                'partition watermark coverage_end',
            ) as string,
            coverageStart: toIsoTimestamp(
                row.coverage_start,
                'partition watermark coverage_start',
            ) as string,
            generationId: row.generation_id,
            indexedThroughOffset: canonicalizeRestoreOffsetDecimalString(
                row.indexed_through_offset,
            ),
            indexedThroughTime: toIsoTimestamp(
                row.indexed_through_time,
                'partition watermark indexed_through_time',
            ) as string,
            instanceId: row.instance_id,
            measuredAt: toIsoTimestamp(
                row.measured_at,
                'partition watermark measured_at',
            ) as string,
            partition: parseNonNegativeInteger(
                row.kafka_partition,
                'partition watermark kafka_partition',
            ),
            source: row.source,
            tenantId: row.tenant_id,
            topic: row.topic,
        };
    }

    async putPartitionWatermark(state: PartitionWatermarkState): Promise<void> {
        await this.ensureReady();

        const indexedThroughOffset = toPgBigIntOffset(
            state.indexedThroughOffset,
            'partition watermark indexed_through_offset',
        );
        const indexedThroughTime = canonicalizeTimestamp(
            state.indexedThroughTime,
            'partition watermark indexed_through_time',
        );
        const coverageStart = canonicalizeTimestamp(
            state.coverageStart,
            'partition watermark coverage_start',
        );
        const coverageEnd = canonicalizeTimestamp(
            state.coverageEnd,
            'partition watermark coverage_end',
        );
        const measuredAt = canonicalizeTimestamp(
            state.measuredAt,
            'partition watermark measured_at',
        );

        await this.pool.query(
            `INSERT INTO ${this.partitionWatermarksTableQualified} (
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                indexed_through_offset,
                indexed_through_time,
                coverage_start,
                coverage_end,
                freshness,
                executability,
                reason_code,
                measured_at,
                updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8::timestamptz, $9::timestamptz,
                $10::timestamptz, 'fresh', 'executable', 'none',
                $11::timestamptz, now()
            )
            ON CONFLICT (
                tenant_id,
                instance_id,
                topic,
                kafka_partition
            ) DO UPDATE SET
                source = EXCLUDED.source,
                generation_id = EXCLUDED.generation_id,
                indexed_through_offset = EXCLUDED.indexed_through_offset,
                indexed_through_time = EXCLUDED.indexed_through_time,
                coverage_start = EXCLUDED.coverage_start,
                coverage_end = EXCLUDED.coverage_end,
                freshness = EXCLUDED.freshness,
                executability = EXCLUDED.executability,
                reason_code = EXCLUDED.reason_code,
                measured_at = EXCLUDED.measured_at,
                updated_at = now()`,
            [
                state.tenantId,
                state.instanceId,
                state.source,
                state.topic,
                state.partition,
                state.generationId,
                indexedThroughOffset,
                indexedThroughTime,
                coverageStart,
                coverageEnd,
                measuredAt,
            ],
        );

        await this.pool.query(
            `INSERT INTO ${this.partitionGenerationsTableQualified} (
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                generation_started_at,
                generation_ended_at,
                max_indexed_offset,
                max_indexed_time
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7::timestamptz, NULL,
                $8, $9::timestamptz
            )
            ON CONFLICT (
                tenant_id,
                instance_id,
                topic,
                kafka_partition,
                generation_id
            ) DO UPDATE SET
                source = EXCLUDED.source,
                max_indexed_offset = EXCLUDED.max_indexed_offset,
                max_indexed_time = EXCLUDED.max_indexed_time,
                generation_ended_at = NULL`,
            [
                state.tenantId,
                state.instanceId,
                state.source,
                state.topic,
                state.partition,
                state.generationId,
                coverageStart,
                indexedThroughOffset,
                indexedThroughTime,
            ],
        );

        await this.pool.query(
            `UPDATE ${this.partitionGenerationsTableQualified}
            SET generation_ended_at = COALESCE(generation_ended_at, $1::timestamptz)
            WHERE tenant_id = $2
              AND instance_id = $3
              AND topic = $4
              AND kafka_partition = $5
              AND generation_id <> $6
              AND generation_ended_at IS NULL`,
            [
                indexedThroughTime,
                state.tenantId,
                state.instanceId,
                state.topic,
                state.partition,
                state.generationId,
            ],
        );
    }

    async recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null> {
        await this.ensureReady();

        const measuredAt = canonicalizeTimestamp(
            input.measuredAt,
            'source coverage measured_at',
        );
        const result = await this.pool.query<PartitionWatermarkRow>(
            `SELECT
                tenant_id,
                instance_id,
                source,
                topic,
                kafka_partition,
                generation_id,
                indexed_through_offset::text AS indexed_through_offset,
                indexed_through_time,
                coverage_start,
                coverage_end,
                measured_at
            FROM ${this.partitionWatermarksTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3`,
            [
                input.tenantId,
                input.instanceId,
                input.source,
            ],
        );
        const watermarks = result.rows.map((row) => {
            return {
                coverageEnd: toIsoTimestamp(
                    row.coverage_end,
                    'source coverage coverage_end',
                ) as string,
                coverageStart: toIsoTimestamp(
                    row.coverage_start,
                    'source coverage coverage_start',
                ) as string,
                generationId: row.generation_id,
                indexedThroughOffset: canonicalizeRestoreOffsetDecimalString(
                    row.indexed_through_offset,
                ),
                indexedThroughTime: toIsoTimestamp(
                    row.indexed_through_time,
                    'source coverage indexed_through_time',
                ) as string,
                instanceId: row.instance_id,
                measuredAt: toIsoTimestamp(
                    row.measured_at,
                    'source coverage measured_at',
                ) as string,
                partition: parseNonNegativeInteger(
                    row.kafka_partition,
                    'source coverage kafka_partition',
                ),
                source: row.source,
                tenantId: row.tenant_id,
                topic: row.topic,
            };
        });
        const state = recomputeCoverageFromWatermarks({
            ...input,
            measuredAt,
        }, watermarks);

        if (state === null) {
            await this.pool.query(
                `DELETE FROM ${this.sourceCoverageTableQualified}
                WHERE tenant_id = $1
                  AND instance_id = $2
                  AND source = $3`,
                [
                    input.tenantId,
                    input.instanceId,
                    input.source,
                ],
            );
            return null;
        }

        await this.pool.query(
            `INSERT INTO ${this.sourceCoverageTableQualified} (
                tenant_id,
                instance_id,
                source,
                earliest_indexed_time,
                latest_indexed_time,
                generation_span,
                measured_at
            ) VALUES (
                $1, $2, $3, $4::timestamptz, $5::timestamptz, $6::jsonb, $7::timestamptz
            )
            ON CONFLICT (tenant_id, instance_id, source) DO UPDATE SET
                earliest_indexed_time = EXCLUDED.earliest_indexed_time,
                latest_indexed_time = EXCLUDED.latest_indexed_time,
                generation_span = EXCLUDED.generation_span,
                measured_at = EXCLUDED.measured_at`,
            [
                state.tenantId,
                state.instanceId,
                state.source,
                canonicalizeTimestamp(
                    state.earliestIndexedTime,
                    'source coverage earliest_indexed_time',
                ),
                canonicalizeTimestamp(
                    state.latestIndexedTime,
                    'source coverage latest_indexed_time',
                ),
                JSON.stringify(state.generationSpan),
                canonicalizeTimestamp(
                    state.measuredAt,
                    'source coverage measured_at',
                ),
            ],
        );

        return cloneState(state);
    }

    async getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null> {
        await this.ensureReady();
        const result = await this.pool.query<SourceCoverageRow>(
            `SELECT
                tenant_id,
                instance_id,
                source,
                earliest_indexed_time,
                latest_indexed_time,
                generation_span,
                measured_at
            FROM ${this.sourceCoverageTableQualified}
            WHERE tenant_id = $1
              AND instance_id = $2
              AND source = $3
            LIMIT 1`,
            [
                tenantId,
                instanceId,
                source,
            ],
        );

        if (result.rowCount !== 1) {
            return null;
        }

        const row = result.rows[0];

        return {
            earliestIndexedTime: toIsoTimestamp(
                row.earliest_indexed_time,
                'source coverage earliest_indexed_time',
            ) as string,
            generationSpan: parseJsonObject(
                row.generation_span,
                'source coverage generation_span',
            ),
            instanceId: row.instance_id,
            latestIndexedTime: toIsoTimestamp(
                row.latest_indexed_time,
                'source coverage latest_indexed_time',
            ) as string,
            measuredAt: toIsoTimestamp(
                row.measured_at,
                'source coverage measured_at',
            ) as string,
            source: row.source,
            tenantId: row.tenant_id,
        };
    }

    async putSourceProgress(
        input: SourceProgressState | SourceProgressPutInput,
    ): Promise<void> {
        await this.ensureReady();
        const resolved = resolveSourceProgressPutInput(input);
        await this.assertRuntimeScopeSourceUri(resolved.scope);
        const state = resolved.state;

        await this.pool.query(
            `INSERT INTO ${this.sourceProgressTableQualified} (
                ingest_scope_id,
                source_uri,
                cursor,
                last_batch_size,
                last_indexed_event_time,
                last_indexed_offset,
                last_lag_seconds,
                processed_count,
                updated_at,
                last_observed_tenant_id,
                last_observed_instance_id,
                last_observed_source
            ) VALUES (
                $1, $2, $3, $4, $5::timestamptz, $6, $7, $8, $9::timestamptz,
                $10, $11, $12
            )
            ON CONFLICT (ingest_scope_id) DO UPDATE SET
                source_uri = EXCLUDED.source_uri,
                cursor = EXCLUDED.cursor,
                last_batch_size = EXCLUDED.last_batch_size,
                last_indexed_event_time = EXCLUDED.last_indexed_event_time,
                last_indexed_offset = EXCLUDED.last_indexed_offset,
                last_lag_seconds = EXCLUDED.last_lag_seconds,
                processed_count = EXCLUDED.processed_count,
                updated_at = EXCLUDED.updated_at,
                last_observed_tenant_id = EXCLUDED.last_observed_tenant_id,
                last_observed_instance_id = EXCLUDED.last_observed_instance_id,
                last_observed_source = EXCLUDED.last_observed_source`,
            [
                resolved.scope.ingestScopeId,
                resolved.scope.sourceUri,
                state.cursor,
                Math.max(0, state.lastBatchSize),
                state.lastIndexedEventTime === null
                    ? null
                    : canonicalizeTimestamp(
                        state.lastIndexedEventTime,
                        'source progress last_indexed_event_time',
                    ),
                state.lastIndexedOffset === null
                    ? null
                    : toPgBigIntOffset(
                        state.lastIndexedOffset,
                        'source progress last_indexed_offset',
                    ),
                state.lastLagSeconds === null
                    ? null
                    : Math.max(0, state.lastLagSeconds),
                Math.max(0, state.processedCount),
                canonicalizeTimestamp(
                    state.updatedAt,
                    'source progress updated_at',
                ),
                readOptionalString(state.tenantId),
                readOptionalString(state.instanceId),
                readOptionalString(state.source),
            ],
        );
    }

    async getSourceProgress(
        scopeOrTenantId: SourceProgressScopeInput | string,
        instanceId?: string,
        source?: string,
    ): Promise<SourceProgressState | null> {
        await this.ensureReady();
        const scope = resolveSourceProgressScope(
            scopeOrTenantId,
            instanceId,
            source,
        );
        const legacyScope = resolveLegacyScopeHint(
            scopeOrTenantId,
            instanceId,
            source,
        );
        await this.assertRuntimeScopeSourceUri(scope, {
            registerIfMissing: false,
        });
        const result = await this.pool.query<SourceProgressRow>(
            `SELECT
                ingest_scope_id,
                source_uri,
                cursor,
                last_batch_size,
                last_indexed_event_time,
                last_indexed_offset::text AS last_indexed_offset,
                last_lag_seconds,
                processed_count,
                updated_at,
                last_observed_tenant_id,
                last_observed_instance_id,
                last_observed_source
            FROM ${this.sourceProgressTableQualified}
            WHERE ingest_scope_id = $1
            LIMIT 1`,
            [
                scope.ingestScopeId,
            ],
        );

        if (result.rowCount !== 1) {
            return null;
        }

        const row = result.rows[0];
        const observedTenantId = readOptionalString(row.last_observed_tenant_id)
            || legacyScope?.tenantId
            || UNKNOWN_SCOPE.tenantId;
        const observedInstanceId =
            readOptionalString(row.last_observed_instance_id)
            || legacyScope?.instanceId
            || UNKNOWN_SCOPE.instanceId;
        const observedSource = readOptionalString(row.last_observed_source)
            || legacyScope?.source
            || readRequiredRuntimeText(
                row.source_uri,
                'source progress source_uri',
            );
        this.runtimeScopeSourceUris.set(
            scope.ingestScopeId,
            readRequiredRuntimeText(
                row.source_uri,
                'source progress source_uri',
            ),
        );

        return {
            cursor: row.cursor,
            instanceId: observedInstanceId,
            lastBatchSize: parseNonNegativeInteger(
                row.last_batch_size,
                'source progress last_batch_size',
            ),
            lastIndexedEventTime: toIsoTimestamp(
                row.last_indexed_event_time,
                'source progress last_indexed_event_time',
            ),
            lastIndexedOffset: row.last_indexed_offset === null
                ? null
                : canonicalizeRestoreOffsetDecimalString(row.last_indexed_offset),
            lastLagSeconds: row.last_lag_seconds === null
                ? null
                : parseNonNegativeInteger(
                    row.last_lag_seconds,
                    'source progress last_lag_seconds',
                ),
            processedCount: parseNonNegativeInteger(
                row.processed_count,
                'source progress processed_count',
            ),
            source: observedSource,
            tenantId: observedTenantId,
            updatedAt: toIsoTimestamp(
                row.updated_at,
                'source progress updated_at',
            ) as string,
        };
    }

    async upsertBackfillRun(state: BackfillRunState): Promise<void> {
        await this.ensureReady();

        const scoped = state as BackfillRunState & {
            instanceId?: string;
            source?: string;
            tenantId?: string;
        };
        const tenantId = readOptionalString(scoped.tenantId)
            || UNKNOWN_SCOPE.tenantId;
        const instanceId = readOptionalString(scoped.instanceId)
            || UNKNOWN_SCOPE.instanceId;
        const source = readOptionalString(scoped.source) || UNKNOWN_SCOPE.source;
        const updatedAt = canonicalizeTimestamp(
            state.updatedAt,
            'backfill run updated_at',
        );

        await this.pool.query(
            `INSERT INTO ${this.backfillRunsTableQualified} (
                run_id,
                tenant_id,
                instance_id,
                source,
                mode,
                status,
                pause_reason_code,
                throttle_batch_size,
                max_realtime_lag_seconds,
                last_cursor,
                rows_processed,
                started_at,
                updated_at,
                completed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12::timestamptz, $13::timestamptz, $14::timestamptz
            )
            ON CONFLICT (run_id) DO UPDATE SET
                mode = EXCLUDED.mode,
                status = EXCLUDED.status,
                pause_reason_code = EXCLUDED.pause_reason_code,
                throttle_batch_size = EXCLUDED.throttle_batch_size,
                max_realtime_lag_seconds = EXCLUDED.max_realtime_lag_seconds,
                last_cursor = EXCLUDED.last_cursor,
                rows_processed = EXCLUDED.rows_processed,
                updated_at = EXCLUDED.updated_at,
                completed_at = EXCLUDED.completed_at`,
            [
                state.runId,
                tenantId,
                instanceId,
                source,
                state.mode,
                state.status,
                state.reasonCode,
                Math.max(1, state.throttleBatchSize),
                Math.max(0, state.maxRealtimeLagSeconds),
                state.cursor,
                Math.max(0, state.processedCount),
                updatedAt,
                updatedAt,
                state.status === 'completed' ? updatedAt : null,
            ],
        );
    }

    async getBackfillRun(runId: string): Promise<BackfillRunState | null> {
        await this.ensureReady();
        const result = await this.pool.query<BackfillRunRow>(
            `SELECT
                run_id,
                mode,
                status,
                pause_reason_code,
                throttle_batch_size,
                max_realtime_lag_seconds,
                last_cursor,
                rows_processed,
                updated_at
            FROM ${this.backfillRunsTableQualified}
            WHERE run_id = $1
            LIMIT 1`,
            [
                runId,
            ],
        );

        if (result.rowCount !== 1) {
            return null;
        }

        const row = result.rows[0];

        return {
            cursor: row.last_cursor,
            maxRealtimeLagSeconds: parseNonNegativeInteger(
                row.max_realtime_lag_seconds,
                'backfill run max_realtime_lag_seconds',
            ),
            mode: row.mode as BackfillRunState['mode'],
            processedCount: parseNonNegativeInteger(
                row.rows_processed,
                'backfill run rows_processed',
            ),
            reasonCode: row.pause_reason_code,
            runId: row.run_id,
            status: row.status as BackfillRunState['status'],
            throttleBatchSize: parseNonNegativeInteger(
                row.throttle_batch_size,
                'backfill run throttle_batch_size',
            ),
            updatedAt: toIsoTimestamp(
                row.updated_at,
                'backfill run updated_at',
            ) as string,
        };
    }

    private async ensureReady(): Promise<void> {
        await this.ready;
    }

    private async assertRuntimeScopeSourceUri(
        scope: ResolvedRuntimeScope,
        options: {
            registerIfMissing?: boolean;
        } = {},
    ): Promise<void> {
        const registerIfMissing = options.registerIfMissing !== false;
        const knownSourceUri = this.runtimeScopeSourceUris.get(scope.ingestScopeId);

        if (knownSourceUri && knownSourceUri !== scope.sourceUri) {
            throw buildRuntimeScopeSourceConflictError(
                scope.ingestScopeId,
                scope.sourceUri,
                knownSourceUri,
            );
        }

        const result = await this.pool.query<RuntimeScopeSourceUriRow>(
            `SELECT source_uri
            FROM ${this.sourceProgressTableQualified}
            WHERE ingest_scope_id = $1
            LIMIT 1`,
            [
                scope.ingestScopeId,
            ],
        );
        const persistedSourceUri = result.rowCount === 1
            ? readRequiredRuntimeText(
                result.rows[0].source_uri,
                'source progress source_uri',
            )
            : null;

        if (persistedSourceUri && persistedSourceUri !== scope.sourceUri) {
            throw buildRuntimeScopeSourceConflictError(
                scope.ingestScopeId,
                scope.sourceUri,
                persistedSourceUri,
            );
        }

        if (persistedSourceUri) {
            this.runtimeScopeSourceUris.set(
                scope.ingestScopeId,
                persistedSourceUri,
            );
            return;
        }

        if (registerIfMissing) {
            this.runtimeScopeSourceUris.set(
                scope.ingestScopeId,
                scope.sourceUri,
            );
        }
    }

    private async tableExists(
        tableQualified: string,
    ): Promise<boolean> {
        try {
            await this.pool.query(
                `SELECT 1 FROM ${tableQualified} LIMIT 1`,
            );
            return true;
        } catch (error: unknown) {
            const message = String((error as Error)?.message || error);

            if (
                message.includes('does not exist')
                || message.includes('relation')
            ) {
                return false;
            }

            throw error;
        }
    }

    private async initialize(): Promise<void> {
        await this.pool.query(`CREATE SCHEMA IF NOT EXISTS "${this.schemaName}"`);
        await this.pool.query(this.createIndexEventsSql());
        await this.pool.query(this.createPartitionWatermarksSql());
        await this.pool.query(this.createPartitionGenerationsSql());
        await this.pool.query(this.createSourceCoverageSql());
        await this.pool.query(this.createBackfillRunsSql());
        if (!(await this.tableExists(this.sourceProgressTableQualified))) {
            await this.pool.query(this.createSourceProgressSql());
        }
        if (!(await this.tableExists(this.sourceLeaderLeasesTableQualified))) {
            await this.pool.query(this.createSourceLeaderLeasesSql());
        }

        const countResult = await this.pool.query<{
            count: string;
        }>(
            `SELECT COUNT(*)::text AS count
            FROM ${this.indexEventsTableQualified}`,
        );

        this.indexedEventCount = parseNonNegativeInteger(
            countResult.rows[0]?.count || 0,
            'indexed event count',
        );
    }

    private createIndexEventsSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.indexEventsTableQualified} (
    id BIGSERIAL,
    tenant_id TEXT,
    instance_id TEXT,
    source TEXT,
    app TEXT,
    table_name TEXT,
    record_sys_id TEXT,
    attachment_sys_id TEXT,
    media_id TEXT,
    event_id TEXT,
    event_type TEXT,
    operation TEXT,
    schema_version INTEGER,
    sys_updated_on TEXT,
    sys_mod_count INTEGER,
    event_time TIMESTAMPTZ,
    topic TEXT,
    kafka_partition INTEGER,
    kafka_offset TEXT,
    content_type TEXT,
    size_bytes BIGINT,
    sha256_plain CHAR(64),
    artifact_key TEXT,
    manifest_key TEXT,
    artifact_kind TEXT,
    generation_id TEXT,
    indexed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_index_events_dedupe
ON ${this.indexEventsTableQualified} (
    tenant_id,
    instance_id,
    source,
    topic,
    kafka_partition,
    generation_id,
    event_id
);

CREATE INDEX IF NOT EXISTS ix_index_events_lookup
ON ${this.indexEventsTableQualified} (
    tenant_id,
    instance_id,
    source,
    table_name,
    record_sys_id,
    event_time
);

CREATE INDEX IF NOT EXISTS ix_index_events_partition_offsets
ON ${this.indexEventsTableQualified} (
    tenant_id,
    instance_id,
    topic,
    kafka_partition,
    kafka_offset DESC
);
`;
    }

    private createPartitionWatermarksSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.partitionWatermarksTableQualified} (
    tenant_id TEXT,
    instance_id TEXT,
    source TEXT,
    topic TEXT,
    kafka_partition INTEGER,
    generation_id TEXT,
    indexed_through_offset TEXT,
    indexed_through_time TIMESTAMPTZ,
    coverage_start TIMESTAMPTZ,
    coverage_end TIMESTAMPTZ,
    freshness TEXT,
    executability TEXT,
    reason_code TEXT,
    measured_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_partition_watermarks_scope
ON ${this.partitionWatermarksTableQualified} (
    tenant_id,
    instance_id,
    topic,
    kafka_partition
);

CREATE INDEX IF NOT EXISTS ix_partition_watermarks_source
ON ${this.partitionWatermarksTableQualified} (
    tenant_id,
    instance_id,
    source,
    topic,
    kafka_partition
);
`;
    }

    private createPartitionGenerationsSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.partitionGenerationsTableQualified} (
    tenant_id TEXT,
    instance_id TEXT,
    source TEXT,
    topic TEXT,
    kafka_partition INTEGER,
    generation_id TEXT,
    generation_started_at TIMESTAMPTZ,
    generation_ended_at TIMESTAMPTZ,
    max_indexed_offset TEXT,
    max_indexed_time TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_partition_generations_scope
ON ${this.partitionGenerationsTableQualified} (
    tenant_id,
    instance_id,
    topic,
    kafka_partition,
    generation_id
);
`;
    }

    private createSourceCoverageSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.sourceCoverageTableQualified} (
    tenant_id TEXT,
    instance_id TEXT,
    source TEXT,
    earliest_indexed_time TIMESTAMPTZ,
    latest_indexed_time TIMESTAMPTZ,
    generation_span JSONB,
    measured_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_source_coverage_scope
ON ${this.sourceCoverageTableQualified} (
    tenant_id,
    instance_id,
    source
);
`;
    }

    private createBackfillRunsSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.backfillRunsTableQualified} (
    run_id TEXT,
    tenant_id TEXT,
    instance_id TEXT,
    source TEXT,
    mode TEXT,
    status TEXT,
    pause_reason_code TEXT,
    throttle_batch_size INTEGER,
    max_realtime_lag_seconds INTEGER,
    last_cursor TEXT,
    rows_processed BIGINT,
    started_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_backfill_runs_run_id
ON ${this.backfillRunsTableQualified} (
    run_id
);

CREATE INDEX IF NOT EXISTS ix_backfill_runs_lookup
ON ${this.backfillRunsTableQualified} (
    tenant_id,
    instance_id,
    source,
    mode,
    status,
    updated_at DESC
);
`;
    }

    private createSourceProgressSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.sourceProgressTableQualified} (
    ingest_scope_id TEXT PRIMARY KEY,
    source_uri TEXT NOT NULL,
    cursor TEXT,
    last_batch_size INTEGER NOT NULL CHECK (last_batch_size >= 0),
    last_indexed_event_time TIMESTAMPTZ,
    last_indexed_offset TEXT,
    last_lag_seconds INTEGER CHECK (last_lag_seconds >= 0),
    processed_count BIGINT NOT NULL CHECK (processed_count >= 0),
    updated_at TIMESTAMPTZ NOT NULL,
    last_observed_tenant_id TEXT,
    last_observed_instance_id TEXT,
    last_observed_source TEXT
);

CREATE INDEX IF NOT EXISTS ix_source_progress_runtime_updated_at
ON ${this.sourceProgressTableQualified} (
    updated_at DESC
);
`;
    }

    private createSourceLeaderLeasesSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.sourceLeaderLeasesTableQualified} (
    ingest_scope_id TEXT PRIMARY KEY,
    holder_id TEXT NOT NULL,
    lease_expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_source_leader_leases_active
ON ${this.sourceLeaderLeasesTableQualified} (
    lease_expires_at DESC
);
`;
    }
}
