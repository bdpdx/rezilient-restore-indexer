import {
    canonicalizeRestoreOffsetDecimalString,
    canonicalizeIsoDateTimeWithMillis,
    isoDateTimeWithMillis,
} from '@rezilient/types';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_COVERAGE_WINDOW_VERSION,
    RESTORE_FRESHNESS_GATE_VERSION,
    RESTORE_INDEX_ROW_VERSION,
    RESTORE_WATERMARK_STATE_VERSION,
} from './constants';
import {
    buildRestoreWatermark,
    evaluateFreshnessState,
    type FreshnessGateInput,
    type FreshnessGatePolicy,
} from './freshness';
import { normalizeOperationalMetadata } from './metadata';
import type { RestoreIndexStore } from './store';
import type {
    IndexArtifactInput,
    IndexedArtifactResult,
    IndexedEventRecord,
    PartitionScope,
    PartitionWatermarkState,
    ProcessBatchResult,
    SourceCoverageState,
    SourceProgressState,
} from './types';

export class WatermarkInvariantError extends Error {
    constructor(message: string) {
        super(message);
        this.name = 'WatermarkInvariantError';
    }
}

export type RestoreIndexerOptions = {
    freshnessPolicy: FreshnessGatePolicy;
};

export type PartitionWatermarkStatus = {
    contract_version: string;
    executability: 'executable' | 'preview_only' | 'blocked';
    freshness_gate_version: string;
    freshness: 'fresh' | 'stale' | 'unknown';
    gate_timed_out: boolean;
    lag_seconds: number | null;
    reason_code: string;
    scope: {
        instance_id: string;
        partition: number;
        source: string;
        tenant_id: string;
        topic: string;
    };
    watermark: ReturnType<typeof buildRestoreWatermark> | null;
    watermark_state_version: string;
};

export type SourceCoverageWindow = {
    contract_version: string;
    coverage_window_version: string;
    earliest_indexed_time: string;
    generation_span: Record<string, string>;
    instance_id: string;
    latest_indexed_time: string;
    measured_at: string;
    source: string;
    tenant_id: string;
};

export type SourceProgressWindow = {
    contract_version: string;
    cursor: string | null;
    instance_id: string;
    last_batch_size: number;
    last_indexed_event_time: string | null;
    last_indexed_offset: string | null;
    last_lag_seconds: number | null;
    processed_count: number;
    source: string;
    tenant_id: string;
    updated_at: string;
};

export type SourceProgressUpdateInput = {
    cursor: string | null;
    instanceId: string;
    lastBatchSize: number;
    lastIndexedEventTime: string | null;
    lastIndexedOffset: string | null;
    lastLagSeconds: number | null;
    measuredAt: string;
    processedDelta: number;
    source: string;
    tenantId: string;
};

function maxIso(left: string, right: string): string {
    return left >= right ? left : right;
}

function minIso(left: string, right: string): string {
    return left <= right ? left : right;
}

function maxNullableIso(
    left: string | null,
    right: string | null,
): string | null {
    if (left === null) {
        return right;
    }

    if (right === null) {
        return left;
    }

    return maxIso(left, right);
}

function canonicalizeOffset(
    value: string | number,
    fieldPath: string,
): string {
    try {
        return canonicalizeRestoreOffsetDecimalString(value);
    } catch {
        throw new Error(`Invalid ${fieldPath} value: ${String(value)}`);
    }
}

function compareOffsets(
    left: string,
    right: string,
): number {
    const leftCanonical = canonicalizeOffset(left, 'offset');
    const rightCanonical = canonicalizeOffset(right, 'offset');
    const leftValue = BigInt(leftCanonical);
    const rightValue = BigInt(rightCanonical);

    if (leftValue === rightValue) {
        return 0;
    }

    return leftValue > rightValue ? 1 : -1;
}

function maxNullableOffset(
    left: string | number | null,
    right: string | number | null,
): string | null {
    const leftCanonical = left === null
        ? null
        : canonicalizeOffset(
            left,
            'source progress lastIndexedOffset',
        );
    const rightCanonical = right === null
        ? null
        : canonicalizeOffset(
            right,
            'source progress lastIndexedOffset',
        );

    if (leftCanonical === null) {
        return rightCanonical;
    }

    if (rightCanonical === null) {
        return leftCanonical;
    }

    return compareOffsets(leftCanonical, rightCanonical) >= 0
        ? leftCanonical
        : rightCanonical;
}

function nowIso(): string {
    return new Date().toISOString();
}

function canonicalizeNullableIso(
    value: string | null,
): string | null {
    if (value === null) {
        return null;
    }

    return canonicalizeIsoDateTimeWithMillis(value);
}

function canonicalizeWatermarkTimestamps(
    state: PartitionWatermarkState,
): PartitionWatermarkState {
    return {
        ...state,
        coverageEnd: canonicalizeIsoDateTimeWithMillis(state.coverageEnd),
        coverageStart: canonicalizeIsoDateTimeWithMillis(state.coverageStart),
        indexedThroughOffset: canonicalizeOffset(
            state.indexedThroughOffset,
            'partition watermark indexedThroughOffset',
        ),
        indexedThroughTime: canonicalizeIsoDateTimeWithMillis(
            state.indexedThroughTime,
        ),
        measuredAt: canonicalizeIsoDateTimeWithMillis(state.measuredAt),
    };
}

function buildEventRecord(
    input: IndexArtifactInput,
    normalized: ReturnType<typeof normalizeOperationalMetadata>,
    measuredAt: string,
): IndexedEventRecord {
    return {
        artifactKey: input.manifest.artifact_key,
        artifactKind: input.manifest.artifact_kind,
        app: input.manifest.app,
        generationId: input.generationId,
        indexedAt: measuredAt,
        ingestionMode: input.ingestionMode,
        instanceId: normalized.partitionScope.instanceId,
        manifestVersion: input.manifest.manifest_version,
        metadata: normalized.metadata,
        objectKeyLayoutVersion: input.manifest.object_key_layout_version,
        partitionScope: normalized.partitionScope,
        table: input.manifest.table,
        tenantId: normalized.partitionScope.tenantId,
    };
}

function buildNextWatermark(
    current: PartitionWatermarkState | null,
    nextOffset: string,
    nextEventTime: string,
    partitionScope: PartitionScope,
    generationId: string,
    measuredAt: string,
): PartitionWatermarkState {
    const nextOffsetCanonical = canonicalizeOffset(
        nextOffset,
        'normalized metadata offset',
    );

    if (current === null) {
        return {
            coverageEnd: nextEventTime,
            coverageStart: nextEventTime,
            generationId,
            indexedThroughOffset: nextOffsetCanonical,
            indexedThroughTime: nextEventTime,
            instanceId: partitionScope.instanceId,
            measuredAt,
            partition: partitionScope.partition,
            source: partitionScope.source,
            tenantId: partitionScope.tenantId,
            topic: partitionScope.topic,
        };
    }

    if (current.generationId !== generationId) {
        return {
            coverageEnd: nextEventTime,
            coverageStart: nextEventTime,
            generationId,
            indexedThroughOffset: nextOffsetCanonical,
            indexedThroughTime: nextEventTime,
            instanceId: partitionScope.instanceId,
            measuredAt,
            partition: partitionScope.partition,
            source: partitionScope.source,
            tenantId: partitionScope.tenantId,
            topic: partitionScope.topic,
        };
    }

    if (compareOffsets(nextOffsetCanonical, current.indexedThroughOffset) < 0) {
        throw new WatermarkInvariantError(
            `Offset rewind in generation ${generationId}: `
            + `${nextOffsetCanonical} < ${current.indexedThroughOffset}`,
        );
    }

    const indexedThroughOffset = compareOffsets(
        current.indexedThroughOffset,
        nextOffsetCanonical,
    ) >= 0
        ? current.indexedThroughOffset
        : nextOffsetCanonical;

    return {
        coverageEnd: maxIso(current.coverageEnd, nextEventTime),
        coverageStart: minIso(current.coverageStart, nextEventTime),
        generationId,
        indexedThroughOffset,
        indexedThroughTime: maxIso(
            current.indexedThroughTime,
            nextEventTime,
        ),
        instanceId: partitionScope.instanceId,
        measuredAt,
        partition: partitionScope.partition,
        source: partitionScope.source,
        tenantId: partitionScope.tenantId,
        topic: partitionScope.topic,
    };
}

function readOptionalString(
    value: unknown,
): string | undefined {
    if (typeof value !== 'string') {
        return undefined;
    }

    if (value.trim() === '') {
        return undefined;
    }

    return value;
}

function readOptionalPartition(
    value: unknown,
): number | undefined {
    if (typeof value === 'number' && Number.isInteger(value)) {
        return value;
    }

    if (typeof value !== 'string') {
        return undefined;
    }

    const trimmed = value.trim();

    if (!/^\d+$/.test(trimmed)) {
        return undefined;
    }

    return Number.parseInt(trimmed, 10);
}

function readErrorDetails(
    error: unknown,
): {
    errorMessage: string;
    errorName: string;
} {
    if (error instanceof Error) {
        return {
            errorMessage: error.message,
            errorName: error.name,
        };
    }

    return {
        errorMessage: String(error),
        errorName: 'UnknownError',
    };
}

function buildArtifactFailureContext(
    input: IndexArtifactInput,
): {
    artifact_key: string;
    event_id: string;
    partition?: number;
    source?: string;
    tenant_id?: string;
    topic?: string;
} {
    const metadata = input.metadata;
    const metadataEventId = readOptionalString(metadata.event_id);
    const metadataTenantId = readOptionalString(metadata.tenant_id);
    const metadataSource = readOptionalString(metadata.source);
    const metadataTopic = readOptionalString(metadata.topic);
    const metadataPartition = readOptionalPartition(metadata.partition);

    return {
        artifact_key: input.manifest.artifact_key,
        event_id: metadataEventId || input.manifest.event_id,
        partition: metadataPartition ?? input.manifest.partition,
        source: metadataSource || input.manifest.source,
        tenant_id: metadataTenantId || input.tenantId,
        topic: metadataTopic || input.manifest.topic,
    };
}

function logArtifactBatchFailure(
    input: IndexArtifactInput,
    error: unknown,
): void {
    const context = buildArtifactFailureContext(input);
    const details = readErrorDetails(error);

    console.error(
        'restore-indexer artifact batch item failed',
        {
            ...context,
            error_message: details.errorMessage,
            error_name: details.errorName,
        },
    );
}

export class RestoreIndexerService {
    constructor(
        private readonly store: RestoreIndexStore,
        private readonly options: RestoreIndexerOptions,
    ) {}

    async indexArtifact(
        input: IndexArtifactInput,
    ): Promise<IndexedArtifactResult> {
        const measuredAt = canonicalizeIsoDateTimeWithMillis(
            isoDateTimeWithMillis.parse(input.indexedAt || nowIso()),
        );
        const normalized = normalizeOperationalMetadata(input);

        if (normalized.metadata.offset === undefined) {
            throw new Error('Normalized metadata is missing offset');
        }

        const eventRecord = buildEventRecord(
            input,
            normalized,
            measuredAt,
        );
        const eventWriteState = await this.store.upsertIndexedEvent(
            eventRecord,
        );
        const currentRaw = await this.store.getPartitionWatermark(
            normalized.partitionScope,
        );
        const current = currentRaw === null
            ? null
            : canonicalizeWatermarkTimestamps(currentRaw);

        if (eventWriteState === 'existing') {
            if (current === null) {
                const freshWatermark = buildNextWatermark(
                    null,
                    normalized.offset,
                    normalized.eventTime,
                    normalized.partitionScope,
                    input.generationId,
                    measuredAt,
                );

                await this.store.putPartitionWatermark(freshWatermark);

                return {
                    eventWriteState,
                    watermark: freshWatermark,
                };
            }

            return {
                eventWriteState,
                watermark: current,
            };
        }

        const nextWatermark = buildNextWatermark(
            current,
            normalized.offset,
            normalized.eventTime,
            normalized.partitionScope,
            input.generationId,
            measuredAt,
        );

        await this.store.putPartitionWatermark(nextWatermark);
        await this.store.recomputeSourceCoverage({
            instanceId: normalized.partitionScope.instanceId,
            measuredAt,
            source: normalized.partitionScope.source,
            tenantId: normalized.partitionScope.tenantId,
        });

        return {
            eventWriteState,
            watermark: nextWatermark,
        };
    }

    async processBatch(
        inputs: IndexArtifactInput[],
    ): Promise<ProcessBatchResult> {
        let inserted = 0;
        let existing = 0;
        let failures = 0;

        for (const input of inputs) {
            try {
                const result = await this.indexArtifact(input);

                if (result.eventWriteState === 'inserted') {
                    inserted += 1;
                } else {
                    existing += 1;
                }
            } catch (error: unknown) {
                failures += 1;
                logArtifactBatchFailure(input, error);
            }
        }

        return {
            existing,
            failures,
            inserted,
        };
    }

    async getPartitionWatermarkStatus(
        scope: PartitionScope,
        gate: FreshnessGateInput,
    ): Promise<PartitionWatermarkStatus> {
        const measuredAt = canonicalizeIsoDateTimeWithMillis(
            isoDateTimeWithMillis.parse(gate.now),
        );
        const watermarkRaw = await this.store.getPartitionWatermark(scope);
        const watermark = watermarkRaw === null
            ? null
            : canonicalizeWatermarkTimestamps(watermarkRaw);
        const evaluation = evaluateFreshnessState(
            watermark,
            this.options.freshnessPolicy,
            gate,
        );

        return {
            contract_version: RESTORE_CONTRACT_VERSION,
            executability: evaluation.executability,
            freshness_gate_version: RESTORE_FRESHNESS_GATE_VERSION,
            freshness: evaluation.freshness,
            gate_timed_out: evaluation.gateTimedOut,
            lag_seconds: evaluation.lagSeconds,
            reason_code: evaluation.reasonCode,
            scope: {
                instance_id: scope.instanceId,
                partition: scope.partition,
                source: scope.source,
                tenant_id: scope.tenantId,
                topic: scope.topic,
            },
            watermark: watermark === null
                ? null
                : buildRestoreWatermark(watermark, evaluation, measuredAt),
            watermark_state_version: RESTORE_WATERMARK_STATE_VERSION,
        };
    }

    async getSourceCoverageWindow(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageWindow | null> {
        const coverage = await this.store.getSourceCoverage(
            tenantId,
            instanceId,
            source,
        );

        if (coverage === null) {
            return null;
        }

        return buildSourceCoverageWindow(coverage);
    }

    async refreshSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
        measuredAt: string,
    ): Promise<SourceCoverageWindow | null> {
        const coverage = await this.store.recomputeSourceCoverage({
            instanceId,
            measuredAt: canonicalizeIsoDateTimeWithMillis(
                isoDateTimeWithMillis.parse(measuredAt),
            ),
            source,
            tenantId,
        });

        if (coverage === null) {
            return null;
        }

        return buildSourceCoverageWindow(coverage);
    }

    async getSourceProgress(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceProgressWindow | null> {
        const progress = await this.store.getSourceProgress(
            tenantId,
            instanceId,
            source,
        );

        if (progress === null) {
            return null;
        }

        return buildSourceProgressWindow(progress);
    }

    async recordSourceProgress(
        input: SourceProgressUpdateInput,
    ): Promise<SourceProgressWindow> {
        const measuredAt = canonicalizeIsoDateTimeWithMillis(
            isoDateTimeWithMillis.parse(input.measuredAt),
        );
        const current = await this.store.getSourceProgress(
            input.tenantId,
            input.instanceId,
            input.source,
        );
        const inputLastIndexedEventTime = canonicalizeNullableIso(
            input.lastIndexedEventTime,
        );
        const currentLastIndexedEventTime = canonicalizeNullableIso(
            current?.lastIndexedEventTime ?? null,
        );
        const nextState: SourceProgressState = {
            cursor: input.cursor,
            instanceId: input.instanceId,
            lastBatchSize: input.lastBatchSize,
            lastIndexedEventTime: maxNullableIso(
                currentLastIndexedEventTime,
                inputLastIndexedEventTime,
            ),
            lastIndexedOffset: maxNullableOffset(
                current?.lastIndexedOffset ?? null,
                input.lastIndexedOffset,
            ),
            lastLagSeconds: input.lastLagSeconds,
            processedCount: (current?.processedCount ?? 0)
                + Math.max(0, input.processedDelta),
            source: input.source,
            tenantId: input.tenantId,
            updatedAt: measuredAt,
        };

        await this.store.putSourceProgress(nextState);

        return buildSourceProgressWindow(nextState);
    }
}

function buildSourceCoverageWindow(
    coverage: SourceCoverageState,
): SourceCoverageWindow {
    return {
        contract_version: RESTORE_CONTRACT_VERSION,
        coverage_window_version: RESTORE_COVERAGE_WINDOW_VERSION,
        earliest_indexed_time: coverage.earliestIndexedTime,
        generation_span: coverage.generationSpan,
        instance_id: coverage.instanceId,
        latest_indexed_time: coverage.latestIndexedTime,
        measured_at: coverage.measuredAt,
        source: coverage.source,
        tenant_id: coverage.tenantId,
    };
}

function buildSourceProgressWindow(
    progress: SourceProgressState,
): SourceProgressWindow {
    const lastIndexedOffset = progress.lastIndexedOffset === null
        ? null
        : canonicalizeOffset(
            progress.lastIndexedOffset,
            'source progress lastIndexedOffset',
        );

    return {
        contract_version: RESTORE_CONTRACT_VERSION,
        cursor: progress.cursor,
        instance_id: progress.instanceId,
        last_batch_size: progress.lastBatchSize,
        last_indexed_event_time: progress.lastIndexedEventTime,
        last_indexed_offset: lastIndexedOffset,
        last_lag_seconds: progress.lastLagSeconds,
        processed_count: progress.processedCount,
        source: progress.source,
        tenant_id: progress.tenantId,
        updated_at: progress.updatedAt,
    };
}

export const INDEX_ROW_VERSION = RESTORE_INDEX_ROW_VERSION;
