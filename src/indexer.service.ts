import { isoDateTimeWithMillis } from '@rezilient/types';
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

function maxIso(left: string, right: string): string {
    return left >= right ? left : right;
}

function minIso(left: string, right: string): string {
    return left <= right ? left : right;
}

function nowIso(): string {
    return new Date().toISOString();
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
    nextOffset: number,
    nextEventTime: string,
    partitionScope: PartitionScope,
    generationId: string,
    measuredAt: string,
): PartitionWatermarkState {
    if (current === null) {
        return {
            coverageEnd: nextEventTime,
            coverageStart: nextEventTime,
            generationId,
            indexedThroughOffset: nextOffset,
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
            indexedThroughOffset: nextOffset,
            indexedThroughTime: nextEventTime,
            instanceId: partitionScope.instanceId,
            measuredAt,
            partition: partitionScope.partition,
            source: partitionScope.source,
            tenantId: partitionScope.tenantId,
            topic: partitionScope.topic,
        };
    }

    if (nextOffset < current.indexedThroughOffset) {
        throw new WatermarkInvariantError(
            `Offset rewind in generation ${generationId}: `
            + `${nextOffset} < ${current.indexedThroughOffset}`,
        );
    }

    return {
        coverageEnd: maxIso(current.coverageEnd, nextEventTime),
        coverageStart: minIso(current.coverageStart, nextEventTime),
        generationId,
        indexedThroughOffset: Math.max(
            current.indexedThroughOffset,
            nextOffset,
        ),
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

export class RestoreIndexerService {
    constructor(
        private readonly store: RestoreIndexStore,
        private readonly options: RestoreIndexerOptions,
    ) {}

    async indexArtifact(
        input: IndexArtifactInput,
    ): Promise<IndexedArtifactResult> {
        const measuredAt = isoDateTimeWithMillis.parse(
            input.indexedAt || nowIso(),
        );
        const normalized = normalizeOperationalMetadata(input);

        if (normalized.metadata.offset === undefined) {
            throw new Error('Normalized metadata is missing offset');
        }

        const current = await this.store.getPartitionWatermark(
            normalized.partitionScope,
        );
        const nextWatermark = buildNextWatermark(
            current,
            normalized.metadata.offset,
            normalized.eventTime,
            normalized.partitionScope,
            input.generationId,
            measuredAt,
        );
        const eventRecord = buildEventRecord(input, normalized, measuredAt);
        const eventWriteState = await this.store.upsertIndexedEvent(eventRecord);

        if (eventWriteState === 'inserted') {
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

        if (current === null) {
            await this.store.putPartitionWatermark(nextWatermark);
            return {
                eventWriteState,
                watermark: nextWatermark,
            };
        }

        return {
            eventWriteState,
            watermark: current,
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
            } catch (_error: unknown) {
                failures += 1;
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
        const measuredAt = isoDateTimeWithMillis.parse(gate.now);
        const watermark = await this.store.getPartitionWatermark(scope);
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
            measuredAt: isoDateTimeWithMillis.parse(measuredAt),
            source,
            tenantId,
        });

        if (coverage === null) {
            return null;
        }

        return buildSourceCoverageWindow(coverage);
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

export const INDEX_ROW_VERSION = RESTORE_INDEX_ROW_VERSION;
