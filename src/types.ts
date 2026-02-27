import type {
    RestoreReasonCode,
    RestoreWatermarkExecutability,
    RestoreWatermarkFreshness,
    RrsOperationalMetadata,
} from '@rezilient/types';

export type RestoreArtifactKind =
    | 'cdc'
    | 'schema'
    | 'media'
    | 'error'
    | 'repair';

export type IngestionMode =
    | 'realtime'
    | 'bootstrap'
    | 'gap_repair';

export type BackfillMode =
    | 'bootstrap'
    | 'gap_repair';

export type BackfillStatus =
    | 'running'
    | 'paused'
    | 'completed'
    | 'failed';

export type ArtifactManifestInput = {
    artifact_key: string;
    artifact_kind: RestoreArtifactKind;
    app: string | null;
    contract_version: string;
    event_id: string;
    event_time: string;
    event_type: string;
    tenant_id?: string;
    instance_id: string;
    manifest_version: string;
    metadata_allowlist_version: string;
    object_key_layout_version: string;
    offset?: string;
    partition?: number;
    source: string;
    table: string | null;
    topic?: string;
};

export type IndexArtifactInput = {
    tenantId: string;
    generationId: string;
    ingestionMode: IngestionMode;
    manifest: ArtifactManifestInput;
    metadata: Record<string, unknown>;
    indexedAt?: string;
};

export type PartitionScope = {
    tenantId: string;
    instanceId: string;
    source: string;
    topic: string;
    partition: number;
};

export type IndexedEventRecord = {
    artifactKey: string;
    artifactKind: RestoreArtifactKind;
    app: string | null;
    generationId: string;
    indexedAt: string;
    ingestionMode: IngestionMode;
    instanceId: string;
    manifestVersion: string;
    metadata: RrsOperationalMetadata;
    objectKeyLayoutVersion: string;
    partitionScope: PartitionScope;
    table: string | null;
    tenantId: string;
};

export type PartitionWatermarkState = {
    coverageEnd: string;
    coverageStart: string;
    generationId: string;
    indexedThroughOffset: string;
    indexedThroughTime: string;
    instanceId: string;
    measuredAt: string;
    partition: number;
    source: string;
    tenantId: string;
    topic: string;
};

export type SourceCoverageState = {
    earliestIndexedTime: string;
    generationSpan: Record<string, string>;
    instanceId: string;
    latestIndexedTime: string;
    measuredAt: string;
    source: string;
    tenantId: string;
};

export type SourceProgressState = {
    cursor: string | null;
    instanceId: string;
    lastBatchSize: number;
    lastIndexedEventTime: string | null;
    lastIndexedOffset: string | null;
    lastLagSeconds: number | null;
    processedCount: number;
    source: string;
    tenantId: string;
    updatedAt: string;
};

export type FreshnessEvaluation = {
    executability: RestoreWatermarkExecutability;
    freshness: RestoreWatermarkFreshness;
    gateTimedOut: boolean;
    lagSeconds: number | null;
    reasonCode: RestoreReasonCode;
};

export type IndexedArtifactResult = {
    eventWriteState: 'inserted' | 'existing';
    watermark: PartitionWatermarkState;
};

export type ProcessBatchResult = {
    inserted: number;
    existing: number;
    failures: number;
};

export type BackfillRunState = {
    cursor: string | null;
    maxRealtimeLagSeconds: number;
    mode: BackfillMode;
    processedCount: number;
    reasonCode: string;
    runId: string;
    status: BackfillStatus;
    throttleBatchSize: number;
    updatedAt: string;
};

export type BackfillBatch = {
    done: boolean;
    items: IndexArtifactInput[];
    nextCursor: string | null;
    realtimeLagSeconds: number;
};
