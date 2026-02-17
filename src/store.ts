import type {
    BackfillRunState,
    IndexedEventRecord,
    PartitionScope,
    PartitionWatermarkState,
    SourceCoverageState,
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

export interface RestoreIndexStore {
    getBackfillRun(runId: string): Promise<BackfillRunState | null>;
    getIndexedEventCount(): number;
    getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null>;
    getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null>;
    putPartitionWatermark(state: PartitionWatermarkState): Promise<void>;
    recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null>;
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

    getIndexedEventCount(): number {
        return this.indexedEvents.size;
    }

    async upsertIndexedEvent(
        record: IndexedEventRecord,
    ): Promise<'inserted' | 'existing'> {
        const key = eventKey(record);

        if (this.indexedEvents.has(key)) {
            return 'existing';
        }

        this.indexedEvents.set(key, record);

        return 'inserted';
    }

    async getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null> {
        return this.partitionWatermarks.get(partitionKey(scope)) || null;
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
            state,
        );
    }

    async recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null> {
        let earliest: string | null = null;
        let latest: string | null = null;
        const generationSpan: Record<string, string> = {};

        for (const watermark of this.partitionWatermarks.values()) {
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
            const key = sourceKey(
                input.tenantId,
                input.instanceId,
                input.source,
            );

            this.sourceCoverage.delete(key);
            return null;
        }

        const state: SourceCoverageState = {
            earliestIndexedTime: earliest,
            generationSpan,
            instanceId: input.instanceId,
            latestIndexedTime: latest,
            measuredAt: input.measuredAt,
            source: input.source,
            tenantId: input.tenantId,
        };

        this.sourceCoverage.set(
            sourceKey(input.tenantId, input.instanceId, input.source),
            state,
        );

        return state;
    }

    async getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null> {
        return this.sourceCoverage.get(
            sourceKey(tenantId, instanceId, source),
        ) || null;
    }

    async upsertBackfillRun(state: BackfillRunState): Promise<void> {
        this.backfillRuns.set(state.runId, state);
    }

    async getBackfillRun(runId: string): Promise<BackfillRunState | null> {
        return this.backfillRuns.get(runId) || null;
    }
}
