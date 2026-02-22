import { canonicalizeRestoreOffsetDecimalString } from '@rezilient/types';
import type { RestoreIndexerService } from './indexer.service';
import type {
    IndexArtifactInput,
    ProcessBatchResult,
} from './types';

export type ArtifactBatch = {
    items: IndexArtifactInput[];
    nextCursor: string | null;
    realtimeLagSeconds: number | null;
};

export interface ArtifactBatchSource {
    readBatch(input: {
        cursor: string | null;
        limit: number;
    }): Promise<ArtifactBatch>;
}

export interface SourceLeaderLeaseManager {
    acquireSourceLeaderLease(input: {
        holderId: string;
        instanceId: string;
        leaseDurationSeconds: number;
        source: string;
        tenantId: string;
    }): Promise<boolean>;
    releaseSourceLeaderLease(input: {
        holderId: string;
        instanceId: string;
        source: string;
        tenantId: string;
    }): Promise<void>;
}

export type SourceProgressScope = {
    instanceId: string;
    source: string;
    tenantId: string;
};

export type SourceLeaderLeaseOptions = {
    holderId: string;
    leaseDurationSeconds: number;
    manager: SourceLeaderLeaseManager;
};

export type WorkerRunSummary = ProcessBatchResult & {
    batchSize: number;
    cursor: string | null;
    realtimeLagSeconds: number | null;
};

export type WorkerLoopSummary = {
    cycles: number;
    emptyBatches: number;
    existing: number;
    failures: number;
    inserted: number;
};

export type WorkerContinuousOptions = {
    maxCycles?: number;
};

export type RestoreIndexerWorkerOptions = {
    leaderLease?: SourceLeaderLeaseOptions;
    pollIntervalMs?: number;
    sleep?: (ms: number) => Promise<void>;
    sourceProgressScope?: SourceProgressScope;
    timeProvider?: () => string;
};

const DEFAULT_POLL_INTERVAL_MS = 1000;

function nowIso(): string {
    return new Date().toISOString();
}

function sleepMs(
    ms: number,
): Promise<void> {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

function maxIso(
    left: string,
    right: string,
): string {
    return left >= right ? left : right;
}

function normalizeOffset(value: unknown): string | null {
    if (typeof value !== 'string' && typeof value !== 'number') {
        return null;
    }

    try {
        return canonicalizeRestoreOffsetDecimalString(value);
    } catch {
        return null;
    }
}

function compareOffsets(
    left: string,
    right: string,
): number {
    const leftValue = BigInt(left);
    const rightValue = BigInt(right);

    if (leftValue === rightValue) {
        return 0;
    }

    return leftValue > rightValue ? 1 : -1;
}

function readOffset(item: IndexArtifactInput): string | null {
    const metadataOffset = item.metadata.offset;
    const metadataCanonical = normalizeOffset(metadataOffset);

    if (metadataCanonical !== null) {
        return metadataCanonical;
    }

    return normalizeOffset(item.manifest.offset);
}

function latestEventTime(
    items: IndexArtifactInput[],
): string | null {
    let latest: string | null = null;

    for (const item of items) {
        const eventTime = item.manifest.event_time;

        if (latest === null) {
            latest = eventTime;
            continue;
        }

        latest = maxIso(latest, eventTime);
    }

    return latest;
}

function latestOffset(
    items: IndexArtifactInput[],
): string | null {
    let latest: string | null = null;

    for (const item of items) {
        const offset = readOffset(item);

        if (offset === null) {
            continue;
        }

        if (latest === null || compareOffsets(offset, latest) > 0) {
            latest = offset;
        }
    }

    return latest;
}

export class RestoreIndexerWorker {
    private cursor: string | null = null;

    private cursorLoaded = false;

    private paused = false;

    private stopRequested = false;

    private readonly pollIntervalMs: number;

    private readonly sleep: (ms: number) => Promise<void>;

    private readonly leaderLease?: SourceLeaderLeaseOptions;

    private readonly sourceProgressScope?: SourceProgressScope;

    private readonly timeProvider: () => string;

    private isLeader = false;

    constructor(
        private readonly source: ArtifactBatchSource,
        private readonly indexer: RestoreIndexerService,
        private readonly batchSize: number,
        options: RestoreIndexerWorkerOptions = {},
    ) {
        this.pollIntervalMs = options.pollIntervalMs
            ?? DEFAULT_POLL_INTERVAL_MS;
        this.sleep = options.sleep ?? sleepMs;
        this.leaderLease = options.leaderLease;
        this.sourceProgressScope = options.sourceProgressScope;
        this.timeProvider = options.timeProvider ?? nowIso;

        if (this.leaderLease && !this.sourceProgressScope) {
            throw new Error(
                'leader lease requires sourceProgressScope to be configured',
            );
        }
    }

    pause(): void {
        this.paused = true;
    }

    resume(): void {
        this.paused = false;
    }

    requestStop(): void {
        this.stopRequested = true;
    }

    async runOnce(): Promise<WorkerRunSummary> {
        if (this.paused) {
            return {
                batchSize: 0,
                cursor: this.cursor,
                existing: 0,
                failures: 0,
                inserted: 0,
                realtimeLagSeconds: null,
            };
        }

        const hasLeadership = await this.ensureLeadership();

        if (!hasLeadership) {
            return {
                batchSize: 0,
                cursor: this.cursor,
                existing: 0,
                failures: 0,
                inserted: 0,
                realtimeLagSeconds: null,
            };
        }

        await this.loadCursorIfNeeded();

        const batch = await this.source.readBatch({
            cursor: this.cursor,
            limit: this.batchSize,
        });
        const result = await this.indexer.processBatch(batch.items);
        const advanceCursor = result.failures === 0;
        const nextCursor = advanceCursor
            ? batch.nextCursor
            : this.cursor;

        this.cursor = nextCursor;

        await this.persistSourceProgress(batch, result, {
            advanceCursor,
            nextCursor,
        });

        return {
            ...result,
            batchSize: batch.items.length,
            cursor: nextCursor,
            realtimeLagSeconds: batch.realtimeLagSeconds,
        };
    }

    async runContinuously(
        options: WorkerContinuousOptions = {},
    ): Promise<WorkerLoopSummary> {
        const summary: WorkerLoopSummary = {
            cycles: 0,
            emptyBatches: 0,
            existing: 0,
            failures: 0,
            inserted: 0,
        };

        try {
            while (!this.stopRequested) {
                const run = await this.runOnce();

                summary.cycles += 1;
                summary.inserted += run.inserted;
                summary.existing += run.existing;
                summary.failures += run.failures;

                if (run.batchSize === 0) {
                    summary.emptyBatches += 1;
                }

                if (
                    options.maxCycles !== undefined
                    && summary.cycles >= options.maxCycles
                ) {
                    break;
                }

                if (this.stopRequested) {
                    break;
                }

                if (run.batchSize === 0) {
                    await this.sleep(this.pollIntervalMs);
                }
            }
        } finally {
            await this.releaseLeadership();
        }

        return summary;
    }

    private async ensureLeadership(): Promise<boolean> {
        if (!this.leaderLease || !this.sourceProgressScope) {
            return true;
        }

        const acquired = await this.leaderLease.manager.acquireSourceLeaderLease({
            holderId: this.leaderLease.holderId,
            instanceId: this.sourceProgressScope.instanceId,
            leaseDurationSeconds: this.leaderLease.leaseDurationSeconds,
            source: this.sourceProgressScope.source,
            tenantId: this.sourceProgressScope.tenantId,
        });

        if (acquired !== this.isLeader) {
            const message = acquired
                ? 'restore-indexer leader lease acquired'
                : 'restore-indexer leader lease unavailable';

            console.log(message, {
                holder_id: this.leaderLease.holderId,
                instance_id: this.sourceProgressScope.instanceId,
                source: this.sourceProgressScope.source,
                tenant_id: this.sourceProgressScope.tenantId,
            });
        }

        this.isLeader = acquired;

        return acquired;
    }

    private async releaseLeadership(): Promise<void> {
        if (
            !this.leaderLease
            || !this.sourceProgressScope
            || !this.isLeader
        ) {
            return;
        }

        this.isLeader = false;

        try {
            await this.leaderLease.manager.releaseSourceLeaderLease({
                holderId: this.leaderLease.holderId,
                instanceId: this.sourceProgressScope.instanceId,
                source: this.sourceProgressScope.source,
                tenantId: this.sourceProgressScope.tenantId,
            });
        } catch (error: unknown) {
            console.error(
                'restore-indexer leader lease release failed',
                error,
            );
        }
    }

    private async loadCursorIfNeeded(): Promise<void> {
        if (this.cursorLoaded) {
            return;
        }

        this.cursorLoaded = true;

        if (!this.sourceProgressScope) {
            return;
        }

        const progress = await this.indexer.getSourceProgress(
            this.sourceProgressScope.tenantId,
            this.sourceProgressScope.instanceId,
            this.sourceProgressScope.source,
        );

        if (progress) {
            this.cursor = progress.cursor;
        }
    }

    private async persistSourceProgress(
        batch: ArtifactBatch,
        result: ProcessBatchResult,
        cursor: {
            advanceCursor: boolean;
            nextCursor: string | null;
        },
    ): Promise<void> {
        if (!this.sourceProgressScope) {
            return;
        }

        const progressItems = cursor.advanceCursor ? batch.items : [];

        await this.indexer.recordSourceProgress({
            cursor: cursor.nextCursor,
            instanceId: this.sourceProgressScope.instanceId,
            lastBatchSize: batch.items.length,
            lastIndexedEventTime: latestEventTime(progressItems),
            lastIndexedOffset: latestOffset(progressItems),
            lastLagSeconds: cursor.advanceCursor
                ? batch.realtimeLagSeconds
                : null,
            measuredAt: this.timeProvider(),
            processedDelta: result.inserted + result.existing + result.failures,
            source: this.sourceProgressScope.source,
            tenantId: this.sourceProgressScope.tenantId,
        });
    }
}

export class InMemoryArtifactBatchSource implements ArtifactBatchSource {
    private readonly queue: IndexArtifactInput[] = [];

    constructor(
        initial: IndexArtifactInput[] = [],
        private realtimeLagSeconds: number | null = null,
    ) {
        this.queue.push(...initial);
    }

    enqueue(items: IndexArtifactInput[]): void {
        this.queue.push(...items);
    }

    setRealtimeLagSeconds(lagSeconds: number | null): void {
        this.realtimeLagSeconds = lagSeconds;
    }

    async readBatch(input: {
        cursor: string | null;
        limit: number;
    }): Promise<ArtifactBatch> {
        let start = 0;

        if (input.cursor !== null) {
            const parsed = Number.parseInt(input.cursor, 10);

            if (Number.isFinite(parsed) && parsed >= 0) {
                start = parsed;
            }
        }

        const boundedStart = Math.min(start, this.queue.length);
        const boundedEnd = Math.min(
            this.queue.length,
            boundedStart + input.limit,
        );

        return {
            items: this.queue.slice(boundedStart, boundedEnd),
            nextCursor: String(boundedEnd),
            realtimeLagSeconds: this.realtimeLagSeconds,
        };
    }
}
