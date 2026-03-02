import { canonicalizeRestoreOffsetDecimalString } from '@rezilient/types';
import type { SourceCursorMode } from './env';
import type { RestoreIndexerService } from './indexer.service';
import {
    parseSourceCursorState,
    serializeSourceCursorState,
    type SourceCursorReplayDefaults,
    type SourceCursorV3State,
} from './source-cursor';
import type { RestoreIndexStore } from './store';
import type {
    IndexArtifactInput,
    ProcessBatchResult,
    SourceProgressState,
} from './types';

export type ArtifactBatch = {
    items: IndexArtifactInput[];
    nextCursor: string | null;
    realtimeLagSeconds: number | null;
    scanCounters?: ArtifactBatchScanCounters;
};

export type ArtifactBatchScanCounters = {
    fastPathSelectedKeyCount: number;
    replayCycleRan: boolean;
    replayOnlyHitCount: number;
    replayPathSelectedKeyCount: number;
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
        leaseDurationSeconds: number;
    } & (SourceProgressScope | SourceIngestScope)): Promise<boolean>;
    releaseSourceLeaderLease(input: {
        holderId: string;
    } & (SourceProgressScope | SourceIngestScope)): Promise<void>;
}

export type SourceProgressScope = {
    instanceId: string;
    source: string;
    tenantId: string;
};

export type SourceIngestScope = {
    ingestScopeId: string;
    sourceUri: string;
};

export type SourceProgressStore = Pick<
    RestoreIndexStore,
    'getSourceProgress' | 'putSourceProgress'
>;

type SourceRuntimeScope =
    | {
        mode: 'legacy';
        scope: SourceProgressScope;
    }
    | {
        mode: 'ingest';
        scope: SourceIngestScope;
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
    sourceCursorMode?: SourceCursorMode;
    sourceIngestScope?: SourceIngestScope;
    sourceProgressScope?: SourceProgressScope;
    sourceProgressStore?: SourceProgressStore;
    timeProvider?: () => string;
};

const DEFAULT_POLL_INTERVAL_MS = 1000;
const V1_OBJECT_KEY_LAYOUT_VERSION = 'rec.object-key-layout.v1';
const V2_OBJECT_KEY_LAYOUT_VERSION = 'rec.object-key-layout.v2';
const WORKER_CURSOR_REPLAY_DEFAULTS: SourceCursorReplayDefaults = {
    enabled: false,
    lowerBound: null,
};

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

function maxNullableIso(
    left: string | null,
    right: string | null,
): string | null {
    if (right === null) {
        return left;
    }

    if (left === null) {
        return right;
    }

    return maxIso(left, right);
}

function buildLegacyIngestScopeId(
    scope: SourceProgressScope,
): string {
    return `legacy:${JSON.stringify([
        scope.tenantId,
        scope.instanceId,
        scope.source,
    ])}`;
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

function maxNullableOffset(
    left: string | null,
    right: string | null,
): string | null {
    if (left === null) {
        return right;
    }

    if (right === null) {
        return left;
    }

    return compareOffsets(left, right) >= 0 ? left : right;
}

function incrementOffset(
    offset: string,
): string {
    return (BigInt(offset) + 1n).toString();
}

function readOffset(item: IndexArtifactInput): string | null {
    const metadataOffset = item.metadata.offset;
    const metadataCanonical = normalizeOffset(metadataOffset);

    if (metadataCanonical !== null) {
        return metadataCanonical;
    }

    return normalizeOffset(item.manifest.offset);
}

function readTopic(
    item: IndexArtifactInput,
): string | null {
    const metadataTopic = item.metadata.topic;

    if (typeof metadataTopic === 'string' && metadataTopic.trim().length > 0) {
        return metadataTopic.trim();
    }

    if (
        typeof item.manifest.topic === 'string'
        && item.manifest.topic.trim().length > 0
    ) {
        return item.manifest.topic.trim();
    }

    return null;
}

function parsePartition(
    value: unknown,
): number | null {
    if (typeof value === 'number' && Number.isInteger(value) && value >= 0) {
        return value;
    }

    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();

    if (!/^\d+$/.test(trimmed)) {
        return null;
    }

    return Number.parseInt(trimmed, 10);
}

function readPartition(
    item: IndexArtifactInput,
): number | null {
    const metadataPartition = parsePartition(item.metadata.partition);

    if (metadataPartition !== null) {
        return metadataPartition;
    }

    return parsePartition(item.manifest.partition);
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

type V2ShardProgressCandidate = {
    eventTime: string;
    lastKey: string;
    offset: string;
    shardKey: string;
};

type V2ShardProgressUpdate = {
    advancedShardCount: number;
    updated: boolean;
};

type LayoutMixCounts = {
    otherCount: number;
    v1Count: number;
    v2Count: number;
};

function buildV2ShardProgressCandidate(
    item: IndexArtifactInput,
): V2ShardProgressCandidate | null {
    if (item.manifest.object_key_layout_version !== V2_OBJECT_KEY_LAYOUT_VERSION) {
        return null;
    }

    const topic = readTopic(item);
    const partition = readPartition(item);
    const offset = readOffset(item);

    if (topic === null || partition === null || offset === null) {
        return null;
    }

    return {
        eventTime: item.manifest.event_time,
        lastKey: item.manifest.artifact_key,
        offset,
        shardKey: `${topic}|${partition}`,
    };
}

function updateCursorV2ShardProgress(
    cursorState: SourceCursorV3State,
    items: IndexArtifactInput[],
    measuredAt: string,
): V2ShardProgressUpdate {
    let updated = false;
    const advancedShards = new Set<string>();

    for (const item of items) {
        const candidate = buildV2ShardProgressCandidate(item);

        if (candidate === null) {
            continue;
        }

        const candidateNextOffset = incrementOffset(candidate.offset);
        const existingShard = cursorState.v2.by_shard[candidate.shardKey];

        if (!existingShard) {
            cursorState.v2.by_shard[candidate.shardKey] = {
                last_event_time: candidate.eventTime,
                last_key: candidate.lastKey,
                next_offset: candidateNextOffset,
            };
            advancedShards.add(candidate.shardKey);
            updated = true;
            continue;
        }

        let shardUpdated = false;

        if (
            compareOffsets(candidateNextOffset, existingShard.next_offset) > 0
        ) {
            existingShard.next_offset = candidateNextOffset;
            existingShard.last_key = candidate.lastKey;
            advancedShards.add(candidate.shardKey);
            shardUpdated = true;
        } else if (existingShard.last_key === null) {
            existingShard.last_key = candidate.lastKey;
            shardUpdated = true;
        }

        const mergedEventTime = maxNullableIso(
            existingShard.last_event_time,
            candidate.eventTime,
        );

        if (mergedEventTime !== existingShard.last_event_time) {
            existingShard.last_event_time = mergedEventTime;
            shardUpdated = true;
        }

        if (shardUpdated) {
            updated = true;
        }
    }

    if (updated) {
        cursorState.v2.last_reconcile_at = measuredAt;
    }

    return {
        advancedShardCount: advancedShards.size,
        updated,
    };
}

function summarizeLayoutMix(
    items: IndexArtifactInput[],
): LayoutMixCounts {
    const counts: LayoutMixCounts = {
        otherCount: 0,
        v1Count: 0,
        v2Count: 0,
    };

    for (const item of items) {
        const version = item.manifest.object_key_layout_version;

        if (version === V1_OBJECT_KEY_LAYOUT_VERSION) {
            counts.v1Count += 1;
            continue;
        }

        if (version === V2_OBJECT_KEY_LAYOUT_VERSION) {
            counts.v2Count += 1;
            continue;
        }

        counts.otherCount += 1;
    }

    return counts;
}

function defaultScanCounters(): ArtifactBatchScanCounters {
    return {
        fastPathSelectedKeyCount: 0,
        replayCycleRan: false,
        replayOnlyHitCount: 0,
        replayPathSelectedKeyCount: 0,
    };
}

function classifyCursorKind(
    cursor: string | null,
): 'null' | 'empty' | 'json_like' | 'legacy_string' {
    if (cursor === null) {
        return 'null';
    }

    const trimmed = cursor.trimStart();

    if (!trimmed) {
        return 'empty';
    }

    return trimmed.startsWith('{')
        ? 'json_like'
        : 'legacy_string';
}

function summarizeCursor(
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

type CursorResolution = {
    nextCursor: string | null;
    v2ShardAdvancements: number;
};

type CursorHealthSummary = {
    parseFailed: boolean;
    v2LastReconcileAt: string | null;
    v2ShardsTracked: number;
};

type BatchOutcomeLogPayload = {
    advance_cursor: boolean;
    batch_size: number;
    cursor_after: string | null;
    cursor_before: string | null;
    existing: number;
    failures: number;
    fast_path_selected_key_count: number;
    inserted: number;
    realtime_lag_seconds: number | null;
    replay_cycle_ran: boolean;
    replay_only_hit_count: number;
    replay_path_selected_key_count: number;
    source_cursor_mode: SourceCursorMode;
    v2_last_reconcile_at: string | null;
    v2_shard_advancements: number;
    v2_shard_health_parse_failed: boolean;
    v2_shards_tracked: number;
    v2_primary_mode_legacy_v1_items_present: boolean;
    v2_ready_no_v1_seen: boolean;
    version_mix_other_count: number;
    version_mix_v1_count: number;
    version_mix_v2_count: number;
};

function isIdleBatchOutcome(
    payload: BatchOutcomeLogPayload,
): boolean {
    return payload.batch_size === 0
        && payload.inserted === 0
        && payload.existing === 0
        && payload.failures === 0;
}

export class RestoreIndexerWorker {
    private cursor: string | null = null;

    private cursorLoaded = false;

    private paused = false;

    private stopRequested = false;

    private readonly pollIntervalMs: number;

    private readonly sleep: (ms: number) => Promise<void>;

    private readonly leaderLease?: SourceLeaderLeaseOptions;

    private readonly sourceCursorMode: SourceCursorMode;

    private readonly sourceProgressStore?: SourceProgressStore;

    private readonly sourceRuntimeScope?: SourceRuntimeScope;

    private readonly timeProvider: () => string;

    private isLeader = false;

    private lastBatchOutcomeLogStateKey: string | null = null;

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
        this.sourceCursorMode = options.sourceCursorMode ?? 'mixed';
        this.sourceProgressStore = options.sourceProgressStore;
        this.timeProvider = options.timeProvider ?? nowIso;

        if (options.sourceIngestScope) {
            this.sourceRuntimeScope = {
                mode: 'ingest',
                scope: options.sourceIngestScope,
            };
        } else if (options.sourceProgressScope) {
            this.sourceRuntimeScope = {
                mode: 'legacy',
                scope: options.sourceProgressScope,
            };
        } else {
            this.sourceRuntimeScope = undefined;
        }

        if (this.leaderLease && !this.sourceRuntimeScope) {
            throw new Error(
                'leader lease requires sourceProgressScope to be configured',
            );
        }

        if (
            this.sourceRuntimeScope?.mode === 'ingest'
            && !this.sourceProgressStore
        ) {
            throw new Error(
                'sourceProgressStore is required for sourceIngestScope',
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

        const previousCursor = this.cursor;
        let batch: ArtifactBatch;

        try {
            batch = await this.source.readBatch({
                cursor: this.cursor,
                limit: this.batchSize,
            });
        } catch (error) {
            this.logSourceCursorFailure(previousCursor, error);
            throw error;
        }

        const result = await this.indexer.processBatch(batch.items);
        const advanceCursor = result.failures === 0;
        const measuredAt = this.timeProvider();
        const cursorResolution = this.resolveNextCursor(batch, {
            advanceCursor,
            measuredAt,
        });
        const nextCursor = cursorResolution.nextCursor;

        this.cursor = nextCursor;

        await this.persistSourceProgress(batch, result, {
            advanceCursor,
            measuredAt,
            nextCursor,
        });
        this.logBatchOutcome(batch, result, {
            advanceCursor,
            nextCursor,
            previousCursor,
            v2ShardAdvancements: cursorResolution.v2ShardAdvancements,
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
        if (!this.leaderLease || !this.sourceRuntimeScope) {
            return true;
        }

        const acquired = this.sourceRuntimeScope.mode === 'ingest'
            ? await this.leaderLease.manager.acquireSourceLeaderLease({
                holderId: this.leaderLease.holderId,
                ingestScopeId: this.sourceRuntimeScope.scope.ingestScopeId,
                leaseDurationSeconds: this.leaderLease.leaseDurationSeconds,
                sourceUri: this.sourceRuntimeScope.scope.sourceUri,
            })
            : await this.leaderLease.manager.acquireSourceLeaderLease({
                holderId: this.leaderLease.holderId,
                instanceId: this.sourceRuntimeScope.scope.instanceId,
                leaseDurationSeconds: this.leaderLease.leaseDurationSeconds,
                source: this.sourceRuntimeScope.scope.source,
                tenantId: this.sourceRuntimeScope.scope.tenantId,
            });

        if (acquired !== this.isLeader) {
            const message = acquired
                ? 'restore-indexer leader lease acquired'
                : 'restore-indexer leader lease unavailable';

            if (this.sourceRuntimeScope.mode === 'ingest') {
                console.log(message, {
                    holder_id: this.leaderLease.holderId,
                    ingest_scope_id: this.sourceRuntimeScope.scope.ingestScopeId,
                    source_uri: this.sourceRuntimeScope.scope.sourceUri,
                });
            } else {
                console.log(message, {
                    holder_id: this.leaderLease.holderId,
                    instance_id: this.sourceRuntimeScope.scope.instanceId,
                    source: this.sourceRuntimeScope.scope.source,
                    tenant_id: this.sourceRuntimeScope.scope.tenantId,
                });
            }
        }

        this.isLeader = acquired;

        return acquired;
    }

    private async releaseLeadership(): Promise<void> {
        if (
            !this.leaderLease
            || !this.sourceRuntimeScope
            || !this.isLeader
        ) {
            return;
        }

        this.isLeader = false;

        try {
            if (this.sourceRuntimeScope.mode === 'ingest') {
                await this.leaderLease.manager.releaseSourceLeaderLease({
                    holderId: this.leaderLease.holderId,
                    ingestScopeId: this.sourceRuntimeScope.scope.ingestScopeId,
                    sourceUri: this.sourceRuntimeScope.scope.sourceUri,
                });
            } else {
                await this.leaderLease.manager.releaseSourceLeaderLease({
                    holderId: this.leaderLease.holderId,
                    instanceId: this.sourceRuntimeScope.scope.instanceId,
                    source: this.sourceRuntimeScope.scope.source,
                    tenantId: this.sourceRuntimeScope.scope.tenantId,
                });
            }
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

        if (!this.sourceRuntimeScope) {
            return;
        }

        const progress = this.sourceRuntimeScope.mode === 'ingest'
            ? await this.sourceProgressStore?.getSourceProgress({
                ingestScopeId: this.sourceRuntimeScope.scope.ingestScopeId,
                sourceUri: this.sourceRuntimeScope.scope.sourceUri,
            }) || null
            : await this.indexer.getSourceProgress(
                this.sourceRuntimeScope.scope.tenantId,
                this.sourceRuntimeScope.scope.instanceId,
                this.sourceRuntimeScope.scope.source,
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
            measuredAt: string;
            nextCursor: string | null;
        },
    ): Promise<void> {
        if (!this.sourceRuntimeScope) {
            return;
        }

        if (this.sourceRuntimeScope.mode === 'ingest') {
            await this.persistIngestScopeProgress(batch, result, cursor);
            return;
        }

        const progressItems = cursor.advanceCursor ? batch.items : [];

        await this.indexer.recordSourceProgress({
            cursor: cursor.nextCursor,
            instanceId: this.sourceRuntimeScope.scope.instanceId,
            lastBatchSize: batch.items.length,
            lastIndexedEventTime: latestEventTime(progressItems),
            lastIndexedOffset: latestOffset(progressItems),
            lastLagSeconds: cursor.advanceCursor
                ? batch.realtimeLagSeconds
                : null,
            measuredAt: cursor.measuredAt,
            processedDelta: result.inserted + result.existing,
            source: this.sourceRuntimeScope.scope.source,
            tenantId: this.sourceRuntimeScope.scope.tenantId,
        });
    }

    private async persistIngestScopeProgress(
        batch: ArtifactBatch,
        result: ProcessBatchResult,
        cursor: {
            advanceCursor: boolean;
            measuredAt: string;
            nextCursor: string | null;
        },
    ): Promise<void> {
        if (
            !this.sourceProgressStore
            || !this.sourceRuntimeScope
            || this.sourceRuntimeScope.mode !== 'ingest'
        ) {
            return;
        }

        const scope = this.sourceRuntimeScope.scope;
        const progressItems = cursor.advanceCursor ? batch.items : [];
        const current = await this.sourceProgressStore.getSourceProgress({
            ingestScopeId: scope.ingestScopeId,
            sourceUri: scope.sourceUri,
        });
        const nextState: SourceProgressState = {
            cursor: cursor.nextCursor,
            instanceId: current?.instanceId || scope.ingestScopeId,
            lastBatchSize: batch.items.length,
            lastIndexedEventTime: maxNullableIso(
                current?.lastIndexedEventTime ?? null,
                latestEventTime(progressItems),
            ),
            lastIndexedOffset: maxNullableOffset(
                current?.lastIndexedOffset ?? null,
                latestOffset(progressItems),
            ),
            lastLagSeconds: cursor.advanceCursor
                ? batch.realtimeLagSeconds
                : null,
            processedCount: (current?.processedCount ?? 0)
                + Math.max(0, result.inserted + result.existing),
            source: current?.source || scope.sourceUri,
            tenantId: current?.tenantId || 'runtime-scope',
            updatedAt: cursor.measuredAt,
        };

        await this.sourceProgressStore.putSourceProgress({
            ingestScopeId: scope.ingestScopeId,
            sourceUri: scope.sourceUri,
            state: nextState,
        });
    }

    private resolveNextCursor(
        batch: ArtifactBatch,
        input: {
            advanceCursor: boolean;
            measuredAt: string;
        },
    ): CursorResolution {
        if (!input.advanceCursor) {
            return {
                nextCursor: this.cursor,
                v2ShardAdvancements: 0,
            };
        }

        const nextCursor = batch.nextCursor;
        const hasV2Items = batch.items.some((item) => {
            return item.manifest.object_key_layout_version
                === V2_OBJECT_KEY_LAYOUT_VERSION;
        });

        if (!hasV2Items) {
            return {
                nextCursor,
                v2ShardAdvancements: 0,
            };
        }

        const cursorState = parseSourceCursorState(
            nextCursor,
            WORKER_CURSOR_REPLAY_DEFAULTS,
        );
        const cursorUpdate = updateCursorV2ShardProgress(
            cursorState,
            batch.items,
            input.measuredAt,
        );

        if (!cursorUpdate.updated) {
            return {
                nextCursor,
                v2ShardAdvancements: cursorUpdate.advancedShardCount,
            };
        }

        return {
            nextCursor: serializeSourceCursorState(cursorState),
            v2ShardAdvancements: cursorUpdate.advancedShardCount,
        };
    }

    private logBatchOutcome(
        batch: ArtifactBatch,
        result: ProcessBatchResult,
        cursor: {
            advanceCursor: boolean;
            nextCursor: string | null;
            previousCursor: string | null;
            v2ShardAdvancements: number;
        },
    ): void {
        const scanCounters = batch.scanCounters || defaultScanCounters();
        const layoutMix = summarizeLayoutMix(batch.items);
        const cursorHealth = this.summarizeCursorHealth(cursor.nextCursor);
        const payload: BatchOutcomeLogPayload = {
            advance_cursor: cursor.advanceCursor,
            batch_size: batch.items.length,
            cursor_after: cursor.nextCursor,
            cursor_before: cursor.previousCursor,
            existing: result.existing,
            failures: result.failures,
            fast_path_selected_key_count:
                scanCounters.fastPathSelectedKeyCount,
            inserted: result.inserted,
            realtime_lag_seconds: batch.realtimeLagSeconds,
            replay_cycle_ran: scanCounters.replayCycleRan,
            replay_only_hit_count: scanCounters.replayOnlyHitCount,
            replay_path_selected_key_count:
                scanCounters.replayPathSelectedKeyCount,
            source_cursor_mode: this.sourceCursorMode,
            v2_last_reconcile_at: cursorHealth.v2LastReconcileAt,
            v2_shard_advancements: cursor.v2ShardAdvancements,
            v2_shard_health_parse_failed: cursorHealth.parseFailed,
            v2_shards_tracked: cursorHealth.v2ShardsTracked,
            v2_primary_mode_legacy_v1_items_present:
                this.sourceCursorMode === 'v2_primary'
                && layoutMix.v1Count > 0,
            v2_ready_no_v1_seen:
                layoutMix.v2Count > 0
                && layoutMix.v1Count === 0,
            version_mix_other_count: layoutMix.otherCount,
            version_mix_v1_count: layoutMix.v1Count,
            version_mix_v2_count: layoutMix.v2Count,
        };
        const stateKey = JSON.stringify(payload);
        const isIdle = isIdleBatchOutcome(payload);

        if (
            isIdle
            && this.lastBatchOutcomeLogStateKey === stateKey
        ) {
            return;
        }

        this.lastBatchOutcomeLogStateKey = stateKey;
        console.log('restore-indexer batch processed', payload);

        if (
            this.sourceCursorMode === 'v2_primary'
            && layoutMix.v1Count > 0
        ) {
            console.warn(
                'restore-indexer v2_primary observed legacy v1 manifests',
                {
                    legacy_v1_count: layoutMix.v1Count,
                    v2_count: layoutMix.v2Count,
                },
            );
        }
    }

    private summarizeCursorHealth(
        cursor: string | null,
    ): CursorHealthSummary {
        try {
            const state = parseSourceCursorState(
                cursor,
                WORKER_CURSOR_REPLAY_DEFAULTS,
            );

            return {
                parseFailed: false,
                v2LastReconcileAt: state.v2.last_reconcile_at,
                v2ShardsTracked: Object.keys(state.v2.by_shard).length,
            };
        } catch {
            return {
                parseFailed: true,
                v2LastReconcileAt: null,
                v2ShardsTracked: 0,
            };
        }
    }

    private logSourceCursorFailure(
        cursor: string | null,
        error: unknown,
    ): void {
        const errorMessage = String((error as Error)?.message || error);

        if (!errorMessage.includes('source cursor parse failure')) {
            return;
        }

        console.error(
            'restore-indexer source cursor parse failed; failing closed',
            {
                cursor_kind: classifyCursorKind(cursor),
                cursor_preview: summarizeCursor(cursor),
                error_message: errorMessage,
                ingest_scope_id: this.sourceRuntimeScope?.mode === 'ingest'
                    ? this.sourceRuntimeScope.scope.ingestScopeId
                    : null,
                instance_id: this.sourceRuntimeScope?.mode === 'legacy'
                    ? this.sourceRuntimeScope.scope.instanceId
                    : null,
                source: this.sourceRuntimeScope?.mode === 'legacy'
                    ? this.sourceRuntimeScope.scope.source
                    : null,
                source_uri: this.sourceRuntimeScope?.mode === 'ingest'
                    ? this.sourceRuntimeScope.scope.sourceUri
                    : null,
                tenant_id: this.sourceRuntimeScope?.mode === 'legacy'
                    ? this.sourceRuntimeScope.scope.tenantId
                    : null,
            },
        );
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
            scanCounters: {
                fastPathSelectedKeyCount: boundedEnd - boundedStart,
                replayCycleRan: false,
                replayOnlyHitCount: 0,
                replayPathSelectedKeyCount: 0,
            },
        };
    }
}
