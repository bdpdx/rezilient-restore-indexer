import { DatabaseSync } from 'node:sqlite';
import type {
    BackfillRunState,
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
    getSourceProgress(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceProgressState | null>;
    putPartitionWatermark(state: PartitionWatermarkState): Promise<void>;
    putSourceProgress(state: SourceProgressState): Promise<void>;
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

    private readonly sourceProgress = new Map<string, SourceProgressState>();

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

        this.indexedEvents.set(key, cloneState(record));

        return 'inserted';
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

    async putSourceProgress(state: SourceProgressState): Promise<void> {
        this.sourceProgress.set(
            sourceKey(state.tenantId, state.instanceId, state.source),
            cloneState(state),
        );
    }

    async getSourceProgress(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceProgressState | null> {
        const state = this.sourceProgress.get(
            sourceKey(tenantId, instanceId, source),
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
}

interface SnapshotRow {
    state_json: string;
    version: number;
}

type RestoreIndexSnapshot = {
    backfill_runs: Record<string, BackfillRunState>;
    indexed_events_by_key: Record<string, IndexedEventRecord>;
    partition_watermarks_by_key: Record<string, PartitionWatermarkState>;
    source_coverage_by_key: Record<string, SourceCoverageState>;
    source_progress_by_key: Record<string, SourceProgressState>;
};

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS rri_state_snapshots (
    snapshot_id INTEGER PRIMARY KEY CHECK (snapshot_id = 1),
    version INTEGER NOT NULL,
    state_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
`;

const SELECT_SNAPSHOT_SQL = `
SELECT version, state_json
FROM rri_state_snapshots
WHERE snapshot_id = 1
`;

const INSERT_SNAPSHOT_SQL = `
INSERT INTO rri_state_snapshots (
    snapshot_id,
    version,
    state_json,
    updated_at
) VALUES (
    1,
    ?,
    ?,
    ?
)
`;

const UPDATE_SNAPSHOT_SQL = `
UPDATE rri_state_snapshots
SET version = ?, state_json = ?, updated_at = ?
WHERE snapshot_id = 1
`;

function createEmptySnapshot(): RestoreIndexSnapshot {
    return {
        backfill_runs: {},
        indexed_events_by_key: {},
        partition_watermarks_by_key: {},
        source_coverage_by_key: {},
        source_progress_by_key: {},
    };
}

function serializeSnapshot(snapshot: RestoreIndexSnapshot): string {
    return JSON.stringify(snapshot);
}

function parseSnapshot(stateJson: string): RestoreIndexSnapshot {
    const parsed = JSON.parse(stateJson) as unknown;

    if (!parsed || typeof parsed !== 'object') {
        throw new Error('invalid persisted restore-index snapshot payload');
    }

    const snapshot = parsed as Partial<RestoreIndexSnapshot>;

    if (
        !snapshot.backfill_runs
        || typeof snapshot.backfill_runs !== 'object'
    ) {
        throw new Error('invalid persisted backfill_runs payload');
    }

    if (
        !snapshot.indexed_events_by_key
        || typeof snapshot.indexed_events_by_key !== 'object'
    ) {
        throw new Error('invalid persisted indexed_events_by_key payload');
    }

    if (
        !snapshot.partition_watermarks_by_key
        || typeof snapshot.partition_watermarks_by_key !== 'object'
    ) {
        throw new Error('invalid persisted partition_watermarks_by_key payload');
    }

    if (
        !snapshot.source_coverage_by_key
        || typeof snapshot.source_coverage_by_key !== 'object'
    ) {
        throw new Error('invalid persisted source_coverage_by_key payload');
    }

    if (
        !snapshot.source_progress_by_key
        || typeof snapshot.source_progress_by_key !== 'object'
    ) {
        throw new Error('invalid persisted source_progress_by_key payload');
    }

    return cloneState({
        backfill_runs: snapshot.backfill_runs as Record<string, BackfillRunState>,
        indexed_events_by_key:
            snapshot.indexed_events_by_key as Record<string, IndexedEventRecord>,
        partition_watermarks_by_key:
            snapshot.partition_watermarks_by_key as Record<
                string,
                PartitionWatermarkState
            >,
        source_coverage_by_key:
            snapshot.source_coverage_by_key as Record<string, SourceCoverageState>,
        source_progress_by_key:
            snapshot.source_progress_by_key as Record<string, SourceProgressState>,
    });
}

export class SqliteRestoreIndexStore implements RestoreIndexStore {
    private readonly database: DatabaseSync;

    private snapshot: RestoreIndexSnapshot = createEmptySnapshot();

    constructor(private readonly dbPath: string) {
        this.database = new DatabaseSync(dbPath);
        this.database.exec('PRAGMA journal_mode = WAL');
        this.database.exec('PRAGMA synchronous = NORMAL');
        this.database.exec(CREATE_TABLE_SQL);
        this.ensureSnapshotRow();
        this.snapshot = this.readSnapshot();
    }

    getIndexedEventCount(): number {
        return Object.keys(this.snapshot.indexed_events_by_key).length;
    }

    async upsertIndexedEvent(
        record: IndexedEventRecord,
    ): Promise<'inserted' | 'existing'> {
        return this.mutateSnapshot((snapshot) => {
            const key = eventKey(record);

            if (snapshot.indexed_events_by_key[key]) {
                return 'existing';
            }

            snapshot.indexed_events_by_key[key] = cloneState(record);

            return 'inserted';
        });
    }

    async getPartitionWatermark(
        scope: PartitionScope,
    ): Promise<PartitionWatermarkState | null> {
        const state = this.snapshot.partition_watermarks_by_key[
            partitionKey(scope)
        ];

        return state ? cloneState(state) : null;
    }

    async putPartitionWatermark(state: PartitionWatermarkState): Promise<void> {
        this.mutateSnapshot((snapshot) => {
            snapshot.partition_watermarks_by_key[
                partitionKey({
                    instanceId: state.instanceId,
                    partition: state.partition,
                    source: state.source,
                    tenantId: state.tenantId,
                    topic: state.topic,
                })
            ] = cloneState(state);
        });
    }

    async recomputeSourceCoverage(input: {
        instanceId: string;
        measuredAt: string;
        source: string;
        tenantId: string;
    }): Promise<SourceCoverageState | null> {
        return this.mutateSnapshot((snapshot) => {
            const key = sourceKey(
                input.tenantId,
                input.instanceId,
                input.source,
            );
            const state = recomputeCoverageFromWatermarks(
                input,
                Object.values(snapshot.partition_watermarks_by_key),
            );

            if (state === null) {
                delete snapshot.source_coverage_by_key[key];
                return null;
            }

            snapshot.source_coverage_by_key[key] = cloneState(state);

            return cloneState(state);
        });
    }

    async getSourceCoverage(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceCoverageState | null> {
        const state = this.snapshot.source_coverage_by_key[
            sourceKey(tenantId, instanceId, source)
        ];

        return state ? cloneState(state) : null;
    }

    async putSourceProgress(state: SourceProgressState): Promise<void> {
        this.mutateSnapshot((snapshot) => {
            snapshot.source_progress_by_key[
                sourceKey(state.tenantId, state.instanceId, state.source)
            ] = cloneState(state);
        });
    }

    async getSourceProgress(
        tenantId: string,
        instanceId: string,
        source: string,
    ): Promise<SourceProgressState | null> {
        const state = this.snapshot.source_progress_by_key[
            sourceKey(tenantId, instanceId, source)
        ];

        return state ? cloneState(state) : null;
    }

    async upsertBackfillRun(state: BackfillRunState): Promise<void> {
        this.mutateSnapshot((snapshot) => {
            snapshot.backfill_runs[state.runId] = cloneState(state);
        });
    }

    async getBackfillRun(runId: string): Promise<BackfillRunState | null> {
        const state = this.snapshot.backfill_runs[runId];

        return state ? cloneState(state) : null;
    }

    private mutateSnapshot<T>(
        mutator: (snapshot: RestoreIndexSnapshot) => T,
    ): T {
        this.database.exec('BEGIN IMMEDIATE');

        try {
            const row = this.selectSnapshotForTransaction();
            const working = parseSnapshot(row.state_json);
            const result = mutator(working);
            const nextVersion = row.version + 1;
            const updateStatement = this.database.prepare(UPDATE_SNAPSHOT_SQL);

            updateStatement.run(
                nextVersion,
                serializeSnapshot(working),
                new Date().toISOString(),
            );
            this.database.exec('COMMIT');
            this.snapshot = working;

            return result;
        } catch (error) {
            this.database.exec('ROLLBACK');
            throw error;
        }
    }

    private ensureSnapshotRow(): void {
        const row = this.selectSnapshot();

        if (row) {
            return;
        }

        const insert = this.database.prepare(INSERT_SNAPSHOT_SQL);

        insert.run(
            0,
            serializeSnapshot(createEmptySnapshot()),
            new Date().toISOString(),
        );
    }

    private readSnapshot(): RestoreIndexSnapshot {
        const row = this.selectSnapshot();

        if (!row) {
            return createEmptySnapshot();
        }

        return parseSnapshot(row.state_json);
    }

    private selectSnapshot(): SnapshotRow | undefined {
        const statement = this.database.prepare(SELECT_SNAPSHOT_SQL);

        return statement.get() as SnapshotRow | undefined;
    }

    private selectSnapshotForTransaction(): SnapshotRow {
        const row = this.selectSnapshot();

        if (row) {
            return row;
        }

        const emptySnapshot = createEmptySnapshot();
        const insert = this.database.prepare(INSERT_SNAPSHOT_SQL);

        insert.run(
            0,
            serializeSnapshot(emptySnapshot),
            new Date().toISOString(),
        );

        return {
            state_json: serializeSnapshot(emptySnapshot),
            version: 0,
        };
    }
}
