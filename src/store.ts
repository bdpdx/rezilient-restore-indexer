import { Pool, type PoolClient, type PoolConfig } from 'pg';
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

type SnapshotRow = {
    state_json: unknown;
    version: number;
};

type RestoreIndexSnapshot = {
    backfill_runs: Record<string, BackfillRunState>;
    indexed_events_by_key: Record<string, IndexedEventRecord>;
    partition_watermarks_by_key: Record<string, PartitionWatermarkState>;
    source_coverage_by_key: Record<string, SourceCoverageState>;
    source_progress_by_key: Record<string, SourceProgressState>;
};

export type PostgresRestoreIndexStoreOptions = {
    pool?: Pool;
    poolConfig?: Omit<PoolConfig, 'connectionString'>;
    schemaName?: string;
    tableName?: string;
};

const DEFAULT_SNAPSHOT_SCHEMA = 'public';
const DEFAULT_SNAPSHOT_TABLE = 'rri_state_snapshots';

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

function parseSnapshot(raw: unknown): RestoreIndexSnapshot {
    let parsed: unknown = raw;

    if (typeof raw === 'string') {
        parsed = JSON.parse(raw) as unknown;
    }

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

    private readonly snapshotTableQualified: string;

    private snapshot: RestoreIndexSnapshot = createEmptySnapshot();

    constructor(
        pgUrl: string,
        options: PostgresRestoreIndexStoreOptions = {},
    ) {
        const connectionString = String(pgUrl || '').trim();

        if (!connectionString && !options.pool) {
            throw new Error('REZ_RESTORE_PG_URL is required');
        }

        const schemaName = validateSqlIdentifier(
            options.schemaName || DEFAULT_SNAPSHOT_SCHEMA,
            'snapshot schema name',
        );
        const tableName = validateSqlIdentifier(
            options.tableName || DEFAULT_SNAPSHOT_TABLE,
            'snapshot table name',
        );

        this.snapshotTableQualified = `"${schemaName}"."${tableName}"`;

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
        return Object.keys(this.snapshot.indexed_events_by_key).length;
    }

    async close(): Promise<void> {
        if (!this.ownsPool) {
            return;
        }

        await this.pool.end();
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
        await this.ensureReady();

        const state = this.snapshot.partition_watermarks_by_key[
            partitionKey(scope)
        ];

        return state ? cloneState(state) : null;
    }

    async putPartitionWatermark(state: PartitionWatermarkState): Promise<void> {
        await this.mutateSnapshot((snapshot) => {
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
        await this.ensureReady();

        const state = this.snapshot.source_coverage_by_key[
            sourceKey(tenantId, instanceId, source)
        ];

        return state ? cloneState(state) : null;
    }

    async putSourceProgress(state: SourceProgressState): Promise<void> {
        await this.mutateSnapshot((snapshot) => {
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
        await this.ensureReady();

        const state = this.snapshot.source_progress_by_key[
            sourceKey(tenantId, instanceId, source)
        ];

        return state ? cloneState(state) : null;
    }

    async upsertBackfillRun(state: BackfillRunState): Promise<void> {
        await this.mutateSnapshot((snapshot) => {
            snapshot.backfill_runs[state.runId] = cloneState(state);
        });
    }

    async getBackfillRun(runId: string): Promise<BackfillRunState | null> {
        await this.ensureReady();

        const state = this.snapshot.backfill_runs[runId];

        return state ? cloneState(state) : null;
    }

    private async ensureReady(): Promise<void> {
        await this.ready;
    }

    private async initialize(): Promise<void> {
        await this.pool.query(this.createTableSql());
        await this.pool.query(
            `INSERT INTO ${this.snapshotTableQualified} (
                snapshot_id,
                version,
                state_json,
                updated_at
            )
            SELECT
                1,
                $1::bigint,
                $2::jsonb,
                now()
            WHERE NOT EXISTS (
                SELECT 1
                FROM ${this.snapshotTableQualified}
                WHERE snapshot_id = 1
            )`,
            [
                0,
                serializeSnapshot(createEmptySnapshot()),
            ],
        );

        const row = await this.selectSnapshot();

        if (!row) {
            throw new Error('failed to load restore-index snapshot state');
        }

        this.snapshot = parseSnapshot(row.state_json);
    }

    private async mutateSnapshot<T>(
        mutator: (snapshot: RestoreIndexSnapshot) => T,
    ): Promise<T> {
        await this.ensureReady();

        const client = await this.pool.connect();

        try {
            await client.query('BEGIN');

            const row = await this.selectSnapshotForTransaction(client);
            const working = parseSnapshot(row.state_json);
            const result = mutator(working);
            const nextVersion = row.version + 1;

            await client.query(
                `UPDATE ${this.snapshotTableQualified}
                SET version = $1::bigint,
                    state_json = $2::jsonb,
                    updated_at = now()
                WHERE snapshot_id = 1`,
                [
                    nextVersion,
                    serializeSnapshot(working),
                ],
            );

            await client.query('COMMIT');

            this.snapshot = cloneState(working);

            return result;
        } catch (error) {
            await client.query('ROLLBACK');
            throw error;
        } finally {
            client.release();
        }
    }

    private async selectSnapshot(): Promise<SnapshotRow | null> {
        const result = await this.pool.query<SnapshotRow>(
            `SELECT version, state_json
            FROM ${this.snapshotTableQualified}
            WHERE snapshot_id = 1
            ORDER BY version DESC
            LIMIT 1`,
        );

        if (result.rowCount !== 1) {
            return null;
        }

        return result.rows[0];
    }

    private async selectSnapshotForTransaction(
        client: PoolClient,
    ): Promise<SnapshotRow> {
        const result = await client.query<SnapshotRow>(
            `SELECT version, state_json
            FROM ${this.snapshotTableQualified}
            WHERE snapshot_id = 1
            ORDER BY version DESC
            LIMIT 1
            FOR UPDATE`,
        );

        if (result.rowCount === 1) {
            return result.rows[0];
        }

        const emptySnapshot = createEmptySnapshot();

        await client.query(
            `INSERT INTO ${this.snapshotTableQualified} (
                snapshot_id,
                version,
                state_json,
                updated_at
            ) VALUES (
                1,
                $1::bigint,
                $2::jsonb,
                now()
            )`,
            [
                0,
                serializeSnapshot(emptySnapshot),
            ],
        );

        return {
            state_json: serializeSnapshot(emptySnapshot),
            version: 0,
        };
    }

    private createTableSql(): string {
        return `
CREATE TABLE IF NOT EXISTS ${this.snapshotTableQualified} (
    snapshot_id INTEGER,
    version BIGINT,
    state_json JSONB,
    updated_at TIMESTAMPTZ
)
`;
    }
}
