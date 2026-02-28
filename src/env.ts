import type { IngestionMode } from './types';
import type { SourceCursorReplayDefaults } from './source-cursor';
import type { SourceProgressScope } from './worker';

export type ArtifactSourceMode =
    | 'rec_manifest_object_store'
    | 'in_memory_scaffold';

export type SourceCursorMode =
    | 'mixed'
    | 'v2_primary';

export type RecManifestCursorReplayEnv = SourceCursorReplayDefaults & {
    intervalSeconds: number;
    maxKeysPerCycle: number;
    maxPagesPerCycle: number;
};

export type RecManifestObjectStoreSourceEnv = {
    accessKeyId?: string;
    bucket: string;
    cursorMode: SourceCursorMode;
    cursorReplay: RecManifestCursorReplayEnv;
    endpoint?: string;
    forcePathStyle: boolean;
    generationId: string;
    ingestionMode: IngestionMode;
    manifestPrefix: string;
    maxAttempts: number;
    region: string;
    retryBaseMs: number;
    secretAccessKey?: string;
    sourceProgressScope: SourceProgressScope;
    sessionToken?: string;
    tenantId: string;
};

export type RestoreIndexerEnv = {
    artifactSourceMode: ArtifactSourceMode;
    backfillBatchSize: number;
    backfillMaxLagSeconds: number;
    batchSize: number;
    defaultTenant: string;
    freshnessTimeoutSeconds: number;
    leaderLease: {
        enabled: boolean;
        holderId?: string;
        leaseDurationSeconds: number;
    };
    pollIntervalMs: number;
    recManifestSource?: RecManifestObjectStoreSourceEnv;
    restorePgUrl: string;
    staleAfterSeconds: number;
};

function parsePositiveInt(
    value: string | undefined,
    fallback: number,
): number {
    if (!value) {
        return fallback;
    }

    const parsed = Number.parseInt(value, 10);

    if (!Number.isFinite(parsed) || parsed <= 0) {
        return fallback;
    }

    return parsed;
}

function readOptionalString(
    value: string | undefined,
): string | undefined {
    if (value === undefined) {
        return undefined;
    }

    const trimmed = String(value).trim();

    return trimmed || undefined;
}

function readRequiredString(
    env: NodeJS.ProcessEnv,
    key: string,
): string {
    const value = readOptionalString(env[key]);

    if (!value) {
        throw new Error(`${key} is required`);
    }

    return value;
}

function parseBoolean(
    value: string | undefined,
    fallback: boolean,
    key: string,
): boolean {
    const normalized = readOptionalString(value)?.toLowerCase();

    if (!normalized) {
        return fallback;
    }

    if (
        normalized === '1'
        || normalized === 'true'
        || normalized === 'yes'
        || normalized === 'on'
    ) {
        return true;
    }

    if (
        normalized === '0'
        || normalized === 'false'
        || normalized === 'no'
        || normalized === 'off'
    ) {
        return false;
    }

    throw new Error(`${key} must be true or false when provided`);
}

function parseIngestionMode(
    value: string | undefined,
): IngestionMode {
    const normalized = readOptionalString(value);

    if (!normalized) {
        return 'realtime';
    }

    if (
        normalized === 'realtime'
        || normalized === 'bootstrap'
        || normalized === 'gap_repair'
    ) {
        return normalized;
    }

    throw new Error(
        'REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE must be one of '
        + 'realtime|bootstrap|gap_repair when provided',
    );
}

function parseArtifactSourceMode(
    value: string | undefined,
): ArtifactSourceMode {
    const normalized = readOptionalString(value)
        || 'rec_manifest_object_store';

    if (
        normalized === 'rec_manifest_object_store'
        || normalized === 'in_memory_scaffold'
    ) {
        return normalized;
    }

    throw new Error(
        'REZ_RESTORE_INDEXER_ARTIFACT_SOURCE must be one of '
        + 'rec_manifest_object_store|in_memory_scaffold',
    );
}

function parseSourceCursorMode(
    value: string | undefined,
): SourceCursorMode {
    const normalized = readOptionalString(value) || 'mixed';

    if (
        normalized === 'mixed'
        || normalized === 'v2_primary'
    ) {
        return normalized;
    }

    throw new Error(
        'REZ_RESTORE_INDEXER_SOURCE_CURSOR_MODE must be one of '
        + 'mixed|v2_primary when provided',
    );
}

export function parseIndexerEnv(
    env: NodeJS.ProcessEnv,
): RestoreIndexerEnv {
    const defaultTenant = String(
        env.REZ_RESTORE_INDEXER_DEFAULT_TENANT || 'tenant-local-dev',
    ).trim();

    if (!defaultTenant) {
        throw new Error(
            'REZ_RESTORE_INDEXER_DEFAULT_TENANT must not be empty',
        );
    }

    const restorePgUrl = readOptionalString(env.REZ_RESTORE_PG_URL)
        || readOptionalString(env.REZ_RESTORE_INDEXER_PG_URL);

    if (!restorePgUrl) {
        throw new Error(
            'REZ_RESTORE_PG_URL is required',
        );
    }

    const artifactSourceMode = parseArtifactSourceMode(
        env.REZ_RESTORE_INDEXER_ARTIFACT_SOURCE,
    );
    const leaderElectionEnabled = parseBoolean(
        env.REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED,
        artifactSourceMode === 'rec_manifest_object_store',
        'REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED',
    );
    const leaderLeaseDurationSeconds = parsePositiveInt(
        env.REZ_RESTORE_INDEXER_LEADER_LEASE_SECONDS,
        30,
    );
    const leaderId = readOptionalString(
        env.REZ_RESTORE_INDEXER_LEADER_ID,
    );
    let recManifestSource: RecManifestObjectStoreSourceEnv | undefined;

    if (artifactSourceMode === 'rec_manifest_object_store') {
        const accessKeyId = readOptionalString(
            env.REZ_RESTORE_INDEXER_SOURCE_ACCESS_KEY_ID,
        );
        const secretAccessKey = readOptionalString(
            env.REZ_RESTORE_INDEXER_SOURCE_SECRET_ACCESS_KEY,
        );
        const sessionToken = readOptionalString(
            env.REZ_RESTORE_INDEXER_SOURCE_SESSION_TOKEN,
        );

        if (
            (accessKeyId && !secretAccessKey)
            || (!accessKeyId && secretAccessKey)
        ) {
            throw new Error(
                'REZ_RESTORE_INDEXER_SOURCE_ACCESS_KEY_ID and '
                + 'REZ_RESTORE_INDEXER_SOURCE_SECRET_ACCESS_KEY must be '
                + 'set together when provided',
            );
        }

        const tenantId = readOptionalString(
            env.REZ_RESTORE_INDEXER_SOURCE_TENANT_ID,
        ) || defaultTenant;
        const cursorReplay: RecManifestCursorReplayEnv = {
            enabled: parseBoolean(
                env.REZ_RESTORE_INDEXER_SOURCE_REPLAY_ENABLED,
                true,
                'REZ_RESTORE_INDEXER_SOURCE_REPLAY_ENABLED',
            ),
            intervalSeconds: parsePositiveInt(
                env.REZ_RESTORE_INDEXER_SOURCE_REPLAY_INTERVAL_SECONDS,
                60,
            ),
            lowerBound: readOptionalString(
                env.REZ_RESTORE_INDEXER_SOURCE_REPLAY_LOWER_BOUND,
            ) || null,
            maxKeysPerCycle: parsePositiveInt(
                env.REZ_RESTORE_INDEXER_SOURCE_REPLAY_MAX_KEYS_PER_CYCLE,
                1000,
            ),
            maxPagesPerCycle: parsePositiveInt(
                env.REZ_RESTORE_INDEXER_SOURCE_REPLAY_MAX_PAGES_PER_CYCLE,
                2,
            ),
        };
        const sourceProgressScope: SourceProgressScope = {
            instanceId: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID',
            ),
            source: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_URI',
            ),
            tenantId,
        };

        recManifestSource = {
            accessKeyId,
            bucket: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_BUCKET',
            ),
            cursorMode: parseSourceCursorMode(
                env.REZ_RESTORE_INDEXER_SOURCE_CURSOR_MODE,
            ),
            cursorReplay,
            endpoint: readOptionalString(
                env.REZ_RESTORE_INDEXER_SOURCE_ENDPOINT,
            ),
            forcePathStyle: parseBoolean(
                env.REZ_RESTORE_INDEXER_SOURCE_FORCE_PATH_STYLE,
                false,
                'REZ_RESTORE_INDEXER_SOURCE_FORCE_PATH_STYLE',
            ),
            generationId: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID',
            ),
            ingestionMode: parseIngestionMode(
                env.REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE,
            ),
            manifestPrefix: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_PREFIX',
            ),
            maxAttempts: parsePositiveInt(
                env.REZ_RESTORE_INDEXER_SOURCE_MAX_ATTEMPTS,
                3,
            ),
            region: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_REGION',
            ),
            retryBaseMs: parsePositiveInt(
                env.REZ_RESTORE_INDEXER_SOURCE_RETRY_BASE_MS,
                200,
            ),
            secretAccessKey,
            sourceProgressScope,
            sessionToken,
            tenantId,
        };
    }

    if (
        leaderElectionEnabled
        && artifactSourceMode !== 'rec_manifest_object_store'
    ) {
        throw new Error(
            'REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED=true requires '
            + 'REZ_RESTORE_INDEXER_ARTIFACT_SOURCE=rec_manifest_object_store',
        );
    }

    return {
        artifactSourceMode,
        backfillBatchSize: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_BACKFILL_BATCH_SIZE,
            250,
        ),
        backfillMaxLagSeconds: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_BACKFILL_MAX_LAG_SECONDS,
            180,
        ),
        batchSize: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_BATCH_SIZE,
            500,
        ),
        defaultTenant,
        freshnessTimeoutSeconds: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_FRESHNESS_TIMEOUT_SECONDS,
            60,
        ),
        leaderLease: {
            enabled: leaderElectionEnabled,
            holderId: leaderId,
            leaseDurationSeconds: leaderLeaseDurationSeconds,
        },
        pollIntervalMs: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_POLL_INTERVAL_MS,
            1000,
        ),
        recManifestSource,
        restorePgUrl,
        staleAfterSeconds: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_STALE_AFTER_SECONDS,
            120,
        ),
    };
}
