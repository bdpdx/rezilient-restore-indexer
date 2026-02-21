import type { IngestionMode } from './types';
import type { SourceProgressScope } from './worker';

export type ArtifactSourceMode =
    | 'rec_manifest_object_store'
    | 'in_memory_scaffold';

export type RecManifestObjectStoreSourceEnv = {
    bucket: string;
    endpoint?: string;
    forcePathStyle: boolean;
    generationId: string;
    ingestionMode: IngestionMode;
    manifestPrefix: string;
    maxAttempts: number;
    region: string;
    retryBaseMs: number;
    sourceProgressScope: SourceProgressScope;
    tenantId: string;
};

export type RestoreIndexerEnv = {
    artifactSourceMode: ArtifactSourceMode;
    backfillBatchSize: number;
    backfillMaxLagSeconds: number;
    batchSize: number;
    defaultTenant: string;
    freshnessTimeoutSeconds: number;
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
    let recManifestSource: RecManifestObjectStoreSourceEnv | undefined;

    if (artifactSourceMode === 'rec_manifest_object_store') {
        const tenantId = readOptionalString(
            env.REZ_RESTORE_INDEXER_SOURCE_TENANT_ID,
        ) || defaultTenant;
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
            bucket: readRequiredString(
                env,
                'REZ_RESTORE_INDEXER_SOURCE_BUCKET',
            ),
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
            sourceProgressScope,
            tenantId,
        };
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
