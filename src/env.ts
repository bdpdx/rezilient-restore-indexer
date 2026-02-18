export type RestoreIndexerEnv = {
    backfillBatchSize: number;
    backfillMaxLagSeconds: number;
    batchSize: number;
    defaultTenant: string;
    freshnessTimeoutSeconds: number;
    pollIntervalMs: number;
    stateDbPath: string;
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

    const stateDbPath = String(
        env.REZ_RESTORE_INDEXER_STATE_DB_PATH || '',
    ).trim();

    if (!stateDbPath) {
        throw new Error(
            'REZ_RESTORE_INDEXER_STATE_DB_PATH is required',
        );
    }

    return {
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
        stateDbPath,
        staleAfterSeconds: parsePositiveInt(
            env.REZ_RESTORE_INDEXER_STALE_AFTER_SECONDS,
            120,
        ),
    };
}
