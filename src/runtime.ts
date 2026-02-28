import { hostname as resolveHostname } from 'node:os';
import { parseIndexerEnv, type RecManifestObjectStoreSourceEnv } from './env';
import { RestoreIndexerService } from './indexer.service';
import {
    createS3RecManifestObjectStoreClient,
} from './rec-manifest-object-store-client';
import {
    RecManifestArtifactBatchSource,
    type RecManifestObjectStoreClient,
} from './rec-manifest-source';
import {
    PostgresRestoreIndexStore,
    type RestoreIndexStore,
} from './store';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

export type RuntimeSourceSummary =
    | {
        mode: 'in_memory_scaffold';
    }
    | {
        bucket: string;
        endpoint: string | null;
        mode: 'rec_manifest_object_store';
        prefix: string;
        region: string;
    };

export type RuntimeBootstrap = {
    config: ReturnType<typeof parseIndexerEnv>;
    source: RuntimeSourceSummary;
    worker: RestoreIndexerWorker;
};

export type RuntimeDependencyOverrides = {
    createObjectStoreClient?: (
        source: RecManifestObjectStoreSourceEnv,
    ) => RecManifestObjectStoreClient;
    createStore?: (restorePgUrl: string) => RestoreIndexStore;
};

function buildDefaultLeaderId(): string {
    return `${resolveHostname()}:${process.pid}`;
}

function createRecSource(
    sourceConfig: RecManifestObjectStoreSourceEnv,
    dependencies: RuntimeDependencyOverrides,
): {
    source: RecManifestArtifactBatchSource;
    sourceSummary: RuntimeSourceSummary;
} {
    const objectStoreClientFactory = dependencies.createObjectStoreClient
        || ((input: RecManifestObjectStoreSourceEnv) => {
            return createS3RecManifestObjectStoreClient({
                accessKeyId: input.accessKeyId,
                bucket: input.bucket,
                endpoint: input.endpoint,
                forcePathStyle: input.forcePathStyle,
                region: input.region,
                secretAccessKey: input.secretAccessKey,
                sessionToken: input.sessionToken,
            });
        });
    const objectStoreClient = objectStoreClientFactory(sourceConfig);
    const source = new RecManifestArtifactBatchSource(objectStoreClient, {
        cursorReplay: sourceConfig.cursorReplay,
        generationId: sourceConfig.generationId,
        ingestionMode: sourceConfig.ingestionMode,
        maxAttempts: sourceConfig.maxAttempts,
        prefix: sourceConfig.manifestPrefix,
        retryBaseMs: sourceConfig.retryBaseMs,
        tenantId: sourceConfig.tenantId,
    });
    const sourceSummary: RuntimeSourceSummary = {
        bucket: sourceConfig.bucket,
        endpoint: sourceConfig.endpoint || null,
        mode: 'rec_manifest_object_store',
        prefix: sourceConfig.manifestPrefix,
        region: sourceConfig.region,
    };

    return {
        source,
        sourceSummary,
    };
}

export function createRuntime(
    env: NodeJS.ProcessEnv,
    dependencies: RuntimeDependencyOverrides = {},
): RuntimeBootstrap {
    const config = parseIndexerEnv(env);
    const createStore = dependencies.createStore
        || ((restorePgUrl: string) => {
            return new PostgresRestoreIndexStore(restorePgUrl);
        });
    const store = createStore(config.restorePgUrl);
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: config.staleAfterSeconds,
            timeoutSeconds: config.freshnessTimeoutSeconds,
        },
    });
    let sourceSummary: RuntimeSourceSummary;
    let worker: RestoreIndexerWorker;

    if (config.artifactSourceMode === 'in_memory_scaffold') {
        sourceSummary = {
            mode: 'in_memory_scaffold',
        };
        worker = new RestoreIndexerWorker(
            new InMemoryArtifactBatchSource(),
            indexer,
            config.batchSize,
            {
                pollIntervalMs: config.pollIntervalMs,
            },
        );
    } else {
        if (!config.recManifestSource) {
            throw new Error(
                'rec manifest source config is required for production mode',
            );
        }

        const recSource = createRecSource(
            config.recManifestSource,
            dependencies,
        );

        sourceSummary = recSource.sourceSummary;
        worker = new RestoreIndexerWorker(
            recSource.source,
            indexer,
            config.batchSize,
            {
                leaderLease: config.leaderLease.enabled
                    ? {
                        holderId: config.leaderLease.holderId
                            || buildDefaultLeaderId(),
                        leaseDurationSeconds:
                            config.leaderLease.leaseDurationSeconds,
                        manager: store,
                    }
                    : undefined,
                pollIntervalMs: config.pollIntervalMs,
                sourceProgressScope: config.recManifestSource.sourceProgressScope,
            },
        );
    }

    return {
        config,
        source: sourceSummary,
        worker,
    };
}
