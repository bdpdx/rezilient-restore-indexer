import { parseIndexerEnv } from './env';
import { RestoreIndexerService } from './indexer.service';
import { InMemoryRestoreIndexStore } from './store';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

async function main(): Promise<void> {
    const config = parseIndexerEnv(process.env);
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: config.staleAfterSeconds,
            timeoutSeconds: config.freshnessTimeoutSeconds,
        },
    });
    const source = new InMemoryArtifactBatchSource();
    const worker = new RestoreIndexerWorker(
        source,
        indexer,
        config.batchSize,
    );
    const result = await worker.runOnce();

    console.log('restore-indexer boot complete', {
        backfill_batch_size: config.backfillBatchSize,
        processed: result,
        stale_after_seconds: config.staleAfterSeconds,
    });
}

if (require.main === module) {
    main().catch((error: unknown) => {
        console.error('restore-indexer failed', error);
        process.exitCode = 1;
    });
}
