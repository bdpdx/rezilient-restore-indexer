import { parseIndexerEnv } from './env';
import { RestoreIndexerService } from './indexer.service';
import { SqliteRestoreIndexStore } from './store';
import {
    InMemoryArtifactBatchSource,
    RestoreIndexerWorker,
} from './worker';

async function main(): Promise<void> {
    const config = parseIndexerEnv(process.env);
    const store = new SqliteRestoreIndexStore(config.stateDbPath);
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
        {
            pollIntervalMs: config.pollIntervalMs,
        },
    );
    let stopping = false;

    const onSignal = (signal: NodeJS.Signals): void => {
        if (stopping) {
            return;
        }

        stopping = true;
        console.log('restore-indexer shutdown requested', {
            signal,
        });
        worker.requestStop();
    };
    const signals: NodeJS.Signals[] = ['SIGINT', 'SIGTERM'];

    for (const signal of signals) {
        process.once(signal, onSignal);
    }

    console.log('restore-indexer runtime started', {
        backfill_batch_size: config.backfillBatchSize,
        poll_interval_ms: config.pollIntervalMs,
        state_db_path: config.stateDbPath,
        stale_after_seconds: config.staleAfterSeconds,
    });

    const result = await worker.runContinuously();

    for (const signal of signals) {
        process.removeListener(signal, onSignal);
    }

    console.log('restore-indexer runtime stopped', {
        summary: result,
    });
}

if (require.main === module) {
    main().catch((error: unknown) => {
        console.error('restore-indexer failed', error);
        process.exitCode = 1;
    });
}
