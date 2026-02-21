import { createRuntime } from './runtime';

async function main(): Promise<void> {
    const runtime = createRuntime(process.env);
    const { config, source, worker } = runtime;
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
        artifact_source: source.mode,
        backfill_batch_size: config.backfillBatchSize,
        default_tenant: config.defaultTenant,
        object_store_bucket: source.mode === 'rec_manifest_object_store'
            ? source.bucket
            : null,
        object_store_endpoint: source.mode === 'rec_manifest_object_store'
            ? source.endpoint
            : null,
        object_store_prefix: source.mode === 'rec_manifest_object_store'
            ? source.prefix
            : null,
        object_store_region: source.mode === 'rec_manifest_object_store'
            ? source.region
            : null,
        poll_interval_ms: config.pollIntervalMs,
        restore_pg_url_configured: Boolean(config.restorePgUrl),
        stale_after_seconds: config.staleAfterSeconds,
    });

    if (source.mode === 'in_memory_scaffold') {
        console.warn(
            'restore-indexer scaffold source mode enabled; '
            + 'production ingest disabled by explicit operator opt-in',
        );
    }

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
