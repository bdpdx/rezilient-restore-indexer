import type { RestoreIndexerService } from './indexer.service';
import type {
    IndexArtifactInput,
    ProcessBatchResult,
} from './types';

export interface ArtifactBatchSource {
    readBatch(limit: number): Promise<IndexArtifactInput[]>;
}

export type WorkerRunSummary = ProcessBatchResult & {
    batchSize: number;
};

export class RestoreIndexerWorker {
    private paused = false;

    constructor(
        private readonly source: ArtifactBatchSource,
        private readonly indexer: RestoreIndexerService,
        private readonly batchSize: number,
    ) {}

    pause(): void {
        this.paused = true;
    }

    resume(): void {
        this.paused = false;
    }

    async runOnce(): Promise<WorkerRunSummary> {
        if (this.paused) {
            return {
                batchSize: 0,
                existing: 0,
                failures: 0,
                inserted: 0,
            };
        }

        const batch = await this.source.readBatch(this.batchSize);
        const result = await this.indexer.processBatch(batch);

        return {
            ...result,
            batchSize: batch.length,
        };
    }
}

export class InMemoryArtifactBatchSource implements ArtifactBatchSource {
    private readonly queue: IndexArtifactInput[] = [];

    constructor(initial: IndexArtifactInput[] = []) {
        this.queue.push(...initial);
    }

    enqueue(items: IndexArtifactInput[]): void {
        this.queue.push(...items);
    }

    async readBatch(limit: number): Promise<IndexArtifactInput[]> {
        return this.queue.splice(0, limit);
    }
}
