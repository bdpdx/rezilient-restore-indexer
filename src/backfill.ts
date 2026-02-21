import { randomUUID } from 'node:crypto';
import type { RestoreIndexerService } from './indexer.service';
import type { RestoreIndexStore } from './store';
import type {
    BackfillBatch,
    BackfillMode,
    BackfillRunState,
    IndexArtifactInput,
} from './types';

export interface BackfillBatchSource {
    readBatch(input: {
        cursor: string | null;
        limit: number;
        mode: BackfillMode;
    }): Promise<BackfillBatch>;
}

export type BackfillControllerOptions = {
    maxRealtimeLagSeconds: number;
    runId?: string;
    throttleBatchSize: number;
    timeProvider?: () => string;
};

function nowIso(): string {
    return new Date().toISOString();
}

function cloneState(state: BackfillRunState): BackfillRunState {
    return {
        ...state,
    };
}

export class BackfillController {
    private initialized = false;

    private readonly timeProvider: () => string;

    private state: BackfillRunState;

    constructor(
        private readonly mode: BackfillMode,
        private readonly source: BackfillBatchSource,
        private readonly indexer: RestoreIndexerService,
        private readonly store: RestoreIndexStore,
        private readonly options: BackfillControllerOptions,
    ) {
        this.timeProvider = options.timeProvider || nowIso;

        const initialTime = this.timeProvider();

        this.state = {
            cursor: null,
            maxRealtimeLagSeconds: options.maxRealtimeLagSeconds,
            mode,
            processedCount: 0,
            reasonCode: 'none',
            runId: options.runId || randomUUID(),
            status: 'running',
            throttleBatchSize: options.throttleBatchSize,
            updatedAt: initialTime,
        };
    }

    getState(): BackfillRunState {
        return cloneState(this.state);
    }

    async pauseManual(): Promise<BackfillRunState> {
        await this.ensureInitialized();

        this.state = {
            ...this.state,
            reasonCode: 'paused_manual',
            status: 'paused',
            updatedAt: this.timeProvider(),
        };
        await this.store.upsertBackfillRun(this.state);

        return this.getState();
    }

    async resume(): Promise<BackfillRunState> {
        await this.ensureInitialized();

        if (this.state.status === 'completed') {
            return this.getState();
        }

        this.state = {
            ...this.state,
            reasonCode: 'none',
            status: 'running',
            updatedAt: this.timeProvider(),
        };
        await this.store.upsertBackfillRun(this.state);

        return this.getState();
    }

    async tick(): Promise<BackfillRunState> {
        await this.ensureInitialized();

        if (
            this.state.status === 'paused'
            || this.state.status === 'completed'
            || this.state.status === 'failed'
        ) {
            return this.getState();
        }

        const batch = await this.source.readBatch({
            cursor: this.state.cursor,
            limit: this.state.throttleBatchSize,
            mode: this.mode,
        });

        if (batch.realtimeLagSeconds > this.state.maxRealtimeLagSeconds) {
            this.state = {
                ...this.state,
                reasonCode: 'paused_realtime_lag_guardrail',
                status: 'paused',
                updatedAt: this.timeProvider(),
            };
            await this.store.upsertBackfillRun(this.state);

            return this.getState();
        }

        const items = batch.items.map((item): IndexArtifactInput => {
            return {
                ...item,
                ingestionMode: this.mode,
            };
        });
        const result = await this.indexer.processBatch(items);

        if (result.failures > 0) {
            this.state = {
                ...this.state,
                processedCount: this.state.processedCount
                    + result.inserted
                    + result.existing,
                reasonCode: 'paused_indexing_failures',
                status: 'paused',
                updatedAt: this.timeProvider(),
            };

            await this.store.upsertBackfillRun(this.state);

            return this.getState();
        }

        this.state = {
            ...this.state,
            cursor: batch.nextCursor,
            processedCount: this.state.processedCount
                + result.inserted
                + result.existing,
            reasonCode: 'none',
            status: batch.done ? 'completed' : 'running',
            updatedAt: this.timeProvider(),
        };

        await this.store.upsertBackfillRun(this.state);

        return this.getState();
    }

    private async ensureInitialized(): Promise<void> {
        if (this.initialized) {
            return;
        }

        const persisted = await this.store.getBackfillRun(this.state.runId);

        if (persisted && persisted.mode === this.mode) {
            this.state = cloneState(persisted);
        } else {
            await this.store.upsertBackfillRun(this.state);
        }

        this.initialized = true;
    }
}

export class InMemoryBackfillBatchSource implements BackfillBatchSource {
    private cursor = 0;

    constructor(
        private readonly items: IndexArtifactInput[],
        private readonly realtimeLagSeconds: number,
    ) {}

    async readBatch(input: {
        cursor: string | null;
        limit: number;
        mode: BackfillMode;
    }): Promise<BackfillBatch> {
        void input.mode;

        if (input.cursor !== null) {
            const parsed = Number.parseInt(input.cursor, 10);

            if (Number.isFinite(parsed) && parsed >= 0) {
                this.cursor = parsed;
            }
        }

        const start = this.cursor;
        const end = Math.min(this.items.length, start + input.limit);
        const batchItems = this.items.slice(start, end);

        this.cursor = end;

        return {
            done: this.cursor >= this.items.length,
            items: batchItems,
            nextCursor: String(this.cursor),
            realtimeLagSeconds: this.realtimeLagSeconds,
        };
    }
}
