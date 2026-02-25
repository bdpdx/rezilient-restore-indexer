import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    RestoreIndexerService,
    type SourceProgressUpdateInput,
} from './indexer.service.js';
import { InMemoryRestoreIndexStore } from './store.js';
import { buildTestInput } from './test-helpers.js';

function createIndexer() {
    const store = new InMemoryRestoreIndexStore();
    const indexer = new RestoreIndexerService(store, {
        freshnessPolicy: {
            staleAfterSeconds: 120,
            timeoutSeconds: 60,
        },
    });

    return { indexer, store };
}

describe('indexer helper functions (tested via indexArtifact)', () => {
    it('canonicalizeOffset: valid decimal string', async () => {
        const { indexer } = createIndexer();
        const result = await indexer.indexArtifact(
            buildTestInput({ offset: '42' }),
        );
        assert.equal(
            result.watermark.indexedThroughOffset,
            '42',
        );
    });

    it('canonicalizeOffset: throws on non-decimal string',
    async () => {
        const { indexer } = createIndexer();
        await assert.rejects(
            indexer.indexArtifact(
                buildTestInput({
                    offset: '12',
                    metadata: { offset: '12.34' },
                }),
            ),
            /must be non-negative integer offset as decimal string/,
        );
    });

    it('canonicalizeOffset: handles BigInt-scale offsets',
    async () => {
        const { indexer } = createIndexer();
        const bigOffset = '9007199254740993';
        const result = await indexer.indexArtifact(
            buildTestInput({
                offset: bigOffset,
                metadata: { offset: bigOffset },
            }),
        );
        assert.equal(
            result.watermark.indexedThroughOffset,
            bigOffset,
        );
    });

    it('compareOffsets: left < right returns negative (via rewind)',
    async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-1',
                generationId: 'gen-cmp',
                offset: 10,
            }),
        );
        await assert.rejects(
            indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-2',
                    generationId: 'gen-cmp',
                    offset: 5,
                }),
            ),
            { name: 'WatermarkInvariantError' },
        );
    });

    it('compareOffsets: left > right advances watermark',
    async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-1',
                generationId: 'gen-adv',
                offset: 5,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-2',
                generationId: 'gen-adv',
                offset: 10,
            }),
        );
        assert.equal(
            result.watermark.indexedThroughOffset,
            '10',
        );
    });

    it('compareOffsets: handles offsets beyond MAX_SAFE_INTEGER',
    async () => {
        const { indexer } = createIndexer();
        const first = '9007199254740993';
        const second = '9007199254740994';
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-big-1',
                generationId: 'gen-big',
                metadata: { offset: first },
                offset: first,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-big-2',
                generationId: 'gen-big',
                metadata: { offset: second },
                offset: second,
            }),
        );
        assert.equal(
            result.watermark.indexedThroughOffset,
            second,
        );
    });
});

describe('buildNextWatermark (tested via indexArtifact)', () => {
    it('creates new watermark from null (first event)', async () => {
        const { indexer } = createIndexer();
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-first',
                eventTime: '2026-02-16T12:00:00.000Z',
                generationId: 'gen-first',
                offset: 1,
            }),
        );
        assert.equal(result.eventWriteState, 'inserted');
        assert.equal(result.watermark.generationId, 'gen-first');
        assert.equal(result.watermark.indexedThroughOffset, '1');
        assert.equal(
            result.watermark.coverageStart,
            '2026-02-16T12:00:00.000Z',
        );
        assert.equal(
            result.watermark.coverageEnd,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('extends existing watermark (advancing offset)', async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-ext-1',
                eventTime: '2026-02-16T12:00:00.000Z',
                generationId: 'gen-ext',
                offset: 1,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-ext-2',
                eventTime: '2026-02-16T12:05:00.000Z',
                generationId: 'gen-ext',
                offset: 2,
            }),
        );
        assert.equal(result.watermark.indexedThroughOffset, '2');
    });

    it('extends coverage_start when earlier event arrives',
    async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-cov-1',
                eventTime: '2026-02-16T12:10:00.000Z',
                generationId: 'gen-cov',
                offset: 2,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-cov-2',
                eventTime: '2026-02-16T12:00:00.000Z',
                generationId: 'gen-cov',
                offset: 3,
            }),
        );
        assert.equal(
            result.watermark.coverageStart,
            '2026-02-16T12:00:00.000Z',
        );
        assert.equal(
            result.watermark.coverageEnd,
            '2026-02-16T12:10:00.000Z',
        );
    });

    it('extends coverage_end when later event arrives',
    async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-covend-1',
                eventTime: '2026-02-16T12:00:00.000Z',
                generationId: 'gen-covend',
                offset: 1,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-covend-2',
                eventTime: '2026-02-16T12:30:00.000Z',
                generationId: 'gen-covend',
                offset: 2,
            }),
        );
        assert.equal(
            result.watermark.coverageEnd,
            '2026-02-16T12:30:00.000Z',
        );
    });

    it('resets state on generation change', async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-genreset-1',
                eventTime: '2026-02-16T12:10:00.000Z',
                generationId: 'gen-a',
                offset: 15,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-genreset-2',
                eventTime: '2026-02-16T12:05:00.000Z',
                generationId: 'gen-b',
                offset: 3,
            }),
        );
        assert.equal(result.watermark.generationId, 'gen-b');
        assert.equal(result.watermark.indexedThroughOffset, '3');
    });

    it('throws WatermarkInvariantError on offset rewind',
    async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-rewind-1',
                generationId: 'gen-rewind',
                offset: 10,
            }),
        );
        await assert.rejects(
            indexer.indexArtifact(
                buildTestInput({
                    eventId: 'evt-rewind-2',
                    generationId: 'gen-rewind',
                    offset: 9,
                }),
            ),
            { name: 'WatermarkInvariantError' },
        );
    });

    it('handles duplicate offset gracefully', async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-dupoff-1',
                generationId: 'gen-dupoff',
                offset: 5,
            }),
        );
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-dupoff-2',
                generationId: 'gen-dupoff',
                offset: 10,
            }),
        );
        const result = await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-dupoff-1',
                generationId: 'gen-dupoff',
                offset: 5,
            }),
        );
        assert.equal(result.eventWriteState, 'existing');
        assert.equal(
            result.watermark.indexedThroughOffset,
            '10',
        );
    });
});

describe('RestoreIndexerService.refreshSourceCoverage', () => {
    it('recomputes coverage from existing watermarks', async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-ref-1',
                eventTime: '2026-02-16T10:00:00.000Z',
                offset: 1,
                partition: 0,
            }),
        );
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-ref-2',
                eventTime: '2026-02-16T12:00:00.000Z',
                offset: 1,
                partition: 1,
            }),
        );
        const coverage = await indexer.getSourceCoverageWindow(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
        );
        assert.notEqual(coverage, null);
        assert.equal(
            coverage?.earliest_indexed_time,
            '2026-02-16T10:00:00.000Z',
        );
        assert.equal(
            coverage?.latest_indexed_time,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('returns null when no watermarks exist', async () => {
        const { indexer } = createIndexer();
        const coverage = await indexer.getSourceCoverageWindow(
            'tenant-acme',
            'sn-dev-01',
            'sn://nonexistent.service-now.com',
        );
        assert.equal(coverage, null);
    });
});

describe('RestoreIndexerService.processBatch', () => {
    it('empty batch returns zero counts', async () => {
        const { indexer } = createIndexer();
        const result = await indexer.processBatch([]);
        assert.deepEqual(result, {
            existing: 0,
            failures: 0,
            inserted: 0,
        });
    });

    it('batch with all duplicates returns existing count',
    async () => {
        const { indexer } = createIndexer();
        const input = buildTestInput({ eventId: 'evt-batch-dup' });
        await indexer.indexArtifact(input);
        const result = await indexer.processBatch([input]);
        assert.equal(result.existing, 1);
        assert.equal(result.inserted, 0);
    });

    it('batch with mixed results returns correct counts',
    async () => {
        const { indexer } = createIndexer();
        const existing = buildTestInput({
            eventId: 'evt-batch-mix-1',
            offset: 1,
        });
        await indexer.indexArtifact(existing);

        const fresh = buildTestInput({
            eventId: 'evt-batch-mix-2',
            offset: 2,
        });
        const invalid = buildTestInput({
            eventId: 'evt-batch-mix-fail',
            offset: 3,
            metadata: {
                short_description: 'not allowed',
            },
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            const result = await indexer.processBatch([
                existing, fresh, invalid,
            ]);
            assert.equal(result.existing, 1);
            assert.equal(result.inserted, 1);
            assert.equal(result.failures, 1);
        } finally {
            console.error = originalError;
        }
    });

    it('individual failure does not abort remaining items',
    async () => {
        const { indexer } = createIndexer();
        const invalid = buildTestInput({
            eventId: 'evt-abort-bad',
            offset: 1,
            metadata: {
                short_description: 'not allowed',
            },
        });
        const valid = buildTestInput({
            eventId: 'evt-abort-ok',
            offset: 2,
        });
        const originalError = console.error;

        console.error = () => {};

        try {
            const result = await indexer.processBatch([
                invalid, valid,
            ]);
            assert.equal(result.failures, 1);
            assert.equal(result.inserted, 1);
        } finally {
            console.error = originalError;
        }
    });

    it('logs failure context for errored items', async () => {
        const { indexer } = createIndexer();
        const invalid = buildTestInput({
            eventId: 'evt-log-bad',
            offset: 1,
            metadata: {
                short_description: 'not allowed',
            },
        });
        const logged: unknown[] = [];
        const originalError = console.error;

        console.error = (...args: unknown[]) => {
            logged.push(args);
        };

        try {
            await indexer.processBatch([invalid]);
            assert.equal(logged.length, 1);
            const [msg, ctx] = logged[0] as [string, Record<string, unknown>];
            assert.equal(
                msg,
                'restore-indexer artifact batch item failed',
            );
            assert.equal(ctx.event_id, 'evt-log-bad');
            assert.equal(ctx.error_name, 'ZodError');
        } finally {
            console.error = originalError;
        }
    });
});

function buildProgressInput(
    overrides?: Partial<SourceProgressUpdateInput>,
): SourceProgressUpdateInput {
    return {
        cursor: 'cursor-1',
        instanceId: 'sn-dev-01',
        lastBatchSize: 10,
        lastIndexedEventTime: '2026-02-16T12:00:00.000Z',
        lastIndexedOffset: '42',
        lastLagSeconds: 5,
        measuredAt: '2026-02-16T12:01:00.000Z',
        processedDelta: 10,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        ...overrides,
    };
}

describe('RestoreIndexerService.refreshSourceCoverage (extended)',
() => {
    it('canonicalizes measuredAt timestamp', async () => {
        const { indexer } = createIndexer();
        await indexer.indexArtifact(
            buildTestInput({
                eventId: 'evt-canon-1',
                eventTime: '2026-02-16T10:00:00.000Z',
                offset: 1,
                partition: 0,
            }),
        );
        const coverage = await indexer.refreshSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://acme-dev.service-now.com',
            '2026-02-16T12:00:00Z',
        );
        assert.notEqual(coverage, null);
        assert.equal(
            coverage?.measured_at,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('returns null when no watermarks exist for source',
    async () => {
        const { indexer } = createIndexer();
        const coverage = await indexer.refreshSourceCoverage(
            'tenant-acme',
            'sn-dev-01',
            'sn://nonexistent.service-now.com',
            '2026-02-16T12:00:00.000Z',
        );
        assert.equal(coverage, null);
    });
});

describe('RestoreIndexerService.recordSourceProgress', () => {
    it('records progress for new source', async () => {
        const { indexer } = createIndexer();
        const result = await indexer.recordSourceProgress(
            buildProgressInput(),
        );
        assert.equal(result.instance_id, 'sn-dev-01');
        assert.equal(
            result.source,
            'sn://acme-dev.service-now.com',
        );
        assert.equal(result.tenant_id, 'tenant-acme');
        assert.equal(result.processed_count, 10);
        assert.equal(result.cursor, 'cursor-1');
        assert.equal(result.last_batch_size, 10);
    });

    it('accumulates processedCount across updates', async () => {
        const { indexer } = createIndexer();
        await indexer.recordSourceProgress(
            buildProgressInput({ processedDelta: 10 }),
        );
        const result = await indexer.recordSourceProgress(
            buildProgressInput({
                measuredAt: '2026-02-16T12:02:00.000Z',
                processedDelta: 5,
            }),
        );
        assert.equal(result.processed_count, 15);
    });

    it('uses maxNullableIso for latestEventTime', async () => {
        const { indexer } = createIndexer();
        await indexer.recordSourceProgress(
            buildProgressInput({
                lastIndexedEventTime:
                    '2026-02-16T12:10:00.000Z',
            }),
        );
        const result = await indexer.recordSourceProgress(
            buildProgressInput({
                lastIndexedEventTime:
                    '2026-02-16T12:05:00.000Z',
                measuredAt: '2026-02-16T12:02:00.000Z',
            }),
        );
        assert.equal(
            result.last_indexed_event_time,
            '2026-02-16T12:10:00.000Z',
        );
    });

    it('uses maxNullableOffset for latestOffset', async () => {
        const { indexer } = createIndexer();
        await indexer.recordSourceProgress(
            buildProgressInput({ lastIndexedOffset: '100' }),
        );
        const result = await indexer.recordSourceProgress(
            buildProgressInput({
                lastIndexedOffset: '50',
                measuredAt: '2026-02-16T12:02:00.000Z',
            }),
        );
        assert.equal(result.last_indexed_offset, '100');
    });

    it('persists lag_seconds', async () => {
        const { indexer } = createIndexer();
        const result = await indexer.recordSourceProgress(
            buildProgressInput({ lastLagSeconds: 42 }),
        );
        assert.equal(result.last_lag_seconds, 42);
    });

    it('handles null latestEventTime and latestOffset',
    async () => {
        const { indexer } = createIndexer();
        const result = await indexer.recordSourceProgress(
            buildProgressInput({
                lastIndexedEventTime: null,
                lastIndexedOffset: null,
            }),
        );
        assert.equal(result.last_indexed_event_time, null);
        assert.equal(result.last_indexed_offset, null);
    });

    it('merges with existing progress', async () => {
        const { indexer } = createIndexer();
        await indexer.recordSourceProgress(
            buildProgressInput({
                cursor: 'cursor-1',
                lastBatchSize: 10,
            }),
        );
        const result = await indexer.recordSourceProgress(
            buildProgressInput({
                cursor: 'cursor-2',
                lastBatchSize: 20,
                measuredAt: '2026-02-16T12:02:00.000Z',
            }),
        );
        assert.equal(result.cursor, 'cursor-2');
        assert.equal(result.last_batch_size, 20);
    });
});

describe('RestoreIndexerService.getSourceCoverageWindow', () => {
    it('returns null when no coverage exists for source',
    async () => {
        const { indexer } = createIndexer();
        const result = await indexer.getSourceCoverageWindow(
            'tenant-acme',
            'sn-dev-01',
            'sn://no-such-source.com',
        );
        assert.equal(result, null);
    });
});
