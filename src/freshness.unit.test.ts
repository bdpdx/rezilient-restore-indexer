import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    buildRestoreWatermark,
    evaluateFreshnessState,
} from './freshness.js';
import type { PartitionWatermarkState } from './types.js';

function buildWatermark(
    overrides: Partial<PartitionWatermarkState> = {},
): PartitionWatermarkState {
    return {
        coverageEnd: '2026-02-16T12:00:00.000Z',
        coverageStart: '2026-02-16T11:00:00.000Z',
        generationId: 'gen-01',
        indexedThroughOffset: '100',
        indexedThroughTime: '2026-02-16T12:00:00.000Z',
        instanceId: 'sn-dev-01',
        measuredAt: '2026-02-16T12:01:00.000Z',
        partition: 0,
        source: 'sn://acme-dev.service-now.com',
        tenantId: 'tenant-acme',
        topic: 'rez.cdc',
        ...overrides,
    };
}

const defaultPolicy = {
    staleAfterSeconds: 120,
    timeoutSeconds: 60,
};

describe('evaluateFreshnessState', () => {
    it('returns blocked/unknown when watermark is null', () => {
        const result = evaluateFreshnessState(
            null,
            defaultPolicy,
            { now: '2026-02-16T12:05:00.000Z' },
        );
        assert.equal(result.executability, 'blocked');
        assert.equal(result.freshness, 'unknown');
        assert.equal(result.gateTimedOut, false);
        assert.equal(result.lagSeconds, null);
        assert.equal(
            result.reasonCode,
            'blocked_freshness_unknown',
        );
    });

    it('returns executable/fresh when lag <= staleAfterSeconds',
    () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T12:00:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        assert.equal(result.executability, 'executable');
        assert.equal(result.freshness, 'fresh');
        assert.equal(result.lagSeconds, 60);
        assert.equal(result.reasonCode, 'none');
    });

    it('returns blocked/stale when lag > staleAfterSeconds, no wait',
    () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:57:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.executability, 'blocked');
        assert.equal(result.freshness, 'stale');
        assert.equal(result.lagSeconds, 180);
        assert.equal(result.gateTimedOut, false);
        assert.equal(
            result.reasonCode,
            'blocked_freshness_stale',
        );
    });

    it('returns preview_only/stale when timeout elapsed', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:57:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            {
                now: '2026-02-16T12:02:00.000Z',
                waitStartedAt: '2026-02-16T12:00:00.000Z',
            },
        );
        assert.equal(result.executability, 'preview_only');
        assert.equal(result.freshness, 'stale');
        assert.equal(result.gateTimedOut, true);
    });

    it('returns blocked/stale when timeout not yet elapsed', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:57:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            {
                now: '2026-02-16T12:00:30.000Z',
                waitStartedAt: '2026-02-16T12:00:00.000Z',
            },
        );
        assert.equal(result.executability, 'blocked');
        assert.equal(result.freshness, 'stale');
        assert.equal(result.gateTimedOut, false);
    });

    it('boundary: lag exactly equal to staleAfterSeconds is fresh',
    () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:58:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.executability, 'executable');
        assert.equal(result.freshness, 'fresh');
        assert.equal(result.lagSeconds, 120);
    });

    it('boundary: lag one second over staleAfterSeconds is stale',
    () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:57:59.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.executability, 'blocked');
        assert.equal(result.freshness, 'stale');
        assert.equal(result.lagSeconds, 121);
    });

    it('handles zero staleAfterSeconds', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T12:00:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            { staleAfterSeconds: 0, timeoutSeconds: 60 },
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.executability, 'executable');
        assert.equal(result.freshness, 'fresh');
        assert.equal(result.lagSeconds, 0);
    });

    it('handles very large lag values', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-01-01T00:00:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.executability, 'blocked');
        assert.equal(result.freshness, 'stale');
        assert.ok(result.lagSeconds! > 86400);
    });

    it('clamps negative lag to zero (future event time)', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T12:05:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.lagSeconds, 0);
        assert.equal(result.executability, 'executable');
        assert.equal(result.freshness, 'fresh');
    });

    it('returns gateTimedOut=false when waitStartedAt is null',
    () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:50:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(result.gateTimedOut, false);
    });

    it('returns gateTimedOut=true when wait exceeds timeout', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:50:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            {
                now: '2026-02-16T12:02:00.000Z',
                waitStartedAt: '2026-02-16T12:01:00.000Z',
            },
        );
        assert.equal(result.gateTimedOut, true);
        assert.equal(result.executability, 'preview_only');
    });

    it('returns correct lagSeconds value', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:59:00.000Z',
        });
        const result = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:00:30.000Z' },
        );
        assert.equal(result.lagSeconds, 90);
    });

    it('returns correct reasonCode for each state', () => {
        const wm = buildWatermark({
            indexedThroughTime: '2026-02-16T12:00:00.000Z',
        });
        const fresh = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        assert.equal(fresh.reasonCode, 'none');

        const unknown = evaluateFreshnessState(
            null,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        assert.equal(
            unknown.reasonCode,
            'blocked_freshness_unknown',
        );

        const staleWm = buildWatermark({
            indexedThroughTime: '2026-02-16T11:50:00.000Z',
        });
        const stale = evaluateFreshnessState(
            staleWm,
            defaultPolicy,
            { now: '2026-02-16T12:00:00.000Z' },
        );
        assert.equal(stale.reasonCode, 'blocked_freshness_stale');
    });
});

describe('buildRestoreWatermark', () => {
    it('builds valid RestoreWatermark schema object', () => {
        const wm = buildWatermark();
        const evaluation = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        const result = buildRestoreWatermark(
            wm,
            evaluation,
            '2026-02-16T12:01:00.000Z',
        );
        assert.ok(result);
        assert.equal(result.tenant_id, 'tenant-acme');
        assert.equal(result.instance_id, 'sn-dev-01');
    });

    it('includes coverage_start and coverage_end', () => {
        const wm = buildWatermark();
        const evaluation = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        const result = buildRestoreWatermark(
            wm,
            evaluation,
            '2026-02-16T12:01:00.000Z',
        );
        assert.equal(
            result.coverage_start,
            '2026-02-16T11:00:00.000Z',
        );
        assert.equal(
            result.coverage_end,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('includes executability and freshness', () => {
        const wm = buildWatermark();
        const evaluation = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        const result = buildRestoreWatermark(
            wm,
            evaluation,
            '2026-02-16T12:01:00.000Z',
        );
        assert.equal(result.executability, 'executable');
        assert.equal(result.freshness, 'fresh');
    });

    it('sets correct contract version', () => {
        const wm = buildWatermark();
        const evaluation = evaluateFreshnessState(
            wm,
            defaultPolicy,
            { now: '2026-02-16T12:01:00.000Z' },
        );
        const result = buildRestoreWatermark(
            wm,
            evaluation,
            '2026-02-16T12:01:00.000Z',
        );
        assert.equal(
            result.contract_version,
            'restore.contracts.v1',
        );
    });
});
