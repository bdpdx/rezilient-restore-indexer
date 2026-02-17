import {
    isoDateTimeWithMillis,
    RESTORE_CONTRACT_VERSION,
    RestoreWatermark as RestoreWatermarkSchema,
    type RestoreWatermark,
} from '@rezilient/types';
import type {
    FreshnessEvaluation,
    PartitionWatermarkState,
} from './types';

export type FreshnessGatePolicy = {
    staleAfterSeconds: number;
    timeoutSeconds: number;
};

export type FreshnessGateInput = {
    now: string;
    waitStartedAt?: string;
};

function elapsedSeconds(
    earlierIso: string,
    laterIso: string,
): number {
    const earlierMs = Date.parse(earlierIso);
    const laterMs = Date.parse(laterIso);

    if (!Number.isFinite(earlierMs) || !Number.isFinite(laterMs)) {
        throw new Error('Invalid ISO time supplied to freshness gate');
    }

    const deltaMs = Math.max(0, laterMs - earlierMs);

    return Math.floor(deltaMs / 1000);
}

export function evaluateFreshnessState(
    watermark: PartitionWatermarkState | null,
    policy: FreshnessGatePolicy,
    gate: FreshnessGateInput,
): FreshnessEvaluation {
    const nowIso = isoDateTimeWithMillis.parse(gate.now);

    if (watermark === null) {
        return {
            executability: 'blocked',
            freshness: 'unknown',
            gateTimedOut: false,
            lagSeconds: null,
            reasonCode: 'blocked_freshness_unknown',
        };
    }

    const lagSeconds = elapsedSeconds(
        watermark.indexedThroughTime,
        nowIso,
    );

    if (lagSeconds <= policy.staleAfterSeconds) {
        return {
            executability: 'executable',
            freshness: 'fresh',
            gateTimedOut: false,
            lagSeconds,
            reasonCode: 'none',
        };
    }

    const waitStartedAt = gate.waitStartedAt
        ? isoDateTimeWithMillis.parse(gate.waitStartedAt)
        : undefined;

    const timedOut = waitStartedAt !== undefined
        && elapsedSeconds(waitStartedAt, nowIso) >= policy.timeoutSeconds;

    return {
        executability: timedOut ? 'preview_only' : 'blocked',
        freshness: 'stale',
        gateTimedOut: timedOut,
        lagSeconds,
        reasonCode: 'blocked_freshness_stale',
    };
}

export function buildRestoreWatermark(
    watermark: PartitionWatermarkState,
    evaluation: FreshnessEvaluation,
    measuredAt: string,
): RestoreWatermark {
    return RestoreWatermarkSchema.parse({
        contract_version: RESTORE_CONTRACT_VERSION,
        coverage_end: watermark.coverageEnd,
        coverage_start: watermark.coverageStart,
        executability: evaluation.executability,
        freshness: evaluation.freshness,
        generation_id: watermark.generationId,
        indexed_through_offset: watermark.indexedThroughOffset,
        indexed_through_time: watermark.indexedThroughTime,
        instance_id: watermark.instanceId,
        measured_at: measuredAt,
        partition: watermark.partition,
        reason_code: evaluation.reasonCode,
        source: watermark.source,
        tenant_id: watermark.tenantId,
        topic: watermark.topic,
    });
}
