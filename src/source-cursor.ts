const LEGACY_SOURCE_CURSOR_VERSION = 2 as const;
export const SOURCE_CURSOR_VERSION = 3 as const;

export type SourceCursorReplayDefaults = {
    enabled: boolean;
    lowerBound: string | null;
};

export type SourceCursorReplayState = {
    enabled: boolean;
    lower_bound: string | null;
    last_replay_at: string | null;
};

export type SourceCursorLegacyState = {
    replay: SourceCursorReplayState;
    scan_cursor: string | null;
};

export type SourceCursorV2ShardState = {
    last_event_time: string | null;
    last_key: string | null;
    next_offset: string;
};

export type SourceCursorV2ProgressState = {
    by_shard: Record<string, SourceCursorV2ShardState>;
    last_reconcile_at: string | null;
};

export type SourceCursorV3State = {
    legacy: SourceCursorLegacyState;
    replay: SourceCursorReplayState;
    scan_cursor: string | null;
    v2: SourceCursorV2ProgressState;
    v: typeof SOURCE_CURSOR_VERSION;
};

// Compatibility alias retained while call sites migrate to explicit v3 naming.
export type SourceCursorV2State = SourceCursorV3State;

function isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value)
        && typeof value === 'object'
        && !Array.isArray(value);
}

function readBoolean(
    value: unknown,
    fieldPath: string,
): boolean {
    if (typeof value !== 'boolean') {
        throw new Error(`${fieldPath} must be a boolean`);
    }

    return value;
}

function readNullableString(
    value: unknown,
    fieldPath: string,
): string | null {
    if (value === null) {
        return null;
    }

    if (typeof value !== 'string') {
        throw new Error(`${fieldPath} must be a string or null`);
    }

    if (value.trim().length === 0) {
        throw new Error(`${fieldPath} must not be empty`);
    }

    return value;
}

function readRequiredString(
    value: unknown,
    fieldPath: string,
): string {
    const parsed = readNullableString(value, fieldPath);

    if (parsed === null) {
        throw new Error(`${fieldPath} must be a string`);
    }

    return parsed;
}

function readVersion(value: unknown): typeof SOURCE_CURSOR_VERSION {
    if (value !== SOURCE_CURSOR_VERSION) {
        throw new Error(
            `source cursor version must be ${SOURCE_CURSOR_VERSION}`,
        );
    }

    return SOURCE_CURSOR_VERSION;
}

function readLegacyVersion(
    value: unknown,
): typeof LEGACY_SOURCE_CURSOR_VERSION {
    if (value !== LEGACY_SOURCE_CURSOR_VERSION) {
        throw new Error(
            `source cursor version must be ${LEGACY_SOURCE_CURSOR_VERSION}`,
        );
    }

    return LEGACY_SOURCE_CURSOR_VERSION;
}

function normalizeReplayDefaults(
    defaults: SourceCursorReplayDefaults,
): SourceCursorReplayDefaults {
    return {
        enabled: readBoolean(
            defaults.enabled,
            'source cursor replay defaults.enabled',
        ),
        lowerBound: readNullableString(
            defaults.lowerBound,
            'source cursor replay defaults.lowerBound',
        ),
    };
}

function createEmptyV2ProgressState(): SourceCursorV2ProgressState {
    return {
        by_shard: {},
        last_reconcile_at: null,
    };
}

function cloneReplayState(
    replay: SourceCursorReplayState,
): SourceCursorReplayState {
    return {
        enabled: replay.enabled,
        last_replay_at: replay.last_replay_at,
        lower_bound: replay.lower_bound,
    };
}

function cloneLegacyState(
    legacy: SourceCursorLegacyState,
): SourceCursorLegacyState {
    return {
        replay: cloneReplayState(legacy.replay),
        scan_cursor: legacy.scan_cursor,
    };
}

function cloneV2ProgressState(
    progress: SourceCursorV2ProgressState,
): SourceCursorV2ProgressState {
    const byShard: Record<string, SourceCursorV2ShardState> = {};

    for (const shardKey of Object.keys(progress.by_shard).sort()) {
        const shard = progress.by_shard[shardKey];

        byShard[shardKey] = {
            last_event_time: shard.last_event_time,
            last_key: shard.last_key,
            next_offset: shard.next_offset,
        };
    }

    return {
        by_shard: byShard,
        last_reconcile_at: progress.last_reconcile_at,
    };
}

function buildSourceCursorV3State(input: {
    legacy: SourceCursorLegacyState;
    v2: SourceCursorV2ProgressState;
}): SourceCursorV3State {
    const legacy = cloneLegacyState(input.legacy);

    return {
        legacy,
        replay: cloneReplayState(legacy.replay),
        scan_cursor: legacy.scan_cursor,
        v2: cloneV2ProgressState(input.v2),
        v: SOURCE_CURSOR_VERSION,
    };
}

function parseLegacyState(
    value: unknown,
    fieldPath: string,
): SourceCursorLegacyState {
    if (!isRecord(value)) {
        throw new Error(`${fieldPath} must be an object`);
    }

    const replayValue = value.replay;

    if (!isRecord(replayValue)) {
        throw new Error(`${fieldPath}.replay must be an object`);
    }

    return {
        replay: {
            enabled: readBoolean(
                replayValue.enabled,
                `${fieldPath}.replay.enabled`,
            ),
            last_replay_at: readNullableString(
                replayValue.last_replay_at,
                `${fieldPath}.replay.last_replay_at`,
            ),
            lower_bound: readNullableString(
                replayValue.lower_bound,
                `${fieldPath}.replay.lower_bound`,
            ),
        },
        scan_cursor: readNullableString(
            value.scan_cursor,
            `${fieldPath}.scan_cursor`,
        ),
    };
}

function parseV2ShardState(
    value: unknown,
    fieldPath: string,
): SourceCursorV2ShardState {
    if (!isRecord(value)) {
        throw new Error(`${fieldPath} must be an object`);
    }

    return {
        last_event_time: readNullableString(
            value.last_event_time,
            `${fieldPath}.last_event_time`,
        ),
        last_key: readNullableString(
            value.last_key,
            `${fieldPath}.last_key`,
        ),
        next_offset: readRequiredString(
            value.next_offset,
            `${fieldPath}.next_offset`,
        ),
    };
}

function parseV2ProgressState(
    value: unknown,
    fieldPath: string,
): SourceCursorV2ProgressState {
    if (!isRecord(value)) {
        throw new Error(`${fieldPath} must be an object`);
    }

    if (!isRecord(value.by_shard)) {
        throw new Error(`${fieldPath}.by_shard must be an object`);
    }

    const byShard: Record<string, SourceCursorV2ShardState> = {};

    for (const [shardKey, shardValue] of Object.entries(value.by_shard)) {
        if (typeof shardKey !== 'string' || shardKey.trim().length === 0) {
            throw new Error(`${fieldPath}.by_shard keys must not be empty`);
        }

        byShard[shardKey] = parseV2ShardState(
            shardValue,
            `${fieldPath}.by_shard.${shardKey}`,
        );
    }

    return {
        by_shard: byShard,
        last_reconcile_at: readNullableString(
            value.last_reconcile_at,
            `${fieldPath}.last_reconcile_at`,
        ),
    };
}

export function createSourceCursorState(
    scanCursor: string | null,
    replayDefaults: SourceCursorReplayDefaults,
): SourceCursorV3State {
    const normalizedDefaults = normalizeReplayDefaults(replayDefaults);
    const legacy: SourceCursorLegacyState = {
        replay: {
            enabled: normalizedDefaults.enabled,
            last_replay_at: null,
            lower_bound: normalizedDefaults.lowerBound,
        },
        scan_cursor: readNullableString(
            scanCursor,
            'source cursor state.scan_cursor',
        ),
    };

    return buildSourceCursorV3State({
        legacy,
        v2: createEmptyV2ProgressState(),
    });
}

function parseLegacyJsonCursorState(
    value: unknown,
): SourceCursorLegacyState {
    if (!isRecord(value)) {
        throw new Error('source cursor payload must be an object');
    }

    readLegacyVersion(value.v);

    return parseLegacyState(value, 'source cursor');
}

function parseV3SourceCursorState(
    value: unknown,
): SourceCursorV3State {
    if (!isRecord(value)) {
        throw new Error('source cursor payload must be an object');
    }

    readVersion(value.v);

    const legacy = value.legacy === undefined
        ? parseLegacyState(value, 'source cursor')
        : parseLegacyState(value.legacy, 'source cursor legacy');
    const v2 = value.v2 === undefined
        ? createEmptyV2ProgressState()
        : parseV2ProgressState(value.v2, 'source cursor v2');

    return buildSourceCursorV3State({
        legacy,
        v2,
    });
}

export function parseSourceCursorState(
    cursor: string | null,
    replayDefaults: SourceCursorReplayDefaults,
): SourceCursorV3State {
    if (cursor === null || cursor.length === 0) {
        return createSourceCursorState(null, replayDefaults);
    }

    const trimmedStart = cursor.trimStart();

    if (!trimmedStart) {
        return createSourceCursorState(null, replayDefaults);
    }

    if (!trimmedStart.startsWith('{')) {
        return createSourceCursorState(cursor, replayDefaults);
    }

    try {
        const parsed = JSON.parse(cursor);

        if (!isRecord(parsed)) {
            throw new Error('source cursor payload must be an object');
        }

        if (parsed.v === LEGACY_SOURCE_CURSOR_VERSION) {
            const legacy = parseLegacyJsonCursorState(parsed);

            return buildSourceCursorV3State({
                legacy,
                v2: createEmptyV2ProgressState(),
            });
        }

        if (parsed.v === SOURCE_CURSOR_VERSION) {
            return parseV3SourceCursorState(parsed);
        }

        throw new Error(
            'source cursor version must be '
            + `${LEGACY_SOURCE_CURSOR_VERSION} or ${SOURCE_CURSOR_VERSION}`,
        );
    } catch (error) {
        if (error instanceof Error) {
            throw new Error(
                `invalid source cursor state: ${error.message}`,
            );
        }

        throw new Error('invalid source cursor state');
    }
}

export function serializeSourceCursorState(
    state: unknown,
): string {
    const normalized = parseV3SourceCursorState(state);

    return JSON.stringify({
        legacy: {
            replay: {
                enabled: normalized.legacy.replay.enabled,
                last_replay_at: normalized.legacy.replay.last_replay_at,
                lower_bound: normalized.legacy.replay.lower_bound,
            },
            scan_cursor: normalized.legacy.scan_cursor,
        },
        v2: {
            by_shard: cloneV2ProgressState(normalized.v2).by_shard,
            last_reconcile_at: normalized.v2.last_reconcile_at,
        },
        v: SOURCE_CURSOR_VERSION,
    });
}
