export const SOURCE_CURSOR_VERSION = 2 as const;

export type SourceCursorReplayDefaults = {
    enabled: boolean;
    lowerBound: string | null;
};

export type SourceCursorReplayState = {
    enabled: boolean;
    lower_bound: string | null;
    last_replay_at: string | null;
};

export type SourceCursorV2State = {
    replay: SourceCursorReplayState;
    scan_cursor: string | null;
    v: typeof SOURCE_CURSOR_VERSION;
};

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

function readVersion(value: unknown): typeof SOURCE_CURSOR_VERSION {
    if (value !== SOURCE_CURSOR_VERSION) {
        throw new Error(
            `source cursor version must be ${SOURCE_CURSOR_VERSION}`,
        );
    }

    return SOURCE_CURSOR_VERSION;
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

export function createSourceCursorState(
    scanCursor: string | null,
    replayDefaults: SourceCursorReplayDefaults,
): SourceCursorV2State {
    const normalizedDefaults = normalizeReplayDefaults(replayDefaults);

    return {
        replay: {
            enabled: normalizedDefaults.enabled,
            last_replay_at: null,
            lower_bound: normalizedDefaults.lowerBound,
        },
        scan_cursor: readNullableString(
            scanCursor,
            'source cursor state.scan_cursor',
        ),
        v: SOURCE_CURSOR_VERSION,
    };
}

function parseV2SourceCursorState(
    value: unknown,
): SourceCursorV2State {
    if (!isRecord(value)) {
        throw new Error('source cursor payload must be an object');
    }

    const replayValue = value.replay;

    if (!isRecord(replayValue)) {
        throw new Error('source cursor replay must be an object');
    }

    return {
        replay: {
            enabled: readBoolean(
                replayValue.enabled,
                'source cursor replay.enabled',
            ),
            last_replay_at: readNullableString(
                replayValue.last_replay_at,
                'source cursor replay.last_replay_at',
            ),
            lower_bound: readNullableString(
                replayValue.lower_bound,
                'source cursor replay.lower_bound',
            ),
        },
        scan_cursor: readNullableString(
            value.scan_cursor,
            'source cursor scan_cursor',
        ),
        v: readVersion(value.v),
    };
}

export function parseSourceCursorState(
    cursor: string | null,
    replayDefaults: SourceCursorReplayDefaults,
): SourceCursorV2State {
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
        return parseV2SourceCursorState(JSON.parse(cursor));
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
    state: SourceCursorV2State,
): string {
    const normalized = parseV2SourceCursorState(state);

    return JSON.stringify({
        replay: {
            enabled: normalized.replay.enabled,
            last_replay_at: normalized.replay.last_replay_at,
            lower_bound: normalized.replay.lower_bound,
        },
        scan_cursor: normalized.scan_cursor,
        v: SOURCE_CURSOR_VERSION,
    });
}
