import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    createSourceCursorState,
    parseSourceCursorState,
    serializeSourceCursorState,
    SOURCE_CURSOR_VERSION,
    type SourceCursorReplayDefaults,
} from './source-cursor.js';

const DEFAULT_REPLAY: SourceCursorReplayDefaults = {
    enabled: true,
    lowerBound: null,
};

describe('source-cursor', () => {
    it('creates deterministic v3 payload from defaults', () => {
        const state = createSourceCursorState(null, DEFAULT_REPLAY);

        assert.deepEqual(state, {
            legacy: {
                replay: {
                    enabled: true,
                    last_replay_at: null,
                    lower_bound: null,
                },
                scan_cursor: null,
            },
            replay: {
                enabled: true,
                last_replay_at: null,
                lower_bound: null,
            },
            scan_cursor: null,
            v2: {
                by_shard: {},
                last_reconcile_at: null,
            },
            v: SOURCE_CURSOR_VERSION,
        });
        assert.equal(
            serializeSourceCursorState(state),
            '{"legacy":{"replay":{"enabled":true,"last_replay_at":null,'
                + '"lower_bound":null},"scan_cursor":null},"v2":{"by_shard":{},'
                + '"last_reconcile_at":null},"v":3}',
        );
    });

    it('rejects legacy plain-string cursors', () => {
        const legacyCursor =
            'rez/restore-artifacts/tenant/x/table/y/late.manifest.json';

        assert.throws(
            () => parseSourceCursorState(legacyCursor, DEFAULT_REPLAY),
            /legacy plain-string cursors are not supported/,
        );
    });

    it('rejects existing v2 payloads', () => {
        const v2 = '{"v":2,"scan_cursor":"scan-key","replay":{"enabled":true,'
            + '"lower_bound":"bound","last_replay_at":'
            + '"2026-02-28T12:00:00.000Z"}}';

        assert.throws(
            () => parseSourceCursorState(v2, DEFAULT_REPLAY),
            /source cursor version must be 3/,
        );
    });

    it('parses existing v3 payload with v2 shard progress', () => {
        const v3 = '{"v":3,"legacy":{"scan_cursor":"scan-key","replay":'
            + '{"enabled":true,"lower_bound":"bound","last_replay_at":'
            + '"2026-02-28T12:00:00.000Z"}},"v2":{"by_shard":{"rez.cdc|0":'
            + '{"next_offset":"00000000000000000123","last_key":"k-123",'
            + '"last_event_time":"2026-02-28T12:00:20.000Z"}},'
            + '"last_reconcile_at":"2026-02-28T12:02:00.000Z"}}';
        const parsed = parseSourceCursorState(v3, DEFAULT_REPLAY);

        assert.deepEqual(parsed, {
            legacy: {
                replay: {
                    enabled: true,
                    last_replay_at: '2026-02-28T12:00:00.000Z',
                    lower_bound: 'bound',
                },
                scan_cursor: 'scan-key',
            },
            replay: {
                enabled: true,
                last_replay_at: '2026-02-28T12:00:00.000Z',
                lower_bound: 'bound',
            },
            scan_cursor: 'scan-key',
            v2: {
                by_shard: {
                    'rez.cdc|0': {
                        last_event_time: '2026-02-28T12:00:20.000Z',
                        last_key: 'k-123',
                        next_offset: '00000000000000000123',
                    },
                },
                last_reconcile_at: '2026-02-28T12:02:00.000Z',
            },
            v: SOURCE_CURSOR_VERSION,
        });
    });

    it('rejects malformed JSON cursor payloads', () => {
        const malformed = '{"v":2,"scan_cursor":"scan-key","replay":';
        assert.throws(
            () => parseSourceCursorState(malformed, DEFAULT_REPLAY),
            /invalid source cursor state/,
        );
    });

    it('rejects unsupported cursor versions', () => {
        const wrongVersion = '{"v":1,"scan_cursor":"scan-key","replay":'
            + '{"enabled":true,"lower_bound":null,'
            + '"last_replay_at":null}}';
        assert.throws(
            () => parseSourceCursorState(wrongVersion, DEFAULT_REPLAY),
            /version must be 3/,
        );
    });
});
