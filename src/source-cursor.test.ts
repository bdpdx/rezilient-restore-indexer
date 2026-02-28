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
    it('creates deterministic v2 payload from defaults', () => {
        const state = createSourceCursorState(null, DEFAULT_REPLAY);

        assert.deepEqual(state, {
            replay: {
                enabled: true,
                last_replay_at: null,
                lower_bound: null,
            },
            scan_cursor: null,
            v: SOURCE_CURSOR_VERSION,
        });
        assert.equal(
            serializeSourceCursorState(state),
            '{"replay":{"enabled":true,"last_replay_at":null,'
                + '"lower_bound":null},"scan_cursor":null,"v":2}',
        );
    });

    it('accepts legacy plain-string cursor and upgrades to v2', () => {
        const legacyCursor =
            'rez/restore-artifacts/tenant/x/table/y/late.manifest.json';
        const replayDefaults: SourceCursorReplayDefaults = {
            enabled: false,
            lowerBound: 'rez/restore-artifacts/tenant',
        };
        const parsed = parseSourceCursorState(
            legacyCursor,
            replayDefaults,
        );

        assert.deepEqual(parsed, {
            replay: {
                enabled: false,
                last_replay_at: null,
                lower_bound: 'rez/restore-artifacts/tenant',
            },
            scan_cursor: legacyCursor,
            v: SOURCE_CURSOR_VERSION,
        });
        assert.equal(
            serializeSourceCursorState(parsed),
            '{"replay":{"enabled":false,"last_replay_at":null,'
                + '"lower_bound":"rez/restore-artifacts/tenant"},'
                + '"scan_cursor":"rez/restore-artifacts/tenant/x/table/y/'
                + 'late.manifest.json","v":2}',
        );
    });

    it('parses existing v2 payload and preserves replay state', () => {
        const v2 = '{"v":2,"scan_cursor":"scan-key","replay":{"enabled":true,'
            + '"lower_bound":"bound","last_replay_at":'
            + '"2026-02-28T12:00:00.000Z"}}';
        const parsed = parseSourceCursorState(v2, DEFAULT_REPLAY);

        assert.deepEqual(parsed, {
            replay: {
                enabled: true,
                last_replay_at: '2026-02-28T12:00:00.000Z',
                lower_bound: 'bound',
            },
            scan_cursor: 'scan-key',
            v: SOURCE_CURSOR_VERSION,
        });
    });

    it('rejects malformed v2 payloads', () => {
        const malformed = '{"v":2,"scan_cursor":"scan-key","replay":';
        assert.throws(
            () => parseSourceCursorState(malformed, DEFAULT_REPLAY),
            /invalid source cursor state/,
        );
    });

    it('rejects unsupported v2 versions', () => {
        const wrongVersion = '{"v":1,"scan_cursor":"scan-key","replay":'
            + '{"enabled":true,"lower_bound":null,'
            + '"last_replay_at":null}}';
        assert.throws(
            () => parseSourceCursorState(wrongVersion, DEFAULT_REPLAY),
            /version must be 2/,
        );
    });
});
