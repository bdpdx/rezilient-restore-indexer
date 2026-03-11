import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { test } from 'node:test';
import { Pool } from 'pg';
import { normalizeOperationalMetadata } from './metadata';
import { PostgresRestoreIndexStore } from './store';
import { buildTestInput } from './test-helpers';
import type { IndexedEventRecord } from './types';

const PG_URL = process.env.RRI_TEST_PG_URL || '';

function buildSchemaName(): string {
    return `rez_restore_index_it_${randomUUID().replace(/-/gu, '_')}`;
}

function buildIndexedEventRecord(): IndexedEventRecord {
    const input = buildTestInput({
        eventId: 'evt-real-pg-dedup',
        generationId: 'gen-real-pg-dedup',
        offset: 1,
    });
    const normalized = normalizeOperationalMetadata(input);

    return {
        artifactKey: input.manifest.artifact_key,
        artifactKind: input.manifest.artifact_kind,
        app: input.manifest.app,
        generationId: input.generationId,
        indexedAt: input.indexedAt || '2026-02-16T12:05:00.000Z',
        ingestionMode: input.ingestionMode,
        instanceId: normalized.partitionScope.instanceId,
        manifestVersion: input.manifest.manifest_version,
        metadata: normalized.metadata,
        objectKeyLayoutVersion: input.manifest.object_key_layout_version,
        partitionScope: normalized.partitionScope,
        table: input.manifest.table,
        tenantId: normalized.partitionScope.tenantId,
    };
}

async function countIndexedEvents(
    pgUrl: string,
    schemaName: string,
    eventId: string,
): Promise<number> {
    const pool = new Pool({
        allowExitOnIdle: true,
        connectionString: pgUrl,
        max: 1,
    });

    try {
        const result = await pool.query<{ count: string }>(
            `SELECT count(*)::text AS count
            FROM "${schemaName}"."index_events"
            WHERE event_id = $1`,
            [eventId],
        );

        return Number(result.rows[0]?.count || '0');
    } finally {
        await pool.end();
    }
}

async function dropSchema(
    pgUrl: string,
    schemaName: string,
): Promise<void> {
    const pool = new Pool({
        allowExitOnIdle: true,
        connectionString: pgUrl,
        max: 1,
    });

    try {
        await pool.query(`DROP SCHEMA IF EXISTS "${schemaName}" CASCADE`);
    } finally {
        await pool.end();
    }
}

test(
    'PostgresRestoreIndexStore deduplicates duplicate events on real Postgres',
    {
        skip: !PG_URL && 'RRI_TEST_PG_URL is required',
    },
    async () => {
        const schemaName = buildSchemaName();
        const record = buildIndexedEventRecord();
        const eventId = record.metadata.event_id || record.artifactKey;
        let firstStore: PostgresRestoreIndexStore | null = null;
        let secondStore: PostgresRestoreIndexStore | null = null;

        try {
            firstStore = new PostgresRestoreIndexStore(PG_URL, {
                schemaName,
            });
            const first = await firstStore.upsertIndexedEvent(record);

            assert.equal(first, 'inserted');

            await firstStore.close();
            firstStore = null;

            secondStore = new PostgresRestoreIndexStore(PG_URL, {
                schemaName,
            });
            const duplicate = await secondStore.upsertIndexedEvent(record);

            assert.equal(duplicate, 'existing');
            assert.equal(
                await countIndexedEvents(PG_URL, schemaName, eventId),
                1,
            );
        } finally {
            if (firstStore) {
                await firstStore.close();
            }

            if (secondStore) {
                await secondStore.close();
            }

            if (PG_URL) {
                await dropSchema(PG_URL, schemaName);
            }
        }
    },
);
