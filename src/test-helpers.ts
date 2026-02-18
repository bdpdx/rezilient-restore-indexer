import { canonicalizeRestoreOffsetDecimalString } from '@rezilient/types';
import {
    RESTORE_CONTRACT_VERSION,
    RESTORE_METADATA_ALLOWLIST_VERSION,
} from './constants';
import type {
    ArtifactManifestInput,
    IndexArtifactInput,
} from './types';

export type InputOverrides = {
    eventId?: string;
    eventTime?: string;
    generationId?: string;
    indexedAt?: string;
    ingestionMode?: 'realtime' | 'bootstrap' | 'gap_repair';
    instanceId?: string;
    metadata?: Record<string, unknown>;
    offset?: number | string;
    partition?: number;
    source?: string;
    table?: string;
    tenantId?: string;
    topic?: string;
};

export function buildTestManifest(
    overrides: InputOverrides = {},
): ArtifactManifestInput {
    const eventId = overrides.eventId || 'evt-0001';
    const eventTime = overrides.eventTime || '2026-02-16T12:00:00.000Z';
    const topic = overrides.topic || 'rez.cdc';
    const partition = overrides.partition ?? 0;
    const offset = canonicalizeRestoreOffsetDecimalString(
        overrides.offset ?? 1,
    );
    const source = overrides.source || 'sn://acme-dev.service-now.com';
    const instanceId = overrides.instanceId || 'sn-dev-01';
    const table = overrides.table || 'x_app.ticket';

    return {
        artifact_key: `rez/restore/event=${eventId}.artifact.json`,
        artifact_kind: 'cdc',
        app: 'x_app',
        contract_version: RESTORE_CONTRACT_VERSION,
        event_id: eventId,
        event_time: eventTime,
        event_type: 'cdc.write',
        instance_id: instanceId,
        manifest_version: 'rec.artifact-manifest.v1',
        metadata_allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
        object_key_layout_version: 'rec.object-key-layout.v1',
        offset,
        partition,
        source,
        table,
        topic,
    };
}

export function buildTestInput(
    overrides: InputOverrides = {},
): IndexArtifactInput {
    const tenantId = overrides.tenantId || 'tenant-acme';
    const manifest = buildTestManifest(overrides);

    return {
        generationId: overrides.generationId || 'gen-01',
        indexedAt: overrides.indexedAt || '2026-02-16T12:05:00.000Z',
        ingestionMode: overrides.ingestionMode || 'realtime',
        manifest,
        metadata: {
            __time: manifest.event_time,
            event_id: manifest.event_id,
            event_type: manifest.event_type,
            instance_id: manifest.instance_id,
            offset: manifest.offset,
            operation: 'U',
            partition: manifest.partition,
            record_sys_id: 'abc123',
            schema_version: 1,
            source: manifest.source,
            sys_mod_count: 1,
            sys_updated_on: '2026-02-16 12:00:00',
            table: manifest.table,
            tenant_id: tenantId,
            topic: manifest.topic,
            ...overrides.metadata,
        },
        tenantId,
    };
}
