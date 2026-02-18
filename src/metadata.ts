import {
    canonicalizeIsoDateTimeWithMillis,
    isoDateTimeWithMillis,
    RESTORE_METADATA_ALLOWLIST_VERSION,
    RrsMetadataEnvelope,
    type RrsOperationalMetadata,
} from '@rezilient/types';
import type {
    IndexArtifactInput,
    PartitionScope,
} from './types';

export type NormalizedMetadata = {
    eventTime: string;
    metadata: RrsOperationalMetadata;
    partitionScope: PartitionScope;
};

const INTEGER_FIELDS = [
    'offset',
    'partition',
    'schema_version',
    'size_bytes',
    'sys_mod_count',
] as const;

function toInteger(
    value: unknown,
): number | null {
    if (typeof value === 'number' && Number.isInteger(value)) {
        return value;
    }

    if (typeof value !== 'string') {
        return null;
    }

    const trimmed = value.trim();

    if (!/^\d+$/.test(trimmed)) {
        return null;
    }

    return Number.parseInt(trimmed, 10);
}

function normalizeManifestOffset(
    rawOffset: string | undefined,
): number | undefined {
    if (rawOffset === undefined) {
        return undefined;
    }

    const offset = toInteger(rawOffset);

    if (offset === null) {
        throw new Error(`Invalid manifest offset value: ${rawOffset}`);
    }

    return offset;
}

function ensureTopicPartitionOffset(
    metadata: RrsOperationalMetadata,
): {
    offset: number;
    partition: number;
    topic: string;
} {
    if (!metadata.topic) {
        throw new Error('Operational metadata is missing topic');
    }

    if (metadata.partition === undefined) {
        throw new Error('Operational metadata is missing partition');
    }

    if (metadata.offset === undefined) {
        throw new Error('Operational metadata is missing offset');
    }

    return {
        offset: metadata.offset,
        partition: metadata.partition,
        topic: metadata.topic,
    };
}

function canonicalizeTimestampField(
    fieldPath: 'manifest.event_time' | 'metadata.__time',
    value: unknown,
): string {
    if (typeof value !== 'string') {
        throw new Error(`Invalid ${fieldPath} value type: expected string`);
    }

    try {
        return canonicalizeIsoDateTimeWithMillis(value);
    } catch {
        throw new Error(`Invalid ${fieldPath} value: ${value}`);
    }
}

export function normalizeOperationalMetadata(
    input: IndexArtifactInput,
): NormalizedMetadata {
    const merged: Record<string, unknown> = {
        ...input.metadata,
    };

    if (merged.tenant_id === undefined) {
        merged.tenant_id = input.tenantId;
    }

    if (merged.instance_id === undefined) {
        merged.instance_id = input.manifest.instance_id;
    }

    if (merged.source === undefined) {
        merged.source = input.manifest.source;
    }

    if (merged.table === undefined && input.manifest.table !== null) {
        merged.table = input.manifest.table;
    }

    if (merged.event_id === undefined) {
        merged.event_id = input.manifest.event_id;
    }

    if (merged.event_type === undefined) {
        merged.event_type = input.manifest.event_type;
    }

    if (merged.topic === undefined && input.manifest.topic !== undefined) {
        merged.topic = input.manifest.topic;
    }

    if (
        merged.partition === undefined
        && input.manifest.partition !== undefined
    ) {
        merged.partition = input.manifest.partition;
    }

    if (merged.offset === undefined) {
        const manifestOffset = normalizeManifestOffset(input.manifest.offset);

        if (manifestOffset !== undefined) {
            merged.offset = manifestOffset;
        }
    }

    if (merged.__time === undefined) {
        merged.__time = canonicalizeTimestampField(
            'manifest.event_time',
            input.manifest.event_time,
        );
    } else {
        merged.__time = canonicalizeTimestampField(
            'metadata.__time',
            merged.__time,
        );
    }

    for (const key of INTEGER_FIELDS) {
        const value = merged[key];
        const integer = toInteger(value);

        if (integer !== null) {
            merged[key] = integer;
        }
    }

    const parsed = RrsMetadataEnvelope.parse({
        allowlist_version: RESTORE_METADATA_ALLOWLIST_VERSION,
        metadata: merged,
    });
    const metadata = parsed.metadata;
    const { partition, topic } = ensureTopicPartitionOffset(metadata);

    if (!metadata.instance_id) {
        throw new Error('Operational metadata is missing instance_id');
    }

    if (!metadata.source) {
        throw new Error('Operational metadata is missing source');
    }

    if (!metadata.tenant_id) {
        throw new Error('Operational metadata is missing tenant_id');
    }

    if (!metadata.__time) {
        throw new Error('Operational metadata is missing __time');
    }

    const eventTime = canonicalizeIsoDateTimeWithMillis(
        isoDateTimeWithMillis.parse(metadata.__time),
    );
    metadata.__time = eventTime;

    return {
        eventTime,
        metadata,
        partitionScope: {
            instanceId: metadata.instance_id,
            partition,
            source: metadata.source,
            tenantId: metadata.tenant_id,
            topic,
        },
    };
}
