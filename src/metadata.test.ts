import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { normalizeOperationalMetadata } from './metadata.js';
import { buildTestInput, buildTestManifest } from './test-helpers.js';
import type { IndexArtifactInput } from './types.js';

describe('normalizeOperationalMetadata', () => {
    it('returns normalized metadata for valid input with all fields',
    () => {
        const input = buildTestInput();
        const result = normalizeOperationalMetadata(input);
        assert.ok(result.metadata);
        assert.ok(result.eventTime);
        assert.ok(result.offset);
        assert.ok(result.partitionScope);
    });

    it('accepts matching canonical identity from metadata and manifest', () => {
        const input = buildTestInput({
            tenantId: 'tenant-canonical',
            instanceId: 'instance-canonical',
            source: 'sn://canonical.service-now.com',
            metadata: {
                tenant_id: 'tenant-canonical',
                instance_id: 'instance-canonical',
                source: 'sn://canonical.service-now.com',
            },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.tenant_id, 'tenant-canonical');
        assert.equal(result.metadata.instance_id, 'instance-canonical');
        assert.equal(
            result.metadata.source,
            'sn://canonical.service-now.com',
        );
    });

    it('falls back to manifest.event_time when __time missing',
    () => {
        const input = buildTestInput({
            eventTime: '2026-02-16T12:00:00.000Z',
            metadata: { __time: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.eventTime,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('falls back to manifest.tenant_id when metadata.tenant_id missing',
    () => {
        const input = buildTestInput({
            tenantId: 'tenant-env-fallback-disabled',
            metadata: { tenant_id: undefined },
        });
        input.manifest.tenant_id = 'tenant-manifest-fallback';
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.metadata.tenant_id,
            'tenant-manifest-fallback',
        );
    });

    it('fails closed when tenant_id is missing from metadata and manifest',
    () => {
        const input = buildTestInput({
            tenantId: 'tenant-env-fallback-disabled',
            metadata: { tenant_id: undefined },
        });
        input.manifest.tenant_id = undefined;
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Canonical identity missing required field: tenant_id/,
        );
    });

    it('fails closed when canonical tenant_id mismatches manifest',
    () => {
        const input = buildTestInput({
            tenantId: 'tenant-manifest',
            metadata: {
                tenant_id: 'tenant-mismatch',
            },
        });
        input.manifest.tenant_id = 'tenant-manifest';
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Canonical identity mismatch for tenant_id/,
        );
    });

    it('fails closed when canonical instance_id mismatches manifest',
    () => {
        const input = buildTestInput({
            instanceId: 'instance-manifest',
            metadata: {
                instance_id: 'instance-mismatch',
            },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Canonical identity mismatch for instance_id/,
        );
    });

    it('fails closed when canonical source mismatches manifest',
    () => {
        const input = buildTestInput({
            source: 'sn://manifest.service-now.com',
            metadata: {
                source: 'sn://metadata.service-now.com',
            },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Canonical identity mismatch for source/,
        );
    });

    it('falls back to manifest.source when metadata.source missing',
    () => {
        const input = buildTestInput({
            source: 'sn://manifest-source.service-now.com',
            metadata: { source: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.metadata.source,
            'sn://manifest-source.service-now.com',
        );
    });

    it('falls back to manifest.instance_id when metadata missing',
    () => {
        const input = buildTestInput({
            instanceId: 'manifest-inst-01',
            metadata: { instance_id: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.metadata.instance_id,
            'manifest-inst-01',
        );
    });

    it('falls back to manifest.topic when metadata.topic missing',
    () => {
        const input = buildTestInput({
            topic: 'rez.schema',
            metadata: { topic: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.topic, 'rez.schema');
    });

    it('falls back to manifest.partition when metadata missing',
    () => {
        const input = buildTestInput({
            partition: 3,
            metadata: { partition: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.partition, 3);
    });

    it('falls back to manifest.offset when metadata.offset missing',
    () => {
        const input = buildTestInput({
            offset: 42,
            metadata: { offset: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.offset, '42');
    });

    it('canonicalizes manifest offset to decimal string', () => {
        const input = buildTestInput({
            offset: 100,
            metadata: { offset: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.offset, '100');
    });

    it('throws when manifest offset is non-numeric', () => {
        const manifest = buildTestManifest({ offset: 1 });
        manifest.offset = 'abc';
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: undefined,
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Invalid manifest offset value/,
        );
    });

    it('parses runtime offset from metadata', () => {
        const input = buildTestInput({ offset: '50' });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.offset, '50');
    });

    it('throws when runtime offset is invalid', () => {
        const input = buildTestInput({
            offset: '12',
            metadata: { offset: '12.34' },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /must be non-negative integer offset as decimal string/,
        );
    });

    it('derives partition scope with all fields', () => {
        const input = buildTestInput({
            instanceId: 'sn-dev-01',
            partition: 2,
            source: 'sn://acme.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
        const result = normalizeOperationalMetadata(input);
        assert.deepEqual(result.partitionScope, {
            instanceId: 'sn-dev-01',
            partition: 2,
            source: 'sn://acme.service-now.com',
            tenantId: 'tenant-acme',
            topic: 'rez.cdc',
        });
    });

    it('normalizes partition as integer', () => {
        const input = buildTestInput({
            metadata: { partition: '3' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.partition, 3);
        assert.equal(typeof result.metadata.partition, 'number');
    });

    it('normalizes schema_version as integer', () => {
        const input = buildTestInput({
            metadata: { schema_version: '5' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.schema_version, 5);
    });

    it('normalizes size_bytes as integer', () => {
        const input = buildTestInput({
            metadata: { size_bytes: '1024' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.size_bytes, 1024);
    });

    it('normalizes sys_mod_count as integer', () => {
        const input = buildTestInput({
            metadata: { sys_mod_count: '7' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.sys_mod_count, 7);
    });

    it('handles null/undefined integer fields gracefully', () => {
        const input = buildTestInput({
            metadata: {
                schema_version: undefined,
                size_bytes: undefined,
                sys_mod_count: undefined,
            },
        });
        const result = normalizeOperationalMetadata(input);
        assert.ok(result.metadata);
    });

    it('normalizes optional string fields', () => {
        const input = buildTestInput({
            metadata: {
                record_sys_id: 'abc123',
                table: 'x_app.ticket',
                event_type: 'cdc.write',
            },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.record_sys_id, 'abc123');
        assert.equal(result.metadata.table, 'x_app.ticket');
        assert.equal(result.metadata.event_type, 'cdc.write');
    });

    it('passes through operation field', () => {
        const input = buildTestInput({
            metadata: { operation: 'I' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.operation, 'I');
    });

    it('throws when topic is missing from all sources', () => {
        const manifest = buildTestManifest();
        manifest.topic = undefined;
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: '1',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /missing topic/,
        );
    });

    it('throws when offset is missing from all sources', () => {
        const manifest = buildTestManifest();
        manifest.offset = undefined;
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /missing offset/,
        );
    });

    it('throws when instance_id is empty string', () => {
        const manifest = buildTestManifest();
        manifest.instance_id = '';
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: undefined,
                offset: '1',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /instance_id/,
        );
    });

    it('throws when source is empty string', () => {
        const manifest = buildTestManifest();
        manifest.source = '';
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: '1',
                partition: 0,
                source: undefined,
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /source/,
        );
    });

    it('throws when tenant_id is empty string', () => {
        const manifest = buildTestManifest();
        manifest.tenant_id = undefined;
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: '1',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: undefined,
                topic: 'rez.cdc',
            },
            tenantId: '',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /tenant_id/,
        );
    });

    it('canonicalizes event_time to ISO millis precision', () => {
        const input = buildTestInput({
            eventTime: '2026-02-16T12:00:00.000Z',
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.eventTime,
            '2026-02-16T12:00:00.000Z',
        );
    });

    it('handles second-precision timestamps', () => {
        const input = buildTestInput({
            eventTime: '2026-02-16T12:07:09Z',
            metadata: { __time: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(
            result.eventTime,
            '2026-02-16T12:07:09.000Z',
        );
    });

    it('throws on invalid ISO timestamp in manifest.event_time',
    () => {
        const input = buildTestInput({
            eventTime: '2026-02-16T12:07:09+00:00',
            metadata: { __time: undefined },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Invalid manifest\.event_time value/,
        );
    });

    it('throws on invalid ISO timestamp in metadata.__time', () => {
        const input = buildTestInput({
            metadata: { __time: '2026-02-16T12:07:09+00:00' },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Invalid metadata\.__time value/,
        );
    });

    it('handles minimal valid input (only required fields)', () => {
        const input = buildTestInput();
        const result = normalizeOperationalMetadata(input);
        assert.ok(result.eventTime);
        assert.ok(result.offset);
        assert.ok(result.metadata.topic);
        assert.ok(result.metadata.instance_id);
        assert.ok(result.metadata.source);
        assert.ok(result.metadata.tenant_id);
    });

    it('rejects unrecognized metadata keys', () => {
        const input = buildTestInput({
            metadata: { short_description: 'not allowed' },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
        );
    });
});

describe('toInteger (tested indirectly)', () => {
    it('converts numeric string to integer', () => {
        const input = buildTestInput({
            metadata: { schema_version: '5' },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.schema_version, 5);
        assert.equal(typeof result.metadata.schema_version, 'number');
    });

    it('converts number to integer', () => {
        const input = buildTestInput({
            metadata: { schema_version: 3 },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.schema_version, 3);
    });

    it('returns null for undefined', () => {
        const input = buildTestInput({
            metadata: { schema_version: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.metadata.schema_version, undefined);
    });

    it('non-numeric string fails Zod validation', () => {
        const input = buildTestInput({
            metadata: { sys_mod_count: 'abc' },
        });
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /sys_mod_count/,
        );
    });
});

describe('normalizeManifestOffset (tested indirectly)', () => {
    it('canonicalizes valid decimal offset', () => {
        const input = buildTestInput({
            offset: 100,
            metadata: { offset: undefined },
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.offset, '100');
    });

    it('returns undefined for undefined input', () => {
        const manifest = buildTestManifest();
        manifest.offset = undefined;
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: '1',
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.offset, '1');
    });

    it('throws on non-decimal string offset', () => {
        const manifest = buildTestManifest();
        manifest.offset = 'not-a-number';
        const input: IndexArtifactInput = {
            generationId: 'gen-01',
            ingestionMode: 'realtime',
            manifest,
            metadata: {
                __time: '2026-02-16T12:00:00.000Z',
                event_id: 'evt-0001',
                event_type: 'cdc.write',
                instance_id: 'sn-dev-01',
                offset: undefined,
                partition: 0,
                source: 'sn://acme-dev.service-now.com',
                tenant_id: 'tenant-acme',
                topic: 'rez.cdc',
            },
            tenantId: 'tenant-acme',
        };
        assert.throws(
            () => normalizeOperationalMetadata(input),
            /Invalid manifest offset value/,
        );
    });
});

describe('ensureTopicPartitionOffset (tested indirectly)', () => {
    it('extracts topic, partition, offset from valid metadata',
    () => {
        const input = buildTestInput({
            offset: 42,
            partition: 3,
            topic: 'rez.cdc',
        });
        const result = normalizeOperationalMetadata(input);
        assert.equal(result.partitionScope.topic, 'rez.cdc');
        assert.equal(result.partitionScope.partition, 3);
        assert.equal(result.offset, '42');
    });
});
