import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { parseIndexerEnv } from './env.js';

/**
 * Minimum valid env for in_memory_scaffold mode.
 * Avoids needing REC source vars.
 */
function baseEnv(): Record<string, string> {
    return {
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'in_memory_scaffold',
        REZ_RESTORE_INDEXER_DEFAULT_TENANT: 'test-tenant',
        REZ_RESTORE_PG_URL: 'postgresql://localhost:5432/test',
    };
}

/**
 * Valid env for rec_manifest_object_store mode.
 * Includes all required REC source vars.
 */
function recSourceEnv(): Record<string, string> {
    return {
        REZ_RESTORE_INDEXER_DEFAULT_TENANT: 'test-tenant',
        REZ_RESTORE_PG_URL: 'postgresql://localhost:5432/test',
        REZ_RESTORE_INDEXER_ARTIFACT_SOURCE:
            'rec_manifest_object_store',
        REZ_RESTORE_INDEXER_SOURCE_BUCKET: 'test-bucket',
        REZ_RESTORE_INDEXER_SOURCE_REGION: 'us-east-1',
        REZ_RESTORE_INDEXER_SOURCE_PREFIX: 'rez/restore/',
        REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID: 'gen-01',
        REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID: 'sn-dev-01',
        REZ_RESTORE_INDEXER_SOURCE_URI:
            'sn://acme-dev.service-now.com',
        REZ_RESTORE_INDEXER_SOURCE_TENANT_ID: 'test-tenant',
    };
}

describe('parseIndexerEnv - core config', () => {
    it('returns valid config with all required env vars', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(
            config.restorePgUrl,
            'postgresql://localhost:5432/test',
        );
        assert.equal(config.defaultTenant, 'test-tenant');
        assert.equal(
            config.artifactSourceMode,
            'in_memory_scaffold',
        );
    });

    it('throws when REZ_RESTORE_PG_URL is missing', () => {
        const env = baseEnv();
        delete env.REZ_RESTORE_PG_URL;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_PG_URL is required/,
        );
    });

    it('throws when REZ_RESTORE_PG_URL is empty string', () => {
        const env = { ...baseEnv(), REZ_RESTORE_PG_URL: '' };
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_PG_URL is required/,
        );
    });

    it('throws when REZ_RESTORE_PG_URL is whitespace only', () => {
        const env = { ...baseEnv(), REZ_RESTORE_PG_URL: '   ' };
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_PG_URL is required/,
        );
    });

    it('falls back to REZ_RESTORE_INDEXER_PG_URL', () => {
        const env = baseEnv();
        delete env.REZ_RESTORE_PG_URL;
        env.REZ_RESTORE_INDEXER_PG_URL =
            'postgresql://fallback:5432/db';
        const config = parseIndexerEnv(env);
        assert.equal(
            config.restorePgUrl,
            'postgresql://fallback:5432/db',
        );
    });

    it('defaults DEFAULT_TENANT to tenant-local-dev when missing',
    () => {
        const env = baseEnv();
        delete env.REZ_RESTORE_INDEXER_DEFAULT_TENANT;
        const config = parseIndexerEnv(env);
        assert.equal(config.defaultTenant, 'tenant-local-dev');
    });

    it('defaults DEFAULT_TENANT to tenant-local-dev when empty',
    () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_DEFAULT_TENANT: '',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.defaultTenant, 'tenant-local-dev');
    });

    it('throws when DEFAULT_TENANT is whitespace only', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_DEFAULT_TENANT: '   ',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_DEFAULT_TENANT must not be empty/,
        );
    });

    it('parses artifact source mode rec_manifest_object_store',
    () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(
            config.artifactSourceMode,
            'rec_manifest_object_store',
        );
    });

    it('parses artifact source mode in_memory_scaffold', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(
            config.artifactSourceMode,
            'in_memory_scaffold',
        );
    });

    it('defaults artifact source mode to rec_manifest_object_store',
    () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_ARTIFACT_SOURCE;
        const config = parseIndexerEnv(env);
        assert.equal(
            config.artifactSourceMode,
            'rec_manifest_object_store',
        );
    });

    it('throws on invalid artifact source mode', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_ARTIFACT_SOURCE: 'invalid_mode',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_ARTIFACT_SOURCE must be one of/,
        );
    });

    it('parses batch size as positive integer', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BATCH_SIZE: '100',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.batchSize, 100);
    });

    it('defaults batch size to 500', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.batchSize, 500);
    });

    it('returns default batch size for zero', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BATCH_SIZE: '0',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.batchSize, 500);
    });

    it('returns default batch size for negative value', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BATCH_SIZE: '-5',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.batchSize, 500);
    });

    it('returns default batch size for non-numeric value', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BATCH_SIZE: 'abc',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.batchSize, 500);
    });

    it('parses backfill batch size correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BACKFILL_BATCH_SIZE: '100',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.backfillBatchSize, 100);
    });

    it('defaults backfill batch size to 250', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.backfillBatchSize, 250);
    });

    it('parses stale_after_seconds correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_STALE_AFTER_SECONDS: '60',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.staleAfterSeconds, 60);
    });

    it('defaults stale_after_seconds to 120', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.staleAfterSeconds, 120);
    });

    it('parses freshness_timeout_seconds correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_FRESHNESS_TIMEOUT_SECONDS: '30',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.freshnessTimeoutSeconds, 30);
    });

    it('defaults freshness_timeout_seconds to 60', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.freshnessTimeoutSeconds, 60);
    });

    it('parses backfill_max_lag_seconds correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_BACKFILL_MAX_LAG_SECONDS: '90',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.backfillMaxLagSeconds, 90);
    });

    it('defaults backfill_max_lag_seconds to 180', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.backfillMaxLagSeconds, 180);
    });

    it('parses poll_interval_ms correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_POLL_INTERVAL_MS: '2000',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.pollIntervalMs, 2000);
    });

    it('defaults poll_interval_ms to 1000', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.pollIntervalMs, 1000);
    });

    it('parses leader election enabled true', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: 'true',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.leaderLease.enabled, true);
    });

    it('parses leader election enabled false', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: 'false',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.leaderLease.enabled, false);
    });

    it('defaults leader election to true for rec_manifest mode',
    () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(config.leaderLease.enabled, true);
    });

    it('defaults leader election to false for scaffold mode', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.leaderLease.enabled, false);
    });

    it('throws when leader election enabled for scaffold mode',
    () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: 'true',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /LEADER_ELECTION_ENABLED=true requires/,
        );
    });

    it('parses leader lease seconds correctly', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_LEADER_LEASE_SECONDS: '60',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.leaderLease.leaseDurationSeconds, 60);
    });

    it('defaults leader lease seconds to 30', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.leaderLease.leaseDurationSeconds, 30);
    });

    it('returns default leader lease for non-positive value', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_LEADER_LEASE_SECONDS: '0',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.leaderLease.leaseDurationSeconds, 30);
    });

    it('accepts boolean 1/true/yes/on as true', () => {
        for (const value of [
            '1', 'true', 'yes', 'on',
            'TRUE', 'Yes', 'ON',
        ]) {
            const env = {
                ...recSourceEnv(),
                REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: value,
            };
            const config = parseIndexerEnv(env);
            assert.equal(
                config.leaderLease.enabled,
                true,
                `expected '${value}' to parse as true`,
            );
        }
    });

    it('accepts boolean 0/false/no/off as false', () => {
        for (const value of [
            '0', 'false', 'no', 'off',
            'FALSE', 'No', 'OFF',
        ]) {
            const env = {
                ...baseEnv(),
                REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: value,
            };
            const config = parseIndexerEnv(env);
            assert.equal(
                config.leaderLease.enabled,
                false,
                `expected '${value}' to parse as false`,
            );
        }
    });

    it('throws on unrecognized boolean string', () => {
        const env = {
            ...baseEnv(),
            REZ_RESTORE_INDEXER_LEADER_ELECTION_ENABLED: 'maybe',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /must be true or false/,
        );
    });
});

describe('parseIndexerEnv - REC manifest source config', () => {
    it('parses complete REC manifest source config', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.ok(config.recManifestSource);
        assert.equal(
            config.recManifestSource.bucket,
            'test-bucket',
        );
        assert.equal(
            config.recManifestSource.region,
            'us-east-1',
        );
        assert.equal(
            config.recManifestSource.manifestPrefix,
            'rez/restore/',
        );
        assert.equal(
            config.recManifestSource.generationId,
            'gen-01',
        );
        assert.equal(
            config.recManifestSource.tenantId,
            'test-tenant',
        );
        assert.equal(
            config.recManifestSource.sourceProgressScope.instanceId,
            'sn-dev-01',
        );
        assert.equal(
            config.recManifestSource.sourceProgressScope.source,
            'sn://acme-dev.service-now.com',
        );
    });

    it('throws when BUCKET is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_BUCKET;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_BUCKET is required/,
        );
    });

    it('throws when BUCKET is empty', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_BUCKET: '',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_BUCKET is required/,
        );
    });

    it('throws when REGION is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_REGION;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_REGION is required/,
        );
    });

    it('throws when PREFIX is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_PREFIX;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_PREFIX is required/,
        );
    });

    it('throws when GENERATION_ID is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_GENERATION_ID is required/,
        );
    });

    it('throws when INSTANCE_ID is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_INSTANCE_ID is required/,
        );
    });

    it('throws when URI is missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_URI;
        assert.throws(
            () => parseIndexerEnv(env),
            /REZ_RESTORE_INDEXER_SOURCE_URI is required/,
        );
    });

    it('defaults TENANT_ID to defaultTenant when missing', () => {
        const env = recSourceEnv();
        delete env.REZ_RESTORE_INDEXER_SOURCE_TENANT_ID;
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.tenantId,
            'test-tenant',
        );
    });

    it('parses INGESTION_MODE realtime', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE: 'realtime',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.ingestionMode,
            'realtime',
        );
    });

    it('parses INGESTION_MODE bootstrap', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE: 'bootstrap',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.ingestionMode,
            'bootstrap',
        );
    });

    it('parses INGESTION_MODE gap_repair', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE: 'gap_repair',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.ingestionMode,
            'gap_repair',
        );
    });

    it('defaults INGESTION_MODE to realtime', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(
            config.recManifestSource?.ingestionMode,
            'realtime',
        );
    });

    it('throws on invalid INGESTION_MODE', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_INGESTION_MODE: 'invalid',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /INGESTION_MODE must be one of/,
        );
    });

    it('parses MAX_ATTEMPTS as positive integer', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_MAX_ATTEMPTS: '5',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.recManifestSource?.maxAttempts, 5);
    });

    it('defaults MAX_ATTEMPTS to 3', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(config.recManifestSource?.maxAttempts, 3);
    });

    it('returns default MAX_ATTEMPTS for non-positive value', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_MAX_ATTEMPTS: '0',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.recManifestSource?.maxAttempts, 3);
    });

    it('parses RETRY_BASE_MS as positive integer', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_RETRY_BASE_MS: '500',
        };
        const config = parseIndexerEnv(env);
        assert.equal(config.recManifestSource?.retryBaseMs, 500);
    });

    it('defaults RETRY_BASE_MS to 200', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(config.recManifestSource?.retryBaseMs, 200);
    });

    it('parses ENDPOINT as optional string', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_ENDPOINT:
                'http://localhost:9000',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.endpoint,
            'http://localhost:9000',
        );
    });

    it('omits ENDPOINT when not set', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(
            config.recManifestSource?.endpoint,
            undefined,
        );
    });

    it('parses FORCE_PATH_STYLE true', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_FORCE_PATH_STYLE: 'true',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.forcePathStyle,
            true,
        );
    });

    it('parses FORCE_PATH_STYLE false', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_FORCE_PATH_STYLE: 'false',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.forcePathStyle,
            false,
        );
    });

    it('defaults FORCE_PATH_STYLE to false', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(
            config.recManifestSource?.forcePathStyle,
            false,
        );
    });

    it('parses ACCESS_KEY_ID and SECRET_ACCESS_KEY together',
    () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_ACCESS_KEY_ID: 'AKID123',
            REZ_RESTORE_INDEXER_SOURCE_SECRET_ACCESS_KEY: 'secret',
        };
        const config = parseIndexerEnv(env);
        assert.equal(
            config.recManifestSource?.accessKeyId,
            'AKID123',
        );
        assert.equal(
            config.recManifestSource?.secretAccessKey,
            'secret',
        );
    });

    it('omits credentials when neither is set', () => {
        const config = parseIndexerEnv(recSourceEnv());
        assert.equal(
            config.recManifestSource?.accessKeyId,
            undefined,
        );
        assert.equal(
            config.recManifestSource?.secretAccessKey,
            undefined,
        );
    });

    it('throws when only ACCESS_KEY_ID is set', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_ACCESS_KEY_ID: 'AKID123',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /ACCESS_KEY_ID and.*SECRET_ACCESS_KEY must be set together/,
        );
    });

    it('throws when only SECRET_ACCESS_KEY is set', () => {
        const env = {
            ...recSourceEnv(),
            REZ_RESTORE_INDEXER_SOURCE_SECRET_ACCESS_KEY: 'secret',
        };
        assert.throws(
            () => parseIndexerEnv(env),
            /ACCESS_KEY_ID and.*SECRET_ACCESS_KEY must be set together/,
        );
    });

    it('recManifestSource is undefined for scaffold mode', () => {
        const config = parseIndexerEnv(baseEnv());
        assert.equal(config.recManifestSource, undefined);
    });
});
