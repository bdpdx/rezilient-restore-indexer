import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    S3RecManifestObjectStoreClient,
    createS3RecManifestObjectStoreClient,
} from './rec-manifest-object-store-client.js';
import type { S3Client } from '@aws-sdk/client-s3';

type ListResponse = {
    Contents?: Array<{ Key?: string }>;
    IsTruncated?: boolean;
    NextContinuationToken?: string;
};

type GetResponse = {
    Body?: unknown;
};

function createMockS3Client(options: {
    getResponses?: Record<string, GetResponse>;
    listResponses?: ListResponse[];
}) {
    let listCallIndex = 0;
    const listCalls: Record<string, unknown>[] = [];
    const getCalls: string[] = [];

    return {
        client: {
            send: async (command: unknown) => {
                const cmd = command as {
                    input?: Record<string, unknown>;
                };

                if (
                    cmd.input &&
                    'Prefix' in cmd.input
                ) {
                    listCalls.push(cmd.input);
                    const resp =
                        options.listResponses?.[listCallIndex]
                        ?? { Contents: [], IsTruncated: false };
                    listCallIndex++;

                    return resp;
                }

                if (cmd.input && 'Key' in cmd.input) {
                    const key = cmd.input.Key as string;
                    getCalls.push(key);
                    const resp =
                        options.getResponses?.[key];

                    if (!resp) {
                        throw new Error(
                            `key not found: ${key}`,
                        );
                    }

                    return resp;
                }

                throw new Error('unknown command');
            },
        } as unknown as S3Client,
        getCalls,
        listCalls,
    };
}

function createClient(
    s3: S3Client,
    bucket = 'test-bucket',
) {
    return new S3RecManifestObjectStoreClient(s3, {
        bucket,
        region: 'us-east-1',
    });
}

describe('S3RecManifestObjectStoreClient constructor', () => {
    it('throws when bucket is empty', () => {
        const { client: s3 } = createMockS3Client({});
        assert.throws(
            () => createClient(s3, ''),
            /bucket must not be empty/,
        );
    });

    it('throws when bucket is whitespace only', () => {
        const { client: s3 } = createMockS3Client({});
        assert.throws(
            () => createClient(s3, '  '),
            /bucket must not be empty/,
        );
    });

    it('accepts valid bucket name', () => {
        const { client: s3 } = createMockS3Client({});
        const client = createClient(s3, 'my-bucket');
        assert.ok(client);
    });
});

describe('S3RecManifestObjectStoreClient.listObjectKeys',
() => {
    it('returns empty when limit <= 0', async () => {
        const { client: s3 } = createMockS3Client({});
        const client = createClient(s3);
        const keys = await client.listObjectKeys({
            limit: 0,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, []);
    });

    it('returns negative limit as empty', async () => {
        const { client: s3 } = createMockS3Client({});
        const client = createClient(s3);
        const keys = await client.listObjectKeys({
            limit: -1,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, []);
    });

    it('filters by .manifest.json suffix', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [
                    { Key: 'rez/restore/a.manifest.json' },
                    { Key: 'rez/restore/b.artifact.json' },
                    { Key: 'rez/restore/c.manifest.json' },
                ],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, [
            'rez/restore/a.manifest.json',
            'rez/restore/c.manifest.json',
        ]);
    });

    it('filters by prefix', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [
                    { Key: 'rez/restore/a.manifest.json' },
                    { Key: 'other/b.manifest.json' },
                ],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, [
            'rez/restore/a.manifest.json',
        ]);
    });

    it('paginates with continuation tokens', async () => {
        const mock = createMockS3Client({
            listResponses: [
                {
                    Contents: [
                        {
                            Key: 'rez/restore/'
                                + 'a.manifest.json',
                        },
                    ],
                    IsTruncated: true,
                    NextContinuationToken: 'tok-1',
                },
                {
                    Contents: [
                        {
                            Key: 'rez/restore/'
                                + 'b.manifest.json',
                        },
                    ],
                    IsTruncated: false,
                },
            ],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, [
            'rez/restore/a.manifest.json',
            'rez/restore/b.manifest.json',
        ]);
        assert.equal(mock.listCalls.length, 2);
        assert.equal(
            mock.listCalls[1].ContinuationToken,
            'tok-1',
        );
    });

    it('stops at requested limit', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [
                    { Key: 'rez/restore/a.manifest.json' },
                    { Key: 'rez/restore/b.manifest.json' },
                    { Key: 'rez/restore/c.manifest.json' },
                ],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 2,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.equal(keys.length, 2);
    });

    it('handles empty response', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, []);
    });

    it('handles missing Contents', async () => {
        const mock = createMockS3Client({
            listResponses: [{ IsTruncated: false }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, []);
    });

    it('passes startAfter on first page', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: 'rez/restore/cursor-key',
        });
        assert.equal(
            mock.listCalls[0].StartAfter,
            'rez/restore/cursor-key',
        );
    });

    it('skips items with missing Key', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [
                    { Key: undefined },
                    { Key: 'rez/restore/a.manifest.json' },
                ],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, [
            'rez/restore/a.manifest.json',
        ]);
    });

    it('stops when IsTruncated but no continuation token',
    async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [
                    { Key: 'rez/restore/a.manifest.json' },
                ],
                IsTruncated: true,
                NextContinuationToken: undefined,
            }],
        });
        const client = createClient(mock.client);
        const keys = await client.listObjectKeys({
            limit: 10,
            prefix: 'rez/restore',
            startAfter: null,
        });
        assert.deepEqual(keys, [
            'rez/restore/a.manifest.json',
        ]);
        assert.equal(mock.listCalls.length, 1);
    });
});

describe('S3RecManifestObjectStoreClient.readObjectText',
() => {
    it('throws when key is empty', async () => {
        const { client: s3 } = createMockS3Client({});
        const client = createClient(s3);
        await assert.rejects(
            client.readObjectText(''),
            /key must not be empty/,
        );
    });

    it('throws when key is whitespace only', async () => {
        const { client: s3 } = createMockS3Client({});
        const client = createClient(s3);
        await assert.rejects(
            client.readObjectText('   '),
            /key must not be empty/,
        );
    });

    it('handles string body', async () => {
        const mock = createMockS3Client({
            getResponses: {
                'my-key': {
                    Body: '{"hello":"world"}',
                },
            },
        });
        const client = createClient(mock.client);
        const text = await client.readObjectText('my-key');
        assert.equal(text, '{"hello":"world"}');
    });

    it('handles Uint8Array body', async () => {
        const content = '{"data":42}';
        const body = Buffer.from(content, 'utf8');
        const mock = createMockS3Client({
            getResponses: {
                'binary-key': { Body: body },
            },
        });
        const client = createClient(mock.client);
        const text = await client.readObjectText(
            'binary-key',
        );
        assert.equal(text, '{"data":42}');
    });

    it('handles body with transformToString', async () => {
        const body = {
            transformToString: async () => '{"transformed":true}',
        };
        const mock = createMockS3Client({
            getResponses: {
                'transform-key': { Body: body },
            },
        });
        const client = createClient(mock.client);
        const text = await client.readObjectText(
            'transform-key',
        );
        assert.equal(text, '{"transformed":true}');
    });

    it('handles async iterable body', async () => {
        async function* bodyStream() {
            yield Buffer.from('{"chunk":');
            yield Buffer.from('"one"}');
        }

        const mock = createMockS3Client({
            getResponses: {
                'stream-key': {
                    Body: bodyStream(),
                },
            },
        });
        const client = createClient(mock.client);
        const text = await client.readObjectText(
            'stream-key',
        );
        assert.equal(text, '{"chunk":"one"}');
    });

    it('handles async iterable with string chunks',
    async () => {
        async function* bodyStream() {
            yield '{"str":';
            yield '"chunks"}';
        }

        const mock = createMockS3Client({
            getResponses: {
                'str-stream-key': {
                    Body: bodyStream(),
                },
            },
        });
        const client = createClient(mock.client);
        const text = await client.readObjectText(
            'str-stream-key',
        );
        assert.equal(text, '{"str":"chunks"}');
    });

    it('throws when body is null', async () => {
        const mock = createMockS3Client({
            getResponses: {
                'null-body': { Body: null },
            },
        });
        const client = createClient(mock.client);
        await assert.rejects(
            client.readObjectText('null-body'),
            /missing object body/,
        );
    });

    it('throws when body is undefined', async () => {
        const mock = createMockS3Client({
            getResponses: {
                'undef-body': { Body: undefined },
            },
        });
        const client = createClient(mock.client);
        await assert.rejects(
            client.readObjectText('undef-body'),
            /missing object body/,
        );
    });
});

describe('normalizePrefix (tested via listObjectKeys)',
() => {
    it('strips leading and trailing slashes', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        await client.listObjectKeys({
            limit: 10,
            prefix: '/rez/restore/',
            startAfter: null,
        });
        assert.equal(
            mock.listCalls[0].Prefix,
            'rez/restore/',
        );
    });

    it('preserves inner slashes', async () => {
        const mock = createMockS3Client({
            listResponses: [{
                Contents: [],
                IsTruncated: false,
            }],
        });
        const client = createClient(mock.client);
        await client.listObjectKeys({
            limit: 10,
            prefix: 'a/b/c',
            startAfter: null,
        });
        assert.equal(mock.listCalls[0].Prefix, 'a/b/c/');
    });
});

describe('createS3RecManifestObjectStoreClient', () => {
    it('creates client with minimal config', () => {
        const client = createS3RecManifestObjectStoreClient({
            bucket: 'test-bucket',
            region: 'us-east-1',
        });
        assert.ok(client);
        assert.equal(typeof client.listObjectKeys, 'function');
        assert.equal(typeof client.readObjectText, 'function');
    });

    it('accepts custom endpoint', () => {
        const client = createS3RecManifestObjectStoreClient({
            bucket: 'test-bucket',
            endpoint: 'http://localhost:9000',
            region: 'us-east-1',
        });
        assert.ok(client);
    });

    it('accepts custom credentials', () => {
        const client = createS3RecManifestObjectStoreClient({
            accessKeyId: 'AKIAIOSFODNN7EXAMPLE',
            bucket: 'test-bucket',
            region: 'us-east-1',
            secretAccessKey: 'wJalrXUtnFEMI/K7MDENG/bP',
        });
        assert.ok(client);
    });

    it('accepts forcePathStyle', () => {
        const client = createS3RecManifestObjectStoreClient({
            bucket: 'test-bucket',
            forcePathStyle: true,
            region: 'us-east-1',
        });
        assert.ok(client);
    });
});
