import {
    GetObjectCommand,
    ListObjectsV2Command,
    S3Client,
    type S3ClientConfig,
} from '@aws-sdk/client-s3';
import type { RecManifestObjectStoreClient } from './rec-manifest-source';

const MANIFEST_SUFFIX = '.manifest.json';

export type RecManifestS3ObjectStoreClientConfig = {
    bucket: string;
    endpoint?: string;
    forcePathStyle?: boolean;
    region: string;
};

function toUtf8String(
    value: Uint8Array,
): string {
    return Buffer.from(value).toString('utf8');
}

async function readStreamBody(
    body: AsyncIterable<unknown>,
): Promise<string> {
    const chunks: Buffer[] = [];

    for await (const chunk of body) {
        if (typeof chunk === 'string') {
            chunks.push(Buffer.from(chunk, 'utf8'));
            continue;
        }

        if (chunk instanceof Uint8Array) {
            chunks.push(Buffer.from(chunk));
            continue;
        }

        throw new Error('unsupported object-store body chunk type');
    }

    return Buffer.concat(chunks).toString('utf8');
}

async function readBodyAsString(
    body: unknown,
): Promise<string> {
    if (typeof body === 'string') {
        return body;
    }

    if (body instanceof Uint8Array) {
        return toUtf8String(body);
    }

    const maybeTransform = body as {
        transformToString?: (encoding?: string) => Promise<string>;
    };

    if (typeof maybeTransform.transformToString === 'function') {
        return maybeTransform.transformToString('utf8');
    }

    const maybeStream = body as AsyncIterable<unknown>;

    if (maybeStream && Symbol.asyncIterator in maybeStream) {
        return readStreamBody(maybeStream);
    }

    throw new Error('unsupported object-store body type');
}

function normalizePrefix(prefix: string): string {
    return String(prefix || '').replace(/^\/+|\/+$/g, '');
}

export class S3RecManifestObjectStoreClient
implements RecManifestObjectStoreClient {
    private readonly bucket: string;

    constructor(
        private readonly client: S3Client,
        config: RecManifestS3ObjectStoreClientConfig,
    ) {
        this.bucket = String(config.bucket || '').trim();

        if (!this.bucket) {
            throw new Error('rec manifest source bucket must not be empty');
        }
    }

    async listObjectKeys(input: {
        limit: number;
        prefix: string;
        startAfter: string | null;
    }): Promise<string[]> {
        if (input.limit <= 0) {
            return [];
        }

        const prefix = normalizePrefix(input.prefix);
        const prefixWithSlash = `${prefix}/`;
        const maxManifestCount = Math.min(input.limit, 1000);
        const manifestKeys: string[] = [];
        let continuationToken: string | undefined;

        while (manifestKeys.length < maxManifestCount) {
            const remaining = maxManifestCount - manifestKeys.length;
            const maxKeys = Math.min(Math.max(remaining * 4, 100), 1000);
            const response = await this.client.send(
                new ListObjectsV2Command({
                    Bucket: this.bucket,
                    ContinuationToken: continuationToken,
                    MaxKeys: maxKeys,
                    Prefix: prefixWithSlash,
                    StartAfter: continuationToken
                        ? undefined
                        : input.startAfter || undefined,
                }),
            );
            const contents = response.Contents || [];

            for (const item of contents) {
                const key = item.Key;

                if (!key || !key.endsWith(MANIFEST_SUFFIX)) {
                    continue;
                }

                if (!key.startsWith(prefixWithSlash)) {
                    continue;
                }

                manifestKeys.push(key);

                if (manifestKeys.length >= maxManifestCount) {
                    break;
                }
            }

            if (!response.IsTruncated) {
                break;
            }

            continuationToken = response.NextContinuationToken;

            if (!continuationToken) {
                break;
            }
        }

        return manifestKeys;
    }

    async readObjectText(key: string): Promise<string> {
        const normalizedKey = String(key || '').trim();

        if (!normalizedKey) {
            throw new Error('object-store read key must not be empty');
        }

        const response = await this.client.send(
            new GetObjectCommand({
                Bucket: this.bucket,
                Key: normalizedKey,
            }),
        );

        if (!response.Body) {
            throw new Error(`missing object body for key ${normalizedKey}`);
        }

        return readBodyAsString(response.Body);
    }
}

export function createS3RecManifestObjectStoreClient(
    config: RecManifestS3ObjectStoreClientConfig,
): RecManifestObjectStoreClient {
    const clientConfig: S3ClientConfig = {
        forcePathStyle: Boolean(config.forcePathStyle),
        region: config.region,
    };

    if (config.endpoint) {
        clientConfig.endpoint = config.endpoint;
    }

    const client = new S3Client(clientConfig);

    return new S3RecManifestObjectStoreClient(client, config);
}
