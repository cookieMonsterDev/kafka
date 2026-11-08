import { decodeProtobuf, encodeProtobuf } from './protobuf-schema';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import type { StudioConnectionConfig } from './connection';

export interface RegistryConfig {
  readonly url: string;
  readonly username?: string;
  readonly password?: string;
}
export interface RegistrySchema {
  readonly id: number;
  readonly version: number;
  readonly subject: string;
  readonly schemaType?: string;
  readonly schema: string;
  readonly references?: readonly { readonly name: string; readonly subject: string; readonly version: number }[];
}

/** Registry destinations and credentials are read from trusted startup configuration only. */
export class StudioSchemaRegistry {
  private readonly clients = new Map<string, SchemaRegistry>();
  private readonly connection: StudioConnectionConfig;
  constructor(connection: StudioConnectionConfig) {
    this.connection = connection;
  }

  private config(profile: string | null): RegistryConfig {
    const studio = this.connection.fileConfig?.studio as
      { schemaRegistries?: Record<string, RegistryConfig> } | undefined;
    const configured = studio?.schemaRegistries?.[profile ?? 'default'];
    const env = this.connection.env;
    const config =
      configured ??
      (profile === null && env.KAFKA_STUDIO_SCHEMA_REGISTRY_URL
        ? {
            url: env.KAFKA_STUDIO_SCHEMA_REGISTRY_URL,
            username: env.KAFKA_STUDIO_SCHEMA_REGISTRY_USERNAME,
            password: env.KAFKA_STUDIO_SCHEMA_REGISTRY_PASSWORD,
          }
        : undefined);
    if (!config || typeof config.url !== 'string')
      throw new Error('Configure studio.schemaRegistries for this profile before using schemas');
    const url = new URL(config.url);
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
      throw new Error('Schema Registry URL must be HTTP(S) without embedded credentials, query, or fragment');
    return config;
  }

  private client(profile: string | null): SchemaRegistry {
    const config = this.config(profile);
    const key = profile ?? 'default';
    let client = this.clients.get(key);
    if (!client) {
      client = new SchemaRegistry({
        host: config.url,
        retry: { retries: 0 },
        ...(config.username !== undefined
          ? { auth: { username: config.username, password: config.password ?? '' } }
          : {}),
      });
      this.clients.set(key, client);
    }
    return client;
  }

  async get<T>(profile: string | null, path: string, signal?: AbortSignal): Promise<T> {
    const config = this.config(profile);
    const url = `${config.url.replace(/\/$/, '')}/${path}`;
    const response = await fetch(url, {
      redirect: 'error',
      signal: AbortSignal.any([AbortSignal.timeout(10000), ...(signal ? [signal] : [])]),
      headers:
        config.username === undefined
          ? {}
          : { Authorization: `Basic ${Buffer.from(`${config.username}:${config.password ?? ''}`).toString('base64')}` },
    });
    if (!response.ok) throw new Error(`Schema Registry returned HTTP ${String(response.status)}`);
    return (await response.json()) as T;
  }

  /** Include referenced definitions: equal root text does not imply equal schemas across registries. */
  async snapshot(profile: string | null, subject: string) {
    const signal = AbortSignal.timeout(30000);
    const root = await this.get<RegistrySchema>(
      profile,
      `subjects/${encodeURIComponent(subject)}/versions/latest`,
      signal,
    );
    const visited = new Set([JSON.stringify([root.subject, root.version])]);
    const referencedSchemas: RegistrySchema[] = [];
    const visit = async (schema: RegistrySchema): Promise<void> => {
      for (const reference of schema.references ?? []) {
        const key = JSON.stringify([reference.subject, reference.version]);
        if (visited.has(key)) continue;
        if (referencedSchemas.length >= 32) throw new Error('Schema comparison exceeds 32 references');
        visited.add(key);
        const child = await this.get<RegistrySchema>(
          profile,
          `subjects/${encodeURIComponent(reference.subject)}/versions/${String(reference.version)}`,
          signal,
        );
        // A registry version response must identify its subject/version; use the requested identity too.
        const resolved = { ...child, subject: reference.subject, version: reference.version };
        referencedSchemas.push(resolved);
        await visit(resolved);
      }
    };
    await visit(root);
    return {
      id: root.id,
      version: root.version,
      type: root.schemaType ?? 'AVRO',
      schema: root.schema,
      references: root.references ?? [],
      referencedSchemas,
      referencesComplete: true,
    };
  }

  async decode(profile: string | null, value: string): Promise<unknown> {
    const buffer = Buffer.from(value, 'base64');
    if (buffer.length < 5 || buffer[0] !== 0) throw new Error('Expected a Confluent-framed payload');
    const schema = await this.get<RegistrySchema>(profile, `schemas/ids/${String(buffer.readUInt32BE(1))}`);
    if (schema.schemaType === 'PROTOBUF') return decodeProtobuf(schema, buffer, (path) => this.get(profile, path));
    return (await this.client(profile).decode(buffer)) as unknown;
  }

  async encode(profile: string | null, id: number, value: unknown, messageType?: string): Promise<Buffer> {
    const schema = await this.get<RegistrySchema>(profile, `schemas/ids/${String(id)}`);
    if (schema.schemaType === 'PROTOBUF')
      return encodeProtobuf(schema, id, value, (path) => this.get(profile, path), messageType);
    return this.client(profile).encode(id, value);
  }
}
