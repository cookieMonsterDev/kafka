import http from 'node:http';
import type { AddressInfo } from 'node:net';
import { describe, expect, it } from 'vitest';
import { StudioSchemaRegistry } from './schema-registry';

async function withRegistry(run: (registry: StudioSchemaRegistry) => Promise<void>) {
  const definitions: Record<string, unknown> = {
    '1': { schema: '{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}' },
    '2': {
      schemaType: 'JSON',
      schema: '{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}',
    },
    '3': { schemaType: 'PROTOBUF', schema: 'syntax = "proto3"; message Order { string id = 1; }' },
  };
  const server = http.createServer((req, res) => {
    const id = req.url?.split('/').at(-1) ?? '';
    const definition = definitions[id];
    if (!definition) {
      res.writeHead(404);
      res.end('{}');
      return;
    }
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(definition));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const registry = new StudioSchemaRegistry({
    path: null,
    profiles: {},
    fileConfig: null,
    env: { KAFKA_STUDIO_SCHEMA_REGISTRY_URL: `http://127.0.0.1:${String((server.address() as AddressInfo).port)}` },
  });
  try {
    await run(registry);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}
describe('schema registry codecs', () => {
  it('validates and round-trips Avro, JSON Schema, and Protobuf', async () => {
    await withRegistry(async (registry) => {
      for (const id of [1, 2, 3]) {
        const encoded = await registry.encode(null, id, { id: 'order-1' });
        expect(encoded.readUInt32BE(1)).toBe(id);
        expect(await registry.decode(null, encoded.toString('base64'))).toEqual({ id: 'order-1' });
      }
      await expect(registry.encode(null, 1, { id: 5 })).rejects.toThrow();
      await expect(registry.encode(null, 2, { id: 5 })).rejects.toThrow();
      await expect(registry.encode(null, 3, { id: 5 })).rejects.toThrow();
    });
  });
  it('does not reuse the default registry for a named environment', async () => {
    await withRegistry(async (registry) => {
      await expect(registry.encode('production', 1, { id: 'a' })).rejects.toThrow('Configure');
    });
  });
  it('rejects non-Confluent frames', async () => {
    await withRegistry(async (registry) => {
      await expect(registry.decode(null, Buffer.from('hello').toString('base64'))).rejects.toThrow('Confluent');
    });
  });
});

async function withReferenceRegistries(run: (registry: StudioSchemaRegistry) => Promise<void>, referenceCount = 1) {
  const server = http.createServer((req, res) => {
    const path = req.url ?? '';
    const subject = decodeURIComponent(path.split('/subjects/')[1]?.split('/versions/')[0] ?? '');
    const schema =
      subject === 'orders-value'
        ? {
            id: 1,
            version: 1,
            subject,
            schemaType: 'PROTOBUF',
            schema: 'import "shared.proto"; message Order { Shared value = 1; }',
            references: [{ name: 'shared.proto', subject: 'shared-0', version: 1 }],
          }
        : {
            id: 2,
            version: 1,
            subject,
            schemaType: 'PROTOBUF',
            schema: path.startsWith('/left/')
              ? 'message Shared { string id = 1; }'
              : 'message Shared { int64 id = 1; }',
            references:
              Number(subject.split('-')[1]) + 1 < referenceCount
                ? [{ name: 'next.proto', subject: `shared-${String(Number(subject.split('-')[1]) + 1)}`, version: 1 }]
                : [],
          };
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify(schema));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const url = `http://127.0.0.1:${String((server.address() as AddressInfo).port)}`;
  const registry = new StudioSchemaRegistry({
    path: null,
    env: {},
    profiles: { left: {}, right: {} },
    fileConfig: { studio: { schemaRegistries: { left: { url: `${url}/left` }, right: { url: `${url}/right` } } } },
  });
  try {
    await run(registry);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}

describe('schema comparison coverage', () => {
  it('includes differing referenced definitions even when root schema metadata is identical', async () => {
    await withReferenceRegistries(async (registry) => {
      const [left, right] = await Promise.all([
        registry.snapshot('left', 'orders-value'),
        registry.snapshot('right', 'orders-value'),
      ]);
      expect(left.schema).toBe(right.schema);
      expect(left.references).toEqual(right.references);
      expect(left.referencedSchemas[0]?.schema).not.toBe(right.referencedSchemas[0]?.schema);
      expect(left.referencesComplete).toBe(true);
      expect(right.referencesComplete).toBe(true);
    });
  });
  it('fails explicitly rather than claiming complete coverage beyond the reference limit', async () => {
    await withReferenceRegistries(async (registry) => {
      await expect(registry.snapshot('left', 'orders-value')).rejects.toThrow('32 references');
    }, 33);
  });
});
