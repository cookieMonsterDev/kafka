import type { AddressInfo } from 'node:net';
import { describe, expect, it, vi } from 'vitest';
import { createStudioServer } from '../create-server';
import { Router } from '../router';
import { AdminPool } from '../kafka/admin-pool';
import { createFakeAdmin } from '../kafka/create-fake-admin';
import { createFakeProducer } from '../kafka/create-fake-producer';
import {
  createFakeBatch,
  createFakeKafkaMessage,
  createFakeMessageConsumer,
} from '../kafka/create-fake-message-consumer';
import { ProducerPool } from '../kafka/produce';
import { StudioSchemaRegistry } from '../kafka/schema-registry';
import { registerDebugRoutes } from './debug';

async function withServer(run: (url: string, send: ReturnType<typeof vi.fn>) => Promise<void>, readOnly = false) {
  const send = vi.fn(async () => []);
  const connection = { path: null, fileConfig: null, env: {}, profiles: { staging: {} } };
  const router = new Router();
  registerDebugRoutes(router, {
    connection,
    registry: new StudioSchemaRegistry(connection),
    getActiveProfile: () => null,
    pool: new AdminPool((profile) => ({
      admin: () =>
        createFakeAdmin({
          connect: async () => {},
          disconnect: async () => {},
          fetchTopicMetadata: async () => ({
            topics: [
              {
                name: 'orders',
                partitions: [
                  { partitionErrorCode: 0, partitionId: 0, leader: 1, replicas: [1], isr: [1], offlineReplicas: [] },
                ],
              },
            ],
          }),
          describeConfigs: async () => ({
            resources: [
              {
                errorCode: 0,
                errorMessage: null,
                resourceType: 2,
                resourceName: 'orders',
                configEntries: [
                  {
                    configName: 'retention.ms',
                    configValue: profile === null ? '1000' : '2000',
                    readOnly: false,
                    isDefault: false,
                    isSensitive: false,
                    configSource: 1,
                    configSynonyms: [],
                    configType: 1,
                    configDocumentation: null,
                  },
                  {
                    configName: 'secret',
                    configValue: 'do-not-leak',
                    readOnly: false,
                    isDefault: false,
                    isSensitive: true,
                    configSource: 1,
                    configSynonyms: [],
                    configType: 1,
                    configDocumentation: null,
                  },
                ],
              },
            ],
          }),
          fetchTopicOffsets: async () => [{ partition: 0, offset: 1n, low: 0n, high: 1n }],
        }),
    })),
    producers: new ProducerPool(() => ({
      producer: () => createFakeProducer({ connect: async () => {}, disconnect: async () => {}, send }),
    })),
    consumerFactory: () => ({
      consumer: () =>
        createFakeMessageConsumer({
          connect: async () => {},
          disconnect: async () => {},
          assign: async () => {},
          seek: () => {},
          stream: async function* () {
            yield createFakeBatch({
              topic: 'orders',
              partition: 0,
              highWatermark: 1n,
              fetchedOffset: 0n,
              messages: [createFakeKafkaMessage({ value: Buffer.from('hello') })],
            });
          },
        }),
    }),
  });
  const server = createStudioServer({ router, readOnly });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  try {
    await run(`http://127.0.0.1:${String((server.address() as AddressInfo).port)}`, send);
  } finally {
    server.closeAllConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  }
}
const post = (url: string, path: string, body: unknown) =>
  fetch(`${url}/api/debug/${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
describe('debugger routes', () => {
  it('keeps a POST SSE response open after consuming the body', async () => {
    await withServer(async (url) => {
      const response = await post(url, 'scan', { topics: ['orders'] });
      expect(response.status).toBe(200);
      const events = await response.text();
      expect(events).toContain('event: result');
      expect(events).toContain('"complete":true');
      expect(events).toContain('aGVsbG8=');
    });
  });
  it('preserves binary/null bytes and repeated headers in replay', async () => {
    await withServer(async (url, send) => {
      const response = await post(url, 'replay', {
        topic: 'recovered',
        records: [{ key: '/w==', value: null, headers: { repeat: ['YQ==', null, 'Yg=='] } }],
      });
      const events = await response.text();
      expect(events).toContain('"status":"sent"');
      expect(events).toContain('event: done');
      expect(send).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'recovered',
          messages: [
            { key: Buffer.from([255]), value: null, headers: { repeat: [Buffer.from('a'), null, Buffer.from('b')] } },
          ],
        }),
      );
    });
  });
  it('enforces read-only on replay and scenarios, including direct HTTP callers', async () => {
    await withServer(async (url, send) => {
      expect((await post(url, 'replay', {})).status).toBe(403);
      expect((await post(url, 'scenario', {})).status).toBe(403);
      const scan = await post(url, 'scan', { topics: ['orders'] });
      expect(scan.status).toBe(200);
      await scan.text();
      expect(send).not.toHaveBeenCalled();
    }, true);
  });
  it('validates requests before opening a stream', async () => {
    await withServer(async (url) => {
      expect((await post(url, 'scan', { topics: ['orders'], maxRecords: 10001 })).status).toBe(400);
      expect(
        (await post(url, 'replay', { topic: 'recovered', records: [{ key: null, value: 'bad$$$' }] })).status,
      ).toBe(400);
    });
  });
});

it('compares profiles without exposing sensitive settings or requiring a registry', async () => {
  await withServer(async (url) => {
    const response = await post(url, 'compare', { topic: 'orders', left: null, right: 'staging' });
    expect(response.status).toBe(200);
    const body = await response.text();
    expect(body).toContain('retention.ms');
    expect(body).toContain('1000');
    expect(body).toContain('2000');
    expect(body).not.toContain('do-not-leak');
    expect(body).toContain('unavailable');
  }, true);
});
