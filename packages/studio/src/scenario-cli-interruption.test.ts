import { Kafka } from '@cookiemonsterdev/kafka-core';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it, vi } from 'vitest';
import type * as ConnectionModule from './server/kafka/connection';
import { createKafkaClient } from './server/kafka/connection';
import { createFakeAdmin } from './server/kafka/create-fake-admin';
import { createFakeProducer } from './server/kafka/create-fake-producer';
import { createFakeMessageConsumer, createFakeBatch } from './server/kafka/create-fake-message-consumer';
import { main } from './main';
import type { Runtime } from './runtime';

vi.mock('./server/kafka/connection', async (importOriginal) => ({
  ...(await importOriginal<typeof ConnectionModule>()),
  createKafkaClient: vi.fn(),
}));

describe('scenario interruption exit codes', () => {
  it.each([
    { step: 'produce', signal: 'SIGTERM', code: 143 },
    { step: 'expect', signal: 'SIGINT', code: 130 },
  ] as const)('returns $code when $signal interrupts a $step step', async ({ step, signal, code }) => {
    const directory = await mkdtemp(join(tmpdir(), 'studio-interruption-'));
    const controller = new AbortController();
    const stdout = vi.fn(() => true);
    const runtime: Runtime = {
      cwd: directory,
      argv: ['--scenario', 'scenario.json'],
      env: { KAFKA_CONFIG: join(directory, 'kafka.config.mjs') },
      platform: 'linux',
      stdout: { write: stdout },
      stderr: { write: () => true },
      now: () => new Date(),
      signal: controller.signal,
      exit: () => {
        throw new Error('Unexpected exit');
      },
    };
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      connect: async () => {},
      disconnect,
      fetchTopicOffsets: async () => [{ partition: 0, offset: 0n, low: 0n, high: 0n }],
    });
    const producer = createFakeProducer({
      connect: async () => {},
      disconnect,
      send: async () => {
        controller.abort(signal);
        throw new Error('Send interrupted');
      },
    });
    const consumer = createFakeMessageConsumer({
      connect: async () => {},
      disconnect,
      assign: async () => {},
      seek: () => {},
      stream: async function* () {
        controller.abort(signal);
        yield createFakeBatch({ topic: 'orders', partition: 0, highWatermark: 0n, fetchedOffset: 0n, messages: [] });
      },
    });
    const client = new Kafka({ brokers: ['localhost:9092'] });
    vi.spyOn(client, 'admin').mockReturnValue(admin as ReturnType<typeof client.admin>);
    vi.spyOn(client, 'producer').mockReturnValue(producer as ReturnType<typeof client.producer>);
    vi.spyOn(client, 'consumer').mockReturnValue(consumer as ReturnType<typeof client.consumer>);
    vi.mocked(createKafkaClient).mockReturnValue(client);
    try {
      await writeFile(join(directory, 'kafka.config.mjs'), 'export default {};');
      const scenarioStep =
        step === 'produce'
          ? { kind: step, topic: 'orders', value: 'event' }
          : { kind: step, topic: 'orders', predicates: [{ field: 'key', operator: 'eq', value: 'never' }] };
      await writeFile(
        join(directory, 'scenario.json'),
        JSON.stringify({ version: 1, name: 'interrupt', steps: [scenarioStep] }),
      );
      expect(await main(runtime)).toBe(code);
      expect(stdout).toHaveBeenCalledWith(expect.stringContaining('"status":"failed"'));
      expect(disconnect).toHaveBeenCalled();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
