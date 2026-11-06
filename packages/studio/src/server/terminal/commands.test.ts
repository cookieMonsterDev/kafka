import { PassThrough } from 'node:stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { runEmbeddedCli, embeddedCliConfig, type EmbeddedCliRuntime } from '@cookiemonsterdev/kafka-cli/embedded';
import { studioTerminalCommands } from './commands';
import { AdminPool } from '../kafka/admin-pool';
import { ProducerPool } from '../kafka/produce';
import { createFakeProducer } from '../kafka/create-fake-producer';
import { createFakeAdmin } from '../kafka/create-fake-admin';
import { createFakeMessageConsumer } from '../kafka/create-fake-message-consumer';
import { StudioEventBus } from '../kafka/events';
import { runTail } from '../kafka/tail';
vi.mock('../kafka/tail', () => ({ runTail: vi.fn() }));
afterEach(() => vi.clearAllMocks());

function testHost(readOnly = false) {
  let text = '';
  const write = (chunk: string) => {
    text += chunk;
    return true;
  };
  const send = vi.fn(async () => [
    { topicName: 'orders', partition: 0, errorCode: 0, baseOffset: 1n, logAppendTime: -1n, logStartOffset: 0n },
  ]);
  const pool = new AdminPool(() => ({
    admin: () => createFakeAdmin({ connect: async () => {}, disconnect: async () => {} }),
  }));
  const producers = new ProducerPool(() => ({
    producer: () => createFakeProducer({ connect: async () => {}, disconnect: async () => {}, send }),
  }));
  const commands = studioTerminalCommands({
    readOnly,
    profile: null,
    pool,
    producers,
    consumerFactory: () => ({ consumer: () => createFakeMessageConsumer({}) }),
    events: new StudioEventBus(),
  });
  const runtime: EmbeddedCliRuntime = {
    argv: [],
    env: {},
    cwd: '/',
    stdout: { write },
    stderr: { write },
    stdin: new PassThrough(),
    isTty: false,
    columns: 80,
    now: () => new Date(),
    exit: () => {
      throw new Error('No exit expected');
    },
    signal: new AbortController().signal,
    loadConfig: async () => embeddedCliConfig(null, null),
    openAdmin: async () => {
      throw new Error('No CLI admin expected');
    },
  };
  return {
    send,
    run: (argv: string[]) => runEmbeddedCli({ ...runtime, argv }, { readOnly, additionalCommands: commands }),
    output: () => text,
    dispose: async () => {
      await pool.disposeAll();
      await producers.disposeAll();
    },
  };
}

describe('Studio shell message helpers', () => {
  it('produces messages through the shared producer and preserves value literals', async () => {
    const host = testHost();
    try {
      expect(await host.run(['produce', 'orders', '--value', 'version', '--key', 'order-1'])).toBe(0);
      expect(host.send).toHaveBeenCalledWith(
        expect.objectContaining({
          topic: 'orders',
          messages: [expect.objectContaining({ value: 'version', key: 'order-1' })],
        }),
      );
    } finally {
      await host.dispose();
    }
  });
  it('blocks producing in read-only mode before opening a producer', async () => {
    const host = testHost(true);
    try {
      expect(await host.run(['produce', 'orders', '--value', 'x'])).not.toBe(0);
      expect(host.send).not.toHaveBeenCalled();
      expect(host.output()).toContain('read-only');
    } finally {
      await host.dispose();
    }
  });
  it('reports tail gaps without decoding them as message records', async () => {
    const host = testHost();
    vi.mocked(runTail).mockImplementation(async (_admin, _consumer, stream) => {
      stream.send('gap', { dropped: 8 });
      stream.send('message', {
        partition: 0,
        offset: '9007199254740993',
        timestamp: '1',
        key: null,
        value: Buffer.from('hello').toString('base64'),
        headers: {},
        size: 5,
      });
    });
    try {
      expect(await host.run(['consume', 'orders', '--tail'])).toBe(0);
      expect(host.output()).toContain('Dropped 8');
      expect(host.output()).toContain('hello');
      expect(host.output()).toContain('9007199254740993');
    } finally {
      await host.dispose();
    }
  });
});
