import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createShareConsumer } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(): Cluster {
  return {
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
  } as unknown as Cluster;
}

describe('share-consumer', () => {
  it('rejects an empty subscribe list', () => {
    const consumer = createShareConsumer({
      cluster: fakeCluster(),
      groupId: 'share-1',
      logger: silentLogger,
    });
    expect(() => consumer.subscribe({ topics: [] })).toThrow(KafkaNonRetriableError);
  });

  it('rejects run before subscribe', async () => {
    const consumer = createShareConsumer({
      cluster: fakeCluster(),
      groupId: 'share-1',
      logger: silentLogger,
    });
    await expect(consumer.run({ eachMessage: async () => undefined })).rejects.toThrow('must subscribe before run()');
  });

  it('rejects run without eachMessage or eachBatch', async () => {
    const consumer = createShareConsumer({
      cluster: fakeCluster(),
      groupId: 'share-1',
      logger: silentLogger,
    });
    consumer.subscribe({ topics: ['events'] });
    await expect(consumer.run({})).rejects.toThrow('requires eachMessage or eachBatch');
  });

  it('exposes a namespaced logger', () => {
    const consumer = createShareConsumer({
      cluster: fakeCluster(),
      groupId: 'share-1',
      logger: silentLogger,
    });
    expect(typeof consumer.logger().info).toBe('function');
  });

  it('rejects connect when the signal is already aborted', async () => {
    const connect = vi.fn(async () => undefined);
    const cluster = { connect, disconnect: vi.fn(async () => undefined) } as unknown as Cluster;
    const consumer = createShareConsumer({ cluster, groupId: 'share-1', logger: silentLogger });
    await expect(consumer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    expect(connect).not.toHaveBeenCalled();
  });

  it('disconnects through Symbol.asyncDispose without starting a runner', async () => {
    const disconnect = vi.fn(async () => undefined);
    const cluster = { connect: vi.fn(async () => undefined), disconnect } as unknown as Cluster;
    const consumer = createShareConsumer({ cluster, groupId: 'share-1', logger: silentLogger });
    await consumer[Symbol.asyncDispose]();
    expect(disconnect).toHaveBeenCalled();
  });
});
