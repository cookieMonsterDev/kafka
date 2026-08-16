import { describe, expect, it, vi } from 'vitest';
import type { Cluster } from '../cluster/index';
import { KafkaNonRetriableError } from '../errors';
import { createLogger, LOG_LEVELS } from '../loggers/index';
import { createConsumer, events } from './index';

const silentLogger = createLogger({ level: LOG_LEVELS.NOTHING, logCreator: () => () => {} });

function fakeCluster(): Cluster {
  return {
    connect: vi.fn(async () => undefined),
    disconnect: vi.fn(async () => undefined),
    addMultipleTargetTopics: vi.fn(async () => undefined),
    metadata: vi.fn(async () => ({ topicMetadata: [] })),
    findGroupCoordinator: vi.fn(),
    removeBroker: vi.fn(),
  } as unknown as Cluster;
}

describe('consumer', () => {
  it('throws when heartbeatInterval is greater than or equal to sessionTimeout', () => {
    expect(() =>
      createConsumer({
        cluster: fakeCluster(),
        logger: silentLogger,
        groupId: 'test-group-id',
        heartbeatInterval: 10_000,
        sessionTimeout: 10_000,
      }),
    ).toThrow(KafkaNonRetriableError);
  });

  it('throws when groupId is missing', () => {
    expect(() =>
      createConsumer({
        cluster: fakeCluster(),
        logger: silentLogger,
        groupId: '',
      }),
    ).toThrow('Consumer groupId must be a non-empty string.');
  });

  it('exposes a namespaced logger', () => {
    const consumer = createConsumer({
      cluster: fakeCluster(),
      groupId: 'test-consumer',
      logger: silentLogger,
    });
    expect(consumer.logger()).toBeDefined();
    expect(typeof consumer.logger().info).toBe('function');
  });

  it('rejects an unknown event name', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.on('NON_EXISTENT_EVENT' as never, () => {})).toThrow(
      /Event name should be one of consumer\.events\./,
    );
  });

  it('exposes the public events map', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(consumer.events).toBe(events);
  });

  it('throws when seeking before run()', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.seek({ topic: 't', partition: 0, offset: 1n })).toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
  });

  it('throws when committing offsets before run()', async () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    await expect(consumer.commitOffsets([{ topic: 't', partition: 0, offset: 1n }])).rejects.toThrow(
      'Consumer group was not initialized, consumer#run must be called first',
    );
  });

  it('rejects a negative seek offset that is not earliest/latest', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(() => consumer.seek({ topic: 't', partition: 0, offset: -5n })).toThrow(
      'Offset must not be a negative number',
    );
  });

  it('paused() returns an empty list before run()', () => {
    const consumer = createConsumer({ cluster: fakeCluster(), groupId: 'g', logger: silentLogger });
    expect(consumer.paused()).toEqual([]);
  });

  it('rejects connect when the signal is already aborted', async () => {
    const connect = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), connect } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });
    await expect(consumer.connect({ signal: AbortSignal.abort() })).rejects.toThrow(/aborted/i);
    expect(connect).not.toHaveBeenCalled();
  });

  it('disconnects through Symbol.asyncDispose', async () => {
    const disconnect = vi.fn(async () => undefined);
    const cluster = { ...fakeCluster(), disconnect } as unknown as Cluster;
    const consumer = createConsumer({ cluster, groupId: 'g', logger: silentLogger });
    await consumer[Symbol.asyncDispose]();
    expect(disconnect).toHaveBeenCalled();
  });
});
