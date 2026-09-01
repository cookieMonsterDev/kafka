import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicProducersCommand } from './producers';

describe('topicProducersCommand', () => {
  it('requires a topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(topicProducersCommand.run(context)).rejects.toThrow(/requires a topic name/);
  });

  it('queries the given partitions directly when --partition is provided', async () => {
    const describeProducers = vi.fn(async () => [
      {
        topic: 'orders',
        partition: 1,
        activeProducers: [
          {
            producerId: 42n,
            producerEpoch: 0,
            lastSequence: 3,
            lastTimestamp: 1000n,
            coordinatorEpoch: 0,
            currentTransactionStartOffset: null,
          },
        ],
      },
    ]);
    const admin = createFakeAdmin({ describeProducers, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partition: [1] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicProducersCommand.run(context);

    expect(code).toBe(0);
    expect(describeProducers).toHaveBeenCalledWith({ topicPartitions: [{ topic: 'orders', partitions: [1] }] });
  });

  it('passes --broker-id through when given', async () => {
    const describeProducers = vi.fn(async () => []);
    const admin = createFakeAdmin({ describeProducers, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partition: [0], 'broker-id': '2' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicProducersCommand.run(context);
    expect(describeProducers).toHaveBeenCalledWith({
      topicPartitions: [{ topic: 'orders', partitions: [0] }],
      brokerId: '2',
    });
  });

  it('auto-discovers every partition when --partition is omitted', async () => {
    const describeTopicPartitions = vi.fn(async () => ({
      topics: [
        {
          name: 'orders',
          topicId: Buffer.alloc(16),
          isInternal: false,
          topicAuthorizedOperations: 0,
          partitions: [
            {
              partitionIndex: 0,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1],
              isr: [1],
              eligibleLeaderReplicas: null,
              lastKnownElr: null,
              offlineReplicas: [],
            },
            {
              partitionIndex: 1,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1],
              isr: [1],
              eligibleLeaderReplicas: null,
              lastKnownElr: null,
              offlineReplicas: [],
            },
          ],
        },
      ],
      nextCursor: null,
    }));
    const describeProducers = vi.fn(async () => []);
    const admin = createFakeAdmin({ describeTopicPartitions, describeProducers, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicProducersCommand.run(context);
    expect(describeProducers).toHaveBeenCalledWith({ topicPartitions: [{ topic: 'orders', partitions: [0, 1] }] });
  });

  it('reports "(no partitions)" and skips describeProducers when the topic has none', async () => {
    const describeTopicPartitions = vi.fn(async () => ({
      topics: [
        { name: 'orders', topicId: Buffer.alloc(16), isInternal: false, topicAuthorizedOperations: 0, partitions: [] },
      ],
      nextCursor: null,
    }));
    const describeProducers = vi.fn(async () => []);
    const admin = createFakeAdmin({ describeTopicPartitions, describeProducers, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicProducersCommand.run(context);

    expect(code).toBe(0);
    expect(describeProducers).not.toHaveBeenCalled();
    expect(stdoutWrite.mock.calls[0]?.[0]).toContain('no partitions');
  });

  it('renders active producer state as a human table', async () => {
    const describeProducers = vi.fn(async () => [
      {
        topic: 'orders',
        partition: 0,
        activeProducers: [
          {
            producerId: 42n,
            producerEpoch: 1,
            lastSequence: 3,
            lastTimestamp: 1700000000000n,
            coordinatorEpoch: 0,
            currentTransactionStartOffset: null,
          },
        ],
      },
    ]);
    const admin = createFakeAdmin({ describeProducers, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partition: [0] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await topicProducersCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).toContain('PRODUCER_ID');
    expect(written).toContain('42');
  });

  it('renders a bigint producerId as a JSON string', async () => {
    const describeProducers = vi.fn(async () => [
      {
        topic: 'orders',
        partition: 0,
        activeProducers: [
          {
            producerId: 9007199254740993n,
            producerEpoch: 0,
            lastSequence: 0,
            lastTimestamp: 0n,
            coordinatorEpoch: 0,
            currentTransactionStartOffset: null,
          },
        ],
      },
    ]);
    const admin = createFakeAdmin({ describeProducers, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partition: [0] },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await topicProducersCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).toContain('"9007199254740993"');
  });

  it('disconnects even when describeProducers throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeProducers: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', partition: [0] },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicProducersCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
