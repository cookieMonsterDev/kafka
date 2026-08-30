import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicDescribeCommand } from './describe';

describe('topicDescribeCommand', () => {
  it('describes the given topics and renders a partition table', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeTopicPartitions: async ({ topics }) => ({
        nextCursor: null,
        topics: topics.map((topic) => ({
          name: typeof topic === 'string' ? topic : topic.topic,
          topicId: Buffer.alloc(16),
          isInternal: false,
          topicAuthorizedOperations: 0,
          partitions: [
            {
              partitionIndex: 0,
              leader: 1,
              leaderEpoch: 0,
              replicas: [1, 2],
              isr: [1, 2],
              eligibleLeaderReplicas: null,
              lastKnownElr: null,
              offlineReplicas: [],
            },
          ],
        })),
      }),
      disconnect,
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicDescribeCommand.run(context);

    expect(code).toBe(0);
    expect(disconnect).toHaveBeenCalledTimes(1);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('orders'));
  });

  it('serializes the topicId as a UUID in JSON output', async () => {
    const admin = createFakeAdmin({
      describeTopicPartitions: async ({ topics }) => ({
        nextCursor: null,
        topics: topics.map((topic) => ({
          name: typeof topic === 'string' ? topic : topic.topic,
          topicId: Buffer.alloc(16, 1),
          isInternal: false,
          topicAuthorizedOperations: 0,
          partitions: [],
        })),
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await topicDescribeCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    const parsed = JSON.parse(written) as { topics: { topicId: string }[] };
    expect(parsed.topics[0]?.topicId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
  });

  it('requires at least one topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: [] });
    await expect(topicDescribeCommand.run(context)).rejects.toThrow(/at least one topic/);
  });

  it('throws a usage error when --brokers is missing', async () => {
    const { context } = createFakeCommandContext({ positionals: ['orders'] });
    await expect(topicDescribeCommand.run(context)).rejects.toThrow(/--brokers/);
  });
});
