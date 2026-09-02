import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterLogDirsCommand } from './log-dirs';

function fakeLogDirs() {
  return {
    brokers: [
      {
        brokerId: 1,
        logDirs: [
          {
            errorCode: 0,
            logDir: '/data/kafka-1',
            topics: [
              {
                topic: 'orders',
                partitions: [{ partition: 0, size: 1024n, offsetLag: 0n, isFuture: false }],
              },
              {
                topic: 'payments',
                partitions: [{ partition: 0, size: 2048n, offsetLag: 5n, isFuture: false }],
              },
            ],
          },
        ],
      },
    ],
  };
}

describe('clusterLogDirsCommand', () => {
  it('renders one row per topic partition across every log dir', async () => {
    const describeLogDirs = async () => fakeLogDirs();
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await clusterLogDirsCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('/data/kafka-1');
    expect(written).toContain('orders');
    expect(written).toContain('payments');
    expect(written).toContain('1024');
  });

  it('passes --broker through as numeric brokerIds', async () => {
    const describeLogDirs = vi.fn(async () => fakeLogDirs());
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', broker: [1, 2] },
      openAdmin: async () => admin,
    });

    await clusterLogDirsCommand.run(context);

    expect(describeLogDirs).toHaveBeenCalledWith({ brokerIds: [1, 2] });
  });

  it('filters to matching topics only, client-side, in both human and json output', async () => {
    const describeLogDirs = async () => fakeLogDirs();
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['orders'] },
      openAdmin: async () => admin,
      format: 'json',
    });

    await clusterLogDirsCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as {
      brokers: { logDirs: { topics: { topic: string }[] }[] }[];
    };
    const topics = written.brokers[0]!.logDirs[0]!.topics;
    expect(topics.map((t) => t.topic)).toEqual(['orders']);
  });

  it('renders a placeholder when nothing matches', async () => {
    const describeLogDirs = async () => ({ brokers: [] });
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterLogDirsCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no matching log dirs)');
  });
});
