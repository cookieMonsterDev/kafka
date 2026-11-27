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

  it('renders each log dir with its volume sizes and cordoning', async () => {
    const logDirs = fakeLogDirs();
    const [logDir] = logDirs.brokers[0]!.logDirs;
    const describeLogDirs = async () => ({
      brokers: [
        {
          brokerId: 1,
          logDirs: [
            { ...logDir!, totalBytes: 107374182400n, usableBytes: 53687091200n, isCordoned: true },
            { errorCode: 0, logDir: '/data/kafka-old', topics: [] },
          ],
        },
      ],
    });
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await clusterLogDirsCommand.run(context);

    const lines = stdoutWrite.mock.calls[0]![0].split('\n');
    expect(lines[0]).toMatch(/BROKER\s+LOG_DIR\s+TOTAL_BYTES\s+USABLE_BYTES\s+CORDONED/);
    expect(lines.find((line) => line.includes('/data/kafka-1'))).toMatch(/107374182400\s+53687091200\s+true/);
    expect(lines.find((line) => line.includes('/data/kafka-old'))).toMatch(/-\s+-\s+-/);
  });

  it('keeps the log dirs when no partition matches the topic filter', async () => {
    const describeLogDirs = async () => fakeLogDirs();
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', topic: ['missing'] },
      openAdmin: async () => admin,
    });

    await clusterLogDirsCommand.run(context);

    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('/data/kafka-1');
    expect(written).toContain('(no matching partitions)');
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

  it('treats empty --broker and --topic lists, as the parser gives them, as no filter', async () => {
    const describeLogDirs = vi.fn(async () => fakeLogDirs());
    const admin = createFakeAdmin({ describeLogDirs, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', broker: [], topic: [] },
      openAdmin: async () => admin,
    });

    await clusterLogDirsCommand.run(context);

    expect(describeLogDirs).toHaveBeenCalledWith({});
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('orders');
    expect(written).toContain('payments');
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
