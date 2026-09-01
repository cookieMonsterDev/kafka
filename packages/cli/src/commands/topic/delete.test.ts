import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { topicDeleteCommand } from './delete';

function unknownTopicError(): Error {
  return Object.assign(new Error('This server does not host this topic-partition'), {
    name: 'KafkaProtocolError',
    type: 'UNKNOWN_TOPIC_OR_PARTITION',
  });
}

describe('topicDeleteCommand', () => {
  it('deletes a single topic with --yes', async () => {
    const deleteTopics = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(deleteTopics).toHaveBeenCalledWith({ topics: ['orders'] });
  });

  it('requires at least one topic name', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, positionals: [] });
    await expect(topicDeleteCommand.run(context)).rejects.toThrow(/at least one topic/);
  });

  it('aborts without --yes off a TTY (the fake context is never a TTY)', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: ['orders'] });
    await expect(topicDeleteCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation entirely when cli.confirmDestructive is false', async () => {
    const deleteTopics = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['orders'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(0);
  });

  it('requires --force to delete an internal topic, even with --yes', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['__consumer_offsets'],
    });
    await expect(topicDeleteCommand.run(context)).rejects.toThrow(/--force/);
  });

  it('cli.confirmDestructive: false does not waive the --force tier for an internal topic', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['__consumer_offsets'],
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });
    await expect(topicDeleteCommand.run(context)).rejects.toThrow(/--force/);
  });

  it('deletes an internal topic once --yes and --force are both given', async () => {
    const deleteTopics = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, force: true },
      positionals: ['__consumer_offsets'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(0);
  });

  it('requires --force for more than 10 topics in one call', async () => {
    const topics = Array.from({ length: 11 }, (_, i) => `topic-${i}`);
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: topics,
    });
    await expect(topicDeleteCommand.run(context)).rejects.toThrow(/--force/);
  });

  it('allows exactly 10 topics without --force', async () => {
    const deleteTopics = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const topics = Array.from({ length: 10 }, (_, i) => `topic-${i}`);
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: topics,
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(0);
  });

  it('rethrows a single-topic failure when --if-exists is not set', async () => {
    const admin = createFakeAdmin({
      deleteTopics: async () => {
        throw unknownTopicError();
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicDeleteCommand.run(context)).rejects.toThrow();
  });

  it('treats a missing single topic as success with --if-exists', async () => {
    const admin = createFakeAdmin({
      deleteTopics: async () => {
        throw unknownTopicError();
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'if-exists': true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(0);
  });

  it('fans out one call per topic when more than one topic is given', async () => {
    const deleteTopics = vi.fn(async () => {});
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(deleteTopics).toHaveBeenCalledTimes(2);
    expect(deleteTopics).toHaveBeenCalledWith({ topics: ['orders'] });
    expect(deleteTopics).toHaveBeenCalledWith({ topics: ['payments'] });
  });

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const deleteTopics = vi.fn(async ({ topics }: { topics: string[] }) => {
      if (topics[0] === 'payments') throw new Error('boom');
    });
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const deleteTopics = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(1);
  });

  it('honors --if-exists across a fanned-out batch', async () => {
    const deleteTopics = vi.fn(async ({ topics }: { topics: string[] }) => {
      if (topics[0] === 'payments') throw unknownTopicError();
    });
    const admin = createFakeAdmin({ deleteTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, 'if-exists': true },
      positionals: ['orders', 'payments'],
      openAdmin: async () => admin,
    });

    const code = await topicDeleteCommand.run(context);
    expect(code).toBe(0);
  });

  it('disconnects even when a single-topic delete throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteTopics: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['orders'],
      openAdmin: async () => admin,
    });

    await expect(topicDeleteCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
