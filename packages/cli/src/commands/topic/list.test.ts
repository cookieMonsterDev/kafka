import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { topicListCommand } from './list';

describe('topicListCommand', () => {
  it('lists topics as a human table', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({ listTopics: async () => ['orders', 'payments'], disconnect });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await topicListCommand.run(context);

    expect(code).toBe(0);
    expect(disconnect).toHaveBeenCalledTimes(1);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('orders'));
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('payments'));
  });

  it('reports "(no topics)" in human mode when there are none', async () => {
    const admin = createFakeAdmin({ listTopics: async () => [], disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await topicListCommand.run(context);
    expect(stdoutWrite).toHaveBeenCalledWith('(no topics)\n');
  });

  it('emits a JSON document with the topics array', async () => {
    const admin = createFakeAdmin({ listTopics: async () => ['orders'], disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await topicListCommand.run(context);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(JSON.parse(written)).toEqual({ topics: ['orders'] });
  });

  it('omits brokers from openAdmin when --brokers is missing, deferring to a lower config layer', async () => {
    const admin = createFakeAdmin({ listTopics: async () => [], disconnect: async () => {} });
    const { context, openAdmin } = createFakeCommandContext({ openAdmin: async () => admin });

    await topicListCommand.run(context);

    expect(openAdmin).toHaveBeenCalledWith(expect.objectContaining({ brokers: undefined }));
  });
});
