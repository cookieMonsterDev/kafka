import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { scramListCommand } from './list';

describe('scramListCommand', () => {
  it('describes every user when no positionals are given', async () => {
    const describeUserScramCredentials = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);

    expect(code).toBe(0);
    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: undefined });
  });

  it('passes explicit user names through', async () => {
    const describeUserScramCredentials = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);

    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: ['alice', 'bob'] });
  });

  it('renders "(no users)" when nothing matches', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({ results: [] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no users)');
  });

  it('renders one row per credential and formats the mechanism by name', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({
        results: [
          {
            user: 'alice',
            errorCode: 0,
            errorMessage: null,
            credentialInfos: [{ mechanism: 1, iterations: 8192 }],
          },
        ],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('alice');
    expect(written).toContain('SCRAM_SHA_256');
    expect(written).toContain('8192');
  });

  it('renders a user with no credentials as "(none)"', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({
        results: [{ user: 'alice', errorCode: 0, errorMessage: null, credentialInfos: [] }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(none)');
  });

  it('surfaces a per-user error in the STATUS column', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({
        results: [{ user: 'ghost', errorCode: 90, errorMessage: 'no such user', credentialInfos: [] }],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('no such user');
  });

  it('reports json output with the raw results shape', async () => {
    const results = [{ user: 'alice', errorCode: 0, errorMessage: null, credentialInfos: [] }];
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({ results }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await scramListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { results: unknown };
    expect(written.results).toEqual(results);
  });

  it('disconnects even when describeUserScramCredentials throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(scramListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
