import type { Admin } from '@cookiemonsterdev/kafka-core';
import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { scramListCommand } from './list';

function fakeResult(user: string, credentialInfos: { mechanism: number; iterations: number }[] = []) {
  return { user, errorCode: 0, errorMessage: null, credentialInfos };
}

describe('scramListCommand', () => {
  it('describes every user with one call when no positionals are given', async () => {
    const describeUserScramCredentials = vi.fn(async () => ({ results: [] }));
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);

    expect(code).toBe(0);
    expect(describeUserScramCredentials).toHaveBeenCalledTimes(1);
    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: undefined });
  });

  it('describes a single named user with one call', async () => {
    const describeUserScramCredentials = vi.fn(async () => ({ results: [fakeResult('alice')] }));
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);

    expect(code).toBe(0);
    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: ['alice'] });
  });

  it('fans out one call per user when more than one user is given', async () => {
    const describeUserScramCredentials = vi.fn(
      async (options: Parameters<Admin['describeUserScramCredentials']>[0] = {}) => ({
        results: [fakeResult(options.users![0]!)],
      }),
    );
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);

    expect(code).toBe(0);
    expect(describeUserScramCredentials).toHaveBeenCalledTimes(2);
    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: ['alice'] });
    expect(describeUserScramCredentials).toHaveBeenCalledWith({ users: ['bob'] });
  });

  it('returns exit 4 on a fanned-out partial failure — a user with no credential throws rather than returning an error row', async () => {
    const describeUserScramCredentials = vi.fn(
      async (options: Parameters<Admin['describeUserScramCredentials']>[0] = {}) => {
        if (options.users![0] === 'ghost') throw new Error('RESOURCE_NOT_FOUND');
        return { results: [fakeResult(options.users![0]!)] };
      },
    );
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice', 'ghost'],
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);
    expect(code).toBe(4);
  });

  it('returns exit 1 when every fanned-out call fails', async () => {
    const describeUserScramCredentials = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ describeUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);
    expect(code).toBe(1);
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
        results: [fakeResult('alice', [{ mechanism: 1, iterations: 8192 }])],
      }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
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
      describeUserScramCredentials: async () => ({ results: [fakeResult('alice')] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    await scramListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(none)');
  });

  it('treats a missing result as a failure for that user', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({ results: [] }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    const code = await scramListCommand.run(context);
    expect(code).toBe(1);
  });

  it('reports json output with the resolved per-user shape', async () => {
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => ({ results: [fakeResult('alice')] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await scramListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { results: { user: string; ok: boolean }[] };
    expect(written.results).toEqual([{ user: 'alice', ok: true, credentials: [] }]);
  });

  it('disconnects even when describeUserScramCredentials throws for a single user', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeUserScramCredentials: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    await expect(scramListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });

  it('disconnects even when listing every user throws', async () => {
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
