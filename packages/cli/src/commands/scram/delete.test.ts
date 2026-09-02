import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { scramDeleteCommand } from './delete';

describe('scramDeleteCommand', () => {
  it('requires at least one user name', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: [],
    });
    await expect(scramDeleteCommand.run(context)).rejects.toThrow(/at least one user/);
  });

  it('requires --mechanism', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['alice'],
    });
    await expect(scramDeleteCommand.run(context)).rejects.toThrow(/--mechanism/);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256' },
      positionals: ['alice'],
    });
    await expect(scramDeleteCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'alice', errorCode: 0, errorMessage: null }],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256' },
      positionals: ['alice'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await scramDeleteCommand.run(context)).toBe(0);
  });

  it('deletes the given mechanism for every positional in one call', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [
        { user: 'alice', errorCode: 0, errorMessage: null },
        { user: 'bob', errorCode: 0, errorMessage: null },
      ],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-512', yes: true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    const code = await scramDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      deletions: [
        { name: 'alice', mechanism: 2 },
        { name: 'bob', mechanism: 2 },
      ],
    });
  });

  it('derives a partial failure (exit 4) when one deletion fails', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => ({
        results: [
          { user: 'alice', errorCode: 0, errorMessage: null },
          { user: 'bob', errorCode: 90, errorMessage: 'no such user' },
        ],
      }),
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    expect(await scramDeleteCommand.run(context)).toBe(4);
  });

  it('disconnects even when alterUserScramCredentials throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    await expect(scramDeleteCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
