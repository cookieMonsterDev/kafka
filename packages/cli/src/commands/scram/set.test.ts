import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { createFakeStdin } from '../../testing/create-fake-stdin';
import { scramSetCommand } from './set';

describe('scramSetCommand', () => {
  it('requires at least one user name', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: [],
    });
    await expect(scramSetCommand.run(context)).rejects.toThrow(/at least one user/);
  });

  it('requires --mechanism', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'password-stdin': true },
      positionals: ['alice'],
    });
    await expect(scramSetCommand.run(context)).rejects.toThrow(/--mechanism/);
  });

  it('requires --password-stdin — a plain password flag is never accepted', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256' },
      positionals: ['alice'],
    });
    await expect(scramSetCommand.run(context)).rejects.toThrow(CliUsageError);
    await expect(scramSetCommand.run(context)).rejects.toThrow(/--password-stdin/);
  });

  it('rejects an empty password read from stdin', async () => {
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice'],
      stdin,
    });
    const promise = scramSetCommand.run(context);
    stdin.emitEnd();
    await expect(promise).rejects.toThrow(/empty password/);
  });

  it('reads the password from stdin and upserts every positional with the same credential', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [
        { user: 'alice', errorCode: 0, errorMessage: null },
        { user: 'bob', errorCode: 0, errorMessage: null },
      ],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2\n');
    stdin.emitEnd();
    const code = await promise;

    expect(code).toBe(0);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      upsertions: [
        { name: 'alice', mechanism: 1, iterations: undefined, password: 'hunter2' },
        { name: 'bob', mechanism: 1, iterations: undefined, password: 'hunter2' },
      ],
    });
  });

  it('passes --iterations through when given', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'alice', errorCode: 0, errorMessage: null }],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        mechanism: 'scram-sha-512',
        iterations: 8192,
        'password-stdin': true,
      },
      positionals: ['alice'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2');
    stdin.emitEnd();
    await promise;

    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      upsertions: [{ name: 'alice', mechanism: 2, iterations: 8192, password: 'hunter2' }],
    });
  });

  it('never echoes the password back on stdout in human output', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => ({ results: [{ user: 'alice', errorCode: 0, errorMessage: null }] }),
      disconnect: async () => {},
    });
    const stdin = createFakeStdin();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2');
    stdin.emitEnd();
    await promise;

    expect(stdoutWrite.mock.calls[0]![0]).not.toContain('hunter2');
  });

  it('never echoes the password back in json output', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => ({ results: [{ user: 'alice', errorCode: 0, errorMessage: null }] }),
      disconnect: async () => {},
    });
    const stdin = createFakeStdin();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice'],
      openAdmin: async () => admin,
      stdin,
      format: 'json',
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2');
    stdin.emitEnd();
    await promise;

    expect(stdoutWrite.mock.calls[0]![0]).not.toContain('hunter2');
  });

  it('derives a partial failure (exit 4) when one user fails', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => ({
        results: [
          { user: 'alice', errorCode: 0, errorMessage: null },
          { user: 'bob', errorCode: 58, errorMessage: 'unsupported SASL mechanism' },
        ],
      }),
      disconnect: async () => {},
    });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2');
    stdin.emitEnd();
    expect(await promise).toBe(4);
  });

  it('disconnects even when alterUserScramCredentials throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2');
    stdin.emitEnd();

    await expect(promise).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
