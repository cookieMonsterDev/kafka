import type { Admin } from '@cookiemonsterdev/kafka-core';
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

  it('reads the password from stdin and sets it for a single user with one call', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'alice', errorCode: 0, errorMessage: null }],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', 'password-stdin': true },
      positionals: ['alice'],
      openAdmin: async () => admin,
      stdin,
    });

    const promise = scramSetCommand.run(context);
    stdin.emitData('hunter2\n');
    stdin.emitEnd();
    const code = await promise;

    expect(code).toBe(0);
    expect(alterUserScramCredentials).toHaveBeenCalledTimes(1);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      upsertions: [{ name: 'alice', mechanism: 1, iterations: undefined, password: 'hunter2' }],
    });
  });

  it('fans out one call per user when more than one user is given, reusing the same password', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'x', errorCode: 0, errorMessage: null }],
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
    stdin.emitData('hunter2');
    stdin.emitEnd();
    await promise;

    expect(alterUserScramCredentials).toHaveBeenCalledTimes(2);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      upsertions: [{ name: 'alice', mechanism: 1, iterations: undefined, password: 'hunter2' }],
    });
    expect(alterUserScramCredentials).toHaveBeenCalledWith({
      upsertions: [{ name: 'bob', mechanism: 1, iterations: undefined, password: 'hunter2' }],
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

  it('returns exit 4 on a fanned-out partial failure', async () => {
    const alterUserScramCredentials = vi.fn(async (options: Parameters<Admin['alterUserScramCredentials']>[0]) => {
      const name = options.upsertions![0]!.name;
      if (name === 'bob') throw new Error('unsupported SASL mechanism');
      return { results: [{ user: name, errorCode: 0, errorMessage: null }] };
    });
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
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

  it('propagates a single-user call failure rather than reporting a result row', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => {
        throw new Error('unsupported SASL mechanism');
      },
      disconnect: async () => {},
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
    await expect(promise).rejects.toThrow('unsupported SASL mechanism');
  });

  it('disconnects even when a single-user call throws', async () => {
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
