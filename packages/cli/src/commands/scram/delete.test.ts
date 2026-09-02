import type { Admin } from '@cookiemonsterdev/kafka-core';
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

  it('deletes a single user with one call', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'alice', errorCode: 0, errorMessage: null }],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-512', yes: true },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    const code = await scramDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(alterUserScramCredentials).toHaveBeenCalledTimes(1);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({ deletions: [{ name: 'alice', mechanism: 2 }] });
  });

  it('fans out one call per user when more than one user is given', async () => {
    const alterUserScramCredentials = vi.fn(async () => ({
      results: [{ user: 'x', errorCode: 0, errorMessage: null }],
    }));
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    const code = await scramDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(alterUserScramCredentials).toHaveBeenCalledTimes(2);
    expect(alterUserScramCredentials).toHaveBeenCalledWith({ deletions: [{ name: 'alice', mechanism: 1 }] });
    expect(alterUserScramCredentials).toHaveBeenCalledWith({ deletions: [{ name: 'bob', mechanism: 1 }] });
  });

  it('returns exit 4 on a fanned-out partial failure — a user with no credential throws rather than returning an error row', async () => {
    const alterUserScramCredentials = vi.fn(async (options: Parameters<Admin['alterUserScramCredentials']>[0]) => {
      const name = options.deletions![0]!.name;
      if (name === 'bob') throw new Error('RESOURCE_NOT_FOUND');
      return { results: [{ user: name, errorCode: 0, errorMessage: null }] };
    });
    const admin = createFakeAdmin({ alterUserScramCredentials, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: ['alice', 'bob'],
      openAdmin: async () => admin,
    });

    expect(await scramDeleteCommand.run(context)).toBe(4);
  });

  it('propagates a single-user call failure rather than reporting a result row', async () => {
    const admin = createFakeAdmin({
      alterUserScramCredentials: async () => {
        throw new Error('RESOURCE_NOT_FOUND');
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', mechanism: 'scram-sha-256', yes: true },
      positionals: ['alice'],
      openAdmin: async () => admin,
    });

    await expect(scramDeleteCommand.run(context)).rejects.toThrow('RESOURCE_NOT_FOUND');
  });

  it('disconnects even when a single-user call throws', async () => {
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
