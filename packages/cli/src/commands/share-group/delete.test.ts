import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { shareGroupDeleteCommand } from './delete';

function deleteGroupsError(groups: { groupId: string; errorCode: number; error?: { message?: string } }[]): Error {
  return Object.assign(new Error('Error in DeleteShareGroups'), { name: 'KafkaDeleteGroupsError', groups });
}

describe('shareGroupDeleteCommand', () => {
  it('requires at least one group id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', yes: true }, positionals: [] });
    await expect(shareGroupDeleteCommand.run(context)).rejects.toThrow(/at least one group/);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: ['g1'] });
    await expect(shareGroupDeleteCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const deleteShareGroups = vi.fn(async () => []);
    const admin = createFakeAdmin({ deleteShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['g1'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await shareGroupDeleteCommand.run(context)).toBe(0);
  });

  it('deletes every group in a single call', async () => {
    const deleteShareGroups = vi.fn(async () => [
      { groupId: 'g1', errorCode: 0 },
      { groupId: 'g2', errorCode: 0 },
    ]);
    const admin = createFakeAdmin({ deleteShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    const code = await shareGroupDeleteCommand.run(context);

    expect(code).toBe(0);
    expect(deleteShareGroups).toHaveBeenCalledTimes(1);
    expect(deleteShareGroups).toHaveBeenCalledWith(['g1', 'g2']);
  });

  it('derives a partial failure (exit 4) from KafkaDeleteGroupsError.groups', async () => {
    const deleteShareGroups = vi.fn(async () => {
      throw deleteGroupsError([{ groupId: 'g2', errorCode: 25 }]);
    });
    const admin = createFakeAdmin({ deleteShareGroups, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await shareGroupDeleteCommand.run(context);

    expect(code).toBe(4);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { results: { groupId: string; ok: boolean }[] };
    expect(written.results).toEqual([
      { groupId: 'g1', ok: true },
      { groupId: 'g2', ok: false, detail: 'failed (code 25)' },
    ]);
  });

  it('returns exit 1 when KafkaDeleteGroupsError names every requested group', async () => {
    const deleteShareGroups = vi.fn(async () => {
      throw deleteGroupsError([
        { groupId: 'g1', errorCode: 25 },
        { groupId: 'g2', errorCode: 25 },
      ]);
    });
    const admin = createFakeAdmin({ deleteShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['g1', 'g2'],
      openAdmin: async () => admin,
    });

    expect(await shareGroupDeleteCommand.run(context)).toBe(1);
  });

  it('rethrows a non-KafkaDeleteGroupsError failure', async () => {
    const deleteShareGroups = vi.fn(async () => {
      throw new Error('connection reset');
    });
    const admin = createFakeAdmin({ deleteShareGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupDeleteCommand.run(context)).rejects.toThrow('connection reset');
  });

  it('disconnects even when deleteShareGroups throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteShareGroups: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['g1'],
      openAdmin: async () => admin,
    });

    await expect(shareGroupDeleteCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
