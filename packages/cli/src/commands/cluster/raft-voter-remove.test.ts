import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterRaftVoterRemoveCommand } from './raft-voter-remove';

const VOTER_DIRECTORY_ID = '3c48b6f0-1234-4a5b-8c9d-0123456789ab';

describe('clusterRaftVoterRemoveCommand', () => {
  it('requires --voter-id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-directory-id': VOTER_DIRECTORY_ID, yes: true },
    });

    await expect(clusterRaftVoterRemoveCommand.run(context)).rejects.toThrow(/requires --voter-id/);
  });

  it('requires --voter-directory-id', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', 'voter-id': 4, yes: true } });

    await expect(clusterRaftVoterRemoveCommand.run(context)).rejects.toThrow(/requires --voter-directory-id/);
  });

  it('rejects a malformed --voter-directory-id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-id': 4, 'voter-directory-id': 'not-a-uuid', yes: true },
    });

    await expect(clusterRaftVoterRemoveCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-id': 4, 'voter-directory-id': VOTER_DIRECTORY_ID },
    });

    await expect(clusterRaftVoterRemoveCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('removes the voter and calls removeRaftVoter with the expected shape', async () => {
    const removeRaftVoter = vi.fn(async () => {});
    const admin = createFakeAdmin({ removeRaftVoter, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': VOTER_DIRECTORY_ID,
        'cluster-id': 'my-cluster',
        yes: true,
      },
      openAdmin: async () => admin,
    });

    const code = await clusterRaftVoterRemoveCommand.run(context);

    expect(code).toBe(0);
    expect(removeRaftVoter).toHaveBeenCalledWith({
      clusterId: 'my-cluster',
      voterId: 4,
      voterDirectoryId: Buffer.from(VOTER_DIRECTORY_ID.replaceAll('-', ''), 'hex'),
    });
    expect(stdoutWrite.mock.calls[0]![0]).toContain('Voter 4 removed');
  });

  it('disconnects even when removeRaftVoter throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      removeRaftVoter: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-id': 4, 'voter-directory-id': VOTER_DIRECTORY_ID, yes: true },
      openAdmin: async () => admin,
    });

    await expect(clusterRaftVoterRemoveCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
