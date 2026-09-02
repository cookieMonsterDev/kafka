import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { clusterRaftVoterAddCommand } from './raft-voter-add';

const VOTER_DIRECTORY_ID = '3c48b6f0-1234-4a5b-8c9d-0123456789ab';

describe('clusterRaftVoterAddCommand', () => {
  it('requires --voter-id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-directory-id': VOTER_DIRECTORY_ID, listener: ['C=localhost:9093'] },
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow(/requires --voter-id/);
  });

  it('requires --voter-directory-id', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-id': 4, listener: ['C=localhost:9093'] },
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow(/requires --voter-directory-id/);
  });

  it('rejects a malformed --voter-directory-id', async () => {
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': 'not-a-uuid',
        listener: ['C=localhost:9093'],
      },
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('requires at least one --listener', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'voter-id': 4, 'voter-directory-id': VOTER_DIRECTORY_ID },
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow(/at least one --listener/);
  });

  it('rejects a malformed --listener', async () => {
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': VOTER_DIRECTORY_ID,
        listener: ['bogus'],
      },
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('parses --listener entries and calls addRaftVoter with the expected shape', async () => {
    const addRaftVoter = vi.fn(async () => {});
    const admin = createFakeAdmin({ addRaftVoter, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': VOTER_DIRECTORY_ID,
        listener: ['CONTROLLER=localhost:9093'],
        'cluster-id': 'my-cluster',
        'ack-when-committed': true,
      },
      openAdmin: async () => admin,
    });

    const code = await clusterRaftVoterAddCommand.run(context);

    expect(code).toBe(0);
    expect(addRaftVoter).toHaveBeenCalledWith({
      clusterId: 'my-cluster',
      timeoutMs: undefined,
      voterId: 4,
      voterDirectoryId: Buffer.from(VOTER_DIRECTORY_ID.replaceAll('-', ''), 'hex'),
      listeners: [{ name: 'CONTROLLER', host: 'localhost', port: 9093 }],
      ackWhenCommitted: true,
    });
  });

  it('--dry-run prints the plan and never opens an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': VOTER_DIRECTORY_ID,
        listener: ['CONTROLLER=localhost:9093'],
        'dry-run': true,
      },
      openAdmin,
    });

    const code = await clusterRaftVoterAddCommand.run(context);

    expect(code).toBe(0);
    expect(openAdmin).not.toHaveBeenCalled();
    expect(stdoutWrite.mock.calls[0]![0]).toContain('CONTROLLER=localhost:9093');
  });

  it('disconnects even when addRaftVoter throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      addRaftVoter: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'voter-id': 4,
        'voter-directory-id': VOTER_DIRECTORY_ID,
        listener: ['CONTROLLER=localhost:9093'],
      },
      openAdmin: async () => admin,
    });

    await expect(clusterRaftVoterAddCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
