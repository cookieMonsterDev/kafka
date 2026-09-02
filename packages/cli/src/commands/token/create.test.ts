import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { tokenCreateCommand } from './create';

const RESULT = {
  owner: { principalType: 'User', name: 'alice' },
  issueTimestamp: 1n,
  expiryTimestamp: 2n,
  maxTimestamp: 3n,
  tokenId: 'token-1',
  hmac: Buffer.from('secret'),
};

describe('tokenCreateCommand', () => {
  it('creates a token with no options by default', async () => {
    const createDelegationToken = vi.fn(async () => RESULT);
    const admin = createFakeAdmin({ createDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await tokenCreateCommand.run(context);

    expect(code).toBe(0);
    expect(createDelegationToken).toHaveBeenCalledWith({
      owner: undefined,
      renewers: undefined,
      maxLifeTimeMs: undefined,
    });
  });

  it('parses --owner, --renewer, and --max-life-time-ms', async () => {
    const createDelegationToken = vi.fn(async () => RESULT);
    const admin = createFakeAdmin({ createDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        owner: 'User:alice',
        renewer: ['User:bob', 'User:carol'],
        'max-life-time-ms': '86400000',
      },
      openAdmin: async () => admin,
    });

    await tokenCreateCommand.run(context);

    expect(createDelegationToken).toHaveBeenCalledWith({
      owner: { principalType: 'User', name: 'alice' },
      renewers: [
        { principalType: 'User', name: 'bob' },
        { principalType: 'User', name: 'carol' },
      ],
      maxLifeTimeMs: 86400000n,
    });
  });

  it('rejects a malformed --owner', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', owner: 'alice' } });
    await expect(tokenCreateCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('redacts the hmac by default', async () => {
    const admin = createFakeAdmin({ createDelegationToken: async () => RESULT, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await tokenCreateCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { hmac: string };
    expect(written.hmac).toBe('[REDACTED]');
  });

  it('shows the hmac with --show-secrets', async () => {
    const admin = createFakeAdmin({ createDelegationToken: async () => RESULT, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'show-secrets': true },
      openAdmin: async () => admin,
      format: 'json',
    });

    await tokenCreateCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { hmac: string };
    expect(written.hmac).toBe(Buffer.from('secret').toString('base64'));
  });

  it('disconnects even when createDelegationToken throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      createDelegationToken: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(tokenCreateCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
