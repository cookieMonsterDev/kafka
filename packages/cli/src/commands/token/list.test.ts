import { describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { tokenListCommand } from './list';

const TOKEN = {
  owner: { principalType: 'User', name: 'alice' },
  issueTimestamp: 1n,
  expiryTimestamp: 2n,
  maxTimestamp: 3n,
  tokenId: 'token-1',
  hmac: Buffer.from('secret'),
  renewers: [],
};

describe('tokenListCommand', () => {
  it('lists with no owner filter by default', async () => {
    const describeDelegationToken = vi.fn(async () => ({ tokens: [] }));
    const admin = createFakeAdmin({ describeDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await tokenListCommand.run(context);

    expect(code).toBe(0);
    expect(describeDelegationToken).toHaveBeenCalledWith({ owners: undefined });
  });

  it('parses repeatable --owner into principals', async () => {
    const describeDelegationToken = vi.fn(async () => ({ tokens: [] }));
    const admin = createFakeAdmin({ describeDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', owner: ['User:alice', 'User:bob'] },
      openAdmin: async () => admin,
    });

    await tokenListCommand.run(context);
    expect(describeDelegationToken).toHaveBeenCalledWith({
      owners: [
        { principalType: 'User', name: 'alice' },
        { principalType: 'User', name: 'bob' },
      ],
    });
  });

  it('renders "(no tokens)" when nothing matches', async () => {
    const admin = createFakeAdmin({
      describeDelegationToken: async () => ({ tokens: [] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await tokenListCommand.run(context);
    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no tokens)');
  });

  it('renders a table row per token, without the hmac', async () => {
    const admin = createFakeAdmin({
      describeDelegationToken: async () => ({ tokens: [TOKEN] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await tokenListCommand.run(context);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('token-1');
    expect(written).toContain('User:alice');
    expect(written).not.toContain('secret');
  });

  it('redacts the hmac in json output by default', async () => {
    const admin = createFakeAdmin({
      describeDelegationToken: async () => ({ tokens: [TOKEN] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await tokenListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { tokens: { hmac: string }[] };
    expect(written.tokens[0]!.hmac).toBe('[REDACTED]');
  });

  it('shows the hmac in json output with --show-secrets', async () => {
    const admin = createFakeAdmin({
      describeDelegationToken: async () => ({ tokens: [TOKEN] }),
      disconnect: async () => {},
    });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'show-secrets': true },
      openAdmin: async () => admin,
      format: 'json',
    });

    await tokenListCommand.run(context);
    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { tokens: { hmac: string }[] };
    expect(written.tokens[0]!.hmac).toBe(Buffer.from('secret').toString('base64'));
  });

  it('disconnects even when describeDelegationToken throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeDelegationToken: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(tokenListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
