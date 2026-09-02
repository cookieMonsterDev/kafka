import { describe, expect, it, vi } from 'vitest';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { tokenExpireCommand } from './expire';

const HMAC_B64 = Buffer.from('secret').toString('base64');

describe('tokenExpireCommand', () => {
  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', hmac: HMAC_B64 } });
    await expect(tokenExpireCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const expireDelegationToken = vi.fn(async () => ({ expiryTimestamp: 0n }));
    const admin = createFakeAdmin({ expireDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64 },
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    expect(await tokenExpireCommand.run(context)).toBe(0);
  });

  it('defaults to expiring immediately (-1)', async () => {
    const expireDelegationToken = vi.fn(async () => ({ expiryTimestamp: 0n }));
    const admin = createFakeAdmin({ expireDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64, yes: true },
      openAdmin: async () => admin,
    });

    const code = await tokenExpireCommand.run(context);

    expect(code).toBe(0);
    expect(expireDelegationToken).toHaveBeenCalledWith({ hmac: Buffer.from('secret'), expiryTimePeriodMs: -1n });
  });

  it('parses --expiry-time-period-ms when given', async () => {
    const expireDelegationToken = vi.fn(async () => ({ expiryTimestamp: 0n }));
    const admin = createFakeAdmin({ expireDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64, yes: true, 'expiry-time-period-ms': '60000' },
      openAdmin: async () => admin,
    });

    await tokenExpireCommand.run(context);
    expect(expireDelegationToken).toHaveBeenCalledWith({ hmac: Buffer.from('secret'), expiryTimePeriodMs: 60000n });
  });

  it('disconnects even when expireDelegationToken throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      expireDelegationToken: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64, yes: true },
      openAdmin: async () => admin,
    });

    await expect(tokenExpireCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
