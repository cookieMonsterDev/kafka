import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { createFakeStdin } from '../../testing/create-fake-stdin';
import { tokenRenewCommand } from './renew';

const HMAC_B64 = Buffer.from('secret').toString('base64');

describe('tokenRenewCommand', () => {
  it('requires --hmac or --hmac-stdin', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' } });
    await expect(tokenRenewCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('renews with the decoded hmac and no renewTimePeriodMs by default', async () => {
    const renewDelegationToken = vi.fn(async () => ({ expiryTimestamp: 1000n }));
    const admin = createFakeAdmin({ renewDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64 },
      openAdmin: async () => admin,
    });

    const code = await tokenRenewCommand.run(context);

    expect(code).toBe(0);
    expect(renewDelegationToken).toHaveBeenCalledWith({ hmac: Buffer.from('secret'), renewTimePeriodMs: undefined });
  });

  it('parses --renew-time-period-ms', async () => {
    const renewDelegationToken = vi.fn(async () => ({ expiryTimestamp: 1000n }));
    const admin = createFakeAdmin({ renewDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64, 'renew-time-period-ms': '3600000' },
      openAdmin: async () => admin,
    });

    await tokenRenewCommand.run(context);
    expect(renewDelegationToken).toHaveBeenCalledWith({ hmac: Buffer.from('secret'), renewTimePeriodMs: 3600000n });
  });

  it('reads the hmac from stdin when --hmac-stdin is given', async () => {
    const renewDelegationToken = vi.fn(async () => ({ expiryTimestamp: 1000n }));
    const admin = createFakeAdmin({ renewDelegationToken, disconnect: async () => {} });
    const stdin = createFakeStdin();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'hmac-stdin': true },
      openAdmin: async () => admin,
      stdin,
    });

    const promise = tokenRenewCommand.run(context);
    stdin.emitData(HMAC_B64);
    stdin.emitEnd();
    await promise;

    expect(renewDelegationToken).toHaveBeenCalledWith({ hmac: Buffer.from('secret'), renewTimePeriodMs: undefined });
  });

  it('disconnects even when renewDelegationToken throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      renewDelegationToken: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', hmac: HMAC_B64 },
      openAdmin: async () => admin,
    });

    await expect(tokenRenewCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
