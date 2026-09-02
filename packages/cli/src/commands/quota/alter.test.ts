import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { quotaAlterCommand } from './alter';

describe('quotaAlterCommand', () => {
  it('requires --entity', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', set: { producer_byte_rate: '1' } },
    });
    await expect(quotaAlterCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('requires at least one --set or --unset', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092', entity: { user: 'alice' } } });
    await expect(quotaAlterCommand.run(context)).rejects.toThrow(/--set key=value or --unset key/);
  });

  it('builds ops from --set and --unset in one call', async () => {
    const alterClientQuotas = vi.fn(async () => ({ entries: [{ errorCode: 0, errorMessage: null, entity: [] }] }));
    const admin = createFakeAdmin({ alterClientQuotas, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        entity: { user: 'alice' },
        set: { producer_byte_rate: '1048576' },
        unset: ['request_percentage'],
      },
      openAdmin: async () => admin,
    });

    const code = await quotaAlterCommand.run(context);

    expect(code).toBe(0);
    expect(alterClientQuotas).toHaveBeenCalledWith({
      entries: [
        {
          entity: [{ entityType: 'user', entityName: 'alice' }],
          ops: [
            { key: 'producer_byte_rate', value: 1048576, remove: false },
            { key: 'request_percentage', value: 0, remove: true },
          ],
        },
      ],
      validateOnly: false,
    });
  });

  it('passes --dry-run through as validateOnly', async () => {
    const alterClientQuotas = vi.fn(async () => ({ entries: [{ errorCode: 0, errorMessage: null, entity: [] }] }));
    const admin = createFakeAdmin({ alterClientQuotas, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        entity: { user: 'alice' },
        set: { producer_byte_rate: '1' },
        'dry-run': true,
      },
      openAdmin: async () => admin,
    });

    await quotaAlterCommand.run(context);

    expect(alterClientQuotas).toHaveBeenCalledWith(expect.objectContaining({ validateOnly: true }));
    expect(stdoutWrite.mock.calls[0]![0]).toContain('validated');
  });

  it('propagates a failure rather than reporting an error entry — the API rejects on the first bad op instead of returning one', async () => {
    const admin = createFakeAdmin({
      alterClientQuotas: async () => {
        throw new Error('invalid quota configuration');
      },
      disconnect: async () => {},
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', entity: { user: 'alice' }, set: { producer_byte_rate: '1' } },
      openAdmin: async () => admin,
    });

    await expect(quotaAlterCommand.run(context)).rejects.toThrow('invalid quota configuration');
  });

  it('disconnects even when alterClientQuotas throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      alterClientQuotas: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', entity: { user: 'alice' }, set: { producer_byte_rate: '1' } },
      openAdmin: async () => admin,
    });

    await expect(quotaAlterCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
