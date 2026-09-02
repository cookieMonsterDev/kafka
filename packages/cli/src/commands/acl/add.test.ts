import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { aclAddCommand } from './add';

const BASE_FLAGS = {
  brokers: 'localhost:9092',
  'resource-type': 'topic',
  'resource-name': 'orders',
  operation: ['read'],
};

describe('aclAddCommand', () => {
  it('requires at least one principal', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({ flags: BASE_FLAGS, positionals: [], openAdmin });

    await expect(aclAddCommand.run(context)).rejects.toThrow(CliUsageError);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('requires --resource-type', async () => {
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, 'resource-type': undefined },
      positionals: ['User:alice'],
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow(/--resource-type/);
  });

  it('requires --resource-name', async () => {
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, 'resource-name': undefined },
      positionals: ['User:alice'],
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow(/--resource-name/);
  });

  it('requires at least one --operation', async () => {
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, operation: [] },
      positionals: ['User:alice'],
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow(/--operation/);
  });

  it('rejects an unrecognized --resource-type before opening an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, 'resource-type': 'bogus' },
      positionals: ['User:alice'],
      openAdmin,
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow(CliUsageError);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('rejects an unrecognized --operation before opening an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, operation: ['bogus'] },
      positionals: ['User:alice'],
      openAdmin,
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow(CliUsageError);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('creates a single ACL entry with defaulted host, pattern type, and permission type', async () => {
    const createAcls = vi.fn(async () => true);
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: BASE_FLAGS,
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    const code = await aclAddCommand.run(context);

    expect(code).toBe(0);
    expect(createAcls).toHaveBeenCalledWith({
      acl: [
        {
          principal: 'User:alice',
          host: '*',
          operation: 3,
          permissionType: 3,
          resourceType: 2,
          resourceName: 'orders',
          resourcePatternType: 3,
        },
      ],
    });
  });

  it('resolves explicit --host, --pattern-type, and --permission-type', async () => {
    const createAcls = vi.fn(async () => true);
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, host: '10.0.0.1', 'pattern-type': 'prefixed', 'permission-type': 'deny' },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    await aclAddCommand.run(context);

    expect(createAcls).toHaveBeenCalledWith({
      acl: [
        expect.objectContaining({
          host: '10.0.0.1',
          resourcePatternType: 4,
          permissionType: 2,
        }),
      ],
    });
  });

  it('fans out one createAcls call per principal x operation combination', async () => {
    const createAcls = vi.fn(async (_options: { acl: { principal: string }[] }) => true);
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, operation: ['read', 'write'] },
      positionals: ['User:alice', 'User:bob'],
      openAdmin: async () => admin,
    });

    const code = await aclAddCommand.run(context);

    expect(code).toBe(0);
    expect(createAcls).toHaveBeenCalledTimes(4);
    const principals = createAcls.mock.calls.map((call) => call[0].acl[0]!.principal);
    expect(principals.sort()).toEqual(['User:alice', 'User:alice', 'User:bob', 'User:bob'].sort());
  });

  it('reports a partial batch when only some entries fail', async () => {
    const createAcls = vi.fn(async (options: { acl: { principal: string }[] }) => {
      if (options.acl[0]!.principal === 'User:bob') throw new Error('not authorized');
      return true;
    });
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: BASE_FLAGS,
      positionals: ['User:alice', 'User:bob'],
      openAdmin: async () => admin,
    });

    const code = await aclAddCommand.run(context);

    expect(code).toBe(4);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('User:alice');
    expect(written).toContain('not authorized');
  });

  it('returns operationFailed when every entry fails', async () => {
    const createAcls = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, operation: ['read', 'write'] },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    const code = await aclAddCommand.run(context);

    expect(code).toBe(1);
  });

  it('--dry-run prints the entries that would be created without opening an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { ...BASE_FLAGS, 'dry-run': true },
      positionals: ['User:alice'],
      openAdmin,
    });

    const code = await aclAddCommand.run(context);

    expect(code).toBe(0);
    expect(openAdmin).not.toHaveBeenCalled();
    expect(stdoutWrite.mock.calls[0]![0]).toContain('validated');
  });

  it('reports json output with the raw results shape', async () => {
    const createAcls = vi.fn(async () => true);
    const admin = createFakeAdmin({ createAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: BASE_FLAGS,
      positionals: ['User:alice'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await aclAddCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { results: { principal: string; ok: boolean }[] };
    expect(written.results).toEqual([{ principal: 'User:alice', operation: '3', ok: true }]);
  });

  it('disconnects even when a single-entry call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      createAcls: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: BASE_FLAGS,
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    await expect(aclAddCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
