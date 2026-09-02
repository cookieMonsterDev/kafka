import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { aclListCommand } from './list';

describe('aclListCommand', () => {
  it('defaults every filter field to "any"', async () => {
    const describeAcls = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ describeAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    const code = await aclListCommand.run(context);

    expect(code).toBe(0);
    expect(describeAcls).toHaveBeenCalledWith({
      resourceType: 1,
      resourceName: undefined,
      resourcePatternType: 1,
      principal: undefined,
      host: undefined,
      operation: 1,
      permissionType: 1,
    });
  });

  it('resolves every explicit filter flag by name', async () => {
    const describeAcls = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ describeAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        'resource-type': 'topic',
        'resource-name': 'orders',
        'pattern-type': 'literal',
        principal: 'User:alice',
        host: '10.0.0.1',
        operation: 'read',
        'permission-type': 'allow',
      },
      openAdmin: async () => admin,
    });

    await aclListCommand.run(context);

    expect(describeAcls).toHaveBeenCalledWith({
      resourceType: 2,
      resourceName: 'orders',
      resourcePatternType: 3,
      principal: 'User:alice',
      host: '10.0.0.1',
      operation: 3,
      permissionType: 3,
    });
  });

  it('rejects an unrecognized --resource-type before ever opening an admin connection', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { 'resource-type': 'bogus' },
      openAdmin,
    });

    await expect(aclListCommand.run(context)).rejects.toThrow(CliUsageError);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('renders "(no matching ACLs)" when nothing matches', async () => {
    const describeAcls = vi.fn(async () => ({ resources: [] }));
    const admin = createFakeAdmin({ describeAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await aclListCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('(no matching ACLs)');
  });

  it('renders one table row per resource/principal pair', async () => {
    const describeAcls = vi.fn(async () => ({
      resources: [
        {
          resourceType: 2,
          resourceName: 'orders',
          resourcePatternType: 3,
          acls: [
            { principal: 'User:alice', host: '*', operation: 3, permissionType: 3 },
            { principal: 'User:bob', host: '*', operation: 4, permissionType: 2 },
          ],
        },
      ],
    }));
    const admin = createFakeAdmin({ describeAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await aclListCommand.run(context);

    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('User:alice');
    expect(written).toContain('User:bob');
    expect(written).toContain('TOPIC');
    expect(written).toContain('ALLOW');
    expect(written).toContain('DENY');
  });

  it('reports json output with the raw resources shape', async () => {
    const resources = [
      {
        resourceType: 2,
        resourceName: 'orders',
        resourcePatternType: 3,
        acls: [{ principal: 'User:alice', host: '*', operation: 3, permissionType: 3 }],
      },
    ];
    const describeAcls = vi.fn(async () => ({ resources }));
    const admin = createFakeAdmin({ describeAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
      format: 'json',
    });

    await aclListCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { resources: unknown };
    expect(written.resources).toEqual(resources);
  });

  it('disconnects even when describeAcls throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      describeAcls: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      openAdmin: async () => admin,
    });

    await expect(aclListCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
