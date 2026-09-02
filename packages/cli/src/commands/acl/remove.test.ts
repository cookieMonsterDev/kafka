import { describe, expect, it, vi } from 'vitest';
import { CliUsageError } from '../../args/coerce';
import { CliAbortedError } from '../../errors/aborted-error';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { aclRemoveCommand } from './remove';

function fakeMatchingAcl() {
  return {
    errorCode: 0,
    errorMessage: null,
    resourceType: 2,
    resourceName: 'orders',
    resourcePatternType: 3,
    principal: 'User:alice',
    host: '*',
    operation: 3,
    permissionType: 3,
  };
}

function fakeFilterResponse(matchCount: number) {
  return {
    filterResponses: [
      { errorCode: 0, errorMessage: null, matchingAcls: Array.from({ length: matchCount }, fakeMatchingAcl) },
    ],
  };
}

describe('aclRemoveCommand', () => {
  it('requires at least one principal', async () => {
    const openAdmin = vi.fn();
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: [],
      openAdmin,
    });

    await expect(aclRemoveCommand.run(context)).rejects.toThrow(/at least one principal/);
    expect(openAdmin).not.toHaveBeenCalled();
  });

  it('rejects an unrecognized --resource-type before prompting for confirmation', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'resource-type': 'bogus' },
      positionals: ['User:alice'],
    });

    await expect(aclRemoveCommand.run(context)).rejects.toThrow(CliUsageError);
  });

  it('aborts without --yes off a TTY', async () => {
    const { context } = createFakeCommandContext({ flags: { brokers: 'localhost:9092' }, positionals: ['User:alice'] });
    await expect(aclRemoveCommand.run(context)).rejects.toThrow(CliAbortedError);
  });

  it('skips confirmation when cli.confirmDestructive is false', async () => {
    const deleteAcls = vi.fn(async () => fakeFilterResponse(0));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
      config: { ...EMPTY_RESOLVED_CLI_CONFIG, cli: { confirmDestructive: false } },
    });

    const code = await aclRemoveCommand.run(context);
    expect(code).toBe(0);
  });

  it('defaults every filter field to "any" and passes --yes through', async () => {
    const deleteAcls = vi.fn(async () => fakeFilterResponse(0));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    const code = await aclRemoveCommand.run(context);

    expect(code).toBe(0);
    expect(deleteAcls).toHaveBeenCalledWith({
      filters: [
        {
          principal: 'User:alice',
          host: undefined,
          operation: 1,
          permissionType: 1,
          resourceType: 1,
          resourceName: undefined,
          resourcePatternType: 1,
        },
      ],
    });
  });

  it('resolves explicit filter flags', async () => {
    const deleteAcls = vi.fn(async () => fakeFilterResponse(0));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: {
        brokers: 'localhost:9092',
        yes: true,
        'resource-type': 'topic',
        'resource-name': 'orders',
        'pattern-type': 'literal',
        host: '10.0.0.1',
        operation: 'read',
        'permission-type': 'allow',
      },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    await aclRemoveCommand.run(context);

    expect(deleteAcls).toHaveBeenCalledWith({
      filters: [
        expect.objectContaining({
          resourceType: 2,
          resourceName: 'orders',
          resourcePatternType: 3,
          host: '10.0.0.1',
          operation: 3,
          permissionType: 3,
        }),
      ],
    });
  });

  it('reports the number of matched ACLs removed for a single principal', async () => {
    const deleteAcls = vi.fn(async () => ({
      filterResponses: fakeFilterResponse(2).filterResponses,
    }));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    await aclRemoveCommand.run(context);

    expect(stdoutWrite.mock.calls[0]![0]).toContain('removed (2)');
  });

  it('fans out one deleteAcls call per principal', async () => {
    const deleteAcls = vi.fn(async () => fakeFilterResponse(0));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice', 'User:bob'],
      openAdmin: async () => admin,
    });

    const code = await aclRemoveCommand.run(context);

    expect(code).toBe(0);
    expect(deleteAcls).toHaveBeenCalledTimes(2);
  });

  it('reports a partial batch when only some principals fail', async () => {
    const deleteAcls = vi.fn(async (options: { filters: { principal?: string }[] }) => {
      if (options.filters[0]!.principal === 'User:bob') throw new Error('not authorized');
      return fakeFilterResponse(0);
    });
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice', 'User:bob'],
      openAdmin: async () => admin,
    });

    const code = await aclRemoveCommand.run(context);

    expect(code).toBe(4);
    const written = stdoutWrite.mock.calls[0]![0];
    expect(written).toContain('not authorized');
  });

  it('returns operationFailed when every principal fails', async () => {
    const deleteAcls = vi.fn(async () => {
      throw new Error('boom');
    });
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice', 'User:bob'],
      openAdmin: async () => admin,
    });

    const code = await aclRemoveCommand.run(context);
    expect(code).toBe(1);
  });

  it('reports json output with the raw results shape', async () => {
    const deleteAcls = vi.fn(async () => ({
      filterResponses: fakeFilterResponse(1).filterResponses,
    }));
    const admin = createFakeAdmin({ deleteAcls, disconnect: async () => {} });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
      format: 'json',
    });

    await aclRemoveCommand.run(context);

    const written = JSON.parse(stdoutWrite.mock.calls[0]![0]) as { results: { principal: string; ok: boolean }[] };
    expect(written.results).toEqual([{ principal: 'User:alice', ok: true, matched: 1 }]);
  });

  it('disconnects even when a single-principal call throws', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({
      deleteAcls: async () => {
        throw new Error('boom');
      },
      disconnect,
    });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['User:alice'],
      openAdmin: async () => admin,
    });

    await expect(aclRemoveCommand.run(context)).rejects.toThrow('boom');
    expect(disconnect).toHaveBeenCalledTimes(1);
  });
});
