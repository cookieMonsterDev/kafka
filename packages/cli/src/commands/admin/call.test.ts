import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createFakeAdmin } from '../../testing/create-fake-admin';
import { createFakeCommandContext } from '../../testing/create-command-context';
import { adminCallCommand } from './call';

let dir: string;

beforeEach(() => {
  dir = mkdtempSync(join(tmpdir(), 'kafka-cli-admin-call-'));
});

afterEach(() => {
  rmSync(dir, { recursive: true, force: true });
});

describe('adminCallCommand', () => {
  it('calls a read-only method with no --from-file and no confirmation flags', async () => {
    const disconnect = vi.fn(async () => {});
    const admin = createFakeAdmin({ listTopics: async () => ['orders'], disconnect });
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['listTopics'],
      openAdmin: async () => admin,
      format: 'json',
    });

    const code = await adminCallCommand.run(context);

    expect(code).toBe(0);
    expect(disconnect).toHaveBeenCalledTimes(1);
    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(JSON.parse(written)).toEqual(['orders']);
  });

  it('decodes --from-file arguments and passes them to the method', async () => {
    const path = join(dir, 'args.json');
    writeFileSync(path, JSON.stringify({ groupIds: ['g1'] }));
    const describeGroups = vi.fn(async () => ({ groups: [] }));
    const admin = createFakeAdmin({ describeGroups, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': path },
      positionals: ['describeGroups'],
      openAdmin: async () => admin,
    });

    await adminCallCommand.run(context);
    expect(describeGroups).toHaveBeenCalledWith({ groupIds: ['g1'] });
  });

  it('decodes a bigint: value inside --from-file', async () => {
    const path = join(dir, 'args.json');
    writeFileSync(path, JSON.stringify({ expiryTimestamp: 'bigint:5' }));
    const renewDelegationToken = vi.fn(async () => ({ expiryTimestamp: 5n }));
    const admin = createFakeAdmin({ renewDelegationToken, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', 'from-file': path, yes: true, force: true },
      positionals: ['renewDelegationToken'],
      openAdmin: async () => admin,
    });

    await adminCallCommand.run(context);
    expect(renewDelegationToken).toHaveBeenCalledWith({ expiryTimestamp: 5n });
  });

  it('rejects an unknown method with exit 2 and a suggestion', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['listTopicz'],
    });

    await expect(adminCallCommand.run(context)).rejects.toThrow(/unknown Admin method/);
  });

  it('refuses a non-read-only method without --yes --force', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      positionals: ['createTopics'],
    });

    await expect(adminCallCommand.run(context)).rejects.toThrow(/--yes --force/);
  });

  it('refuses a non-read-only method with only one of --yes/--force', async () => {
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true },
      positionals: ['createTopics'],
    });

    await expect(adminCallCommand.run(context)).rejects.toThrow(/--yes --force/);
  });

  it('allows a non-read-only method with both --yes and --force', async () => {
    const createTopics = vi.fn(async () => true);
    const admin = createFakeAdmin({ createTopics, disconnect: async () => {} });
    const { context } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092', yes: true, force: true },
      positionals: ['createTopics'],
      openAdmin: async () => admin,
    });

    const code = await adminCallCommand.run(context);
    expect(code).toBe(0);
  });
});
