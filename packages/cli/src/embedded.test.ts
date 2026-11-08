import { PassThrough } from 'node:stream';
import { describe, expect, it, vi } from 'vitest';
import { completeEmbeddedCli, embeddedCliConfig, runEmbeddedCli } from './embedded';
import type { Runtime } from './runtime';
import { createFakeAdmin } from './testing/create-fake-admin';

function runtime(argv: string[], overrides: Parameters<typeof createFakeAdmin>[0] = {}) {
  let text = '';
  const write = (chunk: string) => {
    text += chunk;
    return true;
  };
  const admin = createFakeAdmin({ disconnect: async () => {}, ...overrides });
  const openAdmin = vi.fn(async () => admin);
  const value: Runtime = {
    argv,
    env: {},
    cwd: '/',
    stdout: { write },
    stderr: { write },
    stdin: new PassThrough(),
    isTty: false,
    columns: 80,
    now: () => new Date(),
    exit: () => {
      throw new Error('unexpected exit');
    },
    openAdmin,
    loadConfig: async () => embeddedCliConfig(null, null),
    signal: new AbortController().signal,
  };
  return { value, openAdmin, output: () => text };
}

describe('embedded CLI', () => {
  it('uses the real parser and human table formatter', async () => {
    const test = runtime(['topic', 'list'], { listTopics: async () => ['orders'] });
    expect(await runEmbeddedCli(test.value, { readOnly: true })).toBe(0);
    expect(test.output()).toContain('NAME');
    expect(test.output()).toContain('orders');
  });

  it('blocks actual mutations in read-only mode even with all confirmation flags', async () => {
    const deleteTopics = vi.fn(async () => {});
    const test = runtime(['topic', 'delete', 'orders', '--yes', '--force'], { deleteTopics });
    expect(await runEmbeddedCli(test.value, { readOnly: true })).not.toBe(0);
    expect(test.output()).toContain('read-only');
    expect(deleteTopics).not.toHaveBeenCalled();
  });

  it('also guards mutating admin passthrough methods', async () => {
    const deleteTopics = vi.fn(async () => {});
    const test = runtime(['admin', 'call', 'deleteTopics', '--yes', '--force'], { deleteTopics });
    expect(await runEmbeddedCli(test.value, { readOnly: true })).not.toBe(0);
    expect(deleteTopics).not.toHaveBeenCalled();
  });

  it('allows confirmed writes in a writable host', async () => {
    const deleteTopics = vi.fn(async () => {});
    const test = runtime(['topic', 'delete', 'orders', '--yes'], { deleteTopics });
    expect(await runEmbeddedCli(test.value, { readOnly: false })).toBe(0);
    expect(deleteTopics).toHaveBeenCalled();
  });

  it.each([
    ['init'],
    ['studio'],
    ['doctor'],
    ['topic', 'list', '--brokers=other:9092'],
    ['topic', 'list', '--profile', 'prod'],
    ['admin', 'call', 'listTopics', '--from-file', '/etc/passwd'],
  ])('rejects local commands and host overrides: %j', async (...argv) => {
    const test = runtime(argv);
    expect(await runEmbeddedCli(test.value, { readOnly: false })).not.toBe(0);
    expect(test.openAdmin).not.toHaveBeenCalled();
  });

  it('allows reserved flag words in ordinary argument values', async () => {
    const createTopics = vi.fn(async () => true);
    const test = runtime(['topic', 'create', 'profile', '--config', 'brokers=value', '--dry-run'], { createTopics });
    expect(await runEmbeddedCli(test.value, { readOnly: false })).toBe(0);
    expect(createTopics).toHaveBeenCalled();
  });

  it('derives completions from the CLI while excluding host-only flags', () => {
    expect(completeEmbeddedCli(['topic', 'c'])).toContain('create');
    expect(completeEmbeddedCli(['topic', 'create', '--'])).toContain('--partitions');
    expect(completeEmbeddedCli(['topic', 'create', '--'])).not.toContain('--brokers');
    expect(completeEmbeddedCli([''])).not.toContain('init');
  });
});
