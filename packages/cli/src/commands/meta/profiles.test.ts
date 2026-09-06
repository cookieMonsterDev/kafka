import { beforeAll, describe, expect, it } from 'vitest';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import type { ResolvedCliConfig } from '../../config/resolve';
import { profilesCommand } from './profiles';

function config(overrides: Partial<ResolvedCliConfig>): ResolvedCliConfig {
  return { ...EMPTY_RESOLVED_CLI_CONFIG, ...overrides };
}

// profilesCommand lazily imports core on every run() (only place this command needs it — see its
// own comment). Warmed up here, once, so the first test below doesn't risk tripping its own
// timeout paying for that import under load — every later run() hits the module cache.
beforeAll(async () => {
  await import('@cookiemonsterdev/kafka-core');
});

describe('profilesCommand', () => {
  it('reports no profiles configured', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({ config: config({}) });

    const code = await profilesCommand.run(context);

    expect(code).toBe(0);
    expect(stdoutWrite).toHaveBeenCalledWith(expect.stringContaining('no profiles configured'));
  });

  it('lists every profile and marks the active one', async () => {
    const cfg = config({
      profile: 'staging',
      cli: { profiles: { staging: { brokers: ['s:1', 's:2'] }, production: { brokers: ['p:1'] } } },
    });
    const { context, stdoutWrite } = createFakeCommandContext({ config: cfg });

    await profilesCommand.run(context);

    const written = stdoutWrite.mock.calls.map((call) => call[0]).join('');
    expect(written).toContain('staging (active)');
    expect(written).toContain('s:1,s:2');
    expect(written).toContain('production');
    expect(written).not.toContain('production (active)');
  });

  it('redacts a secret field before printing JSON', async () => {
    const cfg = config({
      cli: {
        profiles: { staging: { brokers: ['s:1'], sasl: { mechanism: 'plain', username: 'u', password: 'hunter2' } } },
      },
    });
    const { context, stdoutWrite } = createFakeCommandContext({ config: cfg, format: 'json' });

    await profilesCommand.run(context);

    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    expect(written).not.toContain('hunter2');
    expect(JSON.parse(written).profiles.staging.sasl.password).toBe('[REDACTED]');
  });
});
