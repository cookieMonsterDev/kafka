import { beforeAll, describe, expect, it } from 'vitest';
import type { ResolvedCliConfig } from '../../config/resolve';
import { createFakeCommandContext, EMPTY_RESOLVED_CLI_CONFIG } from '../../testing/create-command-context';
import { doctorCommand } from './doctor';

function config(overrides: Partial<ResolvedCliConfig>): ResolvedCliConfig {
  return { ...EMPTY_RESOLVED_CLI_CONFIG, ...overrides };
}

// doctorCommand lazily imports core on every run() (real work, deliberately not paid by commands
// that never connect). Warmed up here, once, so the first test below doesn't risk tripping its
// own timeout paying for that import under load — every later run() hits the module cache.
beforeAll(async () => {
  await import('@cookiemonsterdev/kafka-core');
});

describe('doctorCommand', () => {
  it('reports brokers resolved and per-key provenance when --brokers is given', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      config: config({}),
    });

    const code = await doctorCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls.map((call) => call[0]).join('');
    expect(written).toContain('brokers resolved  yes');
    expect(written).toContain('brokers');
    expect(written).toContain('explicit');
  });

  it('reports brokers unresolved instead of throwing when nothing supplies one', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({ config: config({}) });

    const code = await doctorCommand.run(context);

    expect(code).toBe(0);
    const written = stdoutWrite.mock.calls.map((call) => call[0]).join('');
    expect(written).toContain('brokers resolved  no');
  });

  it('reports the config path, active profile, and known profiles', async () => {
    const cfg = config({
      path: '/work/kafka.config.ts',
      profile: 'staging',
      cli: { profiles: { staging: {}, production: {} } },
    });
    const { context, stdoutWrite } = createFakeCommandContext({ flags: { brokers: 'a:1' }, config: cfg });

    await doctorCommand.run(context);

    const written = stdoutWrite.mock.calls.map((call) => call[0]).join('');
    expect(written).toContain('/work/kafka.config.ts');
    expect(written).toContain('active profile    staging');
    expect(written).toContain('known profiles    staging, production');
  });

  it('emits valid JSON including configSource when brokers resolve', async () => {
    const { context, stdoutWrite } = createFakeCommandContext({
      flags: { brokers: 'localhost:9092' },
      config: config({}),
      format: 'json',
    });

    await doctorCommand.run(context);

    const written = stdoutWrite.mock.calls[0]?.[0] ?? '';
    const parsed = JSON.parse(written);
    expect(parsed.brokersResolved).toBe(true);
    expect(parsed.configSource.keys.brokers).toBe('explicit');
  });
});
