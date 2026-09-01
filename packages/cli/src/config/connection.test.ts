import { describe, expect, it } from 'vitest';
import { buildConnectionOverrides } from './connection';
import type { ResolvedCliConfig } from './resolve';

function config(overrides: Partial<ResolvedCliConfig> = {}): ResolvedCliConfig {
  return {
    path: null,
    fileConfig: null,
    cli: {},
    profile: null,
    transformFallbackUsed: false,
    ...overrides,
  };
}

describe('buildConnectionOverrides', () => {
  it('flags beat env', () => {
    const overrides = buildConnectionOverrides({ brokers: ['flag:1'] }, { brokers: ['env:1'] }, config());
    expect(overrides.brokers).toEqual(['flag:1']);
  });

  it('env beats an active profile', () => {
    const cfg = config({ profile: 'staging', cli: { profiles: { staging: { brokers: ['profile:1'] } } } });
    const overrides = buildConnectionOverrides({}, { brokers: ['env:1'] }, cfg);
    expect(overrides.brokers).toEqual(['env:1']);
  });

  it('an active profile applies when neither a flag nor env set the key', () => {
    const cfg = config({ profile: 'staging', cli: { profiles: { staging: { brokers: ['profile:1'] } } } });
    const overrides = buildConnectionOverrides({}, {}, cfg);
    expect(overrides.brokers).toEqual(['profile:1']);
  });

  it('omits brokers/clientId entirely when nothing at this layer resolved them', () => {
    const overrides = buildConnectionOverrides({}, {}, config());
    expect(overrides.brokers).toBeUndefined();
    expect(overrides.clientId).toBeUndefined();
  });

  it('merges clientId independently of brokers across layers', () => {
    const cfg = config({ profile: 'staging', cli: { profiles: { staging: { clientId: 'profile-client' } } } });
    const overrides = buildConnectionOverrides({ brokers: ['flag:1'] }, {}, cfg);
    expect(overrides).toEqual({ brokers: ['flag:1'], clientId: 'profile-client' });
  });

  describe('cli.timeoutMs — a default of last resort', () => {
    it('applies connectionTimeout/requestTimeout when nothing else set either', () => {
      const cfg = config({ cli: { timeoutMs: 5000 } });
      const overrides = buildConnectionOverrides({}, {}, cfg);
      expect(overrides.connectionTimeout).toBe(5000);
      expect(overrides.requestTimeout).toBe(5000);
    });

    it('never overrides a value the flags/env/profile layer already set', () => {
      const cfg = config({ cli: { timeoutMs: 5000 } });
      const overrides = buildConnectionOverrides({}, { connectionTimeout: 1234 }, cfg);
      expect(overrides.connectionTimeout).toBe(1234);
      expect(overrides.requestTimeout).toBe(5000);
    });

    it("defers to the config file's own client section instead of adding its own layer", () => {
      // `overrides` never carries the file's own value — that merge happens one layer down,
      // inside `Kafka.from()`. This only proves the cli.timeoutMs default backs off, leaving
      // connectionTimeout unset here so the file's value survives untouched.
      const cfg = config({ cli: { timeoutMs: 5000 }, fileConfig: { client: { connectionTimeout: 999 } } });
      const overrides = buildConnectionOverrides({}, {}, cfg);
      expect(overrides.connectionTimeout).toBeUndefined();
      expect(overrides.requestTimeout).toBe(5000);
    });
  });
});
