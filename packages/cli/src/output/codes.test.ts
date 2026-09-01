import { describe, expect, it } from 'vitest';
import { CONFIG_RESOURCE_TYPES, CONFIG_SOURCE, describeCode, formatCode } from './codes';

describe('CONFIG_RESOURCE_TYPES', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CONFIG_RESOURCE_TYPES).toEqual(core.ConfigResourceTypes);
  });
});

describe('CONFIG_SOURCE', () => {
  it("deep-equals core's real export", async () => {
    const core = await import('@cookiemonsterdev/kafka-core');
    expect(CONFIG_SOURCE).toEqual(core.ConfigSource);
  });
});

describe('describeCode', () => {
  it('resolves a known code to its name', () => {
    expect(describeCode(CONFIG_RESOURCE_TYPES, 2)).toEqual({ name: 'TOPIC', code: 2 });
  });

  it('resolves an unknown code to a null name', () => {
    expect(describeCode(CONFIG_RESOURCE_TYPES, 999)).toEqual({ name: null, code: 999 });
  });
});

describe('formatCode', () => {
  it('formats a known code by name', () => {
    expect(formatCode({ name: 'TOPIC', code: 2 })).toBe('TOPIC');
  });

  it('formats an unknown code as UNKNOWN(n)', () => {
    expect(formatCode({ name: null, code: 999 })).toBe('UNKNOWN(999)');
  });
});
