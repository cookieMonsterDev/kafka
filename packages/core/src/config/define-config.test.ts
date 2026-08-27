import { describe, expect, it } from 'vitest';
import { defineConfig } from './define-config';
import type { KafkaFileConfig } from './types';

describe('defineConfig', () => {
  it('returns the same object for a valid config (identity)', () => {
    const input: KafkaFileConfig = { client: { brokers: ['localhost:9092'] } };

    expect(defineConfig(input)).toBe(input);
  });

  it('freezes the result', () => {
    const result = defineConfig({ client: { brokers: ['localhost:9092'] } });

    expect(Object.isFrozen(result)).toBe(true);
  });

  it.each([
    ['null', null],
    ['an array', []],
    ['a string', 'nope'],
    ['a number', 42],
  ])('throws when the root config is %s', (_label, value) => {
    expect(() => defineConfig(value as unknown as KafkaFileConfig)).toThrow(TypeError);
  });

  it.each([
    ['client', 'nope'],
    ['producer', []],
    ['consumer', 42],
    ['shareConsumer', null],
    ['admin', 'nope'],
  ])('throws when "%s" is not an object', (section, value) => {
    expect(() => defineConfig({ [section]: value })).toThrow(TypeError);
  });

  it('does not throw when a known section is omitted', () => {
    expect(() => defineConfig({})).not.toThrow();
  });

  it('passes unknown top-level keys through byte-identical', () => {
    const input = {
      client: { brokers: ['localhost:9092'] },
      cli: { profiles: { default: { output: 'json' } } },
      someFutureSection: { nested: [1, 2, 3] },
    };

    const result = defineConfig(input);

    expect(result).toBe(input);
    expect(result).toEqual(input);
  });

  it('passes a sync factory through unchanged', () => {
    const factory = (): KafkaFileConfig => ({ client: { brokers: ['localhost:9092'] } });

    expect(defineConfig(factory)).toBe(factory);
  });

  it('passes an async factory through unchanged', () => {
    const factory = async (): Promise<KafkaFileConfig> => ({ client: { brokers: ['localhost:9092'] } });

    expect(defineConfig(factory)).toBe(factory);
  });
});
