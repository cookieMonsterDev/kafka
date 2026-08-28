import { describe, expect, it } from 'vitest';
import { assertValidKafkaFileConfig, defineConfig } from './define-config';

describe('defineConfig', () => {
  it('freezes an object config', () => {
    const config = defineConfig({ client: { brokers: ['a:9092'] } });

    expect(Object.isFrozen(config)).toBe(true);
    expect(config).toEqual({ client: { brokers: ['a:9092'] } });
  });

  it('passes an unknown top-level key through unvalidated, for forward compatibility', () => {
    const config = defineConfig({ client: { brokers: ['a:9092'] }, cli: { profiles: {} } });

    expect(config).toEqual({ client: { brokers: ['a:9092'] }, cli: { profiles: {} } });
  });

  it('throws when a known section is not an object', () => {
    expect(() => defineConfig({ client: 'not-an-object' as never })).toThrow(TypeError);
    expect(() => defineConfig({ producer: 42 as never })).toThrow(TypeError);
  });

  it('passes a sync factory through unchanged, without validating it eagerly', () => {
    const factory = () => ({ client: { brokers: ['a:9092'] } });

    expect(defineConfig(factory)).toBe(factory);
  });

  it('passes an async factory through unchanged', () => {
    const factory = async () => ({ client: { brokers: ['a:9092'] } });

    expect(defineConfig(factory)).toBe(factory);
  });
});

describe('assertValidKafkaFileConfig', () => {
  it('accepts a plain object', () => {
    expect(() => assertValidKafkaFileConfig({ client: { brokers: ['a:9092'] } })).not.toThrow();
  });

  it('rejects a non-object value', () => {
    expect(() => assertValidKafkaFileConfig(null)).toThrow(TypeError);
    expect(() => assertValidKafkaFileConfig('nope')).toThrow(TypeError);
    expect(() => assertValidKafkaFileConfig(42)).toThrow(TypeError);
  });

  it('rejects a known section that is not an object', () => {
    expect(() => assertValidKafkaFileConfig({ admin: [] })).toThrow(TypeError);
  });
});
