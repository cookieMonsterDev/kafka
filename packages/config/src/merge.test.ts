import { describe, expect, it } from 'vitest';
import { mergeConfigLayers } from './merge';

interface Fixture extends Record<string, unknown> {
  clientId?: string;
  logLevel?: number;
  connectionTimeout?: number;
  reconnectBackoffMs?: number;
  enforceRequestTimeout?: boolean;
  brokers?: string[] | (() => string[]);
  retry?: { retries?: number; maxRetryTime?: number };
  sasl?: { mechanism: string; [key: string]: unknown };
  ssl?: boolean | { ca?: string };
  metrics?: unknown;
  socketFactory?: unknown;
  logCreator?: unknown;
  untouched?: string;
}

describe('mergeConfigLayers', () => {
  describe('precedence matrix', () => {
    it.each([
      ['override defined, base defined', { clientId: 'override' }, { clientId: 'base' }, 'override'],
      ['override undefined, base defined', { clientId: undefined }, { clientId: 'base' }, 'base'],
      ['override absent, base defined', {}, { clientId: 'base' }, 'base'],
      ['override defined, base absent', { clientId: 'override' }, {}, 'override'],
      ['override defined, base undefined', { clientId: 'override' }, { clientId: undefined }, 'override'],
    ] satisfies [string, Partial<Fixture>, Partial<Fixture>, string][])('%s', (_label, override, base, expected) => {
      expect(mergeConfigLayers<Fixture>(override, base).clientId).toBe(expected);
    });

    it('omits a key defined nowhere, so the caller default still fires', () => {
      const result = mergeConfigLayers<Fixture>({}, {});

      expect(Object.hasOwn(result, 'clientId')).toBe(false);
    });

    it('omits a key that is undefined in both layers', () => {
      const result = mergeConfigLayers<Fixture>({ clientId: undefined }, { clientId: undefined });

      expect(Object.hasOwn(result, 'clientId')).toBe(false);
    });

    it('an explicit undefined in override does not clobber a defined base value', () => {
      const result = mergeConfigLayers<Fixture>({ clientId: undefined, untouched: 'x' }, { clientId: 'base' });

      expect(result.clientId).toBe('base');
      expect(result.untouched).toBe('x');
    });
  });

  describe('falsy-but-meaningful values survive', () => {
    it.each([
      ['logLevel', 0],
      ['connectionTimeout', 0],
      ['reconnectBackoffMs', 0],
      ['enforceRequestTimeout', false],
      ['clientId', ''],
    ] as const)('%s: %p is not treated as absent', (key, value) => {
      const override = { [key]: value };
      const result = mergeConfigLayers<Fixture>(override, { [key]: 'should-be-overridden' });

      expect(result[key]).toBe(value);
      expect(Object.hasOwn(result, key)).toBe(true);
    });

    it('a falsy base value survives when override omits the key', () => {
      const result = mergeConfigLayers<Fixture>({}, { logLevel: 0, clientId: '' });

      expect(result.logLevel).toBe(0);
      expect(result.clientId).toBe('');
    });
  });

  describe('shallowMergeKeys: opts into one-level-deep merging for named keys', () => {
    it('merges sub-keys, override winning per sub-key', () => {
      const result = mergeConfigLayers<Fixture>(
        { retry: { retries: 10 } },
        { retry: { retries: 5, maxRetryTime: 30_000 } },
        { shallowMergeKeys: ['retry'] },
      );

      expect(result.retry).toEqual({ retries: 10, maxRetryTime: 30_000 });
    });

    it('an undefined sub-key in override does not clobber a defined base sub-key', () => {
      const result = mergeConfigLayers<Fixture>(
        { retry: { retries: undefined, maxRetryTime: 1000 } },
        { retry: { retries: 5 } },
        { shallowMergeKeys: ['retry'] },
      );

      expect(result.retry).toEqual({ retries: 5, maxRetryTime: 1000 });
    });

    it('takes retry from base alone when override never mentions it', () => {
      const result = mergeConfigLayers<Fixture>({}, { retry: { retries: 5 } }, { shallowMergeKeys: ['retry'] });

      expect(result.retry).toEqual({ retries: 5 });
    });

    it('omits retry when neither layer defines any sub-key', () => {
      const result = mergeConfigLayers<Fixture>({ retry: {} }, {}, { shallowMergeKeys: ['retry'] });

      expect(Object.hasOwn(result, 'retry')).toBe(false);
    });

    it('defaults to an empty set — retry is replaced atomically unless a consumer opts in', () => {
      const result = mergeConfigLayers<Fixture>(
        { retry: { retries: 10 } },
        { retry: { retries: 5, maxRetryTime: 30_000 } },
      );

      expect(result.retry).toEqual({ retries: 10 });
    });

    it('honours a custom multi-key set, leaving keys outside it atomic', () => {
      interface WithFoo extends Fixture {
        foo?: { a?: number; b?: number };
      }

      const result = mergeConfigLayers<WithFoo>(
        { retry: { retries: 10 }, foo: { a: 1 }, sasl: { mechanism: 'plain' } },
        { retry: { retries: 5, maxRetryTime: 30_000 }, foo: { a: 0, b: 2 }, sasl: { mechanism: 'scram-sha-256' } },
        { shallowMergeKeys: ['retry', 'foo'] },
      );

      expect(result.retry).toEqual({ retries: 10, maxRetryTime: 30_000 });
      expect(result.foo).toEqual({ a: 1, b: 2 });
      // sasl is outside the custom set, so it stays atomic.
      expect(result.sasl).toEqual({ mechanism: 'plain' });
    });
  });

  describe('atomic replace for discriminated / holder values', () => {
    it('replaces sasl entirely on a different mechanism, leaking no fields', () => {
      const result = mergeConfigLayers<Fixture>(
        { sasl: { mechanism: 'plain', username: 'u', password: 'p' } },
        { sasl: { mechanism: 'scram-sha-256', username: 'other', tokenId: 'leaked-if-merged' } },
      );

      expect(result.sasl).toEqual({ mechanism: 'plain', username: 'u', password: 'p' });
    });

    it('replaces ssl: true over ssl: {ca}', () => {
      const result = mergeConfigLayers<Fixture>({ ssl: true }, { ssl: { ca: 'base-ca' } });

      expect(result.ssl).toBe(true);
    });

    it('replaces ssl: {ca} over ssl: true', () => {
      const result = mergeConfigLayers<Fixture>({ ssl: { ca: 'override-ca' } }, { ssl: true });

      expect(result.ssl).toEqual({ ca: 'override-ca' });
    });

    it('does not concatenate brokers arrays', () => {
      const result = mergeConfigLayers<Fixture>({ brokers: ['override:9092'] }, { brokers: ['base:9092'] });

      expect(result.brokers).toEqual(['override:9092']);
    });

    it('a BrokersFunction replaces an array wholesale', () => {
      const fn = () => ['fn:9092'];
      const result = mergeConfigLayers<Fixture>({ brokers: fn }, { brokers: ['base:9092'] });

      expect(result.brokers).toBe(fn);
    });

    it.each(['metrics', 'socketFactory', 'logCreator'] as const)('replaces %s atomically', (key) => {
      const overrideValue = { marker: 'override' };
      const baseValue = { marker: 'base' };

      const result = mergeConfigLayers<Fixture>({ [key]: overrideValue }, { [key]: baseValue });

      expect(result[key]).toBe(overrideValue);
    });
  });
});
