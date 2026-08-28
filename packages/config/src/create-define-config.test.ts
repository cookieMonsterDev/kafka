import { describe, expect, it } from 'vitest';
import { createDefineConfig, type DefineConfigFactory } from './create-define-config';

interface StudioConfig extends Record<string, unknown> {
  ui?: { port?: number };
  auth?: { token?: string };
}

describe('createDefineConfig', () => {
  const studio: DefineConfigFactory<StudioConfig> = createDefineConfig<StudioConfig>({
    objectSections: ['ui', 'auth'],
  });

  it('returns the same object for a valid config (identity)', () => {
    const input: StudioConfig = { ui: { port: 4000 } };

    expect(studio.defineConfig(input)).toBe(input);
  });

  it('freezes the result', () => {
    const result = studio.defineConfig({ ui: { port: 4000 } });

    expect(Object.isFrozen(result)).toBe(true);
  });

  it('rejects a section outside its own objectSections list, byte-for-byte like a shared section', () => {
    expect(() => studio.defineConfig({ ui: 'nope' } as unknown as StudioConfig)).toThrow(TypeError);
    expect(() => studio.defineConfig({ auth: 42 } as unknown as StudioConfig)).toThrow(TypeError);
  });

  it('does not validate a section outside its own objectSections list', () => {
    // "client" is not one of this factory's known sections, so it passes through unchecked —
    // the same forward-compatibility guarantee as an unknown top-level key.
    expect(() => studio.defineConfig({ client: 'nope' })).not.toThrow();
  });

  it('two factories built with different objectSections reject different keys', () => {
    const kafkaLike = createDefineConfig<{ client?: unknown; producer?: unknown }>({
      objectSections: ['client', 'producer'],
    });

    expect(() => kafkaLike.defineConfig({ client: 'nope' })).toThrow(TypeError);
    // "ui" is unknown to the kafka-like factory, so it is not validated.
    expect(() => kafkaLike.defineConfig({ ui: 'nope' } as never)).not.toThrow();
  });

  it('passes a sync factory through unchanged', () => {
    const configFactory = (): StudioConfig => ({ ui: { port: 4000 } });

    expect(studio.defineConfig(configFactory)).toBe(configFactory);
  });

  it('passes an async factory through unchanged', () => {
    const configFactory = async (): Promise<StudioConfig> => ({ ui: { port: 4000 } });

    expect(studio.defineConfig(configFactory)).toBe(configFactory);
  });

  it('exposes the same validator defineConfig uses, for validating an already-resolved value', () => {
    expect(() => {
      studio.assertValid({ ui: { port: 4000 } });
    }).not.toThrow();
    expect(() => {
      studio.assertValid({ ui: 'nope' });
    }).toThrow(TypeError);
    expect(() => {
      studio.assertValid(null);
    }).toThrow(TypeError);
  });

  it('passes unknown top-level keys through byte-identical', () => {
    const input = { ui: { port: 4000 }, cli: { profiles: {} } };

    const result = studio.defineConfig(input);

    expect(result).toBe(input);
    expect(result).toEqual(input);
  });
});
