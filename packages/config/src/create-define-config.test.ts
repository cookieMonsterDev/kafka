import { describe, expect, it } from 'vitest';
import { createDefineConfig, type DefineConfigFactory } from './create-define-config';

interface AppConfig extends Record<string, unknown> {
  server?: { port?: number };
  auth?: { token?: string };
}

describe('createDefineConfig', () => {
  const app: DefineConfigFactory<AppConfig> = createDefineConfig<AppConfig>({
    objectSections: ['server', 'auth'],
  });

  it('returns the same object for a valid config (identity)', () => {
    const input: AppConfig = { server: { port: 4000 } };

    expect(app.defineConfig(input)).toBe(input);
  });

  it('freezes the result', () => {
    const result = app.defineConfig({ server: { port: 4000 } });

    expect(Object.isFrozen(result)).toBe(true);
  });

  it('rejects a section outside its own objectSections list, byte-for-byte like a shared section', () => {
    expect(() => app.defineConfig({ server: 'nope' } as unknown as AppConfig)).toThrow(TypeError);
    expect(() => app.defineConfig({ auth: 42 } as unknown as AppConfig)).toThrow(TypeError);
  });

  it('does not validate a section outside its own objectSections list', () => {
    // "client" is not one of this factory's known sections, so it passes through unchecked —
    // the same forward-compatibility guarantee as an unknown top-level key.
    expect(() => app.defineConfig({ client: 'nope' })).not.toThrow();
  });

  it('two factories built with different objectSections reject different keys', () => {
    const kafkaLike = createDefineConfig<{ client?: unknown; producer?: unknown }>({
      objectSections: ['client', 'producer'],
    });

    expect(() => kafkaLike.defineConfig({ client: 'nope' })).toThrow(TypeError);
    // "server" is unknown to the kafka-like factory, so it is not validated.
    expect(() => kafkaLike.defineConfig({ server: 'nope' } as never)).not.toThrow();
  });

  it('passes a sync factory through unchanged', () => {
    const configFactory = (): AppConfig => ({ server: { port: 4000 } });

    expect(app.defineConfig(configFactory)).toBe(configFactory);
  });

  it('passes an async factory through unchanged', () => {
    const configFactory = async (): Promise<AppConfig> => ({ server: { port: 4000 } });

    expect(app.defineConfig(configFactory)).toBe(configFactory);
  });

  it('exposes the same validator defineConfig uses, for validating an already-resolved value', () => {
    expect(() => {
      app.assertValid({ server: { port: 4000 } });
    }).not.toThrow();
    expect(() => {
      app.assertValid({ server: 'nope' });
    }).toThrow(TypeError);
    expect(() => {
      app.assertValid(null);
    }).toThrow(TypeError);
  });

  it('passes unknown top-level keys through byte-identical', () => {
    const input = { server: { port: 4000 }, cli: { profiles: {} } };

    const result = app.defineConfig(input);

    expect(result).toBe(input);
    expect(result).toEqual(input);
  });
});
