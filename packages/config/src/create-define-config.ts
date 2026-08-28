import type { AssertValidFileConfig } from './resolve-module';

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function describeType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  return typeof value;
}

export interface CreateDefineConfigOptions {
  /**
   * Top-level keys that, when present, must be a plain object. Every other top-level key passes
   * through unvalidated — an older consumer must not reject a config file written for a newer one.
   */
  objectSections: readonly string[];
}

export interface DefineConfigFactory<T extends Record<string, unknown>> {
  /**
   * Identity helper for a config file's default export: freezes and shallow-validates the known
   * object sections, or passes a sync/async factory through unchanged (its result is validated
   * when it resolves). Generic over its input so the result stays as specific as what was passed
   * in — combine with `satisfies` on the argument to get a compile-time check without widening the
   * literal's inferred type.
   */
  defineConfig<I extends T | (() => T | Promise<T>)>(input: I): I;
  /** The same section validator `defineConfig` uses, for validating an already-resolved config value. */
  assertValid: AssertValidFileConfig<T>;
}

/**
 * Builds a `defineConfig` + `assertValid` pair scoped to one set of known object sections. Each
 * consumer (core's five Kafka sections today, a future `kafka-studio` section set) gets its own
 * factory call instead of this package hardcoding any consumer's shape.
 */
export function createDefineConfig<T extends Record<string, unknown> = Record<string, unknown>>(
  options: CreateDefineConfigOptions,
): DefineConfigFactory<T> {
  const objectSections = options.objectSections;

  function assertValid(config: unknown): asserts config is T {
    if (!isPlainObject(config)) {
      throw new TypeError(`kafka.config: expected an object, got ${describeType(config)}`);
    }

    for (const section of objectSections) {
      const value = config[section];
      if (value !== undefined && !isPlainObject(value)) {
        throw new TypeError(`kafka.config: "${section}" must be an object, got ${describeType(value)}`);
      }
    }
  }

  function defineConfig<I extends T | (() => T | Promise<T>)>(input: I): I {
    if (typeof input === 'function') return input;

    assertValid(input);
    return Object.freeze(input);
  }

  return { defineConfig, assertValid };
}
