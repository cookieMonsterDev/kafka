import type { KafkaFileConfig, KafkaFileConfigInput } from './types';

const KNOWN_OBJECT_SECTIONS = ['client', 'producer', 'consumer', 'shareConsumer', 'admin'] as const;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function describeType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  return typeof value;
}

/**
 * Validates the known top-level sections of a {@link KafkaFileConfig}. Unknown keys pass through
 * untouched — an older core must not reject a config file written for a newer CLI.
 */
export function assertValidKafkaFileConfig(config: unknown): asserts config is KafkaFileConfig {
  if (!isPlainObject(config)) {
    throw new TypeError(`kafka.config: expected an object, got ${describeType(config)}`);
  }

  for (const section of KNOWN_OBJECT_SECTIONS) {
    const value = config[section];
    if (value !== undefined && !isPlainObject(value)) {
      throw new TypeError(`kafka.config: "${section}" must be an object, got ${describeType(value)}`);
    }
  }
}

/**
 * Identity helper for a `kafka.config.*` file's default export: freezes and shallow-validates a
 * plain config object, or passes a sync/async factory through unchanged (its result is validated
 * when it resolves). Named exports are not supported — use `export default defineConfig({...})`.
 *
 * Generic over its input so the result stays as specific as what was passed in — combine with
 * `satisfies KafkaFileConfig` on the argument (`defineConfig({...} satisfies KafkaFileConfig)`)
 * to get a compile-time check without widening the literal's inferred type.
 */
export function defineConfig<T extends KafkaFileConfigInput>(input: T): T {
  if (typeof input === 'function') return input;

  assertValidKafkaFileConfig(input);
  return Object.freeze(input);
}
