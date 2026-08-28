import { createDefineConfig } from '@cookiemonsterdev/kafka-config';
import type { KafkaFileConfig } from './types';

const factory = createDefineConfig<KafkaFileConfig>({
  objectSections: ['client', 'producer', 'consumer', 'shareConsumer', 'admin'],
});

/**
 * Identity helper for a `kafka.config.*` file's default export: `export default defineConfig({...})`.
 * Freezes the config and validates that each known section, when present, is an object; an unknown
 * top-level key passes through unvalidated. Also accepts a sync or async factory unchanged — its
 * result is validated once it resolves.
 */
export function defineConfig<I extends KafkaFileConfig | (() => KafkaFileConfig | Promise<KafkaFileConfig>)>(
  input: I,
): I {
  return factory.defineConfig(input);
}

/** The same section validator {@link defineConfig} uses, for an already-resolved config value. */
export const assertValidKafkaFileConfig = factory.assertValid;
