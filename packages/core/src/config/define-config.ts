import { createDefineConfig } from '@cookiemonsterdev/kafka-config';
import type { KafkaFileConfig, KafkaFileConfigInput } from './types';

const KNOWN_OBJECT_SECTIONS = ['client', 'producer', 'consumer', 'shareConsumer', 'admin'] as const;

const kafkaFileConfig = createDefineConfig<KafkaFileConfig>({ objectSections: KNOWN_OBJECT_SECTIONS });

/**
 * Validates the known top-level sections of a {@link KafkaFileConfig}. Unknown keys pass through
 * untouched — an older core must not reject a config file written for a newer CLI. Also the
 * `assertValid` this package's own {@link import('./load').loadKafkaConfig} injects into the
 * generic loader.
 */
export const assertValidKafkaFileConfig = kafkaFileConfig.assertValid;

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
  return kafkaFileConfig.defineConfig(input);
}
