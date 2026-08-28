/**
 * `kafka.config.*` file discovery, loading, and validation. Imported from the `./config` subpath
 * (`@cookiemonsterdev/kafka-core/config`) — never re-exported from the package root.
 *
 * The generic loader machinery lives in `@cookiemonsterdev/kafka-config` (D1a) — this module
 * re-exports it for compatibility with what `core-v2.1.0` originally shipped, plus core's own
 * Kafka-typed facade (`KafkaFileConfig`, `defineConfig`, `loadKafkaConfig`).
 */
export { defineConfig } from './define-config';
export type { KafkaFileConfig, KafkaFileConfigFactory, KafkaFileConfigInput } from './types';

export { loadKafkaConfig } from './load';
export type { LoadKafkaConfigOptions, LoadKafkaConfigResult } from './load';

export { CANDIDATE_EXTENSIONS, discoverConfigFile } from '@cookiemonsterdev/kafka-config';
export type { DiscoverConfigFileOptions } from '@cookiemonsterdev/kafka-config';

export { loadConfigFileSync } from '@cookiemonsterdev/kafka-config';
export type { LoadConfigFileSyncOptions } from '@cookiemonsterdev/kafka-config';

export { loadConfigFileAsync } from '@cookiemonsterdev/kafka-config';

export { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';

export type { ConfigErrorTag, KafkaConfigErrorOptions } from '@cookiemonsterdev/kafka-config';
export { KafkaConfigError, KafkaConfigRequiresAsyncError } from '@cookiemonsterdev/kafka-config';

export { defaultOnConfigDiagnostic } from '@cookiemonsterdev/kafka-config';
export type { ConfigDiagnostic, OnConfigDiagnostic } from '@cookiemonsterdev/kafka-config';
