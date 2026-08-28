/**
 * `kafka.config.*` file discovery, loading, and validation. Imported from the `./config` subpath
 * (`@cookiemonsterdev/kafka-core/config`) — never re-exported from the package root.
 *
 * This is core's Kafka-typed facade (`KafkaFileConfig`, `defineConfig`, `loadKafkaConfig`) built on
 * top of the generic loader in `@cookiemonsterdev/kafka-config`. Reach for that package directly
 * for the underlying machinery (`discoverConfigFile`, `loadConfigFileSync`/`Async`,
 * `mergeConfigLayers`, `createDefineConfig`, and the rest).
 */
export { defineConfig } from './define-config';
export type { KafkaFileConfig, KafkaFileConfigFactory, KafkaFileConfigInput } from './types';

export { loadKafkaConfig } from './load';
export type { LoadKafkaConfigOptions, LoadKafkaConfigResult } from './load';
