/**
 * `kafka.config.*` file discovery, loading, and validation. Imported from the `./config` subpath
 * (`@cookiemonsterdev/kafka-core/config`) — never re-exported from the package root.
 *
 * The generic loader machinery lives in `@cookiemonsterdev/kafka-config` (D1a) — this module
 * re-exports it for compatibility with what `core-v2.1.0` originally shipped, plus core's own
 * Kafka-typed facade (`KafkaFileConfig`, `defineConfig`, `loadKafkaConfig`), which is not
 * deprecated. The re-exported machinery is: import it from `@cookiemonsterdev/kafka-config`
 * directly instead.
 */
export { defineConfig } from './define-config';
export type { KafkaFileConfig, KafkaFileConfigFactory, KafkaFileConfigInput } from './types';

export { loadKafkaConfig } from './load';
export type { LoadKafkaConfigOptions, LoadKafkaConfigResult } from './load';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { CANDIDATE_EXTENSIONS, discoverConfigFile } from '@cookiemonsterdev/kafka-config';
/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export type { DiscoverConfigFileOptions } from '@cookiemonsterdev/kafka-config';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { loadConfigFileSync } from '@cookiemonsterdev/kafka-config';
/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export type { LoadConfigFileSyncOptions } from '@cookiemonsterdev/kafka-config';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { loadConfigFileAsync } from '@cookiemonsterdev/kafka-config';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { mergeConfigLayers } from '@cookiemonsterdev/kafka-config';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export type { ConfigErrorTag, KafkaConfigErrorOptions } from '@cookiemonsterdev/kafka-config';
/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { KafkaConfigError, KafkaConfigRequiresAsyncError } from '@cookiemonsterdev/kafka-config';

/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export { defaultOnConfigDiagnostic } from '@cookiemonsterdev/kafka-config';
/** @deprecated Import from `@cookiemonsterdev/kafka-config` instead. Removed in core 3.0.0. */
export type { ConfigDiagnostic, OnConfigDiagnostic } from '@cookiemonsterdev/kafka-config';
