/**
 * `kafka.config.*` file discovery, loading, and validation. Imported from the `./config` subpath
 * (`@cookiemonsterdev/kafka-core/config`) — never re-exported from the package root.
 */
export { defineConfig } from './define-config';
export type { KafkaFileConfig, KafkaFileConfigFactory, KafkaFileConfigInput } from './types';

export { CANDIDATE_EXTENSIONS, discoverConfigFile } from './discover';
export type { DiscoverConfigFileOptions } from './discover';

export { loadConfigFileSync } from './load-sync';
export type { LoadConfigFileSyncOptions } from './load-sync';

export { loadConfigFileAsync } from './load-async';

export { loadKafkaConfig } from './load';
export type { LoadKafkaConfigOptions, LoadKafkaConfigResult } from './load';

export type { ConfigErrorTag, KafkaConfigErrorOptions } from './errors';
export { KafkaConfigError, KafkaConfigRequiresAsyncError } from './errors';

export { defaultOnConfigDiagnostic } from './diagnostics';
export type { ConfigDiagnostic, OnConfigDiagnostic } from './diagnostics';
