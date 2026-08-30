/**
 * `<name>.config.*` file discovery, loading, and validation — a generic, zero-runtime-dependency
 * loader shared by every consumer (`@cookiemonsterdev/kafka-core`, the CLI, and future packages).
 * A consumer supplies its own config shape via the four extension points documented on
 * {@link discoverConfigFile}, {@link mergeConfigLayers}, {@link createDefineConfig}, and the `T`
 * type parameter of {@link loadConfigFileSync} / {@link loadConfigFileAsync}.
 */
export { CANDIDATE_EXTENSIONS, discoverConfigFile } from './discover';
export type { DiscoverConfigFileOptions } from './discover';

export { createDefineConfig } from './create-define-config';
export type { CreateDefineConfigOptions, DefineConfigFactory } from './create-define-config';

export { loadConfigFileSync } from './load-sync';
export type { LoadConfigFileSyncOptions } from './load-sync';

export { loadConfigFileAsync } from './load-async';
export type { LoadConfigFileAsyncOptions } from './load-async';

export { loadEnvFiles } from './load-env-files';
export type { LoadEnvFilesOptions, LoadEnvFilesResult } from './load-env-files';

export { mergeConfigLayers } from './merge';
export type { MergeConfigLayersOptions } from './merge';

export { assertPlainObjectFileConfig, extractDefaultExport } from './resolve-module';
export type { AssertValidFileConfig } from './resolve-module';

export type { ConfigErrorTag, KafkaConfigErrorOptions } from './errors';
export { KafkaConfigError, KafkaConfigRequiresAsyncError } from './errors';

export { defaultOnConfigDiagnostic } from './diagnostics';
export type { ConfigDiagnostic, OnConfigDiagnostic } from './diagnostics';

export { areConfigTransformHooksInstalled, installConfigTransformHooks } from './transform-hooks';
