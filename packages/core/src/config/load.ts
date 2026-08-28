import {
  KafkaConfigError as GenericKafkaConfigError,
  KafkaConfigRequiresAsyncError as GenericKafkaConfigRequiresAsyncError,
  loadConfigFileAsync,
  loadConfigFileSync,
  type OnConfigDiagnostic,
} from '@cookiemonsterdev/kafka-config';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from '../errors';
import { assertValidKafkaFileConfig } from './define-config';
import type { KafkaFileConfig } from './types';

/**
 * `@cookiemonsterdev/kafka-config`'s own error classes never extend `KafkaError`, so a raw
 * `require()`/`import()` failure would otherwise leak a foreign error type out of `new Kafka()`.
 * Every load path in this file goes through here so callers only ever see this client's own
 * error hierarchy (`isKafkaError`, `.retriable`, ...) — matched by `.name`, per this repo's
 * error-mapping convention, never `instanceof` across the package boundary.
 */
function toKafkaConfigError(error: unknown, path: string): unknown {
  if (error instanceof GenericKafkaConfigRequiresAsyncError) {
    return new KafkaConfigRequiresAsyncError(path, { cause: error });
  }
  if (error instanceof GenericKafkaConfigError) {
    return new KafkaConfigError(error.tag, error.message, { path, cause: error });
  }
  return error;
}

export interface LoadKafkaConfigOptions {
  /** See `@cookiemonsterdev/kafka-config`'s `LoadConfigFileSyncOptions.allowTransformFallback`. */
  allowTransformFallback?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
}

/**
 * Loads and validates a `kafka.config.*` file synchronously. Memoised per resolved absolute path
 * (by the underlying loader), so N clients pay the cost once per process. A config that requires
 * async work (top-level `await`, or an async factory export) throws
 * {@link KafkaConfigRequiresAsyncError} — use {@link loadKafkaConfigAsync} or `Kafka.fromConfig()`
 * instead.
 */
export function loadKafkaConfig(path: string, options: LoadKafkaConfigOptions = {}): KafkaFileConfig {
  try {
    return loadConfigFileSync<KafkaFileConfig>(path, { ...options, assertValid: assertValidKafkaFileConfig });
  } catch (error) {
    throw toKafkaConfigError(error, path);
  }
}

/**
 * Async sibling of {@link loadKafkaConfig} — the only path for a config file that uses top-level
 * `await` or exports an async factory. Not a superset of the sync loader: it cannot rescue the
 * constructs `allowTransformFallback` handles, since those rely on `require()` hooks that dynamic
 * `import()` never sees.
 */
export async function loadKafkaConfigAsync(path: string): Promise<KafkaFileConfig> {
  try {
    return await loadConfigFileAsync<KafkaFileConfig>(path, { assertValid: assertValidKafkaFileConfig });
  } catch (error) {
    throw toKafkaConfigError(error, path);
  }
}
