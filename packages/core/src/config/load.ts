import {
  loadConfigFileAsync,
  loadConfigFileSync,
  type KafkaConfigError as GenericKafkaConfigError,
  type KafkaConfigRequiresAsyncError as GenericKafkaConfigRequiresAsyncError,
  type OnConfigDiagnostic,
} from '@cookiemonsterdev/kafka-config';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from '../errors';
import { assertValidKafkaFileConfig } from './define-config';
import type { KafkaFileConfig } from './types';

/**
 * Matches a foreign error by `.name` only — this file only *type*-imports the generic package's
 * error classes (`import type`, erased at compile time), so there is no runtime coupling to their
 * identity at all, and this stays correct even across two resolved copies of the package.
 */
function hasName<T extends { name: string }>(error: unknown, name: T['name']): error is T {
  return typeof error === 'object' && error !== null && (error as { name?: unknown }).name === name;
}

/**
 * `@cookiemonsterdev/kafka-config`'s own error classes never extend `KafkaError`, so a raw
 * `require()`/`import()` failure — or any other error the generic loader's own catch blocks don't
 * cover, e.g. a filesystem error reading a `.json` config file — would otherwise leak a foreign
 * error type out of `new Kafka()`. Every load path in this file goes through here so callers only
 * ever see this client's own error hierarchy (`isKafkaError`, `.retriable`, ...): matched by
 * `.name`, never `instanceof`, and with a catch-all fallback so nothing untyped escapes either.
 */
function toKafkaConfigError(error: unknown, path: string): unknown {
  if (hasName<GenericKafkaConfigRequiresAsyncError>(error, 'KafkaConfigRequiresAsyncError')) {
    return new KafkaConfigRequiresAsyncError(path, { cause: error });
  }
  if (hasName<GenericKafkaConfigError>(error, 'KafkaConfigError')) {
    return new KafkaConfigError(error.tag, error.message, { path, cause: error });
  }
  const message = error instanceof Error ? error.message : String(error);
  return new KafkaConfigError('ConfigLoadError', `Failed to load kafka config file "${path}": ${message}`, {
    path,
    cause: error,
  });
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
