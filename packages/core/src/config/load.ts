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

/**
 * Options for the deprecated synchronous {@link loadKafkaConfig}.
 *
 * @deprecated Use {@link loadKafkaConfigAsync}, which takes no options: it never uses the
 * transform-hook rescue, and a construct that needs it fails with an error naming the fix.
 */
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
 *
 * On Bun, `require()` loads TypeScript natively — including top-level `await` — so no transform
 * fallback is needed and a top-level-`await` config loads here too.
 *
 * @deprecated Use {@link loadKafkaConfigAsync} (or `Kafka.fromConfig()`). The synchronous loader keeps
 * working, but it depends on `require()` hooks and cannot load a config that needs async work.
 */
export function loadKafkaConfig(path: string, options: LoadKafkaConfigOptions = {}): KafkaFileConfig {
  try {
    return loadConfigFileSync<KafkaFileConfig>(path, { ...options, assertValid: assertValidKafkaFileConfig });
  } catch (error) {
    throw toKafkaConfigError(error, path);
  }
}

/**
 * Loads and validates a `kafka.config.*` file via dynamic `import()` — the recommended loader, and
 * the only one for a config file that uses top-level `await` or exports an async factory. It has
 * no transform-hook rescue (those rely on `require()` hooks that `import()` never sees): an
 * extensionless relative import, or `export default` under a CommonJS-resolved file, fails with a
 * {@link KafkaConfigError} naming the fix.
 */
export async function loadKafkaConfigAsync(path: string): Promise<KafkaFileConfig> {
  try {
    return await loadConfigFileAsync<KafkaFileConfig>(path, { assertValid: assertValidKafkaFileConfig });
  } catch (error) {
    throw toKafkaConfigError(error, path);
  }
}
