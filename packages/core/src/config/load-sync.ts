import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from './errors';
import { assertResolvedKafkaFileConfig, extractDefaultExport, isThenable } from './resolve-module';
import type { KafkaFileConfig } from './types';

const cache = new Map<string, KafkaFileConfig>();

function parseJson(path: string): unknown {
  const raw = readFileSync(path, 'utf8');
  try {
    return JSON.parse(raw);
  } catch (cause) {
    throw new KafkaConfigError('ConfigLoadError', `Failed to parse kafka config file "${path}" as JSON`, {
      path,
      cause,
    });
  }
}

function requireDefaultExport(path: string): unknown {
  const require = createRequire(pathToFileURL(path));

  let moduleExports: unknown;
  try {
    moduleExports = require(path);
  } catch (error) {
    if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ERR_REQUIRE_ASYNC_MODULE') {
      throw new KafkaConfigRequiresAsyncError(path, { cause: error });
    }
    throw new KafkaConfigError('ConfigLoadError', `Failed to load kafka config file "${path}"`, {
      path,
      cause: error,
    });
  }

  return extractDefaultExport(moduleExports, path);
}

/**
 * Loads a `kafka.config.*` file synchronously — `require()` for `.ts`/`.mts`/`.cts`/`.js`/`.mjs`/
 * `.cjs`, `JSON.parse` for `.json` — and resolves a sync factory export. Results are memoised per
 * resolved absolute path (via `pathToFileURL`, never string concatenation, so paths with spaces or
 * `#` resolve correctly), so N clients pay the cost once per process.
 *
 * A config that requires async work — top-level `await`, or an async factory export — fails fast:
 * {@link KafkaConfigRequiresAsyncError} for the former, `KafkaConfigError` tagged
 * `'ConfigFileInvalid'` for the latter. Use `Kafka.fromConfig()`, or the async loader directly, for
 * either case.
 */
export function loadConfigFileSync(path: string): KafkaFileConfig {
  const cached = cache.get(path);
  if (cached !== undefined) return cached;

  const resolved = extname(path) === '.json' ? parseJson(path) : requireDefaultExport(path);
  const value = typeof resolved === 'function' ? (resolved as () => unknown)() : resolved;

  if (isThenable(value)) {
    throw new KafkaConfigError(
      'ConfigFileInvalid',
      `kafka config file "${path}" exports an async factory. Use Kafka.fromConfig() or the async config loader ` +
        'instead of the synchronous path',
      { path },
    );
  }

  assertResolvedKafkaFileConfig(value, path);
  cache.set(path, value);
  return value;
}
