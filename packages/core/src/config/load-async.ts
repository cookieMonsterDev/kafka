import { readFile } from 'node:fs/promises';
import { extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { KafkaConfigError } from './errors';
import { assertResolvedKafkaFileConfig, extractDefaultExport } from './resolve-module';
import type { KafkaFileConfig } from './types';

async function parseJson(path: string): Promise<unknown> {
  const raw = await readFile(path, 'utf8');
  try {
    return JSON.parse(raw);
  } catch (cause) {
    throw new KafkaConfigError('ConfigLoadError', `Failed to parse kafka config file "${path}" as JSON`, {
      path,
      cause,
    });
  }
}

async function importDefaultExport(path: string): Promise<unknown> {
  let moduleExports: unknown;
  try {
    moduleExports = await import(pathToFileURL(path).href);
  } catch (error) {
    throw new KafkaConfigError('ConfigLoadError', `Failed to load kafka config file "${path}"`, {
      path,
      cause: error,
    });
  }

  return extractDefaultExport(moduleExports, path);
}

/**
 * Loads a `kafka.config.*` file asynchronously via dynamic `import()` (`JSON.parse` for `.json`),
 * and awaits a sync or async factory export. Handles every case the sync loader can, including a
 * config that uses top-level `await` — the documented remedy for
 * {@link import('./errors').KafkaConfigRequiresAsyncError}. Shares its default-export extraction
 * and validation with {@link import('./load-sync').loadConfigFileSync} (via `./resolve-module`) so
 * the two paths cannot drift.
 */
export async function loadConfigFileAsync(path: string): Promise<KafkaFileConfig> {
  const resolved = extname(path) === '.json' ? await parseJson(path) : await importDefaultExport(path);
  const value = typeof resolved === 'function' ? await (resolved as () => unknown)() : resolved;

  assertResolvedKafkaFileConfig(value, path);
  return value;
}
