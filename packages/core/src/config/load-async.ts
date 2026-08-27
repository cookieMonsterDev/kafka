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
 * and awaits a sync or async factory export. Handles the cases the sync loader structurally
 * cannot — a config that uses top-level `await`, or an async factory export — and is the
 * documented remedy for {@link import('./errors').KafkaConfigRequiresAsyncError}. Shares its
 * default-export extraction and validation with {@link import('./load-sync').loadConfigFileSync}
 * (via `./resolve-module`) so the two paths cannot drift on that shared surface.
 *
 * **Not** a superset of the sync loader, though: the D8 transform-hook rescue (a TS `enum`, an
 * extensionless relative import, or `export default` under a CommonJS-resolved file) is built on
 * `node:module`'s `registerHooks`, which only intercepts CommonJS `require()` — it has no effect
 * on `import()`. A config that needs *both* async loading and one of those rescuable constructs
 * has no working path today; avoid the construct, or restructure the config to not need both at
 * once.
 */
export async function loadConfigFileAsync(path: string): Promise<KafkaFileConfig> {
  const resolved = extname(path) === '.json' ? await parseJson(path) : await importDefaultExport(path);
  const value = typeof resolved === 'function' ? await (resolved as () => unknown)() : resolved;

  assertResolvedKafkaFileConfig(value, path);
  return value;
}
