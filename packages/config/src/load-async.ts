import { readFile } from 'node:fs/promises';
import { extname } from 'node:path';
import { pathToFileURL } from 'node:url';
import { KafkaConfigError } from './errors';
import { describeRescue, rethrowIfUnsupportedTypeScriptSyntax } from './load-errors';
import { type AssertValidFileConfig, assertResolvedFileConfig, extractDefaultExport } from './resolve-module';

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
    rethrowIfUnsupportedTypeScriptSyntax(error, path);
    const rescue = error instanceof Error ? describeRescue(error) : null;
    if (rescue != null) {
      throw new KafkaConfigError(
        'ConfigLoadError',
        `Failed to load kafka config file "${path}": it uses ${rescue.detail}. Fix: ${rescue.fix}.`,
        { path, cause: error },
      );
    }
    throw new KafkaConfigError('ConfigLoadError', `Failed to load kafka config file "${path}"`, {
      path,
      cause: error,
    });
  }

  return extractDefaultExport(moduleExports, path);
}

export interface LoadConfigFileAsyncOptions<T = Record<string, unknown>> {
  /** See {@link import('./load-sync').LoadConfigFileSyncOptions.assertValid}. */
  assertValid?: AssertValidFileConfig<T>;
}

/**
 * Loads a `kafka.config.*` file asynchronously via dynamic `import()` (`JSON.parse` for `.json`),
 * and awaits a sync or async factory export. The recommended loader: it loads every format the
 * deprecated sync loader loads without its rescue, plus a config that uses top-level `await` or
 * exports an async factory, and it never touches process-wide `require()` hooks. Shares its default-export
 * extraction and validation with {@link import('./load-sync').loadConfigFileSync} (via
 * `./resolve-module`) so the two paths cannot drift on that shared surface.
 *
 * It has no transform-hook rescue: a relative import missing its file extension, or `export
 * default` in a `.ts`/`.js` file whose module format resolves to CommonJS, fails with a
 * `KafkaConfigError` tagged `'ConfigLoadError'` whose message names the construct and the fix (the
 * same fix the sync loader's `config.transform-fallback` diagnostic suggests). A non-erasable
 * TypeScript construct such as an `enum` fails the same way on both loaders.
 *
 * On Bun, `import()` loads all of these natively, so none of those failures occur there.
 */
export async function loadConfigFileAsync<T = Record<string, unknown>>(
  path: string,
  options: LoadConfigFileAsyncOptions<T> = {},
): Promise<T> {
  const resolved = extname(path) === '.json' ? await parseJson(path) : await importDefaultExport(path);
  const value = typeof resolved === 'function' ? await (resolved as () => unknown)() : resolved;

  assertResolvedFileConfig<T>(value, path, options.assertValid);
  return value;
}
