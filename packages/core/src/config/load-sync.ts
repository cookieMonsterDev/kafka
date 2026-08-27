import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { extname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { defaultOnConfigDiagnostic, type OnConfigDiagnostic } from './diagnostics';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from './errors';
import { assertResolvedKafkaFileConfig, extractDefaultExport, isThenable } from './resolve-module';
import { installConfigTransformHooks } from './transform-hooks';
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

/** Raw `require()`, translating only `ERR_REQUIRE_ASYNC_MODULE`. Every other error is rethrown as-is. */
function requireModuleExportsRaw(path: string): unknown {
  const require = createRequire(pathToFileURL(path));
  try {
    return require(path);
  } catch (error) {
    if (error instanceof Error && (error as NodeJS.ErrnoException).code === 'ERR_REQUIRE_ASYNC_MODULE') {
      throw new KafkaConfigRequiresAsyncError(path, { cause: error });
    }
    throw error;
  }
}

function hasTypeScriptSibling(url: string | undefined): boolean {
  if (url == null) return false;
  try {
    const base = fileURLToPath(url);
    return existsSync(`${base}.ts`) || existsSync(`${base}.mts`);
  } catch {
    return false;
  }
}

interface Rescue {
  detail: string;
  fix: string;
}

const ESM_SYNTAX_UNDER_COMMONJS_PATTERN = /Unexpected token ['"](?:export|import)['"]/;

/** Only the constructs the transform-hook fallback (D8) can actually rescue. */
function describeRescue(error: Error & { code?: string; url?: string }): Rescue | null {
  if (error.code === 'ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX') {
    return {
      detail: 'a TypeScript construct the default strip-only loader cannot handle (e.g. an enum)',
      fix: 'replace the enum with a frozen object or a plain union type, so no transform is required',
    };
  }
  if (error.code === 'ERR_MODULE_NOT_FOUND' && hasTypeScriptSibling(error.url)) {
    return {
      detail: 'a relative import missing its file extension',
      fix: 'add the ".ts" (or ".mts") extension to the import',
    };
  }
  if (error instanceof SyntaxError && ESM_SYNTAX_UNDER_COMMONJS_PATTERN.test(error.message)) {
    return {
      detail: 'ES module syntax (import/export) in a file whose module format resolves to CommonJS',
      fix: 'rename the file to ".mts" so Node always treats it as ESM, or add "type": "module" to the nearest package.json',
    };
  }
  return null;
}

function requireDefaultExport(
  path: string,
  { allowTransformFallback, onDiagnostic }: { allowTransformFallback: boolean; onDiagnostic: OnConfigDiagnostic },
): unknown {
  try {
    return extractDefaultExport(requireModuleExportsRaw(path), path);
  } catch (error) {
    if (error instanceof KafkaConfigRequiresAsyncError || error instanceof KafkaConfigError) {
      throw error;
    }
    if (!(error instanceof Error)) {
      throw error;
    }

    const rescue = describeRescue(error);
    if (rescue == null) {
      throw new KafkaConfigError('ConfigLoadError', `Failed to load kafka config file "${path}"`, {
        path,
        cause: error,
      });
    }

    if (!allowTransformFallback) {
      throw new KafkaConfigError(
        'ConfigLoadError',
        `kafka config file "${path}" needs the TypeScript transform fallback (it uses ${rescue.detail}), which is ` +
          `disabled ("allowTransformFallback: false"). Fix: ${rescue.fix}. Or allow the fallback.`,
        { path, cause: error },
      );
    }

    installConfigTransformHooks();
    onDiagnostic({
      code: 'config.transform-fallback',
      level: 'warn',
      message: `kafka config file "${path}" used the TypeScript transform fallback (it uses ${rescue.detail}). Fix: ${rescue.fix}.`,
      path,
      detail: rescue.detail,
      fix: rescue.fix,
    });

    try {
      return extractDefaultExport(requireModuleExportsRaw(path), path);
    } catch (retryError) {
      throw new KafkaConfigError(
        'ConfigLoadError',
        `Failed to load kafka config file "${path}" even after the transform fallback`,
        { path, cause: retryError },
      );
    }
  }
}

export interface LoadConfigFileSyncOptions {
  /**
   * Rescue a TS `enum` or an extensionless relative import behind a one-time process-wide
   * transform-hook retry (D8). Default `true`. Set `false` for CI: the original error surfaces,
   * with a diagnostic naming the construct and the fix, and hooks are never installed.
   */
  allowTransformFallback?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
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
export function loadConfigFileSync(path: string, options: LoadConfigFileSyncOptions = {}): KafkaFileConfig {
  const cached = cache.get(path);
  if (cached !== undefined) return cached;

  const allowTransformFallback = options.allowTransformFallback ?? true;
  const onDiagnostic = options.onDiagnostic ?? defaultOnConfigDiagnostic;

  const resolved =
    extname(path) === '.json' ? parseJson(path) : requireDefaultExport(path, { allowTransformFallback, onDiagnostic });
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
