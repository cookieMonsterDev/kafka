import { existsSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { extname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { defaultOnConfigDiagnostic, type OnConfigDiagnostic } from './diagnostics';
import { KafkaConfigError, KafkaConfigRequiresAsyncError } from './errors';
import {
  type AssertValidFileConfig,
  assertResolvedFileConfig,
  extractDefaultExport,
  isThenable,
} from './resolve-module';
import { installConfigTransformHooks } from './transform-hooks';

const cache = new Map<string, unknown>();

/**
 * A rescue is only attempted when `allowTransformFallback` is true, so a lenient load and a
 * strict (`allowTransformFallback: false`) load of the same path can legitimately disagree
 * (one rescues and succeeds, the other throws) — the cache key must include the flag or a
 * strict caller could silently receive an earlier lenient call's rescued result.
 */
function cacheKey(path: string, allowTransformFallback: boolean): string {
  return `${allowTransformFallback}:${path}`;
}

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

export interface LoadConfigFileSyncOptions<T = Record<string, unknown>> {
  /**
   * Rescue a TS `enum`, an extensionless relative import, or `export default` under a
   * CommonJS-resolved `.ts`/`.js` file behind a one-time process-wide transform-hook retry (D8).
   * Default `true`. Set `false` for CI: the original error surfaces, with a diagnostic naming the
   * construct and the fix, and hooks are never installed.
   *
   * **Known limitation:** `registerHooks` has no `deregister` on this Node version, so once any
   * earlier call anywhere in the process installs the fallback hooks (i.e. an earlier call used
   * `allowTransformFallback: true`, the default, against a rescuable file), a *later* call with
   * `allowTransformFallback: false` for the same or a different rescuable file is no longer
   * guaranteed to throw — `require()` itself now silently rescues it. For an airtight CI
   * guarantee, set `allowTransformFallback: false` on every call from process start; don't mix it
   * with a lenient call against a potentially-rescuable file in the same process.
   */
  allowTransformFallback?: boolean;
  onDiagnostic?: OnConfigDiagnostic;
  /**
   * Validates the fully-resolved (post-factory) config value. Defaults to accepting any plain
   * object — see {@link import('./resolve-module').assertPlainObjectFileConfig}. A typed consumer
   * (e.g. core's Kafka-typed facade) injects its own section-aware validator here instead of this
   * generic loader importing one.
   */
  assertValid?: AssertValidFileConfig<T>;
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
export function loadConfigFileSync<T = Record<string, unknown>>(
  path: string,
  options: LoadConfigFileSyncOptions<T> = {},
): T {
  const allowTransformFallback = options.allowTransformFallback ?? true;
  const key = cacheKey(path, allowTransformFallback);
  const cached = cache.get(key);
  if (cached !== undefined) {
    // The cache key doesn't (and can't cheaply) capture `assertValid`'s identity, so a second
    // caller with a different validator must still have it run against the cached value here —
    // otherwise it would silently skip validation on a cache hit.
    assertResolvedFileConfig<T>(cached, path, options.assertValid);
    return cached;
  }

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

  assertResolvedFileConfig<T>(value, path, options.assertValid);
  cache.set(key, value);
  return value;
}
