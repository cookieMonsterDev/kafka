import { assertValidKafkaFileConfig } from './define-config';
import { KafkaConfigError } from './errors';
import type { KafkaFileConfig } from './types';

/**
 * A synchronous `require()` of an ESM `.ts`/`.mts`/`.cts` module (Node's `require(esm)`) returns
 * the module namespace object, distinguishable from a plain CJS `module.exports` value by its
 * `Symbol.toStringTag`.
 */
function isModuleNamespace(value: unknown): value is { default?: unknown } {
  return Object.prototype.toString.call(value) === '[object Module]';
}

/**
 * Extracts a config file's default export from whatever `require()`/`import()` returned: the
 * `default` property of an ESM module namespace, or the value itself for a plain CJS
 * `module.exports`. Shared by the sync and async loaders so they cannot drift.
 */
export function extractDefaultExport(moduleExports: unknown, path: string): unknown {
  if (isModuleNamespace(moduleExports)) {
    if (!('default' in moduleExports) || moduleExports.default === undefined) {
      throw new KafkaConfigError(
        'ConfigFileInvalid',
        `kafka config file "${path}" has no default export. Use "export default defineConfig({...})"`,
        { path },
      );
    }
    return moduleExports.default;
  }

  return moduleExports;
}

/** Validates a fully-resolved (post-factory) config value, tagging a failure `ConfigFileInvalid`. */
export function assertResolvedKafkaFileConfig(value: unknown, path: string): asserts value is KafkaFileConfig {
  try {
    assertValidKafkaFileConfig(value);
  } catch (cause) {
    throw new KafkaConfigError(
      'ConfigFileInvalid',
      `kafka config file "${path}" must export an object (got ${typeof value}). Use "export default defineConfig({...})"`,
      { path, cause },
    );
  }
}

export function isThenable(value: unknown): value is PromiseLike<unknown> {
  return typeof value === 'object' && value !== null && typeof (value as { then?: unknown }).then === 'function';
}
