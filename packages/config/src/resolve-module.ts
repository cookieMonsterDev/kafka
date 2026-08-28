import { KafkaConfigError } from './errors';

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

function describeType(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  return typeof value;
}

/** Validates a fully-resolved (post-factory) config value, asserting it has shape `T`. */
export type AssertValidFileConfig<T> = (value: unknown) => asserts value is T;

/**
 * Accepts any plain object. The default validator used when a caller injects none — sufficient
 * for the generic `T = Record<string, unknown>` case; a typed consumer (core's `KafkaFileConfig`)
 * injects its own section-aware validator instead.
 */
export function assertPlainObjectFileConfig(value: unknown): asserts value is Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError(`kafka config: expected an object, got ${describeType(value)}`);
  }
}

/**
 * Validates a fully-resolved (post-factory) config value, tagging a failure `ConfigFileInvalid`.
 * `assertValid` is injected rather than imported, so this generic loader has no knowledge of any
 * consumer's specific config shape; it defaults to {@link assertPlainObjectFileConfig}.
 */
export function assertResolvedFileConfig<T>(
  value: unknown,
  path: string,
  assertValid: AssertValidFileConfig<T> = assertPlainObjectFileConfig,
): asserts value is T {
  try {
    assertValid(value);
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
