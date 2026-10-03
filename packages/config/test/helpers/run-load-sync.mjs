/**
 * Subprocess driver for transform-hooks tests. `module.registerHooks` has no `deregister` on this
 * Node version, so exercising the retry path must happen in a fresh process — never inside a
 * vitest worker, where it would silently change `.ts` resolution for every later import.
 *
 * Plain `.mjs`, not `.ts`: it imports `../../src/*.ts` with an explicit extension, which `node`
 * (native type-stripping) requires for a relative import but `tsc` (`bundler` resolution,
 * extensionless imports) rejects without `allowImportingTsExtensions`.
 *
 * The loader's own extensionless `../../src` imports are made resolvable by
 * `register-src-resolve.mjs`, imported first.
 *
 * Usage: `node run-load-sync.mjs <configPath> [allowTransformFallback=true|false]`. Prints one
 * JSON line to stdout: `{ ok, config | (name, tag, message), diagnostics, hooksInstalled, runtime }`.
 */
import './register-src-resolve.mjs';

const { loadConfigFileSync } = await import('../../src/load-sync.ts');
const { areConfigTransformHooksInstalled } = await import('../../src/transform-hooks.ts');

const [, , configPath, allowTransformFallbackArg] = process.argv;
if (configPath == null) {
  throw new Error('Usage: run-load-sync.mjs <configPath> [allowTransformFallback=true|false]');
}

const allowTransformFallback = allowTransformFallbackArg !== 'false';
const runtime = typeof process.versions.bun === 'string' ? 'bun' : 'node';
const diagnostics = [];

try {
  const config = loadConfigFileSync(configPath, {
    allowTransformFallback,
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
  });
  console.log(
    JSON.stringify({ ok: true, config, diagnostics, hooksInstalled: areConfigTransformHooksInstalled(), runtime }),
  );
} catch (error) {
  console.log(
    JSON.stringify({
      ok: false,
      name: error instanceof Error ? error.constructor.name : typeof error,
      tag: error?.tag,
      message: error instanceof Error ? error.message : String(error),
      diagnostics,
      hooksInstalled: areConfigTransformHooksInstalled(),
      runtime,
    }),
  );
}
