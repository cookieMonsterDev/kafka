/**
 * Subprocess driver for transform-hooks tests. `module.registerHooks` has no `deregister` on this
 * Node version, so exercising the retry path must happen in a fresh process — never inside a
 * vitest worker, where it would silently change `.ts` resolution for every later import.
 *
 * Plain `.mjs`, not `.ts`: it imports `../../../src/config/*.ts` with an explicit extension, which
 * `node` (native type-stripping) requires for a relative import but `tsc` (`bundler` resolution,
 * extensionless imports) rejects without `allowImportingTsExtensions`.
 *
 * `packages/core/src/**` itself uses extensionless relative imports (this repo's convention, for
 * bundler/tsc `moduleResolution: "bundler"`), which plain `node` cannot resolve on its own. A
 * throwaway bootstrap resolve hook — separate from, and unrelated to, `installConfigTransformHooks`
 * under test — appends `.ts` purely so this harness can import the loader's own module graph. It
 * is scoped to `parentURL`s under `src/config/` so it never touches resolution *inside* a fixture
 * config file (that must be rescued by the loader's own hooks, or not at all — the thing under
 * test) and never installs a `load` hook, so it does not affect how anything is compiled.
 *
 * Usage: `node run-load-sync.mjs <configPath> [allowTransformFallback=true|false]`. Prints one
 * JSON line to stdout: `{ ok, config | (name, tag, message), diagnostics, hooksInstalled }`.
 */
import { registerHooks } from 'node:module';

registerHooks({
  resolve(specifier, context, nextResolve) {
    const fromLoaderSource = context.parentURL != null && context.parentURL.includes('/src/config/');
    try {
      return nextResolve(specifier, context);
    } catch (error) {
      if (!fromLoaderSource || error?.code !== 'ERR_MODULE_NOT_FOUND') throw error;
      for (const ext of ['.ts', '.mts']) {
        try {
          return nextResolve(`${specifier}${ext}`, context);
        } catch {
          // try the next candidate extension
        }
      }
      throw error;
    }
  },
});

const { loadConfigFileSync } = await import('../../../src/config/load-sync.ts');
const { areConfigTransformHooksInstalled } = await import('../../../src/config/transform-hooks.ts');

const [, , configPath, allowTransformFallbackArg] = process.argv;
if (configPath == null) {
  throw new Error('Usage: run-load-sync.mjs <configPath> [allowTransformFallback=true|false]');
}

const allowTransformFallback = allowTransformFallbackArg !== 'false';
const diagnostics = [];

try {
  const config = loadConfigFileSync(configPath, {
    allowTransformFallback,
    onDiagnostic: (diagnostic) => diagnostics.push(diagnostic),
  });
  console.log(JSON.stringify({ ok: true, config, diagnostics, hooksInstalled: areConfigTransformHooksInstalled() }));
} catch (error) {
  console.log(
    JSON.stringify({
      ok: false,
      name: error instanceof Error ? error.constructor.name : typeof error,
      tag: error?.tag,
      message: error instanceof Error ? error.message : String(error),
      diagnostics,
      hooksInstalled: areConfigTransformHooksInstalled(),
    }),
  );
}
