/**
 * Bootstrap shared by the subprocess drivers that import the loader's own `../../src/*.ts` graph.
 *
 * `packages/config/src/**` uses extensionless relative imports (this repo's convention, for
 * bundler/tsc `moduleResolution: "bundler"`), which plain `node` cannot resolve on its own. This
 * throwaway resolve hook — separate from, and unrelated to, `installConfigTransformHooks` under
 * test — appends `.ts` purely so a driver can import that graph. It is scoped to `parentURL`s under
 * `packages/config/src/` so it never touches resolution *inside* a fixture config file (that must
 * be rescued by the loader's own hooks, or not at all — the thing under test) and never installs a
 * `load` hook, so it does not affect how anything is compiled.
 *
 * Bun resolves extensionless imports natively and has no `registerHooks`, so there it is skipped.
 * Import this module first: static imports evaluate before the importing driver's body runs.
 */
import * as nodeModule from 'node:module';

if (typeof nodeModule.registerHooks === 'function') {
  nodeModule.registerHooks({
    resolve(specifier, context, nextResolve) {
      const fromLoaderSource = context.parentURL != null && context.parentURL.includes('/packages/config/src/');
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
}
