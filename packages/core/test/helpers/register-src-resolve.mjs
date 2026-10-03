/**
 * Bootstrap for subprocess drivers that import `packages/core/src/*.ts` directly.
 *
 * `packages/core/src/**` uses extensionless relative imports (this repo's convention, for
 * bundler/tsc `moduleResolution: "bundler"`), which plain `node` cannot resolve on its own. This
 * resolve hook appends `.ts` for imports made from `packages/core/src/` only, and installs no
 * `load` hook, so the runtime still type-strips the source as-is.
 *
 * Bun resolves extensionless imports natively and has no `registerHooks`, so there it is skipped.
 * Import this module first: static imports evaluate before the importing driver's body runs.
 */
import * as nodeModule from 'node:module';

if (typeof nodeModule.registerHooks === 'function') {
  nodeModule.registerHooks({
    resolve(specifier, context, nextResolve) {
      const fromCoreSource = context.parentURL != null && context.parentURL.includes('/packages/core/src/');
      try {
        return nextResolve(specifier, context);
      } catch (error) {
        if (!fromCoreSource || error?.code !== 'ERR_MODULE_NOT_FOUND') throw error;
        try {
          return nextResolve(`${specifier}.ts`, context);
        } catch {
          throw error;
        }
      }
    },
  });
}
