import { existsSync, readFileSync } from 'node:fs';
import { registerHooks, stripTypeScriptTypes as stripTypeScriptTypesStripOnly } from 'node:module';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

interface StripTypeScriptTypesOptions {
  mode?: 'strip' | 'transform';
  sourceUrl?: string;
  sourceMap?: boolean;
}

/**
 * `@types/node` 26.2.0 only declares `mode: 'strip'`, lagging this repo's minimum Node (24.18.1),
 * which supports `mode: 'transform'` at runtime — required to rescue a TS `enum`. Re-typed
 * narrowly here; safe to drop once `@types/node` catches up.
 */
const stripTypeScriptTypes = stripTypeScriptTypesStripOnly as unknown as (
  code: string,
  options?: StripTypeScriptTypesOptions,
) => string;

const TS_URL_PATTERN = /\.[cm]?ts$/;
const RETRY_EXTENSIONS = ['.ts', '.mts'];

let installed = false;

/** Node's own algorithm: `.mts`/`.mjs` are always ESM, `.cts`/`.cjs` always CJS, `.ts`/`.js` follow the nearest `package.json#type` (default CJS). */
function detectModuleFormat(path: string): 'module' | 'commonjs' {
  const ext = extname(path);
  if (ext === '.mts' || ext === '.mjs') return 'module';
  if (ext === '.cts' || ext === '.cjs') return 'commonjs';

  let dir = dirname(path);
  for (;;) {
    const packageJsonPath = join(dir, 'package.json');
    if (existsSync(packageJsonPath)) {
      try {
        const packageJson: unknown = JSON.parse(readFileSync(packageJsonPath, 'utf8'));
        const type =
          typeof packageJson === 'object' && packageJson !== null
            ? (packageJson as { type?: unknown }).type
            : undefined;
        return type === 'module' ? 'module' : 'commonjs';
      } catch {
        return 'commonjs';
      }
    }
    const parent = dirname(dir);
    if (parent === dir) return 'commonjs';
    dir = parent;
  }
}

/**
 * Installs synchronous `require()` hooks (Node's `module.registerHooks`) that rescue two cases the
 * default strip-only TypeScript loader cannot handle: a construct that requires an actual
 * transform (a TS `enum`), and a relative import missing its file extension.
 *
 * Installs **once per process** — `registerHooks` has no `deregister` on this Node version, so
 * this is irreversible for the process's lifetime. Call only from the retry path (see
 * `load-sync.ts`), never eagerly: the happy path must never pay for or trigger this.
 *
 * The `resolve` hook always tries the default resolution first and only appends `.ts`/`.mts` on
 * failure, so successful resolutions for the host application's own modules are byte-identical to
 * having no hooks installed at all.
 */
export function installConfigTransformHooks(): void {
  if (installed) return;
  installed = true;

  registerHooks({
    resolve(specifier, context, nextResolve) {
      try {
        return nextResolve(specifier, context);
      } catch (error) {
        if (!(error instanceof Error) || (error as NodeJS.ErrnoException).code !== 'ERR_MODULE_NOT_FOUND') {
          throw error;
        }

        for (const ext of RETRY_EXTENSIONS) {
          try {
            return nextResolve(`${specifier}${ext}`, context);
          } catch {
            // try the next candidate extension
          }
        }

        throw error;
      }
    },
    load(url, context, nextLoad) {
      if (!TS_URL_PATTERN.test(url)) {
        return nextLoad(url, context);
      }

      const path = fileURLToPath(url);
      const source = readFileSync(path, 'utf8');
      const transformed = stripTypeScriptTypes(source, { mode: 'transform', sourceUrl: url });

      return { format: detectModuleFormat(path), source: transformed, shortCircuit: true };
    },
  });
}

export function areConfigTransformHooksInstalled(): boolean {
  return installed;
}
