import { existsSync, readFileSync } from 'node:fs';
import * as nodeModule from 'node:module';
import { dirname, extname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const TS_URL_PATTERN = /\.[cm]?ts$/;
const RETRY_EXTENSIONS = ['.ts', '.mts'];
/** Extensions whose module format Node infers from `package.json#type` — never `.mts`/`.mjs`/`.cts`/`.cjs`, which are pinned by extension. */
const AMBIGUOUS_EXTENSIONS = new Set(['.ts', '.js']);
/** Top-level `import`/`export` syntax — the same signal Node's own ambiguous-format auto-detection looks for. */
const LOOKS_LIKE_ESM_SYNTAX = /^\s*(?:export|import)\b/m;

/**
 * Read off the namespace rather than imported by name: Bun's `node:module` has neither export, and
 * a named import of a missing export is a link-time `SyntaxError` that would stop this whole
 * package (and every consumer) from loading at all.
 */
const { registerHooks, stripTypeScriptTypes } = nodeModule as Partial<
  Pick<typeof nodeModule, 'registerHooks' | 'stripTypeScriptTypes'>
>;

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
 * Installs synchronous `require()` hooks (Node's `module.registerHooks`) that rescue cases the
 * default TypeScript loader cannot handle: a relative import missing its file extension, and
 * `export default` in a `.ts`/`.js` file whose module format resolves to CommonJS despite
 * unambiguously ESM content.
 *
 * Sources are only type-stripped, never transformed: Node 26 removed `stripTypeScriptTypes`'s
 * `mode: 'transform'`, so non-erasable syntax (e.g. a TS `enum`) still fails through these hooks.
 *
 * Installs **once per process** — `registerHooks` has no `deregister` on this Node version, so
 * this is irreversible for the process's lifetime. Call only from the retry path (see
 * `load-sync.ts`), never eagerly: the happy path must never pay for or trigger this.
 *
 * The `resolve` hook always tries the default resolution first and only appends `.ts`/`.mts` on
 * failure, so successful resolutions for the host application's own modules are byte-identical to
 * having no hooks installed at all.
 *
 * A safe no-op on a runtime whose `node:module` lacks `registerHooks` or `stripTypeScriptTypes`
 * (Bun): such a runtime loads TypeScript natively, so there is nothing to rescue.
 *
 * @returns Whether the hooks are installed after this call (now, or by an earlier call). `false`
 * means this runtime has no `require()` hooks and nothing was installed.
 * @deprecated The rescue only exists for the deprecated synchronous loader. Load config files with
 * `loadConfigFileAsync` instead; a construct that needs the rescue fails there with an error naming
 * the fix.
 */
export function installConfigTransformHooks(): boolean {
  if (installed) return true;
  if (registerHooks === undefined || stripTypeScriptTypes === undefined) return false;
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
      const transformed = stripTypeScriptTypes(source, { sourceUrl: url });

      const declaredFormat = detectModuleFormat(path);
      // A `.ts`/`.js` file's format normally follows `package.json#type`, same as Node's own
      // resolution. But we only reach this hook after that resolution already failed once, so if
      // the source unambiguously looks like ESM despite a CommonJS-typed package.json, prefer the
      // content over the (evidently wrong-for-this-file) declared type — the same override Node's
      // built-in ambiguous-format auto-detection applies when `type` is left unset entirely.
      const format =
        AMBIGUOUS_EXTENSIONS.has(extname(path)) &&
        declaredFormat === 'commonjs' &&
        LOOKS_LIKE_ESM_SYNTAX.test(transformed)
          ? 'module'
          : declaredFormat;

      return { format, source: transformed, shortCircuit: true };
    },
  });
  return true;
}

/**
 * Whether {@link installConfigTransformHooks} has installed the hooks in this process. Always
 * `false` on a runtime without `require()` hooks (Bun).
 *
 * @deprecated Only meaningful for the deprecated synchronous loader. Use `loadConfigFileAsync`,
 * which never installs hooks.
 */
export function areConfigTransformHooksInstalled(): boolean {
  return installed;
}
