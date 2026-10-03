import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { KafkaConfigError } from './errors';

function hasTypeScriptSibling(url: string | undefined): boolean {
  if (url == null) return false;
  try {
    const base = fileURLToPath(url);
    return existsSync(`${base}.ts`) || existsSync(`${base}.mts`);
  } catch {
    return false;
  }
}

/** A load failure the sync loader's transform-hook fallback can rescue, and the fix that avoids needing it. */
export interface Rescue {
  detail: string;
  fix: string;
}

const ESM_SYNTAX_UNDER_COMMONJS_PATTERN = /Unexpected token ['"](?:export|import)['"]/;

/**
 * Non-erasable TypeScript (e.g. an `enum`) needs a real transform, which Node no longer offers
 * (`stripTypeScriptTypes`'s `mode: 'transform'` was removed in Node 26), so it is never rescued —
 * it always surfaces as this error, whatever `allowTransformFallback` says.
 */
export function rethrowIfUnsupportedTypeScriptSyntax(error: unknown, path: string): void {
  if (!(error instanceof Error) || (error as NodeJS.ErrnoException).code !== 'ERR_UNSUPPORTED_TYPESCRIPT_SYNTAX') {
    return;
  }
  throw new KafkaConfigError(
    'ConfigLoadError',
    `kafka config file "${path}" (or a module it imports) uses TypeScript syntax that Node's type stripping ` +
      'cannot run (e.g. an enum). Fix: replace the enum with a frozen object or a plain union type, so no ' +
      'transform is required.',
    { path, cause: error },
  );
}

/**
 * Only the constructs the sync loader's transform-hook fallback (D8) can actually rescue. `require()`
 * and `import()` fail on them with the same error shapes, so the async loader uses this too, to
 * name the fix.
 */
export function describeRescue(error: Error & { code?: string; url?: string }): Rescue | null {
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
