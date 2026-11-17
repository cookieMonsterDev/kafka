// Helpers shared by the scripts that post-process `tsc --emitDeclarationOnly` output, so
// `prune-unreachable-declarations.mjs` follows every specifier `add-declaration-extensions.mjs`
// rewrites.
import { statSync } from 'node:fs';

// Relative module specifiers in emitted `.d.ts` files: `from '…'` and `import('…')`, including
// `{@link import('…')}` in JSDoc, which TypeScript resolves the same way.
export const RELATIVE_SPECIFIER =
  /(?<prefix>\bfrom\s*|\bimport\s*\(\s*)(?<quote>['"])(?<specifier>\.\.?(?:\/[^'"\n]*)?)\k<quote>/g;

export function isFile(filePath) {
  return statSync(filePath, { throwIfNoEntry: false })?.isFile() ?? false;
}
