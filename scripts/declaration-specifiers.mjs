// Relative module specifiers in emitted `.d.ts` files: `from '…'` and `import('…')`, including
// `{@link import('…')}` in JSDoc, which TypeScript resolves the same way. Shared so that
// `prune-unreachable-declarations.mjs` follows every specifier `add-declaration-extensions.mjs`
// rewrites. Groups: 1 is the prefix, 2 the quote, 3 the specifier.
export const RELATIVE_SPECIFIER = /(\bfrom\s*|\bimport\s*\(\s*)(['"])(\.\.?(?:\/[^'"\n]*)?)\2/g;
