#!/usr/bin/env node
// Rewrites relative module specifiers in emitted `.d.ts` files to explicit `.js` paths.
//
// Sources use `bundler` resolution with extensionless relative imports, and
// `tsc --emitDeclarationOnly` copies those specifiers verbatim. Every published package is
// `"type": "module"`, so consumers on `moduleResolution: node16`/`nodenext` read the declarations
// as ES modules, which require explicit extensions; every export behind an unresolved specifier
// silently degrades to `any`.
//
// Usage: node scripts/add-declaration-extensions.mjs <dist-dir>
import { readFileSync, readdirSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';

// `from '…'` and `import('…')`, including `{@link import('…')}` in JSDoc, which TypeScript
// resolves the same way.
const RELATIVE_SPECIFIER = /(\bfrom\s*|\bimport\s*\(\s*)(['"])(\.\.?(?:\/[^'"\n]*)?)\2/g;
const EXPLICIT_EXTENSION = /\.(?:js|mjs|cjs|json)$/;

function isFile(filePath) {
  return statSync(filePath, { throwIfNoEntry: false })?.isFile() ?? false;
}

// Mirrors `bundler` resolution, which tries `<specifier>.ts` before `<specifier>/index.ts`;
// `.`, `..`, and a trailing `/` name a directory, so only its index can match.
function resolveSpecifier(declarationFile, specifier) {
  if (EXPLICIT_EXTENSION.test(specifier)) return specifier;

  const target = path.resolve(path.dirname(declarationFile), specifier);
  const isDirectorySpecifier = specifier === '.' || specifier === '..' || specifier.endsWith('/');
  if (!isDirectorySpecifier && isFile(`${target}.d.ts`)) return `${specifier}.js`;
  if (isFile(path.join(target, 'index.d.ts'))) return `${specifier.replace(/\/$/, '')}/index.js`;
  return undefined;
}

// Rewrites the file in place and returns the specifiers it could not resolve.
function rewriteDeclarationFile(declarationFile) {
  const unresolvedSpecifiers = [];
  const source = readFileSync(declarationFile, 'utf8');
  const rewritten = source.replace(RELATIVE_SPECIFIER, (match, prefix, quote, specifier) => {
    const resolved = resolveSpecifier(declarationFile, specifier);
    if (resolved === undefined) {
      unresolvedSpecifiers.push(specifier);
      return match;
    }
    return `${prefix}${quote}${resolved}${quote}`;
  });

  if (rewritten !== source) writeFileSync(declarationFile, rewritten);
  return unresolvedSpecifiers;
}

const distDir = process.argv[2];
if (!distDir) {
  console.error('Usage: node scripts/add-declaration-extensions.mjs <dist-dir>');
  process.exit(1);
}

const problems = [];
for (const entry of readdirSync(distDir, { recursive: true })) {
  if (!entry.endsWith('.d.ts')) continue;

  const declarationFile = path.join(distDir, entry);
  for (const specifier of rewriteDeclarationFile(declarationFile)) {
    problems.push(`${declarationFile}: cannot resolve "${specifier}" to an emitted declaration`);
  }
}

if (problems.length > 0) {
  console.error(problems.join('\n'));
  process.exit(1);
}
