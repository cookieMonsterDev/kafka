#!/usr/bin/env node
// Deletes emitted `.d.ts` files that no import chain from `<dist-dir>/index.d.ts` reaches.
//
// `tsc --emitDeclarationOnly` writes a declaration for every source module, but a package whose
// `exports` map only exposes `.` can only be typed through `index.d.ts`; the rest is dead weight
// in the tarball. Run it after `add-declaration-extensions.mjs`, which turns every relative
// specifier into an explicit `.js` path. If a reached specifier names no emitted declaration, it
// deletes nothing and exits with an error.
//
// Usage: node scripts/prune-unreachable-declarations.mjs <dist-dir>
import { readFileSync, readdirSync, rmSync, rmdirSync, statSync } from 'node:fs';
import path from 'node:path';

import { RELATIVE_SPECIFIER } from './declaration-specifiers.mjs';

function isFile(filePath) {
  return statSync(filePath, { throwIfNoEntry: false })?.isFile() ?? false;
}

// Returns the reachable declaration files as absolute paths, and the specifiers it could not follow.
function findReachableDeclarations(entryFile) {
  const reachable = new Set([entryFile]);
  const pending = [entryFile];
  const problems = [];

  while (pending.length > 0) {
    const declarationFile = pending.pop();
    for (const [, , , specifier] of readFileSync(declarationFile, 'utf8').matchAll(RELATIVE_SPECIFIER)) {
      const target = path.resolve(path.dirname(declarationFile), specifier.replace(/\.js$/, '.d.ts'));
      if (!target.endsWith('.d.ts') || !isFile(target)) {
        problems.push(`${declarationFile}: "${specifier}" names no emitted declaration`);
        continue;
      }
      if (reachable.has(target)) continue;

      reachable.add(target);
      pending.push(target);
    }
  }

  return { reachable, problems };
}

// Removes directories left empty after pruning, deepest first so a parent sees its children gone.
function removeEmptyDirectories(distDir) {
  const directories = readdirSync(distDir, { recursive: true })
    .map((entry) => path.join(distDir, entry))
    .filter((entry) => statSync(entry).isDirectory())
    .sort((left, right) => right.length - left.length);

  for (const directory of directories) {
    if (readdirSync(directory).length === 0) rmdirSync(directory);
  }
}

const distDir = process.argv[2];
if (!distDir) {
  console.error('Usage: node scripts/prune-unreachable-declarations.mjs <dist-dir>');
  process.exit(1);
}

const entryFile = path.resolve(distDir, 'index.d.ts');
if (!isFile(entryFile)) {
  console.error(`${entryFile}: entry declaration not found`);
  process.exit(1);
}

const { reachable, problems } = findReachableDeclarations(entryFile);
if (problems.length > 0) {
  console.error(problems.join('\n'));
  process.exit(1);
}

for (const entry of readdirSync(distDir, { recursive: true })) {
  if (!entry.endsWith('.d.ts')) continue;

  const declarationFile = path.resolve(distDir, entry);
  if (!reachable.has(declarationFile)) rmSync(declarationFile);
}
removeEmptyDirectories(distDir);
