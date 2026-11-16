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
import { readFileSync, readdirSync, rmSync, rmdirSync } from 'node:fs';
import path from 'node:path';

import { isFile, RELATIVE_SPECIFIER } from './declaration-files.mjs';

// Returns the reachable declaration files as absolute paths, and the specifiers it could not follow.
function findReachableDeclarations(entryFile) {
  const reachable = new Set([entryFile]);
  const pending = [entryFile];
  const problems = [];

  while (pending.length > 0) {
    const declarationFile = pending.pop();
    for (const match of readFileSync(declarationFile, 'utf8').matchAll(RELATIVE_SPECIFIER)) {
      const { specifier } = match.groups;
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

const directories = [];
for (const entry of readdirSync(distDir, { recursive: true, withFileTypes: true })) {
  const entryPath = path.resolve(entry.parentPath, entry.name);
  if (entry.isDirectory()) directories.push(entryPath);
  else if (entry.name.endsWith('.d.ts') && !reachable.has(entryPath)) rmSync(entryPath);
}

// Deepest first, so a directory whose subdirectories were all emptied is empty by its turn.
directories.sort((left, right) => right.length - left.length);
for (const directory of directories) {
  if (readdirSync(directory).length === 0) rmdirSync(directory);
}
