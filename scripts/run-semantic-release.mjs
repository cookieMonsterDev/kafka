#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RELEASE_PACKAGES, resolveReleasePackage, UnknownReleasePackageError } from './lib/resolve-release-package.mjs';

const USAGE = `Usage: node scripts/run-semantic-release.mjs <${[...RELEASE_PACKAGES].join('|')}> [--dry-run]`;

let pkg;
try {
  pkg = resolveReleasePackage(process.argv[2]);
} catch (err) {
  if (!(err instanceof UnknownReleasePackageError)) throw err;
  console.error(USAGE);
  process.exit(1);
}

const extra = process.argv.slice(3);

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const cwd = path.join(root, 'packages', pkg);
const bin = path.join(root, 'node_modules', 'semantic-release', 'bin', 'semantic-release.js');

const child = spawn(process.execPath, [bin, ...extra], {
  cwd,
  stdio: 'inherit',
  env: process.env,
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.exit(1);
  }
  process.exit(code ?? 1);
});
