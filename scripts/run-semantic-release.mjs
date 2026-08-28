#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { RELEASE_PACKAGE_NAMES, resolveReleasePackage } from './resolve-release-package.mjs';

const USAGE = `Usage: node scripts/run-semantic-release.mjs <${[...RELEASE_PACKAGE_NAMES].join('|')}> [--dry-run]`;

// The package name can appear anywhere in argv (pnpm always appends a script's trailing CLI
// args to the end, so "pnpm release:dry-run core" arrives as ["--dry-run", "core"]). Everything
// else is forwarded to semantic-release as-is.
const args = process.argv.slice(2);
const pkgIndex = args.findIndex((arg) => RELEASE_PACKAGE_NAMES.has(arg));

if (pkgIndex === -1) {
  console.error(USAGE);
  process.exit(1);
}

const pkg = resolveReleasePackage(args[pkgIndex]);
const extra = [...args.slice(0, pkgIndex), ...args.slice(pkgIndex + 1)];

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const cwd = path.join(root, 'packages', pkg);
const bin = path.join(root, 'node_modules', 'semantic-release', 'bin', 'semantic-release.js');

const child = spawn(process.execPath, [bin, ...extra], {
  cwd,
  stdio: 'inherit',
  env: process.env,
});

child.on('error', (err) => {
  console.error(`Failed to start semantic-release for "${pkg}" in ${cwd}: ${err.message}`);
  process.exit(1);
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.exit(1);
  }
  process.exit(code ?? 1);
});
