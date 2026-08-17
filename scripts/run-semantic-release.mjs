#!/usr/bin/env node
import { spawn } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const pkg = process.argv[2];
const extra = process.argv.slice(3);

if (pkg !== 'core' && pkg !== 'docs') {
  console.error('Usage: node scripts/run-semantic-release.mjs <core|docs> [--dry-run]');
  process.exit(1);
}

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
