import { execFileSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const PACKAGE_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '../..');
const VITE_CLI = join(dirname(createRequire(import.meta.url).resolve('vite/package.json')), 'bin/vite.js');

/** Vite and its React compiler need production mode without changing the test runner's environment. */
export function buildProduction(
  config: 'vite.config.ts' | 'vite.web.config.ts',
  outDir: string,
  options: { readonly manifest?: boolean } = {},
): void {
  const args = [
    VITE_CLI,
    'build',
    '--config',
    config,
    '--mode',
    'production',
    '--outDir',
    outDir,
    '--emptyOutDir',
    '--logLevel',
    'silent',
  ];
  if (options.manifest === true) args.push('--manifest');
  execFileSync(process.execPath, args, {
    cwd: PACKAGE_ROOT,
    env: { ...process.env, NODE_ENV: 'production' },
  });
}
