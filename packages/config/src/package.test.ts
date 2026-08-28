import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const packageJsonPath = join(dirname(fileURLToPath(import.meta.url)), '../package.json');
const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8')) as Record<string, unknown>;

describe('package.json', () => {
  it('is named and typed for the standalone config-loader package', () => {
    expect(packageJson.name).toBe('@cookiemonsterdev/kafka-config');
    expect(packageJson.type).toBe('module');
  });

  it('requires Node >=24.0.0', () => {
    expect(packageJson.engines).toEqual({ node: '>=24.0.0' });
  });

  // If the loader ever needs a runtime dependency, that's a real design decision, not a drive-by
  // `pnpm add`. `scripts/check-publishable-deps.mjs` checks this repo-wide too; this test pins it
  // in-package so it fails fast during local development.
  it('declares no dependencies, peerDependencies, or optionalDependencies', () => {
    expect(packageJson.dependencies).toBeUndefined();
    expect(packageJson.peerDependencies).toBeUndefined();
    expect(packageJson.optionalDependencies).toBeUndefined();
  });
});
