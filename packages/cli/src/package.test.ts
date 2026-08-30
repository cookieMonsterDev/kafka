import { readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

const packageJsonPath = join(dirname(fileURLToPath(import.meta.url)), '../package.json');
const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf8')) as Record<string, unknown>;

describe('package.json', () => {
  it('is named and typed for the cli package', () => {
    expect(packageJson.name).toBe('@cookiemonsterdev/kafka-cli');
    expect(packageJson.type).toBe('module');
  });

  it('requires Node >=24.0.0', () => {
    expect(packageJson.engines).toEqual({ node: '>=24.0.0' });
  });

  // The very first published version was a deliberately inert name-reservation release (no `bin`,
  // no command) to unblock configuring npm Trusted Publishing ahead of schedule — see
  // packages/cli/CHANGELOG.md. That version stays on disk here until an actual release runs;
  // this test only pins the package staying public, never flipping back to private.
  it('is public', () => {
    expect(packageJson.private).toBeUndefined();
  });

  // Every command reads connection options from `--brokers` only, so the only runtime dependency
  // is the client itself. `@cookiemonsterdev/kafka-config` is added later, once a command reads a
  // config file — adding it here would ship an unused dependency. A stray third-party `pnpm add`
  // should fail this test, not slip through review.
  it('declares exactly one dependency: kafka-core', () => {
    expect(packageJson.dependencies).toEqual({
      '@cookiemonsterdev/kafka-core': '^2.0.0',
    });
  });
});
