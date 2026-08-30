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

  // Published early, ahead of the walking skeleton, purely to reserve the npm name and unblock
  // configuring npm Trusted Publishing — see packages/cli/CHANGELOG.md. There is still no `bin`
  // and no command; a real user gains nothing from installing this version.
  it('is public, on a pre-1.0 name-reservation version', () => {
    expect(packageJson.private).toBeUndefined();
    expect(packageJson.version).toBe('0.0.1');
  });

  // The scaffold reads no config file (`--brokers` only) and mounts no command yet, so the only
  // runtime dependency it needs is the client itself. `@cookiemonsterdev/kafka-config` is added
  // later, once a command actually reads a config file — adding it here would ship an unused
  // dependency. A stray third-party `pnpm add` should fail this test, not slip through review.
  it('declares exactly one dependency: kafka-core', () => {
    expect(packageJson.dependencies).toEqual({
      '@cookiemonsterdev/kafka-core': '^2.0.0',
    });
  });
});
