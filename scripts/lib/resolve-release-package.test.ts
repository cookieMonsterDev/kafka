import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { RELEASE_PACKAGES, resolveReleasePackage, UnknownReleasePackageError } from './resolve-release-package.mjs';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..', '..');
const runner = path.join(root, 'scripts', 'run-semantic-release.mjs');

describe('resolveReleasePackage', () => {
  it.each([...RELEASE_PACKAGES])('accepts %s', (pkg) => {
    expect(resolveReleasePackage(pkg)).toBe(pkg);
  });

  it('rejects an unknown package name', () => {
    expect(() => resolveReleasePackage('bogus')).toThrow(UnknownReleasePackageError);
  });

  it('rejects undefined', () => {
    expect(() => resolveReleasePackage(undefined)).toThrow(UnknownReleasePackageError);
  });
});

describe('run-semantic-release.mjs usage', () => {
  it('prints usage and exits 1 for an unknown package', () => {
    const result = spawnSync(process.execPath, [runner, 'bogus'], { encoding: 'utf8' });

    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/^Usage: node scripts\/run-semantic-release\.mjs </);
    for (const pkg of RELEASE_PACKAGES) {
      expect(result.stderr).toContain(pkg);
    }
  });

  it('prints usage and exits 1 when no package is given', () => {
    const result = spawnSync(process.execPath, [runner], { encoding: 'utf8' });

    expect(result.status).toBe(1);
    expect(result.stderr).toMatch(/^Usage: node scripts\/run-semantic-release\.mjs </);
  });
});
