import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { describe, expect, it, vi } from 'vitest';
import { findOwnPackageJson, readOwnVersion, runVersionCommand } from './version';

const THIS_DIR = dirname(fileURLToPath(import.meta.url));

describe('findOwnPackageJson / readOwnVersion', () => {
  it('finds package.json for the real cli package from this module', () => {
    const path = findOwnPackageJson(import.meta.url);
    expect(path.endsWith('packages/cli/package.json')).toBe(true);
  });

  it('reads a real, non-empty semver-shaped version', () => {
    const version = readOwnVersion(import.meta.url);
    expect(version).toMatch(/^\d+\.\d+\.\d+/);
  });

  it('walks up several directories to find it, not just one level', () => {
    const nestedUrl = pathToFileURL(join(THIS_DIR, 'nested/deeper/module.ts')).toString();
    const path = findOwnPackageJson(nestedUrl);
    expect(path.endsWith('packages/cli/package.json')).toBe(true);
  });

  it('throws when no matching package.json exists above the module', () => {
    const dir = mkdtempSync(join(tmpdir(), 'kafka-cli-version-test-'));
    try {
      const moduleUrl = pathToFileURL(join(dir, 'module.ts')).toString();
      expect(() => findOwnPackageJson(moduleUrl)).toThrow(/could not locate/);
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
});

describe('runVersionCommand', () => {
  it('writes the version followed by a newline to stdout and returns 0', () => {
    const write = vi.fn((_chunk: string) => true);
    const runtime = { stdout: { write } } as unknown as Parameters<typeof runVersionCommand>[0];

    const code = runVersionCommand(runtime, import.meta.url);

    expect(code).toBe(0);
    expect(write).toHaveBeenCalledTimes(1);
    expect(write.mock.calls[0]?.[0]).toMatch(/^\d+\.\d+\.\d+.*\n$/);
  });
});
